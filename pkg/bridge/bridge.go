package bridge

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/VulpineOS/foxbridge/pkg/backend"
	"github.com/VulpineOS/foxbridge/pkg/cdp"
)

const syntheticDefaultBrowserContextID = "vulpine-default-context"

// Bridge translates CDP messages to Juggler protocol calls.
type Bridge struct {
	backend    backend.Backend
	isBiDi     bool // true when using BiDi backend (disables $eval combine pattern)
	sessions   *cdp.SessionManager
	server     *cdp.Server
	autoAttach *autoAttachState
	ownership  *ownershipRegistry
	// ctxMap maps numeric CDP execution context IDs to Juggler execution context ID strings
	ctxMapMu   sync.RWMutex
	ctxMap     map[int]string // cdpContextID → jugglerContextID
	ctxOwners  map[int]string // cdpContextID → owning CDP session
	ctxCounter int            // monotonic counter for execution context IDs
	// loaderMap tracks the last loaderId per CDP session for lifecycle event consistency
	loaderMapMu sync.RWMutex
	loaderMap   map[string]string // cdpSessionID → last loaderId
	// latestCtx tracks the most recent Juggler execution context per session
	latestCtxMu sync.RWMutex
	latestCtx   map[string]string // jugglerSessionID → latest executionContextId
	// isolatedWorlds tracks isolated world names per CDP session for re-emission after navigation
	isolatedWorldsMu sync.RWMutex
	isolatedWorlds   map[string][]isolatedWorldInfo // cdpSessionID → list of isolated worlds
	// nodeObjects maps backendNodeId → objectId for DOM.describeNode/resolveNode round-trips
	nodeObjectsMu sync.RWMutex
	nodeObjects   map[int]string    // backendNodeId → objectId
	nodeOwners    map[int]string    // backendNodeId → owning CDP session
	objectOwners  map[string]string // objectId → owning CDP session
	// lastQuerySelector tracks the last intercepted CSS selector per session
	// so we can combine querySelector + userFn into a single evaluate for $eval
	lastQueryMu    sync.RWMutex
	lastQuery      map[string]string // cdpSessionID → CSS selector
	lastQueryAll   map[string]bool   // cdpSessionID → true if querySelectorAll
	lastQuerySkips map[string]int    // cdpSessionID → remaining calls to skip before user fn
	// lastDialogID tracks the last dialog ID per CDP session for handleDialog
	lastDialogMu sync.Mutex
	lastDialog   map[string]string // cdpSessionID → dialogId
	// pdfStreams stores PDF data for IO.read streaming
	pdfStreamsMu sync.Mutex
	pdfStreams   map[string]string // streamHandle → base64 data
	pdfOwners    map[string]string // streamHandle → owning CDP session
	// pendingContextClear tracks sessions that had executionContextsCleared.
	// The next executionContextCreated should trigger isolated world re-emission.
	pendingContextClearMu sync.Mutex
	pendingContextClear   map[string]bool // cdpSessionID → true
	// fetchPatterns retains the CDP-side Fetch.enable filters. Juggler only
	// supports enabling interception for an entire browser context, so the
	// bridge must continue requests that do not match these patterns itself.
	fetchPatternsMu sync.RWMutex
	fetchPatterns   map[string][]fetchRequestPattern // cdpSessionID → patterns
	// deterministicScript is injected into page sessions when deterministic mode is enabled.
	deterministicMu      sync.RWMutex
	deterministicScript  string
	deterministicApplied map[string]bool // cdpSessionID → script installed
}

func (b *Bridge) cdpBrowserContextID(id string) string {
	if id != "" {
		return id
	}
	return syntheticDefaultBrowserContextID
}

func (b *Bridge) isSyntheticDefaultBrowserContextID(id string) bool {
	return id == "" || id == syntheticDefaultBrowserContextID
}

func (b *Bridge) setJugglerBrowserContext(params map[string]interface{}, id string) {
	if b.isSyntheticDefaultBrowserContextID(id) {
		return
	}
	params["browserContextId"] = id
}

// New creates a new Bridge. Set isBiDi to true when using the BiDi backend.
func New(b backend.Backend, sessions *cdp.SessionManager, server *cdp.Server, isBiDi ...bool) *Bridge {
	bidi := len(isBiDi) > 0 && isBiDi[0]
	bridge := &Bridge{
		backend:              b,
		isBiDi:               bidi,
		sessions:             sessions,
		server:               server,
		autoAttach:           newAutoAttachState(),
		ctxMap:               make(map[int]string),
		ctxOwners:            make(map[int]string),
		ctxCounter:           100,
		loaderMap:            make(map[string]string),
		latestCtx:            make(map[string]string),
		isolatedWorlds:       make(map[string][]isolatedWorldInfo),
		nodeObjects:          make(map[int]string),
		nodeOwners:           make(map[int]string),
		objectOwners:         make(map[string]string),
		lastQuery:            make(map[string]string),
		lastQueryAll:         make(map[string]bool),
		lastQuerySkips:       make(map[string]int),
		lastDialog:           make(map[string]string),
		pdfStreams:           make(map[string]string),
		pdfOwners:            make(map[string]string),
		pendingContextClear:  make(map[string]bool),
		fetchPatterns:        make(map[string][]fetchRequestPattern),
		deterministicApplied: make(map[string]bool),
		ownership:            newOwnershipRegistry(),
	}
	if server != nil {
		server.SetConnectionCloseHandler(bridge.ConnectionClosed)
	}
	return bridge
}

// HandleMessage dispatches an incoming CDP message to the appropriate domain handler.
func (b *Bridge) HandleMessage(conn *cdp.Connection, msg *cdp.Message) {
	method := msg.Method
	if conn != nil {
		b.ownership.state(conn)
	}
	if err := b.authorize(conn, msg); err != nil {
		b.sendResponse(conn, msg, nil, err)
		return
	}

	var result json.RawMessage
	var cdpErr *cdp.Error

	switch {
	case strings.HasPrefix(method, "Target."):
		result, cdpErr = b.handleTarget(conn, msg)
	case strings.HasPrefix(method, "Page."):
		result, cdpErr = b.handlePage(conn, msg)
	case strings.HasPrefix(method, "Runtime."):
		result, cdpErr = b.handleRuntime(conn, msg)
	case strings.HasPrefix(method, "Input."):
		result, cdpErr = b.handleInput(conn, msg)
	case strings.HasPrefix(method, "Network."):
		result, cdpErr = b.handleNetwork(conn, msg)
	case strings.HasPrefix(method, "Emulation."):
		result, cdpErr = b.handleEmulation(conn, msg)
	case strings.HasPrefix(method, "DOM."):
		result, cdpErr = b.handleDOM(conn, msg)
	case strings.HasPrefix(method, "Accessibility."):
		result, cdpErr = b.handleAccessibility(conn, msg)
	case strings.HasPrefix(method, "Console."):
		result, cdpErr = b.handleConsole(conn, msg)
	case strings.HasPrefix(method, "Fetch."):
		result, cdpErr = b.handleFetch(conn, msg)
	case strings.HasPrefix(method, "Performance."):
		result, cdpErr = b.handlePerformance(conn, msg)
	case strings.HasPrefix(method, "IO."):
		result, cdpErr = b.handleIO(conn, msg)
	case strings.HasPrefix(method, "CSS."):
		result, cdpErr = b.handleCSS(conn, msg)
	case strings.HasPrefix(method, "DOMStorage."):
		result, cdpErr = b.handleDOMStorage(conn, msg)
	default:
		result, cdpErr = b.handleStub(conn, msg)
	}

	b.sendResponse(conn, msg, result, cdpErr)
}

func (b *Bridge) sendResponse(conn *cdp.Connection, msg *cdp.Message, result json.RawMessage, cdpErr *cdp.Error) {
	if conn == nil {
		return
	}
	resp := &cdp.Message{ID: msg.ID, SessionID: msg.SessionID}
	if cdpErr != nil {
		resp.Error = cdpErr
	} else {
		if result == nil {
			result = json.RawMessage(`{}`)
		}
		resp.Result = result
	}
	if err := conn.Send(resp); err != nil {
		log.Printf("failed to send CDP response for %s: %v", msg.Method, err)
	}
}

func targetScopedMethod(method string) bool {
	for _, prefix := range []string{"Page.", "Runtime.", "Input.", "Network.", "Emulation.", "DOM.", "Accessibility.", "Console.", "Fetch.", "Performance.", "IO.", "CSS.", "DOMStorage."} {
		if strings.HasPrefix(method, prefix) {
			return true
		}
	}
	return false
}

func (b *Bridge) authorize(conn *cdp.Connection, msg *cdp.Message) *cdp.Error {
	if conn == nil {
		return nil
	}
	if msg.SessionID != "" && !b.ownership.sessionOwned(conn, msg.SessionID) {
		return &cdp.Error{Code: -32000, Message: "session not found"}
	}
	if msg.SessionID != "" && targetScopedMethod(msg.Method) && b.ownership.isBrowserSession(msg.SessionID) {
		return &cdp.Error{Code: -32000, Message: "page session required"}
	}
	if msg.SessionID == "" && targetScopedMethod(msg.Method) {
		return &cdp.Error{Code: -32000, Message: "target session required"}
	}
	return nil
}

// ConnectionClosed releases all pages owned by a disconnected CDP client.
func (b *Bridge) ConnectionClosed(conn *cdp.Connection) {
	records := b.ownership.closeConnection(conn)
	for _, record := range records {
		b.closeRecord(record, true)
	}
	b.disableFetchIfUnused()
}

func (b *Bridge) clearConnectionState(records []*targetRecord) {
	for _, record := range records {
		b.loaderMapMu.Lock()
		delete(b.loaderMap, record.pageSessionID)
		b.loaderMapMu.Unlock()

		b.isolatedWorldsMu.Lock()
		delete(b.isolatedWorlds, record.pageSessionID)
		b.isolatedWorldsMu.Unlock()

		b.lastQueryMu.Lock()
		delete(b.lastQuery, record.pageSessionID)
		delete(b.lastQueryAll, record.pageSessionID)
		delete(b.lastQuerySkips, record.pageSessionID)
		b.lastQueryMu.Unlock()

		b.lastDialogMu.Lock()
		delete(b.lastDialog, record.pageSessionID)
		b.lastDialogMu.Unlock()

		b.fetchPatternsMu.Lock()
		delete(b.fetchPatterns, record.pageSessionID)
		b.fetchPatternsMu.Unlock()

		b.pendingContextClearMu.Lock()
		delete(b.pendingContextClear, record.pageSessionID)
		b.pendingContextClearMu.Unlock()

		b.deterministicMu.Lock()
		delete(b.deterministicApplied, record.pageSessionID)
		b.deterministicMu.Unlock()

		b.latestCtxMu.Lock()
		delete(b.latestCtx, record.jugglerSessionID)
		b.latestCtxMu.Unlock()

		b.ctxMapMu.Lock()
		for id, jugglerID := range b.ctxMap {
			if jugglerID == record.jugglerSessionID || b.ctxOwners[id] == record.pageSessionID {
				delete(b.ctxMap, id)
				delete(b.ctxOwners, id)
			}
		}
		b.ctxMapMu.Unlock()

		b.nodeObjectsMu.Lock()
		for id, owner := range b.nodeOwners {
			if owner == record.pageSessionID {
				delete(b.nodeOwners, id)
				delete(b.nodeObjects, id)
			}
		}
		for objectID, owner := range b.objectOwners {
			if owner == record.pageSessionID {
				delete(b.objectOwners, objectID)
			}
		}
		b.nodeObjectsMu.Unlock()

		b.pdfStreamsMu.Lock()
		for handle, owner := range b.pdfOwners {
			if owner == record.pageSessionID {
				delete(b.pdfOwners, handle)
				delete(b.pdfStreams, handle)
			}
		}
		b.pdfStreamsMu.Unlock()

		b.autoAttach.mu.Lock()
		delete(b.autoAttach.pendingFrameIDs, record.jugglerSessionID)
		delete(b.autoAttach.pairs, record.jugglerSessionID)
		b.autoAttach.mu.Unlock()
	}
}

func (b *Bridge) ownedTarget(conn *cdp.Connection, targetID string) bool {
	if conn == nil {
		return true
	}
	return b.ownership.ownerForTarget(targetID) == conn
}

func (b *Bridge) closeRecord(record *targetRecord, closeBackend bool) {
	owner, _, _ := b.ownership.recordDetails(record)
	if !b.ownership.beginCleanup(record) {
		return
	}
	if closeBackend && record.pageSessionID != "" {
		if _, err := b.callJuggler(record.pageSessionID, "Page.close", nil); err != nil {
			log.Printf("[ownership] close target %s: %v", record.pageTargetID, err)
		}
	}
	b.clearConnectionState([]*targetRecord{record})
	b.disableFetchIfUnused()
	b.sendOwnedEvent(owner, "Target.detachedFromTarget", map[string]interface{}{
		"sessionId": record.pageSessionID,
		"targetId":  record.pageTargetID,
	}, "")
	b.sendOwnedEvent(owner, "Target.targetDestroyed", map[string]interface{}{
		"targetId": record.pageTargetID,
	}, "")
	b.sessions.Remove(record.pageSessionID)
	b.sessions.Remove(record.tabSessionID)
	b.autoAttach.mu.Lock()
	if record.jugglerSessionID != "" {
		delete(b.autoAttach.pairs, record.jugglerSessionID)
	}
	pending := b.autoAttach.pending[:0]
	for _, candidate := range b.autoAttach.pending {
		if candidate != record.pair {
			pending = append(pending, candidate)
		}
	}
	b.autoAttach.pending = pending
	b.autoAttach.mu.Unlock()
	b.ownership.remove(record)
}

func (b *Bridge) disableFetchIfUnused() {
	if b.fetchInterceptionEnabled() {
		return
	}
	if _, err := b.callJuggler("", "Browser.setRequestInterception", map[string]interface{}{"enabled": false}); err != nil {
		log.Printf("[ownership] disable request interception: %v", err)
	}
}

func (b *Bridge) sendOwnedEvent(owner *cdp.Connection, method string, params interface{}, sessionID string) {
	if owner == nil {
		return
	}
	raw, _ := json.Marshal(params)
	if err := b.server.Send(owner, &cdp.Message{Method: method, Params: raw, SessionID: sessionID}); err != nil {
		log.Printf("[event] send %s: %v", method, err)
	}
}

func (b *Bridge) publishOwnedPair(pair *targetPair) {
	record := b.ownership.recordForTarget(pair.pageTargetID)
	if record == nil {
		return
	}
	owner, _, cancelled := b.ownership.recordDetails(record)
	if owner == nil || cancelled {
		return
	}
	b.autoAttach.mu.Lock()
	pending := b.autoAttach.pending[:0]
	for _, candidate := range b.autoAttach.pending {
		if candidate != pair {
			pending = append(pending, candidate)
		}
	}
	b.autoAttach.pending = pending
	b.autoAttach.mu.Unlock()
	if b.ownership.discoverEnabled(owner) {
		b.autoAttach.mu.Lock()
		url := pair.url
		b.autoAttach.mu.Unlock()
		if url == "" {
			url = "about:blank"
		}
		_ = b.server.Send(owner, &cdp.Message{
			Method: "Target.targetCreated",
			Params: mustJSON(map[string]interface{}{"targetInfo": map[string]interface{}{
				"targetId": pair.pageTargetID, "type": "page", "title": "", "url": url,
				"attached": true, "canAccessOpener": false, "browserContextId": pair.browserCtxID,
			}}),
		})
	}
	if b.ownership.autoAttachEnabled(owner) {
		b.emitAutoAttachPair(pair)
	}
}

func mustJSON(value interface{}) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

// resolveSession maps a CDP sessionID to a Juggler sessionID.
func (b *Bridge) resolveSession(cdpSessionID string) string {
	if cdpSessionID == "" {
		return ""
	}
	if info, ok := b.sessions.Get(cdpSessionID); ok {
		return info.JugglerSessionID
	}
	return cdpSessionID
}

// callJuggler is a convenience wrapper for backend.Call with session resolution.
func (b *Bridge) callJuggler(cdpSessionID, method string, params interface{}) (json.RawMessage, error) {
	sessionID := b.resolveSession(cdpSessionID)
	var raw json.RawMessage
	if params != nil {
		var err error
		raw, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	}
	return b.backend.Call(sessionID, method, raw)
}

// nextCtxID allocates a new unique execution context ID.
func (b *Bridge) nextCtxID() int {
	b.ctxMapMu.Lock()
	b.ctxCounter++
	id := b.ctxCounter
	b.ctxMapMu.Unlock()
	return id
}

func (b *Bridge) contextOwned(sessionID string, contextID int) bool {
	b.ctxMapMu.RLock()
	owner, known := b.ctxOwners[contextID]
	b.ctxMapMu.RUnlock()
	return !known || owner == sessionID
}

func (b *Bridge) clearContextsForSession(sessionID string) {
	b.ctxMapMu.Lock()
	for id, owner := range b.ctxOwners {
		if owner == sessionID {
			delete(b.ctxMap, id)
			delete(b.ctxOwners, id)
		}
	}
	b.ctxMapMu.Unlock()
}

// isolatedWorldInfo tracks an isolated world for re-emission after navigation.
type isolatedWorldInfo struct {
	WorldName string
	FrameID   string
}

// latestContextForSession returns the most recent Juggler execution context for a CDP session.
func (b *Bridge) latestContextForSession(cdpSessionID string) string {
	jugglerSessionID := b.resolveSession(cdpSessionID)
	b.latestCtxMu.RLock()
	defer b.latestCtxMu.RUnlock()
	return b.latestCtx[jugglerSessionID]
}

func (b *Bridge) cdpFrameIDForSession(cdpSessionID, frameID string) string {
	if frameID == "" {
		return frameID
	}
	info, ok := b.sessions.Get(cdpSessionID)
	if !ok {
		return frameID
	}
	return b.cdpFrameIDForInfo(info, frameID)
}

func (b *Bridge) cdpFrameIDForJugglerSession(jugglerSessionID, frameID string) string {
	if frameID == "" {
		return frameID
	}
	if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok {
		return b.cdpFrameIDForInfo(info, frameID)
	}
	b.autoAttach.mu.Lock()
	pair, ok := b.autoAttach.pairs[jugglerSessionID]
	b.autoAttach.mu.Unlock()
	if ok {
		if info, ok := b.sessions.Get(pair.pageSessionID); ok {
			return b.cdpFrameIDForInfo(info, frameID)
		}
	}
	return frameID
}

func (b *Bridge) cdpFrameIDForInfo(info *cdp.SessionInfo, frameID string) string {
	if info == nil || frameID == "" {
		return frameID
	}
	if info.Type == "page" && info.FrameID != "" && info.TargetID != "" && frameID == info.FrameID {
		return info.TargetID
	}
	return frameID
}

func (b *Bridge) jugglerFrameIDForSession(cdpSessionID, frameID string) string {
	if frameID == "" {
		return frameID
	}
	info, ok := b.sessions.Get(cdpSessionID)
	if !ok {
		return frameID
	}
	if info.Type == "page" && info.FrameID != "" && info.TargetID != "" && frameID == info.TargetID {
		return info.FrameID
	}
	return frameID
}

// emitEvent sends a CDP event only to the owner of its target/session.
func (b *Bridge) emitEvent(method string, params interface{}, sessionID string) {
	var raw json.RawMessage
	if params != nil {
		raw, _ = json.Marshal(params)
	}
	owner := b.ownership.sessionOwner(sessionID)
	if owner == nil {
		owner = b.ownerForEvent(raw, sessionID)
	}
	if owner == nil {
		log.Printf("[event] dropping unowned %s (session=%s)", method, sessionID)
		return
	}
	if err := b.server.Send(owner, &cdp.Message{Method: method, Params: raw, SessionID: sessionID}); err != nil {
		log.Printf("[event] send %s: %v", method, err)
	}
}

func (b *Bridge) sessionForFrame(frameID string) (string, bool) {
	if frameID == "" {
		return "", false
	}
	found := ""
	for _, info := range b.sessions.All() {
		if info.Type != "page" || info.FrameID != frameID || b.ownership.ownerForTarget(info.TargetID) == nil {
			continue
		}
		if found != "" && found != info.SessionID {
			return "", false
		}
		found = info.SessionID
	}
	return found, found != ""
}

func (b *Bridge) ownerForEvent(raw json.RawMessage, sessionID string) *cdp.Connection {
	if sessionID != "" {
		return nil
	}
	var payload struct {
		TargetID   string `json:"targetId"`
		TargetInfo struct {
			TargetID string `json:"targetId"`
		} `json:"targetInfo"`
		FrameID string `json:"frameId"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil
	}
	if payload.TargetID != "" {
		return b.ownership.ownerForTarget(payload.TargetID)
	}
	if payload.TargetInfo.TargetID != "" {
		return b.ownership.ownerForTarget(payload.TargetInfo.TargetID)
	}
	if cdpSessionID, ok := b.sessionForFrame(payload.FrameID); ok {
		return b.ownership.sessionOwner(cdpSessionID)
	}
	return nil
}
