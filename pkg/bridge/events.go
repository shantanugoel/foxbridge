package bridge

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
	"github.com/google/uuid"
)

// targetPair holds the tab+page CDP session IDs for a Juggler target.
type targetPair struct {
	tabSessionID     string
	tabTargetID      string
	pageSessionID    string
	pageTargetID     string
	jugglerSessionID string
	browserCtxID     string
	url              string
	pageAttachedRoot bool
	pageAttachedTab  bool
}

// autoAttachState tracks auto-attach configuration and pending targets.
type autoAttachState struct {
	mu      sync.Mutex
	enabled bool
	// targets that arrived before setAutoAttach — need retroactive emission
	pending []*targetPair
	// all known pairs for lookup
	pairs map[string]*targetPair // keyed by juggler session ID
	// pendingFrameIDs stores frameIDs from executionContextCreated events that
	// fired before the CDP session was registered (keyed by Juggler session ID)
	pendingFrameIDs map[string]string
}

func newAutoAttachState() *autoAttachState {
	return &autoAttachState{
		pairs:           make(map[string]*targetPair),
		pendingFrameIDs: make(map[string]string),
	}
}

// SetupEventSubscriptions subscribes to Juggler events and translates them to CDP events.
func (b *Bridge) SetupEventSubscriptions() {
	// Browser.attachedToTarget — new page created, register session and emit CDP events.
	b.backend.Subscribe("Browser.attachedToTarget", func(sessionID string, params json.RawMessage) {
		log.Printf("[event] Browser.attachedToTarget received: %s", string(params))
		var ev struct {
			SessionID  string `json:"sessionId"`
			TargetInfo struct {
				TargetID         string `json:"targetId"`
				BrowserContextID string `json:"browserContextId"`
				Type             string `json:"type"`
				URL              string `json:"url"`
				OpenerID         string `json:"openerId"`
			} `json:"targetInfo"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Browser.attachedToTarget: %v", err)
			return
		}

		targetID := ev.TargetInfo.TargetID
		jugglerSessionID := ev.SessionID
		targetType := ev.TargetInfo.Type
		browserContextID := b.cdpBrowserContextID(ev.TargetInfo.BrowserContextID)

		// Workers (worker/service_worker) use a single CDP session — no tab/page dual model.
		if targetType == "worker" || targetType == "service_worker" {
			workerSessionID := uuid.New().String()
			b.sessions.Add(&cdp.SessionInfo{
				SessionID:        workerSessionID,
				JugglerSessionID: jugglerSessionID,
				TargetID:         targetID,
				BrowserContextID: browserContextID,
				URL:              ev.TargetInfo.URL,
				Type:             targetType,
			})

			log.Printf("[event] registered %s target=%s session=%s", targetType, targetID, workerSessionID)

			owner := b.ownership.ownerForTarget(ev.TargetInfo.OpenerID)
			record := b.ownership.registerWorker(owner, targetID, workerSessionID, jugglerSessionID)
			recordOwner, _, _ := b.ownership.recordDetails(record)
			b.autoAttach.mu.Lock()
			autoEnabled := b.autoAttach.enabled
			b.autoAttach.mu.Unlock()
			if recordOwner != nil {
				autoEnabled = b.ownership.autoAttachEnabled(recordOwner)
			}

			if _, _, cancelled := b.ownership.recordDetails(record); cancelled {
				b.closeRecord(record, true)
				return
			}
			if autoEnabled {
				b.emitEvent("Target.attachedToTarget", map[string]interface{}{
					"sessionId": workerSessionID,
					"targetInfo": map[string]interface{}{
						"targetId":         targetID,
						"type":             targetType,
						"title":            "",
						"url":              ev.TargetInfo.URL,
						"attached":         true,
						"canAccessOpener":  false,
						"browserContextId": browserContextID,
					},
					"waitingForDebugger": false,
				}, "")
			}
			return
		}

		tabSessionID := uuid.New().String()
		pageSessionID := uuid.New().String()
		tabTargetID := uuid.New().String()

		pair := &targetPair{
			tabSessionID:     tabSessionID,
			tabTargetID:      tabTargetID,
			pageSessionID:    pageSessionID,
			pageTargetID:     targetID,
			jugglerSessionID: jugglerSessionID,
			browserCtxID:     browserContextID,
			url:              ev.TargetInfo.URL,
		}

		// Register the PAGE session (what actually talks to Juggler)
		pageInfo := &cdp.SessionInfo{
			SessionID:        pageSessionID,
			JugglerSessionID: jugglerSessionID,
			TargetID:         targetID,
			BrowserContextID: browserContextID,
			URL:              ev.TargetInfo.URL,
			Type:             "page",
		}
		// Apply any pending frameID that was buffered before session registration
		b.autoAttach.mu.Lock()
		if pendingFrameID, ok := b.autoAttach.pendingFrameIDs[jugglerSessionID]; ok {
			pageInfo.FrameID = pendingFrameID
			delete(b.autoAttach.pendingFrameIDs, jugglerSessionID)
			log.Printf("[event] applied buffered frameID=%s to new session %s", pendingFrameID, pageSessionID)
		}
		b.autoAttach.mu.Unlock()
		b.sessions.Add(pageInfo)
		b.applyDeterministicPrelude(pageSessionID)
		// Register the TAB session (stub — doesn't map to Juggler session to avoid
		// overwriting the PAGE session in jugglerSessions lookup)
		b.sessions.Add(&cdp.SessionInfo{
			SessionID:        tabSessionID,
			JugglerSessionID: "tab:" + jugglerSessionID,
			TargetID:         tabTargetID,
			BrowserContextID: browserContextID,
			URL:              ev.TargetInfo.URL,
			Type:             "tab",
		})

		record := b.ownership.registerPair(b.ownership.ownerForTarget(ev.TargetInfo.OpenerID), pair)
		b.autoAttach.mu.Lock()
		b.autoAttach.pairs[jugglerSessionID] = pair
		b.autoAttach.mu.Unlock()
		recordOwner, _, cancelled := b.ownership.recordDetails(record)
		if cancelled {
			b.closeRecord(record, true)
			return
		}
		if recordOwner != nil {
			b.publishOwnedPair(pair)
		} else {
			b.autoAttach.mu.Lock()
			if b.autoAttach.enabled {
				b.autoAttach.mu.Unlock()
				b.emitAutoAttachPair(pair)
			} else {
				b.autoAttach.pending = append(b.autoAttach.pending, pair)
				b.autoAttach.mu.Unlock()
			}
		}
	})

	// Browser.detachedFromTarget — page destroyed.
	b.backend.Subscribe("Browser.detachedFromTarget", func(sessionID string, params json.RawMessage) {
		var ev struct {
			SessionID string `json:"sessionId"`
			TargetID  string `json:"targetId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Browser.detachedFromTarget: %v", err)
			return
		}

		targetID := ev.TargetID
		if targetID == "" {
			if info, ok := b.sessions.GetByJugglerSession(ev.SessionID); ok {
				targetID = info.TargetID
			}
		}
		if record := b.ownership.recordForTarget(targetID); record != nil {
			b.closeRecord(record, false)
			return
		}
		if info, ok := b.sessions.GetByTarget(targetID); ok {
			b.sessions.Remove(info.SessionID)
		}
	})

	// Page.navigationStarted carries the frame a navigation is actually running
	// in, on every navigation. Foxbridge does not surface it to CDP clients —
	// Chrome has no equivalent — but it is the freshest evidence of which frame
	// is live, so use it to keep the cached main frame from going stale.
	b.backend.Subscribe("Page.navigationStarted", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			FrameID string `json:"frameId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			return
		}
		b.refreshMainFrame(jugglerSessionID, ev.FrameID)
	})

	// Page.navigationCommitted → Page.frameNavigated (session-scoped)
	b.backend.Subscribe("Page.navigationCommitted", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			FrameID      string `json:"frameId"`
			URL          string `json:"url"`
			Name         string `json:"name"`
			NavigationID string `json:"navigationId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Page.navigationCommitted: %v", err)
			return
		}

		b.refreshMainFrame(jugglerSessionID, ev.FrameID)

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.FrameID)

		// Skip intermediate about:blank navigations during reload/redirect.
		// Juggler emits navigation to about:blank before navigating to the real URL.
		// Chrome doesn't do this, and the extra loaderId confuses Puppeteer.
		if ev.URL == "about:blank" {
			if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok && info.URL != "" && info.URL != "about:blank" {
				return // skip — this is an intermediate about:blank during reload
			}
		}

		// Emit executionContextsCleared before the new navigation.
		// BiDi doesn't have an explicit "contexts cleared" event — we emit it on navigation.
		// Juggler emits its own Runtime.executionContextsCleared, so only do this for BiDi.
		if b.isBiDi && cdpSessionID != "" {
			b.clearContextsForSession(cdpSessionID)
			b.emitEvent("Runtime.executionContextsCleared", map[string]interface{}{}, cdpSessionID)
		}

		// Update session URL
		if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok {
			b.sessions.UpdateURL(info.SessionID, ev.URL)
		}
		b.autoAttach.mu.Lock()
		if pair, ok := b.autoAttach.pairs[jugglerSessionID]; ok {
			pair.url = ev.URL
		}
		b.autoAttach.mu.Unlock()

		// Use the Juggler navigationId as loaderId for consistency
		loaderId := ev.NavigationID
		if loaderId == "" {
			loaderId = fmt.Sprintf("loader-%s", jugglerSessionID[:8])
		}

		// Store loaderId so Page.eventFired can use the same one
		b.loaderMapMu.Lock()
		b.loaderMap[cdpSessionID] = loaderId
		b.loaderMapMu.Unlock()

		// Emit lifecycle events in Chrome's order
		b.emitEvent("Page.lifecycleEvent", map[string]interface{}{
			"frameId":   cdpFrameID,
			"loaderId":  loaderId,
			"name":      "init",
			"timestamp": 0,
		}, cdpSessionID)
		b.emitEvent("Page.lifecycleEvent", map[string]interface{}{
			"frameId":   cdpFrameID,
			"loaderId":  loaderId,
			"name":      "commit",
			"timestamp": 0,
		}, cdpSessionID)

		b.emitEvent("Page.frameNavigated", map[string]interface{}{
			"frame": map[string]interface{}{
				"id":                cdpFrameID,
				"url":               ev.URL,
				"loaderId":          loaderId,
				"securityOrigin":    "",
				"mimeType":          "text/html",
				"domainAndRegistry": "",
			},
			"type": "Navigation",
		}, cdpSessionID)

		// NOTE: Isolated world re-emission moved to Page.eventFired(load)
	})

	// Page.eventFired — maps to Page.loadEventFired or Page.domContentEventFired
	b.backend.Subscribe("Page.eventFired", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			Name    string `json:"name"`
			FrameID string `json:"frameId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Page.eventFired: %v", err)
			return
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.FrameID)

		// Use the same loaderId as the navigation that triggered this event
		b.loaderMapMu.RLock()
		loaderId := b.loaderMap[cdpSessionID]
		b.loaderMapMu.RUnlock()
		if loaderId == "" {
			loaderId = "loader-unknown"
		}

		switch ev.Name {
		case "load":
			b.emitEvent("Page.loadEventFired", map[string]interface{}{
				"timestamp": 0,
			}, cdpSessionID)
			b.emitEvent("Page.lifecycleEvent", map[string]interface{}{
				"frameId":   cdpFrameID,
				"loaderId":  loaderId,
				"name":      "load",
				"timestamp": 0,
			}, cdpSessionID)
			b.emitEvent("Page.frameStoppedLoading", map[string]interface{}{
				"frameId": cdpFrameID,
			}, cdpSessionID)

			// NOTE: Isolated worlds are NOT re-emitted here.
			// Puppeteer calls Page.createIsolatedWorld after navigation when needed.
		case "DOMContentLoaded":
			b.emitEvent("Page.domContentEventFired", map[string]interface{}{
				"timestamp": 0,
			}, cdpSessionID)
			b.emitEvent("Page.lifecycleEvent", map[string]interface{}{
				"frameId":   cdpFrameID,
				"loaderId":  loaderId,
				"name":      "DOMContentLoaded",
				"timestamp": 0,
			}, cdpSessionID)
		}
	})

	// Runtime.executionContextsCleared → Runtime.executionContextsCleared
	// Also clear the ctxMap since all old context IDs are now stale
	b.backend.Subscribe("Runtime.executionContextsCleared", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		if cdpSessionID != "" {
			// Clear stale context mappings for this page only.
			b.clearContextsForSession(cdpSessionID)

			b.emitEvent("Runtime.executionContextsCleared", map[string]interface{}{}, cdpSessionID)

			// Mark for isolated world re-emission
			b.pendingContextClearMu.Lock()
			b.pendingContextClear[cdpSessionID] = true
			b.pendingContextClearMu.Unlock()
		}
	})

	// Runtime.executionContextCreated → Runtime.executionContextCreated
	b.backend.Subscribe("Runtime.executionContextCreated", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		ctxID := b.nextCtxID()

		var ev struct {
			ExecutionContextID string `json:"executionContextId"`
			AuxData            struct {
				FrameID string `json:"frameId"`
				Name    string `json:"name"`
			} `json:"auxData"`
			Origin string `json:"origin"`
		}
		json.Unmarshal(params, &ev)

		// Store frame ID if not already set
		if ev.AuxData.FrameID != "" {
			if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok && info.FrameID == "" {
				b.sessions.SetFrameIDIfEmpty(info.SessionID, ev.AuxData.FrameID)
				log.Printf("[event] stored frameID=%s for juggler session %s", ev.AuxData.FrameID, jugglerSessionID)
			} else if !ok {
				// Session not registered yet — buffer the frameId for later
				b.autoAttach.mu.Lock()
				if _, exists := b.autoAttach.pendingFrameIDs[jugglerSessionID]; !exists {
					b.autoAttach.pendingFrameIDs[jugglerSessionID] = ev.AuxData.FrameID
					log.Printf("[event] buffered pending frameID=%s for juggler session %s", ev.AuxData.FrameID, jugglerSessionID)
				}
				b.autoAttach.mu.Unlock()
			}
		}

		// Store the mapping: numeric CDP ID → Juggler string ID
		b.ctxMapMu.Lock()
		b.ctxMap[ctxID] = ev.ExecutionContextID
		b.ctxOwners[ctxID] = cdpSessionID
		b.ctxMapMu.Unlock()

		// Always track the latest context. Juggler creates/destroys contexts rapidly
		// during navigation — only the last surviving one matters.
		b.latestCtxMu.Lock()
		b.latestCtx[jugglerSessionID] = ev.ExecutionContextID
		b.latestCtxMu.Unlock()
		b.ctxMapMu.Lock()
		b.ctxUniqueOwners[ev.ExecutionContextID] = cdpSessionID
		b.ctxMapMu.Unlock()

		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.AuxData.FrameID)

		b.emitEvent("Runtime.executionContextCreated", map[string]interface{}{
			"context": map[string]interface{}{
				"id":       ctxID,
				"origin":   "",
				"name":     ev.AuxData.Name,
				"uniqueId": ev.ExecutionContextID,
				"auxData": map[string]interface{}{
					"isDefault": true,
					"type":      "default",
					"frameId":   cdpFrameID,
				},
			},
		}, cdpSessionID)

		// Re-emit isolated world contexts whenever a new default context appears.
		// Both Juggler and BiDi need this — after navigation, the utility world context
		// is destroyed and Puppeteer needs a new one for $$, $$eval, and other operations.
		if cdpSessionID != "" {
			b.isolatedWorldsMu.RLock()
			worlds := b.isolatedWorlds[cdpSessionID]
			b.isolatedWorldsMu.RUnlock()

			frameID := cdpFrameID
			for _, w := range worlds {
				isoCtxID := b.nextCtxID()
				b.ctxMapMu.Lock()
				b.ctxMap[isoCtxID] = ev.ExecutionContextID
				b.ctxOwners[isoCtxID] = cdpSessionID
				b.ctxMapMu.Unlock()

				uniqueID := fmt.Sprintf("isolated-%s-%s", frameID, w.WorldName)
				b.ctxMapMu.Lock()
				b.ctxUniqueOwners[uniqueID] = cdpSessionID
				b.ctxMapMu.Unlock()
				b.emitEvent("Runtime.executionContextCreated", map[string]interface{}{
					"context": map[string]interface{}{
						"id":       isoCtxID,
						"origin":   "",
						"name":     w.WorldName,
						"uniqueId": uniqueID,
						"auxData": map[string]interface{}{
							"isDefault": false,
							"type":      "isolated",
							"frameId":   frameID,
						},
					},
				}, cdpSessionID)
			}
		}
	})

	// Runtime.executionContextDestroyed → Runtime.executionContextDestroyed
	b.backend.Subscribe("Runtime.executionContextDestroyed", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		var ev struct {
			ExecutionContextID string `json:"executionContextId"`
		}
		json.Unmarshal(params, &ev)

		// Find the numeric CDP context ID for this Juggler context
		var numericID int
		b.ctxMapMu.RLock()
		for k, v := range b.ctxMap {
			if v == ev.ExecutionContextID {
				numericID = k
				break
			}
		}
		b.ctxMapMu.RUnlock()

		if b.isBiDi {
			// BiDi: clean up all mappings pointing to this context ID,
			// including isolated world contexts mapped to the same realm.
			var destroyIDs []int
			b.ctxMapMu.Lock()
			for k, v := range b.ctxMap {
				if v == ev.ExecutionContextID {
					destroyIDs = append(destroyIDs, k)
					delete(b.ctxMap, k)
					delete(b.ctxOwners, k)
				}
			}
			b.ctxMapMu.Unlock()

			b.emitEvent("Runtime.executionContextDestroyed", map[string]interface{}{
				"executionContextId":       numericID,
				"executionContextUniqueId": ev.ExecutionContextID,
			}, cdpSessionID)

			for _, id := range destroyIDs {
				if id != numericID && id > 0 {
					b.emitEvent("Runtime.executionContextDestroyed", map[string]interface{}{
						"executionContextId":       id,
						"executionContextUniqueId": fmt.Sprintf("isolated-derived-%d", id),
					}, cdpSessionID)
				}
			}
		} else {
			// Juggler: clean up only the single mapping
			if numericID > 0 {
				b.ctxMapMu.Lock()
				delete(b.ctxMap, numericID)
				delete(b.ctxOwners, numericID)
				b.ctxMapMu.Unlock()
			}

			b.emitEvent("Runtime.executionContextDestroyed", map[string]interface{}{
				"executionContextId":       numericID,
				"executionContextUniqueId": ev.ExecutionContextID,
			}, cdpSessionID)
		}
	})

	// Runtime.console → Runtime.consoleAPICalled
	b.backend.Subscribe("Runtime.console", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			Type string          `json:"type"`
			Args json.RawMessage `json:"args"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Runtime.console: %v", err)
			return
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)

		b.emitEvent("Runtime.consoleAPICalled", map[string]interface{}{
			"type":               ev.Type,
			"args":               ev.Args,
			"executionContextId": 0,
			"timestamp":          0,
		}, cdpSessionID)
	})

	// Page.frameAttached → Page.frameAttached
	b.backend.Subscribe("Page.frameAttached", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			FrameID       string `json:"frameId"`
			ParentFrameID string `json:"parentFrameId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Page.frameAttached: %v", err)
			return
		}

		// Store the main frame ID (parentFrameId is empty for the main frame)
		if ev.ParentFrameID == "" && ev.FrameID != "" {
			if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok {
				b.sessions.SetFrameID(info.SessionID, ev.FrameID)
			}
		} else if ev.ParentFrameID != "" {
			b.noteSubFrame(jugglerSessionID, ev.FrameID)
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.FrameID)
		cdpParentFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.ParentFrameID)

		b.emitEvent("Page.frameAttached", map[string]interface{}{
			"frameId":       cdpFrameID,
			"parentFrameId": cdpParentFrameID,
		}, cdpSessionID)
	})

	// Page.frameDetached → Page.frameDetached
	b.backend.Subscribe("Page.frameDetached", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			FrameID string `json:"frameId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Page.frameDetached: %v", err)
			return
		}

		// Forget the cached main frame once it goes away. Juggler accepts a stale
		// frameId on Page.navigate, returns a navigationId and then silently does
		// nothing, so holding on to a detached frame turns every later navigation
		// into a no-op that looks like success. Clearing it lets the next
		// frameAttached / executionContextCreated repopulate the live frame.
		if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok &&
			ev.FrameID != "" && info.FrameID == ev.FrameID {
			b.sessions.SetFrameID(info.SessionID, "")
			log.Printf("[event] cleared detached main frameID=%s for session %s", ev.FrameID, info.SessionID)
		}
		b.forgetSubFrame(jugglerSessionID, ev.FrameID)

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.FrameID)

		b.emitEvent("Page.frameDetached", map[string]interface{}{
			"frameId": cdpFrameID,
			"reason":  "remove",
		}, cdpSessionID)
	})

	// Page.dialogOpened → Page.javascriptDialogOpening
	b.backend.Subscribe("Page.dialogOpened", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			Type         string `json:"type"`
			Message      string `json:"message"`
			DefaultValue string `json:"defaultValue"`
			DialogID     string `json:"dialogId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Page.dialogOpened: %v", err)
			return
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)

		// Store dialog ID so handleJavaScriptDialog can include it
		if ev.DialogID != "" && cdpSessionID != "" {
			b.lastDialogMu.Lock()
			b.lastDialog[cdpSessionID] = ev.DialogID
			b.lastDialogMu.Unlock()
		}

		b.emitEvent("Page.javascriptDialogOpening", map[string]interface{}{
			"type":              ev.Type,
			"message":           ev.Message,
			"defaultPrompt":     ev.DefaultValue,
			"hasBrowserHandler": false,
			"url":               "",
		}, cdpSessionID)
	})

	// Page.dialogClosed → Page.javascriptDialogClosed
	b.backend.Subscribe("Page.dialogClosed", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			Accepted bool `json:"accepted"`
		}
		json.Unmarshal(params, &ev)

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)

		b.emitEvent("Page.javascriptDialogClosed", map[string]interface{}{
			"result":    ev.Accepted,
			"userInput": "",
		}, cdpSessionID)
	})

	// Network.requestWillBeSent → Network.requestWillBeSent
	b.backend.Subscribe("Network.requestWillBeSent", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			RequestID     string        `json:"requestId"`
			FrameID       string        `json:"frameId"`
			URL           string        `json:"url"`
			Method        string        `json:"method"`
			Headers       []headerEntry `json:"headers"`
			PostData      string        `json:"postData"`
			IsIntercepted bool          `json:"isIntercepted"`
			IsNavigation  bool          `json:"isNavigationRequest"`
			NavigationID  string        `json:"navigationId"`
			Cause         string        `json:"cause"`
			RedirectURL   string        `json:"redirectedFrom"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			return
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		if cdpSessionID != "" {
			b.ownership.setRequestOwner(ev.RequestID, cdpSessionID)
		}
		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.FrameID)

		cdpHeaders := map[string]string{}
		for _, header := range ev.Headers {
			cdpHeaders[header.Name] = header.Value
		}

		// Detect WebSocket connections from URL scheme
		isWebSocket := strings.HasPrefix(ev.URL, "ws://") || strings.HasPrefix(ev.URL, "wss://")

		if isWebSocket {
			// Emit WebSocket-specific CDP events
			b.emitEvent("Network.webSocketCreated", map[string]interface{}{
				"requestId": ev.RequestID,
				"url":       ev.URL,
			}, cdpSessionID)
		}

		resourceType := "Document"
		if isWebSocket {
			resourceType = "WebSocket"
		} else if !ev.IsNavigation && ev.NavigationID == "" {
			resourceType = "Other"
		}

		b.emitEvent("Network.requestWillBeSent", map[string]interface{}{
			"requestId":   ev.RequestID,
			"loaderId":    ev.RequestID,
			"documentURL": ev.URL,
			"request": map[string]interface{}{
				"url":             ev.URL,
				"method":          ev.Method,
				"headers":         cdpHeaders,
				"initialPriority": "High",
				"referrerPolicy":  "strict-origin-when-cross-origin",
			},
			"timestamp": 0,
			"wallTime":  0,
			"initiator": map[string]interface{}{
				"type": "other",
			},
			"type":    resourceType,
			"frameId": cdpFrameID,
		}, cdpSessionID)

		if ev.IsIntercepted {
			if !b.fetchEnabledForSession(cdpSessionID) {
				go func() {
					if err := b.continueFetchRequest(cdpSessionID, ev.RequestID); err != nil {
						log.Printf("events: failed to continue request for disabled Fetch session: %v", err)
					}
				}()
				return
			}
			if !b.shouldPauseFetchRequest(cdpSessionID, ev.URL, resourceType, "Request") {
				// Backend event handlers run on Juggler's read loop. Continue in a
				// goroutine so the read loop can receive the command response.
				go func() {
					if err := b.continueFetchRequest(cdpSessionID, ev.RequestID); err != nil {
						log.Printf("events: failed to continue request excluded by Fetch patterns: %v", err)
					}
				}()
				return
			}

			request := map[string]interface{}{
				"url":             ev.URL,
				"method":          ev.Method,
				"headers":         cdpHeaders,
				"initialPriority": "High",
				"referrerPolicy":  "strict-origin-when-cross-origin",
			}
			if ev.PostData != "" {
				request["postData"] = ev.PostData
			}
			b.emitEvent("Fetch.requestPaused", map[string]interface{}{
				"requestId":    ev.RequestID,
				"networkId":    ev.RequestID,
				"request":      request,
				"frameId":      cdpFrameID,
				"resourceType": resourceType,
			}, cdpSessionID)
		}
	})

	// Network.responseReceived → Network.responseReceived
	b.backend.Subscribe("Network.responseReceived", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			RequestID       string            `json:"requestId"`
			SecurityDetails json.RawMessage   `json:"securityDetails"`
			FromCache       bool              `json:"fromCache"`
			Headers         map[string]string `json:"headers"`
			Status          int               `json:"status"`
			StatusText      string            `json:"statusText"`
			URL             string            `json:"url"`
			FrameID         string            `json:"frameId"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			return
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		cdpFrameID := b.cdpFrameIDForJugglerSession(jugglerSessionID, ev.FrameID)

		b.emitEvent("Network.responseReceived", map[string]interface{}{
			"requestId": ev.RequestID,
			"loaderId":  ev.RequestID,
			"timestamp": 0,
			"type":      "Document",
			"response": map[string]interface{}{
				"url":               ev.URL,
				"status":            ev.Status,
				"statusText":        ev.StatusText,
				"headers":           ev.Headers,
				"mimeType":          "",
				"connectionReused":  false,
				"connectionId":      0,
				"encodedDataLength": 0,
				"fromDiskCache":     ev.FromCache,
				"fromServiceWorker": false,
				"fromPrefetchCache": false,
				"securityState":     "secure",
			},
			"frameId": cdpFrameID,
		}, cdpSessionID)
	})

	// Network.requestFinished → Network.loadingFinished
	b.backend.Subscribe("Network.requestFinished", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			RequestID string `json:"requestId"`
		}
		json.Unmarshal(params, &ev)

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)

		b.emitEvent("Network.loadingFinished", map[string]interface{}{
			"requestId":         ev.RequestID,
			"timestamp":         0,
			"encodedDataLength": 0,
		}, cdpSessionID)
	})

	// Network.requestFailed → Network.loadingFailed
	b.backend.Subscribe("Network.requestFailed", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			RequestID string `json:"requestId"`
			ErrorCode string `json:"errorCode"`
		}
		json.Unmarshal(params, &ev)

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)

		b.emitEvent("Network.loadingFailed", map[string]interface{}{
			"requestId": ev.RequestID,
			"timestamp": 0,
			"type":      "Document",
			"errorText": ev.ErrorCode,
			"canceled":  false,
		}, cdpSessionID)
		b.ownership.clearRequest(ev.RequestID)
	})

	// WebSocket events → Network.webSocket* CDP events
	b.backend.Subscribe("Page.webSocketCreated", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Network.webSocketCreated", params, cdpSessionID)
	})

	b.backend.Subscribe("Page.webSocketOpened", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Network.webSocketWillSendHandshakeRequest", params, cdpSessionID)
	})

	b.backend.Subscribe("Page.webSocketClosed", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Network.webSocketClosed", params, cdpSessionID)
	})

	b.backend.Subscribe("Page.webSocketFrameSent", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Network.webSocketFrameSent", params, cdpSessionID)
	})

	b.backend.Subscribe("Page.webSocketFrameReceived", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Network.webSocketFrameReceived", params, cdpSessionID)
	})

	// Download events → CDP Page.downloadWillBegin / Page.downloadProgress
	b.backend.Subscribe("Browser.downloadCreated", func(sessionID string, params json.RawMessage) {
		var ev struct {
			UUID              string `json:"uuid"`
			URL               string `json:"url"`
			SuggestedFileName string `json:"suggestedFileName"`
		}
		json.Unmarshal(params, &ev)

		cdpSessionID := b.resolveCDPSession(sessionID)
		if cdpSessionID == "" {
			log.Printf("[event] dropping unattributable download %s", ev.UUID)
			return
		}
		b.emitEvent("Page.downloadWillBegin", map[string]interface{}{
			"frameId": "", "guid": ev.UUID, "url": ev.URL,
			"suggestedFilename": ev.SuggestedFileName,
		}, cdpSessionID)
	})

	b.backend.Subscribe("Browser.downloadFinished", func(sessionID string, params json.RawMessage) {
		var ev struct {
			UUID     string `json:"uuid"`
			Canceled bool   `json:"canceled"`
			Error    string `json:"error"`
		}
		json.Unmarshal(params, &ev)

		state := "completed"
		if ev.Canceled {
			state = "canceled"
		}
		if ev.Error != "" {
			state = "canceled"
		}

		cdpSessionID := b.resolveCDPSession(sessionID)
		if cdpSessionID == "" {
			log.Printf("[event] dropping unattributable download %s", ev.UUID)
			return
		}
		b.emitEvent("Page.downloadProgress", map[string]interface{}{
			"guid": ev.UUID, "state": state,
		}, cdpSessionID)
	})

	// Screencast frame events
	b.backend.Subscribe("Page.screencastFrame", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Page.screencastFrame", params, cdpSessionID)
	})

	// File chooser events
	b.backend.Subscribe("Page.fileChooserOpened", func(jugglerSessionID string, params json.RawMessage) {
		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		b.emitEventRaw("Page.fileChooserOpened", params, cdpSessionID)
	})

	// Browser.requestIntercepted → Fetch.requestPaused
	b.backend.Subscribe("Browser.requestIntercepted", func(jugglerSessionID string, params json.RawMessage) {
		var ev struct {
			RequestID string `json:"requestId"`
			// Juggler sends request fields at top level (not nested in "request")
			URL     string `json:"url"`
			Method  string `json:"method"`
			Headers []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"headers"`
			// Nested format for backwards compatibility
			Request struct {
				URL     string            `json:"url"`
				Method  string            `json:"method"`
				Headers map[string]string `json:"headers"`
			} `json:"request"`
			FrameID             string `json:"frameId"`
			IsNavigationRequest bool   `json:"isNavigationRequest"`
			ResourceType        string `json:"resourceType"`
		}
		if err := json.Unmarshal(params, &ev); err != nil {
			log.Printf("events: failed to parse Browser.requestIntercepted: %v", err)
			return
		}

		cdpSessionID := b.resolveCDPSession(jugglerSessionID)
		if cdpSessionID != "" && b.ownership.sessionOwner(cdpSessionID) == nil {
			cdpSessionID = ""
		}

		// Browser.requestIntercepted is a browser-level event (no juggler session ID).
		// Resolve the CDP session from the frameId so Puppeteer receives it on the page session.
		if cdpSessionID == "" && ev.FrameID != "" {
			if session, ok := b.sessionForFrame(ev.FrameID); ok {
				cdpSessionID = session
			}
		}
		if cdpSessionID != "" && b.ownership.sessionOwner(cdpSessionID) == nil {
			cdpSessionID = ""
		}

		if cdpSessionID == "" {
			go func() {
				if err := b.continueUnattributedFetchRequest(ev.RequestID); err != nil {
					log.Printf("[event] dropping unattributable requestId=%s: %v", ev.RequestID, err)
				}
			}()
			return
		}

		b.ownership.setRequestOwner(ev.RequestID, cdpSessionID)

		// Use top-level fields (new Juggler format) or nested request fields (fallback)
		url := ev.URL
		method := ev.Method
		if url == "" {
			url = ev.Request.URL
			method = ev.Request.Method
		}

		// Convert headers array [{name,value}] to map for CDP
		headerMap := map[string]string{}
		for _, h := range ev.Headers {
			headerMap[h.Name] = h.Value
		}
		if len(headerMap) == 0 {
			headerMap = ev.Request.Headers
		}

		resourceType := ev.ResourceType
		if resourceType == "" {
			resourceType = "Other"
			if ev.IsNavigationRequest {
				resourceType = "Document"
			}
		}

		if !b.shouldPauseFetchRequest(cdpSessionID, url, resourceType, "Request") {
			go func() {
				if err := b.continueFetchRequest(cdpSessionID, ev.RequestID); err != nil {
					log.Printf("events: failed to continue request excluded by Fetch patterns: %v", err)
				}
			}()
			return
		}

		cdpFrameID := ev.FrameID
		if cdpSessionID != "" {
			cdpFrameID = b.cdpFrameIDForSession(cdpSessionID, ev.FrameID)
		}

		log.Printf("[event] Browser.requestIntercepted → Fetch.requestPaused requestId=%s url=%s cdpSession=%s", ev.RequestID, url, cdpSessionID)

		// Emit Network.requestWillBeSent BEFORE Fetch.requestPaused.
		// Puppeteer needs both events with matching requestId/networkId to process interception.
		b.emitEvent("Network.requestWillBeSent", map[string]interface{}{
			"requestId":   ev.RequestID,
			"loaderId":    ev.RequestID,
			"documentURL": url,
			"request": map[string]interface{}{
				"url":             url,
				"method":          method,
				"headers":         headerMap,
				"initialPriority": "High",
				"referrerPolicy":  "strict-origin-when-cross-origin",
			},
			"timestamp": 0,
			"wallTime":  0,
			"initiator": map[string]interface{}{"type": "other"},
			"type":      resourceType,
			"frameId":   cdpFrameID,
		}, cdpSessionID)

		b.emitEvent("Fetch.requestPaused", map[string]interface{}{
			"requestId": ev.RequestID,
			"networkId": ev.RequestID,
			"request": map[string]interface{}{
				"url":             url,
				"method":          method,
				"headers":         headerMap,
				"initialPriority": "High",
				"referrerPolicy":  "strict-origin-when-cross-origin",
			},
			"frameId":      cdpFrameID,
			"resourceType": resourceType,
		}, cdpSessionID)
	})
}

func (b *Bridge) emitAutoAttachPair(pair *targetPair) {
	b.emitPageAttachOnSession(pair, "")
}

// emitPageAttach emits the page-level attachment on the provided parent session.
func (b *Bridge) emitPageAttach(pair *targetPair) {
	b.emitPageAttachOnSession(pair, pair.tabSessionID)
}

// emitPageAttachOnSession emits the page-level attachment on the provided parent session.
// Browser-level auto-attach should emit page attachments on the root session so Playwright
// discovers real pages instead of only seeing a synthetic tab wrapper.
func (b *Bridge) emitPageAttachOnSession(pair *targetPair, parentSessionID string) {
	b.autoAttach.mu.Lock()
	if parentSessionID == "" {
		if pair.pageAttachedRoot {
			b.autoAttach.mu.Unlock()
			return
		}
		pair.pageAttachedRoot = true
	} else {
		// A flattened CDP session ID is global to the connection. Re-emitting the
		// same page session beneath its synthetic tab makes Puppeteer replace the
		// session object; responses then reach the replacement while requests are
		// pending on the original object.
		if pair.pageAttachedRoot {
			b.autoAttach.mu.Unlock()
			return
		}
		if pair.pageAttachedTab {
			b.autoAttach.mu.Unlock()
			return
		}
		pair.pageAttachedTab = true
	}
	b.autoAttach.mu.Unlock()

	b.autoAttach.mu.Lock()
	url := pair.url
	b.autoAttach.mu.Unlock()
	if url == "" {
		url = "about:blank"
	}
	b.emitEvent("Target.attachedToTarget", map[string]interface{}{
		"sessionId": pair.pageSessionID,
		"targetInfo": map[string]interface{}{
			"targetId":         pair.pageTargetID,
			"type":             "page",
			"title":            "",
			"url":              url,
			"attached":         true,
			"canAccessOpener":  false,
			"browserContextId": pair.browserCtxID,
		},
		"waitingForDebugger": true,
	}, parentSessionID)
}

// resolveCDPSession maps a Juggler sessionID to a CDP sessionID.
// For page-level events, we want the PAGE session (not the tab).
func (b *Bridge) resolveCDPSession(jugglerSessionID string) string {
	if jugglerSessionID == "" {
		return ""
	}
	// Look up the pair to get the page session ID
	b.autoAttach.mu.Lock()
	pair, ok := b.autoAttach.pairs[jugglerSessionID]
	b.autoAttach.mu.Unlock()
	if ok {
		return pair.pageSessionID
	}
	// Fallback to session manager
	if info, ok := b.sessions.GetByJugglerSession(jugglerSessionID); ok {
		return info.SessionID
	}
	return ""
}

// emitEventRaw sends a CDP event with raw JSON params.
func (b *Bridge) emitEventRaw(method string, params json.RawMessage, sessionID string) {
	b.emitEvent(method, params, sessionID)
}
