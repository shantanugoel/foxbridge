package bridge

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
	"github.com/google/uuid"
)

func (b *Bridge) handleTarget(conn *cdp.Connection, msg *cdp.Message) (json.RawMessage, *cdp.Error) {
	switch msg.Method {
	case "Target.setDiscoverTargets":
		var params struct {
			Discover bool `json:"discover"`
		}
		if msg.Params != nil {
			if err := json.Unmarshal(msg.Params, &params); err != nil {
				return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
			}
		}
		b.ownership.setDiscover(conn, params.Discover)
		if conn != nil && !params.Discover {
			return json.RawMessage(`{}`), nil
		}
		// Emit targetCreated only for owned page targets.
		for _, info := range b.sessions.All() {
			if info.Type == "tab" || !b.ownedTarget(conn, info.TargetID) {
				continue
			}
			url := info.URL
			if url == "" && info.Type == "page" {
				url = "about:blank"
			}
			b.emitEvent("Target.targetCreated", map[string]interface{}{
				"targetInfo": map[string]interface{}{
					"targetId":         info.TargetID,
					"type":             info.Type,
					"title":            info.Title,
					"url":              url,
					"attached":         true,
					"canAccessOpener":  false,
					"browserContextId": info.BrowserContextID,
				},
			}, "")
		}
		return json.RawMessage(`{}`), nil

	case "Target.setAutoAttach":
		var params struct {
			AutoAttach             bool `json:"autoAttach"`
			WaitForDebuggerOnStart bool `json:"waitForDebuggerOnStart"`
			Flatten                bool `json:"flatten"`
		}
		json.Unmarshal(msg.Params, &params)

		if msg.SessionID == "" {
			if conn != nil {
				b.ownership.setAutoAttach(conn, params.AutoAttach)
				if params.AutoAttach {
					for _, pair := range b.ownership.ownedPairs(conn) {
						b.publishOwnedPair(pair)
					}
				}
			} else {
				// Compatibility path for direct unit calls without a connection.
				b.autoAttach.mu.Lock()
				b.autoAttach.enabled = params.AutoAttach
				pending := b.autoAttach.pending
				b.autoAttach.pending = nil
				b.autoAttach.mu.Unlock()
				if params.AutoAttach {
					for _, pair := range pending {
						b.emitAutoAttachPair(pair)
					}
				}
			}
		} else {
			// Session-level setAutoAttach (tab, page, or worker session).
			if info, ok := b.sessions.Get(msg.SessionID); ok {
				switch info.Type {
				case "tab":
					// Tab session: emit the page attachment if the client asks for it.
					b.autoAttach.mu.Lock()
					for _, pair := range b.autoAttach.pairs {
						if pair.tabSessionID == msg.SessionID {
							b.autoAttach.mu.Unlock()
							b.emitPageAttach(pair)
							goto autoAttachDone
						}
					}
					b.autoAttach.mu.Unlock()
				case "worker", "service_worker":
					// Workers have no children to auto-attach — no-op.
				}
			}
			// Page-session or no match: no-op
		}
	autoAttachDone:
		return json.RawMessage(`{}`), nil

	case "Target.createTarget":
		var params struct {
			URL              string `json:"url"`
			BrowserContextID string `json:"browserContextId"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}
		if conn != nil && !b.isSyntheticDefaultBrowserContextID(params.BrowserContextID) {
			return nil, &cdp.Error{Code: -32000, Message: "browser contexts are disabled"}
		}
		generation := b.ownership.beginCreate(conn)
		defer b.ownership.finishCreate(generation)

		jugglerParams := map[string]interface{}{}
		b.setJugglerBrowserContext(jugglerParams, params.BrowserContextID)

		result, err := b.callJuggler("", "Browser.newPage", jugglerParams)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}

		// Juggler returns { targetId }
		log.Printf("[target] Browser.newPage response: %s", string(result))
		var pageResult struct {
			TargetID string `json:"targetId"`
		}
		json.Unmarshal(result, &pageResult)

		targetID := pageResult.TargetID
		if targetID == "" {
			targetID = uuid.New().String()
		}

		record := b.ownership.claimTarget(conn, targetID, generation)
		if record == nil && conn != nil {
			time.AfterFunc(10*time.Second, func() {
				if b.ownership.expireClaim(targetID, generation) {
					log.Printf("[target] createTarget attach timed out for %s; closing backend target", targetID)
					if _, err := b.callJuggler(targetID, "Page.close", nil); err != nil {
						log.Printf("[target] failed to close timed-out target %s: %v", targetID, err)
					}
				}
			})
		}
		if b.ownership.connectionClosed(conn) {
			if record == nil {
				record = b.ownership.cancelTarget(targetID)
			}
			if record != nil {
				b.closeRecord(record, true)
			}
			return nil, &cdp.Error{Code: -32000, Message: "connection closed"}
		}
		if record != nil {
			recordOwner, pair, cancelled := b.ownership.recordDetails(record)
			if cancelled {
				b.closeRecord(record, true)
				return nil, &cdp.Error{Code: -32000, Message: "connection closed"}
			}
			if recordOwner == conn && pair != nil {
				b.publishOwnedPair(pair)
			}
		}

		if params.URL != "" && params.URL != "about:blank" {
			if cdpSessionID, ok := b.waitForPageSession(targetID, 500*time.Millisecond); ok {
				b.applyDeterministicPrelude(cdpSessionID)
			}
			if err := b.navigateNewTarget(targetID, params.URL); err != nil {
				if record := b.ownership.cancelTarget(targetID); record != nil {
					b.closeRecord(record, true)
				}
				return nil, &cdp.Error{Code: -32000, Message: err.Error()}
			}
		}

		// Return the PAGE targetId (not the tab).
		// Puppeteer's waitForTarget matches on this ID, and TAB targets are filtered
		// out by _isTargetExposed(). The tab attachment happens via the event handler.
		log.Printf("[target] createTarget returning page targetId=%s", targetID)
		return marshalResult(map[string]string{"targetId": targetID})

	case "Target.closeTarget":
		var params struct {
			TargetID string `json:"targetId"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		if !b.ownedTarget(conn, params.TargetID) {
			return nil, &cdp.Error{Code: -32000, Message: fmt.Sprintf("target %s not found", params.TargetID)}
		}
		record := b.ownership.recordForTarget(params.TargetID)
		if record == nil {
			if conn != nil {
				return nil, &cdp.Error{Code: -32000, Message: fmt.Sprintf("target %s not found", params.TargetID)}
			}
			if info, ok := b.sessions.GetByTarget(params.TargetID); ok {
				b.sessions.Remove(info.SessionID)
				return json.RawMessage(`{"success":true}`), nil
			}
			return nil, &cdp.Error{Code: -32000, Message: fmt.Sprintf("target %s not found", params.TargetID)}
		}
		b.closeRecord(record, true)
		return json.RawMessage(`{"success":true}`), nil

	case "Target.createBrowserContext":
		if conn != nil {
			return nil, &cdp.Error{Code: -32000, Message: "browser contexts are disabled"}
		}
		result, err := b.callJuggler("", "Browser.createBrowserContext", nil)
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}

		var ctxResult struct {
			BrowserContextID string `json:"browserContextId"`
		}
		json.Unmarshal(result, &ctxResult)

		return marshalResult(map[string]string{"browserContextId": ctxResult.BrowserContextID})

	case "Target.disposeBrowserContext":
		if conn != nil {
			return nil, &cdp.Error{Code: -32000, Message: "browser contexts are disabled"}
		}
		var params struct {
			BrowserContextID string `json:"browserContextId"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		_, err := b.callJuggler("", "Browser.removeBrowserContext", map[string]string{
			"browserContextId": params.BrowserContextID,
		})
		if err != nil {
			return nil, &cdp.Error{Code: -32000, Message: err.Error()}
		}
		return json.RawMessage(`{}`), nil

	case "Target.getTargets":
		targets := []map[string]interface{}{}
		for _, s := range b.sessions.All() {
			if s.Type == "tab" || !b.ownedTarget(conn, s.TargetID) {
				continue
			}
			targets = append(targets, map[string]interface{}{
				"targetId":         s.TargetID,
				"type":             s.Type,
				"title":            s.Title,
				"url":              s.URL,
				"attached":         true,
				"browserContextId": s.BrowserContextID,
			})
		}
		return marshalResult(map[string]interface{}{"targetInfos": targets})

	case "Target.attachToTarget":
		var params struct {
			TargetID string `json:"targetId"`
			Flatten  bool   `json:"flatten"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, &cdp.Error{Code: -32602, Message: "invalid params"}
		}

		if !b.ownedTarget(conn, params.TargetID) {
			return nil, &cdp.Error{Code: -32000, Message: "target not found"}
		}
		// Check if we already have a session for this target.
		if info, ok := b.sessions.GetByTarget(params.TargetID); ok {
			return marshalResult(map[string]string{"sessionId": info.SessionID})
		}

		// Create a new CDP session for this target.
		sessionID := uuid.New().String()
		b.sessions.Add(&cdp.SessionInfo{
			SessionID:        sessionID,
			JugglerSessionID: params.TargetID, // use targetID as juggler session
			TargetID:         params.TargetID,
			BrowserContextID: b.cdpBrowserContextID(""),
			Type:             "page",
		})

		// Emit Target.attachedToTarget event.
		b.emitEvent("Target.attachedToTarget", map[string]interface{}{
			"sessionId": sessionID,
			"targetInfo": map[string]interface{}{
				"targetId":         params.TargetID,
				"type":             "page",
				"title":            "",
				"url":              "",
				"attached":         true,
				"browserContextId": b.cdpBrowserContextID(""),
			},
			"waitingForDebugger": false,
		}, "")

		return marshalResult(map[string]string{"sessionId": sessionID})

	case "Target.attachToBrowserTarget":
		sessionID := uuid.New().String()
		b.sessions.Add(&cdp.SessionInfo{
			SessionID: sessionID,
			Type:      "browser",
		})
		b.ownership.registerBrowserSession(conn, sessionID)
		return marshalResult(map[string]string{"sessionId": sessionID})

	case "Target.activateTarget":
		var params struct {
			TargetID string `json:"targetId"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil || !b.ownedTarget(conn, params.TargetID) {
			return nil, &cdp.Error{Code: -32000, Message: "target not found"}
		}
		return json.RawMessage(`{}`), nil

	case "Target.detachFromTarget":
		// CDP session detach — just acknowledge, don't close the page
		// Puppeteer calls this when CDPSession.detach() is called
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if msg.Params != nil {
			json.Unmarshal(msg.Params, &params)
		}
		if !b.ownership.sessionOwned(conn, params.SessionID) {
			return nil, &cdp.Error{Code: -32000, Message: "session not found"}
		}
		log.Printf("[target] detachFromTarget: sessionId=%s (no-op, page stays open)", params.SessionID)
		return json.RawMessage(`{}`), nil

	case "Target.getBrowserContexts":
		contexts := b.sessions.GetBrowserContexts()
		if conn != nil {
			contexts = []string{syntheticDefaultBrowserContextID}
		}
		if contexts == nil {
			contexts = []string{}
		}
		// Puppeteer expects defaultBrowserContextId to be present
		defaultCtxID := ""
		if len(contexts) > 0 {
			defaultCtxID = contexts[0]
		}
		return marshalResult(map[string]interface{}{
			"browserContextIds":       contexts,
			"defaultBrowserContextId": defaultCtxID,
		})

	case "Target.getTargetInfo":
		var params struct {
			TargetID string `json:"targetId"`
		}
		json.Unmarshal(msg.Params, &params)
		if params.TargetID == "" {
			return marshalResult(map[string]interface{}{
				"targetInfo": map[string]interface{}{
					"targetId":        "foxbridge-browser",
					"type":            "browser",
					"title":           "",
					"url":             "",
					"attached":        true,
					"canAccessOpener": false,
				},
			})
		}
		if !b.ownedTarget(conn, params.TargetID) {
			return nil, &cdp.Error{Code: -32000, Message: "target not found"}
		}
		if info, ok := b.sessions.GetByTarget(params.TargetID); ok {
			return marshalResult(map[string]interface{}{
				"targetInfo": map[string]interface{}{
					"targetId":         info.TargetID,
					"type":             info.Type,
					"title":            info.Title,
					"url":              info.URL,
					"attached":         true,
					"browserContextId": info.BrowserContextID,
				},
			})
		}
		return nil, &cdp.Error{Code: -32000, Message: "target not found"}

	default:
		return nil, &cdp.Error{Code: -32601, Message: fmt.Sprintf("method not found: %s", msg.Method)}
	}
}

func (b *Bridge) navigateNewTarget(targetID, url string) error {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info, ok := b.sessions.GetByTarget(targetID)
		if ok && info.FrameID != "" {
			_, err := b.callJuggler(info.SessionID, "Page.navigate", map[string]interface{}{
				"frameId": info.FrameID,
				"url":     url,
			})
			if err != nil {
				return fmt.Errorf("navigate new target %s to %s: %w", targetID, url, err)
			}
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("target %s not ready for initial navigation to %s", targetID, url)
}

func (b *Bridge) waitForPageSession(targetID string, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if info, ok := b.sessions.GetByTarget(targetID); ok && info.Type == "page" {
			return info.SessionID, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", false
}

func marshalResult(v interface{}) (json.RawMessage, *cdp.Error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, &cdp.Error{Code: -32000, Message: err.Error()}
	}
	return data, nil
}
