package bridge

import (
	"encoding/json"
	"testing"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
)

func TestFrameDetachedClearsStaleMainFrame(t *testing.T) {
	b, mb := newTestBridge()
	b.SetupEventSubscriptions()
	detach := mb.handlers["Page.frameDetached"]
	if len(detach) == 0 {
		t.Fatal("no Page.frameDetached subscriber registered")
	}
	b.sessions.Add(&cdp.SessionInfo{
		SessionID:        "page-session-1",
		JugglerSessionID: "juggler-1",
		TargetID:         "page-1",
		Type:             "page",
		FrameID:          "mainframe-7",
	})

	// A subframe going away must not disturb the cached main frame.
	detach[0]("juggler-1", json.RawMessage(`{"frameId":"subframe-9"}`))
	if info, _ := b.sessions.Get("page-session-1"); info.FrameID != "mainframe-7" {
		t.Fatalf("subframe detach cleared main frame: %q", info.FrameID)
	}

	// The main frame going away must clear it, so the next navigate re-learns
	// a live frame instead of addressing a dead one.
	detach[0]("juggler-1", json.RawMessage(`{"frameId":"mainframe-7"}`))
	if info, _ := b.sessions.Get("page-session-1"); info.FrameID != "" {
		t.Fatalf("main frame detach left stale frame: %q", info.FrameID)
	}
}

func TestRefreshMainFrameTracksLiveFrame(t *testing.T) {
	b, mb := newTestBridge()
	b.SetupEventSubscriptions()
	b.sessions.Add(&cdp.SessionInfo{
		SessionID:        "page-session-1",
		JugglerSessionID: "juggler-1",
		TargetID:         "page-1",
		Type:             "page",
		FrameID:          "mainframe-1",
	})
	attach := mb.handlers["Page.frameAttached"]
	started := mb.handlers["Page.navigationStarted"]
	if len(attach) == 0 || len(started) == 0 {
		t.Fatal("frame event subscribers not registered")
	}
	frameID := func() string {
		info, _ := b.sessions.Get("page-session-1")
		return info.FrameID
	}

	// A navigation inside a known subframe must not become the main frame.
	attach[0]("juggler-1", json.RawMessage(`{"frameId":"sub-1","parentFrameId":"mainframe-1"}`))
	started[0]("juggler-1", json.RawMessage(`{"frameId":"sub-1","navigationId":"nav-1"}`))
	if got := frameID(); got != "mainframe-1" {
		t.Fatalf("subframe navigation hijacked the main frame: %q", got)
	}

	// A top-level navigation in a replacement frame refreshes the stale cache.
	started[0]("juggler-1", json.RawMessage(`{"frameId":"mainframe-2","navigationId":"nav-2"}`))
	if got := frameID(); got != "mainframe-2" {
		t.Fatalf("main frame not refreshed: %q, want mainframe-2", got)
	}

	// Once a subframe detaches its ID is no longer reserved.
	b.forgetSubFrame("juggler-1", "sub-1")
	if b.isSubFrame("juggler-1", "sub-1") {
		t.Fatal("detached subframe still tracked")
	}
}
