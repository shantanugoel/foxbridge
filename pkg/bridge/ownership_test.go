package bridge

import (
	"testing"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
)

func TestOwnershipClaimBeforeAttach(t *testing.T) {
	r := newOwnershipRegistry()
	owner := &cdp.Connection{}
	generation := r.beginCreate(owner)
	if record := r.claimTarget(owner, "page-1", generation); record != nil {
		t.Fatal("claim should wait for backend attach")
	}

	record := r.registerPair(nil, &targetPair{
		pageTargetID:     "page-1",
		tabTargetID:      "tab-1",
		pageSessionID:    "page-session-1",
		tabSessionID:     "tab-session-1",
		jugglerSessionID: "juggler-1",
	})
	if owner, _, _ := r.recordDetails(record); owner == nil {
		t.Fatal("backend attach did not inherit create owner")
	}
	if r.ownerForTarget("tab-1") != owner {
		t.Fatal("synthetic tab did not share page owner")
	}
}

func TestOwnershipClaimAfterAttach(t *testing.T) {
	r := newOwnershipRegistry()
	owner := &cdp.Connection{}
	record := r.registerPair(nil, &targetPair{
		pageTargetID:     "page-1",
		pageSessionID:    "page-session-1",
		jugglerSessionID: "juggler-1",
	})
	generation := r.beginCreate(owner)
	if got := r.claimTarget(owner, "page-1", generation); got != record {
		t.Fatal("claim did not return attached record")
	}
	if r.ownerForTarget("page-1") != owner {
		t.Fatal("attached record did not become owned")
	}
}

func TestOwnershipRejectsForeignSession(t *testing.T) {
	r := newOwnershipRegistry()
	a := &cdp.Connection{}
	b := &cdp.Connection{}
	r.registerPair(a, &targetPair{pageTargetID: "page-1", pageSessionID: "session-1"})
	if !r.sessionOwned(a, "session-1") {
		t.Fatal("owner lost its session")
	}
	if r.sessionOwned(b, "session-1") {
		t.Fatal("foreign connection owns the session")
	}
	if r.ownerForTarget("missing") != nil {
		t.Fatal("missing target has an owner")
	}
}

func TestOwnershipCloseConnectionIsIdempotent(t *testing.T) {
	r := newOwnershipRegistry()
	owner := &cdp.Connection{}
	r.registerPair(owner, &targetPair{pageTargetID: "page-1", pageSessionID: "session-1"})
	if records := r.closeConnection(owner); len(records) != 1 {
		t.Fatalf("close records = %d, want 1", len(records))
	}
	if records := r.closeConnection(owner); len(records) != 0 {
		t.Fatalf("second close records = %d, want 0", len(records))
	}
	if r.ownerForTarget("page-1") != nil {
		t.Fatal("closed connection still owns target")
	}
}

func TestOwnershipCancelsLateAttachAfterDisconnect(t *testing.T) {
	r := newOwnershipRegistry()
	owner := &cdp.Connection{}
	generation := r.beginCreate(owner)
	r.closeConnection(owner)
	if record := r.claimTarget(owner, "late-page", generation); record != nil {
		t.Fatal("late claim should not own an existing target")
	}
	record := r.registerPair(nil, &targetPair{pageTargetID: "late-page", pageSessionID: "late-session"})
	_, _, cancelled := r.recordDetails(record)
	if !cancelled {
		t.Fatal("late backend attach was not cancelled")
	}
}
