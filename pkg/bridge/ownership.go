package bridge

import (
	"sync"

	"github.com/VulpineOS/foxbridge/pkg/cdp"
)

type targetLifecycle uint8

const (
	targetOpen targetLifecycle = iota
	targetClosing
	targetClosed
)

// targetRecord is the bridge's ownership boundary. Synthetic tab IDs share the
// record with their page and are never exposed by the flat target view.
type targetRecord struct {
	pageTargetID     string
	tabTargetID      string
	pageSessionID    string
	tabSessionID     string
	jugglerSessionID string
	owner            *cdp.Connection
	state            targetLifecycle
	pair             *targetPair
	cleaned          bool
	cancelled        bool
}

type connectionState struct {
	closed     bool
	discover   bool
	autoAttach bool
}

type pendingCreate struct {
	owner      *cdp.Connection
	generation uint64
}

type ownershipRegistry struct {
	mu sync.Mutex

	nextGeneration   uint64
	connections      map[*cdp.Connection]*connectionState
	targets          map[string]*targetRecord
	sessions         map[string]*targetRecord
	browserSessions  map[string]*cdp.Connection
	pending          map[uint64]*pendingCreate
	claims           map[string]*pendingCreate
	cancelledTargets map[string]bool
	requestOwners    map[string]string // backend request ID → CDP session ID
}

func newOwnershipRegistry() *ownershipRegistry {
	return &ownershipRegistry{
		connections:      make(map[*cdp.Connection]*connectionState),
		targets:          make(map[string]*targetRecord),
		sessions:         make(map[string]*targetRecord),
		browserSessions:  make(map[string]*cdp.Connection),
		pending:          make(map[uint64]*pendingCreate),
		claims:           make(map[string]*pendingCreate),
		cancelledTargets: make(map[string]bool),
		requestOwners:    make(map[string]string),
	}
}

func (r *ownershipRegistry) state(conn *cdp.Connection) *connectionState {
	if conn == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stateLocked(conn)
}

func (r *ownershipRegistry) stateLocked(conn *cdp.Connection) *connectionState {
	state := r.connections[conn]
	if state == nil {
		state = &connectionState{}
		r.connections[conn] = state
	}
	return state
}

func (r *ownershipRegistry) setDiscover(conn *cdp.Connection, enabled bool) {
	if conn == nil {
		return
	}
	r.mu.Lock()
	r.stateLocked(conn).discover = enabled
	r.mu.Unlock()
}

func (r *ownershipRegistry) discoverEnabled(conn *cdp.Connection) bool {
	if conn == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.connections[conn]
	return state != nil && !state.closed && state.discover
}

func (r *ownershipRegistry) setAutoAttach(conn *cdp.Connection, enabled bool) {
	if conn == nil {
		return
	}
	r.mu.Lock()
	r.stateLocked(conn).autoAttach = enabled
	r.mu.Unlock()
}

func (r *ownershipRegistry) connectionClosed(conn *cdp.Connection) bool {
	if conn == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connectionClosedLocked(conn)
}

func (r *ownershipRegistry) autoAttachEnabled(conn *cdp.Connection) bool {
	if conn == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.connections[conn]
	return state != nil && !state.closed && state.autoAttach
}

func (r *ownershipRegistry) beginCreate(conn *cdp.Connection) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if conn != nil {
		r.stateLocked(conn)
	}
	r.nextGeneration++
	claim := &pendingCreate{owner: conn, generation: r.nextGeneration}
	r.pending[claim.generation] = claim
	return claim.generation
}

func (r *ownershipRegistry) finishCreate(generation uint64) {
	r.mu.Lock()
	delete(r.pending, generation)
	r.mu.Unlock()
}

// claimTarget performs the second half of Target.createTarget. If the backend
// attach event arrived first, it returns the existing record for publication.
func (r *ownershipRegistry) claimTarget(conn *cdp.Connection, targetID string, generation uint64) *targetRecord {
	if targetID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	claim := r.pending[generation]
	if claim == nil {
		claim = &pendingCreate{owner: conn, generation: generation}
	}
	cancelled := r.connectionClosedLocked(claim.owner)
	if record := r.targets[targetID]; record != nil {
		if cancelled {
			record.cancelled = true
			record.state = targetClosing
		} else if record.owner == nil && record.state == targetOpen {
			record.owner = claim.owner
			r.stateLocked(record.owner)
		}
		return record
	}
	if cancelled {
		r.cancelledTargets[targetID] = true
	} else {
		r.claims[targetID] = claim
	}
	return nil
}

func (r *ownershipRegistry) ownerForTarget(targetID string) *cdp.Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.targets[targetID]
	if record == nil || record.state != targetOpen || r.connectionClosedLocked(record.owner) {
		return nil
	}
	return record.owner
}

func (r *ownershipRegistry) recordForTarget(targetID string) *targetRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.targets[targetID]
}

func (r *ownershipRegistry) recordForSession(sessionID string) *targetRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[sessionID]
}

func (r *ownershipRegistry) sessionOwned(conn *cdp.Connection, sessionID string) bool {
	if conn == nil || sessionID == "" {
		return true
	}
	return r.sessionOwner(sessionID) == conn
}

func (r *ownershipRegistry) registerPair(owner *cdp.Connection, pair *targetPair) *targetRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	cancelled := r.cancelledTargets[pair.pageTargetID]
	if claim := r.claims[pair.pageTargetID]; claim != nil {
		owner = claim.owner
		cancelled = r.connectionClosedLocked(owner)
		delete(r.claims, pair.pageTargetID)
	}
	delete(r.cancelledTargets, pair.pageTargetID)
	if owner != nil && !cancelled {
		r.stateLocked(owner)
	} else if cancelled {
		owner = nil
	}
	record := &targetRecord{
		pageTargetID:     pair.pageTargetID,
		tabTargetID:      pair.tabTargetID,
		pageSessionID:    pair.pageSessionID,
		tabSessionID:     pair.tabSessionID,
		jugglerSessionID: pair.jugglerSessionID,
		owner:            owner,
		state:            targetOpen,
		pair:             pair,
		cancelled:        cancelled,
	}
	if cancelled {
		record.state = targetClosing
	}
	r.targets[pair.pageTargetID] = record
	r.targets[pair.tabTargetID] = record
	r.sessions[pair.pageSessionID] = record
	r.sessions[pair.tabSessionID] = record
	return record
}

func (r *ownershipRegistry) registerWorker(owner *cdp.Connection, targetID, sessionID, jugglerSessionID string) *targetRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	cancelled := r.cancelledTargets[targetID]
	if claim := r.claims[targetID]; claim != nil {
		owner = claim.owner
		cancelled = r.connectionClosedLocked(owner)
		delete(r.claims, targetID)
	}
	delete(r.cancelledTargets, targetID)
	if owner != nil && !cancelled {
		r.stateLocked(owner)
	} else if cancelled {
		owner = nil
	}
	record := &targetRecord{
		pageTargetID:     targetID,
		pageSessionID:    sessionID,
		jugglerSessionID: jugglerSessionID,
		owner:            owner,
		state:            targetOpen,
		cancelled:        cancelled,
	}
	if cancelled {
		record.state = targetClosing
	}
	r.targets[targetID] = record
	r.sessions[sessionID] = record
	return record
}

func (r *ownershipRegistry) registerBrowserSession(conn *cdp.Connection, sessionID string) {
	if conn == nil || sessionID == "" {
		return
	}
	r.mu.Lock()
	r.stateLocked(conn)
	r.browserSessions[sessionID] = conn
	r.mu.Unlock()
}

func (r *ownershipRegistry) sessionOwner(sessionID string) *cdp.Connection {
	if sessionID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if record := r.sessions[sessionID]; record != nil {
		if record.state == targetOpen && !r.connectionClosedLocked(record.owner) {
			return record.owner
		}
		return nil
	}
	owner := r.browserSessions[sessionID]
	if owner != nil && !r.connectionClosedLocked(owner) {
		return owner
	}
	return nil
}

func (r *ownershipRegistry) isBrowserSession(sessionID string) bool {
	r.mu.Lock()
	_, ok := r.browserSessions[sessionID]
	r.mu.Unlock()
	return ok
}

func (r *ownershipRegistry) recordDetails(record *targetRecord) (*cdp.Connection, *targetPair, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if record == nil {
		return nil, nil, false
	}
	return record.owner, record.pair, record.cancelled
}

func (r *ownershipRegistry) expireClaim(targetID string, generation uint64) *targetRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	claim := r.claims[targetID]
	if claim == nil || claim.generation != generation {
		return nil
	}
	delete(r.claims, targetID)
	r.cancelledTargets[targetID] = true
	if record := r.targets[targetID]; record != nil {
		record.cancelled = true
		record.state = targetClosing
		return record
	}
	return nil
}

func (r *ownershipRegistry) cancelTarget(targetID string) *targetRecord {
	if targetID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if claim := r.claims[targetID]; claim != nil {
		delete(r.claims, targetID)
	}
	r.cancelledTargets[targetID] = true
	if record := r.targets[targetID]; record != nil {
		record.cancelled = true
		record.state = targetClosing
		return record
	}
	return nil
}

func (r *ownershipRegistry) setRequestOwner(requestID, sessionID string) {
	if requestID == "" || sessionID == "" {
		return
	}
	r.mu.Lock()
	r.requestOwners[requestID] = sessionID
	r.mu.Unlock()
}

func (r *ownershipRegistry) requestOwned(sessionID, requestID string) bool {
	if requestID == "" || sessionID == "" {
		return false
	}
	r.mu.Lock()
	owner, known := r.requestOwners[requestID]
	r.mu.Unlock()
	return known && owner == sessionID
}

func (r *ownershipRegistry) clearRequest(requestID string) {
	if requestID == "" {
		return
	}
	r.mu.Lock()
	delete(r.requestOwners, requestID)
	r.mu.Unlock()
}

func (r *ownershipRegistry) clearRequests(sessionID string) {
	r.mu.Lock()
	for requestID, owner := range r.requestOwners {
		if owner == sessionID {
			delete(r.requestOwners, requestID)
		}
	}
	r.mu.Unlock()
}

func (r *ownershipRegistry) connectionClosedLocked(conn *cdp.Connection) bool {
	if conn == nil {
		return true
	}
	state := r.connections[conn]
	return state == nil || state.closed
}

func (r *ownershipRegistry) ownedPairs(conn *cdp.Connection) []*targetPair {
	if conn == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[*targetRecord]bool)
	var pairs []*targetPair
	for _, record := range r.targets {
		if seen[record] || record.owner != conn || record.state != targetOpen || record.pair == nil {
			continue
		}
		seen[record] = true
		pairs = append(pairs, record.pair)
	}
	return pairs
}

func (r *ownershipRegistry) beginCleanup(record *targetRecord) bool {
	if record == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if record.cleaned || record.state == targetClosed {
		return false
	}
	record.state = targetClosing
	record.cleaned = true
	return true
}

func (r *ownershipRegistry) remove(record *targetRecord) {
	if record == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.targets[record.pageTargetID] == record {
		delete(r.targets, record.pageTargetID)
	}
	if record.tabTargetID != "" && r.targets[record.tabTargetID] == record {
		delete(r.targets, record.tabTargetID)
	}
	if r.sessions[record.pageSessionID] == record {
		delete(r.sessions, record.pageSessionID)
	}
	if record.tabSessionID != "" && r.sessions[record.tabSessionID] == record {
		delete(r.sessions, record.tabSessionID)
	}
	record.state = targetClosed
}

// closeConnection marks the owner gone before cleanup so no final events can
// leak to a disconnected client. It returns the pages that still need closing.
func (r *ownershipRegistry) closeConnection(conn *cdp.Connection) []*targetRecord {
	if conn == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.stateLocked(conn)
	if state.closed {
		return nil
	}
	state.closed = true
	for generation, claim := range r.pending {
		if claim.owner == conn {
			delete(r.pending, generation)
		}
	}
	for targetID, claim := range r.claims {
		if claim.owner == conn {
			delete(r.claims, targetID)
			r.cancelledTargets[targetID] = true
		}
	}
	for sessionID, owner := range r.browserSessions {
		if owner == conn {
			delete(r.browserSessions, sessionID)
		}
	}
	var records []*targetRecord
	seen := make(map[*targetRecord]bool)
	for _, record := range r.targets {
		if record.owner == conn && !seen[record] && record.state == targetOpen {
			record.owner = nil
			record.state = targetClosing
			seen[record] = true
			records = append(records, record)
		}
	}
	return records
}
