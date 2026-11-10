package admission

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

func (e *Error) Error() string { return e.Message }

var (
	ErrQueueFull   = &Error{"account_queue_full", "account queue is full", 503}
	ErrTimeout     = &Error{"account_queue_timeout", "account queue wait timed out", 503}
	ErrPaused      = &Error{"account_paused", "account is paused", 503}
	ErrDraining    = &Error{"plugin_draining", "plugin is draining active requests", 503}
	ErrCompleted   = &Error{"request_completed", "request already completed or canceled", 409}
	ErrIdentity    = &Error{"invalid_request_identity", "request and selected account identifiers are required", 400}
	ErrGlobalQueue = &Error{"global_queue_full", "plugin global waiting limit reached", 503}
)

const maxWaiting = 1024
const maxAccounts = 4096
const maxEvents = 256
const maxSessions = 8192

// Counters are process-local diagnostic counters. They deliberately contain
// no request or credential identifiers and are reset when the CPA process
// restarts.
type Counters struct {
	Enqueued         uint64 `json:"enqueued"`
	Admitted         uint64 `json:"admitted"`
	Completed        uint64 `json:"completed"`
	Cancelled        uint64 `json:"cancelled"`
	TimedOut         uint64 `json:"timed_out"`
	QueueFull        uint64 `json:"queue_full"`
	GlobalQueueFull  uint64 `json:"global_queue_full"`
	PausedRejected   uint64 `json:"paused_rejected"`
	DrainingRejected uint64 `json:"draining_rejected"`
	OrphanedComplete uint64 `json:"orphaned_complete"`
	NoFreeAccount    uint64 `json:"no_free_account"`
	PriorityOverflow uint64 `json:"priority_overflow"`
	AffinityKept     uint64 `json:"affinity_kept"`
	AffinityMoved    uint64 `json:"affinity_moved"`
	AffinityNew      uint64 `json:"affinity_new"`
	AffinityNone     uint64 `json:"affinity_none"`
}

// Event is a redacted lifecycle event retained in the in-memory ring buffer.
// AccountTag and RequestTag are short one-way identifiers, not raw values.
type Event struct {
	At         time.Time `json:"at"`
	Kind       string    `json:"kind"`
	AccountTag string    `json:"account_tag,omitempty"`
	RequestTag string    `json:"request_tag,omitempty"`
	WaitMS     int64     `json:"wait_ms,omitempty"`
	InFlight   int       `json:"in_flight"`
	Queued     int       `json:"queued"`
	Detail     string    `json:"detail,omitempty"`
}

type request struct {
	id, account        string
	ready              chan struct{}
	active             bool
	err                error
	queuedAt, deadline time.Time
}

type account struct {
	active int
	queue  []*request
}

// reservation is a scheduler pick whose admission has not arrived yet. note
// describes the pick and is appended to the admission event it leads to.
type reservation struct {
	account string
	expires time.Time
	note    string
}

// binding ties a session to the account its latest request was sent to.
type binding struct {
	account string
	expires time.Time
}

// Engine owns all local accounting. It never expires an active execution slot:
// only request.complete may prove that an upstream request has ended.
type Engine struct {
	mu           sync.Mutex
	cfg          Config
	accounts     map[string]*account
	accountStats map[string]*Counters
	requests     map[string]*request
	completed    map[string]time.Time
	counters     Counters
	events       []Event
	eventSink    func(Event)
	// reserved maps a host selection ID to its pick until the matching
	// admission arrives or the entry expires. It only steers later picks away
	// from a slot that is about to be taken; admission never consults it.
	reserved       map[string]reservation
	reservationTTL time.Duration
	// sessions maps an opaque session key to its binding. A binding lapses
	// once the session has been idle for sessionTTL.
	sessions   map[string]binding
	sessionTTL time.Duration
	waiting    int
	draining   bool
	changed    chan struct{}
}

func New(c Config) (*Engine, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Engine{cfg: c.Clone(), accounts: map[string]*account{}, accountStats: map[string]*Counters{}, reserved: map[string]reservation{}, reservationTTL: 5 * time.Second, sessions: map[string]binding{}, sessionTTL: time.Hour, requests: map[string]*request{}, completed: map[string]time.Time{}, changed: make(chan struct{}), events: make([]Event, 0, maxEvents)}, nil
}

func (e *Engine) notifyLocked() { close(e.changed); e.changed = make(chan struct{}) }

// SetEventSink installs a non-blocking observer for lifecycle events. The
// callback runs while the engine lock is held and must never perform blocking
// work; the plugin runtime uses a bounded channel to persist events later.
func (e *Engine) SetEventSink(sink func(Event)) {
	e.mu.Lock()
	e.eventSink = sink
	e.mu.Unlock()
}

func (e *Engine) accountLocked(id string) *account {
	a := e.accounts[id]
	if a == nil {
		a = &account{}
		e.accounts[id] = a
	}
	return a
}

func tag(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:12]
}

func (e *Engine) statsLocked(accountID string) *Counters {
	s := e.accountStats[accountID]
	if s == nil {
		s = &Counters{}
		e.accountStats[accountID] = s
	}
	return s
}

func (e *Engine) recordLocked(kind, requestID, accountID string, waitMS int64, detail string) {
	a := e.accounts[accountID]
	active, queued := 0, 0
	if a != nil {
		active, queued = a.active, len(a.queue)
	}
	event := Event{At: time.Now().UTC(), Kind: kind, WaitMS: waitMS, InFlight: active, Queued: queued, Detail: detail}
	if accountID != "" {
		event.AccountTag = tag(accountID)
	}
	if requestID != "" {
		event.RequestTag = tag(requestID)
	}
	e.events = append(e.events, event)
	if len(e.events) > maxEvents {
		copy(e.events, e.events[len(e.events)-maxEvents:])
		e.events = e.events[:maxEvents]
	}
	if e.eventSink != nil {
		e.eventSink(event)
	}
}

// RecordEvent adds a lifecycle event that is not tied to an admission request,
// such as plugin startup or shutdown. It is also sent to the persistence sink.
func (e *Engine) RecordEvent(kind, detail string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordLocked(kind, "", "", 0, detail)
}

// reservedLocked drops expired picks and counts the live ones per account.
// Expiry keeps a pick whose request never arrives from making its account
// look busy for long.
func (e *Engine) reservedLocked(now time.Time) map[string]int {
	counts := map[string]int{}
	for id, r := range e.reserved {
		if !now.Before(r.expires) {
			delete(e.reserved, id)
			continue
		}
		counts[r.account]++
	}
	return counts
}

// sessionsLocked drops lapsed bindings and counts the live ones per account.
func (e *Engine) sessionsLocked(now time.Time) map[string]int {
	counts := map[string]int{}
	for key, b := range e.sessions {
		if !now.Before(b.expires) {
			delete(e.sessions, key)
			continue
		}
		counts[b.account]++
	}
	return counts
}

// bindLocked records that the session's latest request went to the account.
// At capacity the binding closest to lapsing makes room.
func (e *Engine) bindLocked(session, accountID string, now time.Time) {
	e.sessions[session] = binding{account: accountID, expires: now.Add(e.sessionTTL)}
	if len(e.sessions) <= maxSessions {
		return
	}
	oldest, found := "", false
	for key, b := range e.sessions {
		if !found || b.expires.Before(e.sessions[oldest].expires) {
			oldest, found = key, true
		}
	}
	delete(e.sessions, oldest)
}

// releaseReservationLocked ends the reservation of the pick whose admission
// just arrived. A host that does not repeat the selection ID at admission
// still frees one pick for the account, so reservations cannot pile up. A
// known ID that is already gone is a repeated callback and frees nothing.
func (e *Engine) releaseReservationLocked(selectionID, accountID string) {
	if selectionID != "" {
		delete(e.reserved, selectionID)
		return
	}
	oldest, found := "", false
	for id, r := range e.reserved {
		if r.account == accountID && (!found || r.expires.Before(e.reserved[oldest].expires)) {
			oldest, found = id, true
		}
	}
	if found {
		delete(e.reserved, oldest)
	}
}

func (e *Engine) pruneLocked(now time.Time) {
	for id, t := range e.completed {
		if now.After(t) {
			delete(e.completed, id)
		}
	}
	// Tombstones are bounded and short-lived, unlike active execution records.
	if len(e.completed) >= 8192 {
		var oldest string
		var ts time.Time
		for id, t := range e.completed {
			if oldest == "" || t.Before(ts) {
				oldest, ts = id, t
			}
		}
		delete(e.completed, oldest)
	}
	for id, a := range e.accounts {
		if a.active == 0 && len(a.queue) == 0 {
			delete(e.accounts, id)
		}
	}
}

func (e *Engine) removeLocked(r *request) {
	a := e.accounts[r.account]
	if a == nil {
		return
	}
	if r.active {
		a.active--
		r.active = false
	} else {
		for i, v := range a.queue {
			if v == r {
				a.queue = append(a.queue[:i], a.queue[i+1:]...)
				e.waiting--
				break
			}
		}
	}
	delete(e.requests, r.id)
}

func (e *Engine) dispatchLocked(id string) {
	a := e.accounts[id]
	if a == nil {
		return
	}
	p := e.cfg.Policy(id)
	for len(a.queue) > 0 {
		r := a.queue[0]
		var err error
		if e.draining {
			err = ErrDraining
		} else if p.Paused {
			err = ErrPaused
		} else if !time.Now().Before(r.deadline) {
			err = ErrTimeout
		}
		if err != nil {
			e.removeLocked(r)
			r.err = err
			s := e.statsLocked(r.account)
			switch err {
			case ErrTimeout:
				e.counters.TimedOut++
				s.TimedOut++
			case ErrPaused:
				e.counters.PausedRejected++
				s.PausedRejected++
			case ErrDraining:
				e.counters.DrainingRejected++
				s.DrainingRejected++
			}
			waitMS := time.Since(r.queuedAt).Milliseconds()
			e.recordLocked(err.(*Error).Code, r.id, r.account, waitMS, "queued request released")
			close(r.ready)
			continue
		}
		if a.active >= p.MaxConcurrency {
			break
		}
		a.queue = a.queue[1:]
		e.waiting--
		a.active++
		r.active = true
		close(r.ready)
	}
}

// Acquire is idempotent for an already admitted request. A retry on a different
// account releases the previous slot before entering the new account's FIFO.
func (e *Engine) Acquire(ctx context.Context, id, accountID string) error {
	return e.AcquirePicked(ctx, id, accountID, "")
}

// AcquirePicked is Acquire for a request whose account came from Choose under
// selectionID. The pick's reservation ends in the same locked step as
// admission, so the slot is never counted twice nor briefly seen as free.
func (e *Engine) AcquirePicked(ctx context.Context, id, accountID, selectionID string) error {
	if id == "" || accountID == "" || len(id) > 512 || len(accountID) > 512 {
		return ErrIdentity
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	note := e.reserved[selectionID].note
	e.releaseReservationLocked(selectionID, accountID)
	now := time.Now()
	e.pruneLocked(now)
	if _, ok := e.completed[id]; ok {
		e.mu.Unlock()
		return ErrCompleted
	}
	if e.draining {
		e.counters.DrainingRejected++
		e.statsLocked(accountID).DrainingRejected++
		e.recordLocked(ErrDraining.Code, id, accountID, 0, "request rejected before queue")
		e.mu.Unlock()
		return ErrDraining
	}
	p := e.cfg.Policy(accountID)
	if p.Paused {
		e.counters.PausedRejected++
		e.statsLocked(accountID).PausedRejected++
		e.recordLocked(ErrPaused.Code, id, accountID, 0, "request rejected before queue")
		e.mu.Unlock()
		return ErrPaused
	}
	r := e.requests[id]
	if r != nil && r.account != accountID {
		old := r.account
		e.removeLocked(r)
		if r.err == nil {
			r.err = ErrCompleted
		}
		select {
		case <-r.ready:
		default:
			close(r.ready)
		}
		e.dispatchLocked(old)
		r = nil
	}
	if r != nil && r.active {
		e.mu.Unlock()
		return nil
	}
	if r == nil {
		if _, ok := e.accounts[accountID]; !ok && len(e.accounts) >= maxAccounts {
			e.mu.Unlock()
			return ErrGlobalQueue
		}
		a := e.accountLocked(accountID)
		e.dispatchLocked(accountID)
		r = &request{id: id, account: accountID, ready: make(chan struct{}), queuedAt: now, deadline: now.Add(p.Wait())}
		if a.active < p.MaxConcurrency && len(a.queue) == 0 {
			r.active = true
			a.active++
			e.requests[id] = r
			e.counters.Admitted++
			e.statsLocked(accountID).Admitted++
			e.recordLocked("admitted", id, accountID, 0, "immediate"+note)
			close(r.ready)
			e.notifyLocked()
			e.mu.Unlock()
			return nil
		}
		if len(a.queue) >= p.MaxQueue {
			e.counters.QueueFull++
			e.statsLocked(accountID).QueueFull++
			e.recordLocked(ErrQueueFull.Code, id, accountID, 0, "account queue full")
			e.mu.Unlock()
			return ErrQueueFull
		}
		if e.waiting >= maxWaiting {
			e.counters.GlobalQueueFull++
			e.statsLocked(accountID).GlobalQueueFull++
			e.recordLocked(ErrGlobalQueue.Code, id, accountID, 0, "global queue full")
			e.mu.Unlock()
			return ErrGlobalQueue
		}
		a.queue = append(a.queue, r)
		e.waiting++
		e.requests[id] = r
		e.counters.Enqueued++
		e.statsLocked(accountID).Enqueued++
		e.recordLocked("enqueued", id, accountID, 0, "waiting for account slot"+note)
		e.notifyLocked()
	}
	deadline := r.deadline
	e.mu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var waitErr error
	select {
	case <-ctx.Done():
		waitErr = ctx.Err()
	case <-timer.C:
		waitErr = ErrTimeout
	case <-r.ready:
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	// Never run a request whose cancellation won the race with admission.
	if err := ctx.Err(); err != nil {
		waitErr = err
	}
	if waitErr != nil {
		if e.requests[id] == r {
			e.removeLocked(r)
			r.err = waitErr
			if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
				e.counters.Cancelled++
				e.statsLocked(accountID).Cancelled++
				e.recordLocked("request_cancelled", id, accountID, time.Since(r.queuedAt).Milliseconds(), "wait context ended")
			} else if errors.Is(waitErr, ErrTimeout) {
				e.counters.TimedOut++
				e.statsLocked(accountID).TimedOut++
				e.recordLocked(ErrTimeout.Code, id, accountID, time.Since(r.queuedAt).Milliseconds(), "wait timeout")
			}
			select {
			case <-r.ready:
			default:
				close(r.ready)
			}
			e.dispatchLocked(accountID)
			e.notifyLocked()
		}
		return waitErr
	}
	if !r.active || e.requests[id] != r {
		return ErrCompleted
	}
	e.counters.Admitted++
	e.statsLocked(accountID).Admitted++
	e.recordLocked("admitted", id, accountID, time.Since(r.queuedAt).Milliseconds(), "released from queue")
	return nil
}

func (e *Engine) Complete(id string) {
	if id == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneLocked(time.Now())
	e.completed[id] = time.Now().Add(2 * time.Minute)
	if r := e.requests[id]; r != nil {
		wasActive := r.active
		e.removeLocked(r)
		r.err = ErrCompleted
		if wasActive {
			e.counters.Completed++
			e.statsLocked(r.account).Completed++
			e.recordLocked("completed", id, r.account, time.Since(r.queuedAt).Milliseconds(), "host completion callback")
		} else {
			e.counters.Cancelled++
			e.statsLocked(r.account).Cancelled++
			e.recordLocked("request_cancelled", id, r.account, time.Since(r.queuedAt).Milliseconds(), "host completion while queued")
		}
		select {
		case <-r.ready:
		default:
			close(r.ready)
		}
		e.dispatchLocked(r.account)
	} else {
		e.counters.OrphanedComplete++
		e.recordLocked("orphaned_complete", id, "", 0, "completion callback had no active request")
	}
	e.notifyLocked()
}

func (e *Engine) Update(c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = c.Clone()
	if e.cfg.DisableSessionAffinity {
		clear(e.sessions)
	}
	for id := range e.accounts {
		e.dispatchLocked(id)
	}
	e.notifyLocked()
	return nil
}

// Resume never wipes active accounting. This avoids restoring availability by
// accidentally oversubscribing an upstream account after a failed reload.
func (e *Engine) Resume() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.draining && len(e.requests) > 0 {
		return ErrDraining
	}
	e.draining = false
	e.notifyLocked()
	return nil
}

func (e *Engine) BeginDrain() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.draining = true
	for id := range e.accounts {
		e.dispatchLocked(id)
	}
	e.notifyLocked()
}

func (e *Engine) Drain(ctx context.Context) error {
	e.BeginDrain()
	for {
		e.mu.Lock()
		n := len(e.requests)
		ch := e.changed
		e.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

type AccountSnapshot struct {
	ID           string   `json:"id"`
	Policy       Policy   `json:"policy"`
	Override     bool     `json:"override"`
	InFlight     int      `json:"in_flight"`
	Queued       int      `json:"queued"`
	OldestWaitMS int64    `json:"oldest_wait_ms"`
	Counters     Counters `json:"counters"`
}

type Snapshot struct {
	Draining         bool              `json:"draining"`
	Waiting          int               `json:"waiting"`
	GlobalQueueLimit int               `json:"global_queue_limit"`
	Accounts         []AccountSnapshot `json:"accounts"`
	Counters         Counters          `json:"counters"`
	RecentEvents     []Event           `json:"recent_events"`
}

func (e *Engine) Snapshot(ids []string) Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	for id := range e.accounts {
		set[id] = true
	}
	for id := range e.cfg.Accounts {
		set[id] = true
	}
	for id := range e.accountStats {
		set[id] = true
	}
	out := Snapshot{Draining: e.draining, Waiting: e.waiting, GlobalQueueLimit: maxWaiting, Accounts: []AccountSnapshot{}, Counters: e.counters, RecentEvents: append([]Event(nil), e.events...)}
	for id := range set {
		s := AccountSnapshot{ID: id, Policy: e.cfg.Policy(id)}
		_, s.Override = e.cfg.Accounts[id]
		if stats := e.accountStats[id]; stats != nil {
			s.Counters = *stats
		}
		if a := e.accounts[id]; a != nil {
			s.InFlight = a.active
			s.Queued = len(a.queue)
			if len(a.queue) > 0 {
				s.OldestWaitMS = time.Since(a.queue[0].queuedAt).Milliseconds()
			}
		}
		out.Accounts = append(out.Accounts, s)
	}
	sort.Slice(out.Accounts, func(i, j int) bool { return out.Accounts[i].ID < out.Accounts[j].ID })
	return out
}

// Candidate is an account the host offers for one selection. A larger
// Priority is preferred, matching the host's own priority tiers.
type Candidate struct {
	ID       string
	Priority int
}

// Choose picks the account one request should run on.
//
// With session affinity, a request whose session is bound to an offered
// account goes there while that account has a free slot, whatever its
// priority tier. Otherwise an account with a free slot wins: highest priority
// tier first, then least loaded, then fewest bound sessions. In "queue" mode
// only the highest offered tier is considered, as the host itself would do;
// in "fallback" mode a full tier overflows to lower tiers. When nothing is
// free the request goes to the shortest relative queue. The session is then
// bound to the chosen account, so it follows its latest request.
//
// session is an opaque key, empty when the request has no session identity.
// A pick made under a selectionID is remembered until AcquirePicked sees that
// ID, so concurrent picks do not all land on the same free slot.
func (e *Engine) Choose(candidates []Candidate, selectionID, session string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.draining {
		return "", ErrDraining
	}
	overflow := e.cfg.PinnedFullBehavior == "fallback"
	top := 0
	for i, c := range candidates {
		if i == 0 || c.Priority > top {
			top = c.Priority
		}
	}
	now := time.Now()
	reserved := e.reservedLocked(now)
	affinity := !e.cfg.DisableSessionAffinity
	var sessions map[string]int
	bound := ""
	if affinity {
		sessions = e.sessionsLocked(now)
		bound = e.sessions[session].account
	}
	best, bestFree, bestScore, considered := Candidate{}, false, 0, 0
	for _, c := range candidates {
		p := e.cfg.Policy(c.ID)
		if p.Paused {
			continue
		}
		active, queued := 0, 0
		if a := e.accounts[c.ID]; a != nil {
			active, queued = a.active, len(a.queue)
		}
		load := active + reserved[c.ID]
		free := load < p.MaxConcurrency && queued == 0
		if free && bound != "" && c.ID == bound {
			best, bestFree = c, true
			break
		}
		if !overflow && c.Priority != top {
			continue
		}
		considered++
		// Only real occupancy may rule an account out; a reserved pick is a
		// guess and must not turn into a rejection.
		if (active >= p.MaxConcurrency || queued > 0) && queued >= p.MaxQueue {
			continue
		}
		score := load * 1000 / p.MaxConcurrency
		if !free {
			score = (queued + max(load-p.MaxConcurrency, 0) + 1) * 1000 / p.MaxConcurrency
		}
		var better bool
		switch {
		case best.ID == "":
			better = true
		case free != bestFree:
			better = free
		case !free:
			// Equal queues: wait where the session already is.
			better = score < bestScore || (score == bestScore && (c.ID == bound || (best.ID != bound && c.Priority > best.Priority)))
		case c.Priority != best.Priority:
			better = c.Priority > best.Priority
		case score != bestScore:
			better = score < bestScore
		default:
			// Equally idle accounts: spread new sessions instead of always
			// starting them on the first account offered.
			better = sessions[c.ID] < sessions[best.ID]
		}
		if better {
			best, bestFree, bestScore = c, free, score
		}
	}
	if best.ID == "" {
		return "", ErrQueueFull
	}
	note := ""
	if affinity {
		switch {
		case session == "":
			e.counters.AffinityNone++
			note = "; affinity=none"
		case bound == "":
			e.counters.AffinityNew++
			note = "; affinity=new session=" + tag(session)
		case bound == best.ID:
			e.counters.AffinityKept++
			note = "; affinity=kept session=" + tag(session)
		default:
			e.counters.AffinityMoved++
			note = "; affinity=moved session=" + tag(session)
		}
		if session != "" {
			e.bindLocked(session, best.ID, now)
		}
	}
	if selectionID != "" && len(e.reserved) < maxAccounts {
		e.reserved[selectionID] = reservation{account: best.ID, expires: now.Add(e.reservationTTL), note: note}
	}
	detail := fmt.Sprintf("offered=%d considered=%d", len(candidates), considered)
	switch {
	case !bestFree:
		e.counters.NoFreeAccount++
		e.recordLocked("no_free_account", "", best.ID, 0, "no free slot at selection; "+detail)
	case best.Priority != top && best.ID != bound:
		e.counters.PriorityOverflow++
		e.recordLocked("priority_overflow", "", best.ID, 0, "higher priority accounts are full or paused; "+detail)
	}
	return best.ID, nil
}

func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return &Error{"request_canceled", "request canceled while waiting", 499}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	return &Error{"admission_internal_error", "admission failed", 503}
}
