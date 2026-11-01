package admission

import (
	"context"
	"errors"
	"sort"
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

// Engine owns all local accounting. It never expires an active execution slot:
// only request.complete may prove that an upstream request has ended.
type Engine struct {
	mu        sync.Mutex
	cfg       Config
	accounts  map[string]*account
	requests  map[string]*request
	completed map[string]time.Time
	waiting   int
	draining  bool
	changed   chan struct{}
}

func New(c Config) (*Engine, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Engine{cfg: c.Clone(), accounts: map[string]*account{}, requests: map[string]*request{}, completed: map[string]time.Time{}, changed: make(chan struct{})}, nil
}

func (e *Engine) notifyLocked() { close(e.changed); e.changed = make(chan struct{}) }

func (e *Engine) accountLocked(id string) *account {
	a := e.accounts[id]
	if a == nil {
		a = &account{}
		e.accounts[id] = a
	}
	return a
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
	now := time.Now()
	e.pruneLocked(now)
	if _, ok := e.completed[id]; ok {
		e.mu.Unlock()
		return ErrCompleted
	}
	if e.draining {
		e.mu.Unlock()
		return ErrDraining
	}
	p := e.cfg.Policy(accountID)
	if p.Paused {
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
			close(r.ready)
			e.notifyLocked()
			e.mu.Unlock()
			return nil
		}
		if len(a.queue) >= p.MaxQueue {
			e.mu.Unlock()
			return ErrQueueFull
		}
		if e.waiting >= maxWaiting {
			e.mu.Unlock()
			return ErrGlobalQueue
		}
		a.queue = append(a.queue, r)
		e.waiting++
		e.requests[id] = r
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
		e.removeLocked(r)
		r.err = ErrCompleted
		select {
		case <-r.ready:
		default:
			close(r.ready)
		}
		e.dispatchLocked(r.account)
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
	ID           string `json:"id"`
	Policy       Policy `json:"policy"`
	Override     bool   `json:"override"`
	InFlight     int    `json:"in_flight"`
	Queued       int    `json:"queued"`
	OldestWaitMS int64  `json:"oldest_wait_ms"`
}

type Snapshot struct {
	Draining         bool              `json:"draining"`
	Waiting          int               `json:"waiting"`
	GlobalQueueLimit int               `json:"global_queue_limit"`
	Accounts         []AccountSnapshot `json:"accounts"`
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
	out := Snapshot{Draining: e.draining, Waiting: e.waiting, GlobalQueueLimit: maxWaiting, Accounts: []AccountSnapshot{}}
	for id := range set {
		s := AccountSnapshot{ID: id, Policy: e.cfg.Policy(id)}
		_, s.Override = e.cfg.Accounts[id]
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

// Choose prefers immediately executable accounts; otherwise it selects the
// shortest normalized waiting load. A pinned candidate must not silently fail
// over because it is full. It can still queue in Acquire.
func (e *Engine) Choose(ids []string, pinned string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.draining {
		return "", ErrDraining
	}
	best := ""
	bestScore := int(^uint(0) >> 1)
	for _, id := range ids {
		if pinned != "" && id != pinned {
			continue
		}
		p := e.cfg.Policy(id)
		if p.Paused {
			continue
		}
		a := e.accounts[id]
		active, queued := 0, 0
		if a != nil {
			active = a.active
			queued = len(a.queue)
		}
		score := active * 1000 / p.MaxConcurrency
		if active >= p.MaxConcurrency || queued > 0 {
			if queued >= p.MaxQueue {
				continue
			}
			score = 1000000 + (queued+1)*1000/p.MaxConcurrency
		}
		if score < bestScore {
			best, bestScore = id, score
		}
	}
	if best == "" {
		return "", ErrQueueFull
	}
	return best, nil
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
