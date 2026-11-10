package admission

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func engine(t *testing.T, limit, queue, wait int) *Engine {
	t.Helper()
	c := DefaultConfig()
	c.Defaults = Policy{MaxConcurrency: limit, MaxQueue: queue, WaitTimeoutMS: wait}
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func mustAcquire(t *testing.T, e *Engine, id, account string) {
	t.Helper()
	if err := e.Acquire(context.Background(), id, account); err != nil {
		t.Fatal(err)
	}
}
func waitQueued(t *testing.T, e *Engine, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.Snapshot(nil).Waiting == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue did not reach %d: %+v", n, e.Snapshot(nil))
}

func TestExactLimitAndPerAccountIsolation(t *testing.T) {
	e := engine(t, 10, 0, 100)
	for i := 0; i < 10; i++ {
		mustAcquire(t, e, fmt.Sprint(i), "a")
	}
	if err := e.Acquire(context.Background(), "overflow", "a"); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	mustAcquire(t, e, "other", "b")
	e.Complete("0")
	e.Complete("0")
	mustAcquire(t, e, "replacement", "a")
	if e.Snapshot([]string{"a"}).Accounts[0].InFlight != 10 {
		t.Fatal(e.Snapshot(nil))
	}
}

func TestFIFOAndNoBarging(t *testing.T) {
	e := engine(t, 1, 3, 2000)
	mustAcquire(t, e, "active", "a")
	done := make(chan string, 3)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprint(i)
		go func() {
			if err := e.Acquire(context.Background(), id, "a"); err != nil {
				done <- "error"
			} else {
				done <- id
			}
		}()
		waitQueued(t, e, i)
	}
	if err := e.Acquire(context.Background(), "barger", "a"); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	e.Complete("active")
	for i := 1; i <= 3; i++ {
		select {
		case id := <-done:
			if id != fmt.Sprint(i) {
				t.Fatalf("order=%s want %d", id, i)
			}
			e.Complete(id)
		case <-time.After(3 * time.Second):
			t.Fatal("queue stuck")
		}
	}
}

func TestQueueCancellationAndCompletion(t *testing.T) {
	for _, cancelViaCompletion := range []bool{false, true} {
		e := engine(t, 1, 2, 1000)
		mustAcquire(t, e, "active", "a")
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- e.Acquire(ctx, "queued", "a") }()
		waitQueued(t, e, 1)
		if cancelViaCompletion {
			e.Complete("queued")
		} else {
			cancel()
		}
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("canceled waiter admitted")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation stuck")
		}
		cancel()
		waitQueued(t, e, 0)
		if e.Snapshot(nil).Accounts[0].InFlight != 1 {
			t.Fatal("released live request")
		}
		e.Complete("active")
		mustAcquire(t, e, "next", "a")
	}
}

func TestTimeoutDoesNotLeak(t *testing.T) {
	e := engine(t, 1, 1, 20)
	mustAcquire(t, e, "active", "a")
	if err := e.Acquire(context.Background(), "wait", "a"); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	waitQueued(t, e, 0)
	e.Complete("active")
	mustAcquire(t, e, "next", "a")
}

func TestDiagnosticsRecordQueueTimeoutAndCompletion(t *testing.T) {
	e := engine(t, 1, 1, 20)
	mustAcquire(t, e, "active", "account")
	if err := e.Acquire(context.Background(), "wait", "account"); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	e.Complete("active")
	s := e.Snapshot(nil)
	if s.Counters.TimedOut != 1 || s.Counters.Completed != 1 || s.Counters.Enqueued != 1 {
		t.Fatalf("unexpected counters: %+v", s.Counters)
	}
	if len(s.RecentEvents) < 3 {
		t.Fatalf("expected lifecycle events: %+v", s.RecentEvents)
	}
	for _, event := range s.RecentEvents {
		if event.AccountTag == "account" || event.RequestTag == "active" || event.RequestTag == "wait" {
			t.Fatalf("diagnostic event leaked raw identifier: %+v", event)
		}
	}
}

func TestDiagnosticsRecordOrphanCompletion(t *testing.T) {
	e := engine(t, 1, 0, 100)
	e.Complete("unknown")
	s := e.Snapshot(nil)
	if s.Counters.OrphanedComplete != 1 {
		t.Fatalf("unexpected counters: %+v", s.Counters)
	}
	if len(s.RecentEvents) != 1 || s.RecentEvents[0].Kind != "orphaned_complete" {
		t.Fatalf("unexpected events: %+v", s.RecentEvents)
	}
}

func TestDuplicateAndFailover(t *testing.T) {
	e := engine(t, 1, 0, 100)
	mustAcquire(t, e, "r", "a")
	mustAcquire(t, e, "r", "a")
	mustAcquire(t, e, "r", "b")
	s := e.Snapshot([]string{"a", "b"})
	if s.Accounts[0].InFlight != 0 || s.Accounts[1].InFlight != 1 {
		t.Fatal(s)
	}
	e.Complete("r")
	e.Complete("r")
	if err := e.Acquire(context.Background(), "r", "a"); !errors.Is(err, ErrCompleted) {
		t.Fatal(err)
	}
}

func TestPolicyUpdateDoesNotResetActiveSlots(t *testing.T) {
	e := engine(t, 3, 2, 1000)
	for _, id := range []string{"1", "2", "3"} {
		mustAcquire(t, e, id, "a")
	}
	c := DefaultConfig()
	c.Defaults = Policy{MaxConcurrency: 1, MaxQueue: 2, WaitTimeoutMS: 1000}
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- e.Acquire(context.Background(), "4", "a") }()
	waitQueued(t, e, 1)
	e.Complete("1")
	e.Complete("2")
	select {
	case <-result:
		t.Fatal("admitted before usage below new limit")
	case <-time.After(10 * time.Millisecond):
	}
	e.Complete("3")
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestPauseRejectsWaitersButDoesNotKillActive(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	mustAcquire(t, e, "active", "a")
	result := make(chan error, 1)
	go func() { result <- e.Acquire(context.Background(), "wait", "a") }()
	waitQueued(t, e, 1)
	c := DefaultConfig()
	c.Accounts["a"] = Policy{MaxConcurrency: 1, MaxQueue: 2, WaitTimeoutMS: 1000, Paused: true}
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrPaused) {
		t.Fatal(err)
	}
	if e.Snapshot(nil).Accounts[0].InFlight != 1 {
		t.Fatal("active request was killed")
	}
}

func TestRepeatedEmptyDrainResumeDoesNotLockOutFirstRequest(t *testing.T) {
	e := engine(t, 1, 0, 100)
	for i := 0; i < 100; i++ {
		if err := e.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := e.Resume(); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprint(i)
		mustAcquire(t, e, id, "a")
		e.Complete(id)
	}
}

func TestFailedDrainCannotBlindlyClearActiveAccounting(t *testing.T) {
	e := engine(t, 1, 1, 1000)
	mustAcquire(t, e, "active", "a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := e.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := e.Resume(); !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
	e.Complete("active")
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, e, "next", "a")
}

// tiers mirrors the production shape behind the 60 s 503s: one Team account
// alone in the top tier and a roomier ProLite account one tier below.
var tiers = []Candidate{{ID: "team", Priority: 0}, {ID: "prolite", Priority: -10}}

func tieredEngine(t *testing.T, behavior string) *Engine {
	t.Helper()
	c := DefaultConfig()
	c.PinnedFullBehavior = behavior
	c.Defaults = Policy{MaxConcurrency: 1, MaxQueue: 2, WaitTimeoutMS: 1000}
	c.Accounts["prolite"] = Policy{MaxConcurrency: 3, MaxQueue: 3, WaitTimeoutMS: 1000}
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// mustChoose makes one scheduler pick under the host selection ID sel.
func mustChoose(t *testing.T, e *Engine, candidates []Candidate, sel, want string) {
	t.Helper()
	if id, err := e.Choose(candidates, sel); err != nil || id != want {
		t.Fatalf("chose %q err=%v, want %q", id, err, want)
	}
}

// route performs what the host does for one request: pick, then admit on the
// picked account under the same selection ID.
func route(t *testing.T, e *Engine, candidates []Candidate, id, want string) {
	t.Helper()
	mustChoose(t, e, candidates, id, want)
	if err := e.AcquirePicked(context.Background(), id, want, id); err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerPrefersFreeAccountInSameTier(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	same := []Candidate{{ID: "a"}, {ID: "b"}}
	route(t, e, same, "first", "a")
	route(t, e, same, "second", "b")
}

func TestSchedulerQueueModeStaysInHighestTier(t *testing.T) {
	e := tieredEngine(t, "queue")
	route(t, e, tiers, "active", "team")
	mustChoose(t, e, tiers, "next", "team")
	s := e.Snapshot(nil)
	if s.Counters.NoFreeAccount != 1 || s.Counters.PriorityOverflow != 0 {
		t.Fatalf("unexpected diagnostics: %+v", s.Counters)
	}
}

func TestSchedulerFallbackOverflowsToLowerTier(t *testing.T) {
	e := tieredEngine(t, "fallback")
	route(t, e, tiers, "active", "team")
	route(t, e, tiers, "overflow", "prolite")
	s := e.Snapshot(nil)
	if s.Counters.PriorityOverflow != 1 || s.Counters.Enqueued != 0 {
		t.Fatalf("unexpected diagnostics: %+v", s.Counters)
	}
	last := s.RecentEvents[len(s.RecentEvents)-2]
	if last.Kind != "priority_overflow" || last.AccountTag != tag("prolite") {
		t.Fatalf("unexpected overflow event: %+v", s.RecentEvents)
	}
	// The top tier is preferred again as soon as it has room.
	e.Complete("active")
	route(t, e, tiers, "back", "team")
}

func TestSchedulerFallbackQueuesWhenEveryTierIsFull(t *testing.T) {
	e := tieredEngine(t, "fallback")
	route(t, e, tiers, "team-active", "team")
	for i := 0; i < 3; i++ {
		route(t, e, tiers, fmt.Sprintf("prolite-%d", i), "prolite")
	}
	// ProLite has three slots, so one waiter there is a shorter relative
	// queue than one waiter behind Team's single slot.
	mustChoose(t, e, tiers, "next", "prolite")
	if s := e.Snapshot(nil); s.Counters.NoFreeAccount != 1 {
		t.Fatalf("unexpected diagnostics: %+v", s.Counters)
	}
}

func TestSchedulerFallbackSkipsPausedAndQueuesOnlyCandidate(t *testing.T) {
	e := tieredEngine(t, "fallback")
	c := e.cfg.Clone()
	c.Accounts["prolite"] = Policy{MaxConcurrency: 3, MaxQueue: 3, WaitTimeoutMS: 1000, Paused: true}
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	route(t, e, tiers, "active", "team")
	mustChoose(t, e, tiers, "", "team")
	// With only the full account offered, as during a cooldown of the rest,
	// the request still queues there rather than being rejected.
	mustChoose(t, e, tiers[:1], "", "team")
}

func TestSchedulerRejectsWhenNoCandidateCanQueue(t *testing.T) {
	e := engine(t, 1, 0, 1000)
	only := []Candidate{{ID: "a"}}
	route(t, e, only, "active", "a")
	if _, err := e.Choose(only, "next"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v", err)
	}
	if _, err := e.Choose(nil, "none"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v", err)
	}
}

// Picks arrive before their admissions when requests are concurrent. Without
// remembering them, every pick would see the same free Team slot.
func TestSchedulerConcurrentPicksDoNotShareOneFreeSlot(t *testing.T) {
	e := tieredEngine(t, "fallback")
	mustChoose(t, e, tiers, "first", "team")
	mustChoose(t, e, tiers, "second", "prolite")
	for id, account := range map[string]string{"first": "team", "second": "prolite"} {
		if err := e.AcquirePicked(context.Background(), id, account, id); err != nil {
			t.Fatal(err)
		}
	}
	s := e.Snapshot(nil)
	if s.Counters.Enqueued != 0 || s.Counters.Admitted != 2 {
		t.Fatalf("picks collided: %+v", s.Counters)
	}
	// Admission ended both reservations: Team is offered again once free.
	e.Complete("first")
	mustChoose(t, e, tiers, "third", "team")
}

// An admission on one account leaves another account's reservation alone, so
// that slot is still held for its own pick.
func TestSchedulerReservationSurvivesAdmissionElsewhere(t *testing.T) {
	e := tieredEngine(t, "fallback")
	mustChoose(t, e, tiers, "picked", "team")
	if err := e.AcquirePicked(context.Background(), "other", "prolite", "unrelated"); err != nil {
		t.Fatal(err)
	}
	mustChoose(t, e, tiers, "later", "prolite")
}

// A host that does not repeat the selection ID at admission must not leave
// reservations behind: the next request would be diverted for no reason.
func TestSchedulerReservationReleasedWithoutSelectionID(t *testing.T) {
	e := tieredEngine(t, "fallback")
	mustChoose(t, e, tiers, "picked", "team")
	mustAcquire(t, e, "first", "team")
	e.Complete("first")
	mustChoose(t, e, tiers, "next", "team")
}

func TestSchedulerForgetsPickWhoseRequestNeverArrives(t *testing.T) {
	e := tieredEngine(t, "queue")
	mustChoose(t, e, tiers, "lost", "team")
	mustChoose(t, e, tiers, "next", "team")
	if s := e.Snapshot(nil); s.Counters.NoFreeAccount != 1 {
		t.Fatalf("second pick should have seen the first: %+v", s.Counters)
	}
	// Neither request arrives; let both reservations run out.
	e.mu.Lock()
	for id, r := range e.reserved {
		r.expires = time.Now().Add(-time.Second)
		e.reserved[id] = r
	}
	e.mu.Unlock()
	mustChoose(t, e, tiers, "after", "team")
	if s := e.Snapshot(nil); s.Counters.NoFreeAccount != 1 {
		t.Fatalf("expired picks still counted: %+v", s.Counters)
	}
	// A reservation never blocks admission itself.
	mustAcquire(t, e, "real", "team")
}

func TestConcurrentAccounting(t *testing.T) {
	e := engine(t, 3, 200, 10000)
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			if err := e.Acquire(context.Background(), id, "a"); err != nil {
				t.Error(err)
				return
			}
			n := running.Add(1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
			e.Complete(id)
		}(i)
	}
	wg.Wait()
	if peak.Load() > 3 {
		t.Fatalf("oversubscribed: %d", peak.Load())
	}
	s := e.Snapshot(nil)
	for _, a := range s.Accounts {
		if a.InFlight != 0 || a.Queued != 0 {
			t.Fatal(s)
		}
	}
}

func TestStoreRoundTripAndValidation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state", "policies.json")
	c := DefaultConfig()
	c.Accounts["a"] = Policy{MaxConcurrency: 7, MaxQueue: 0, WaitTimeoutMS: 100}
	if err := Save(p, c); err != nil {
		t.Fatal(err)
	}
	got, err := Load(p, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got.Policy("a").MaxConcurrency != 7 {
		t.Fatal(got)
	}
	bad := c.Clone()
	bad.Defaults.MaxConcurrency = 0
	if err := Save(p, bad); err == nil {
		t.Fatal("accepted zero concurrency")
	}
	got, err = Load(p, DefaultConfig())
	if err != nil || got.Policy("a").MaxConcurrency != 7 {
		t.Fatal("invalid update changed file")
	}
	if _, err := DecodeConfig([]byte(`{"version":1,"unknown":true}`)); err == nil {
		t.Fatal("accepted unknown fields")
	}
}

func TestIndependentAccountOverridesAndInheritance(t *testing.T) {
	c := DefaultConfig()
	c.Defaults = Policy{MaxConcurrency: 2, MaxQueue: 0, WaitTimeoutMS: 100}
	c.Accounts["a"] = Policy{MaxConcurrency: 1, MaxQueue: 0, WaitTimeoutMS: 100}
	c.Accounts["b"] = Policy{MaxConcurrency: 3, MaxQueue: 0, WaitTimeoutMS: 100}
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for id, limit := range map[string]int{"a": 1, "b": 3, "inherits": 2} {
		for i := 0; i < limit; i++ {
			mustAcquire(t, e, fmt.Sprintf("%s-%d", id, i), id)
		}
		if err := e.Acquire(context.Background(), id+"-overflow", id); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("%s: %v", id, err)
		}
	}
}

func TestConcurrentDuplicateWaitersOnlyConsumeOneSlot(t *testing.T) {
	e := engine(t, 1, 3, 1000)
	mustAcquire(t, e, "active", "a")
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- e.Acquire(context.Background(), "same", "a") }()
	}
	waitQueued(t, e, 1)
	e.Complete("active")
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	s := e.Snapshot(nil)
	if s.Accounts[0].InFlight != 1 || s.Waiting != 0 {
		t.Fatal(s)
	}
	e.Complete("same")
}

func TestDrainRejectsQueuedRequestsWithoutReleasingActive(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	mustAcquire(t, e, "active", "a")
	result := make(chan error, 1)
	go func() { result <- e.Acquire(context.Background(), "wait", "a") }()
	waitQueued(t, e, 1)
	e.BeginDrain()
	if err := <-result; !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
	s := e.Snapshot(nil)
	if !s.Draining || s.Waiting != 0 || s.Accounts[0].InFlight != 1 {
		t.Fatal(s)
	}
	e.Complete("active")
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, e, "new", "a")
}
