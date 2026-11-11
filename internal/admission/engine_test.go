package admission

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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

// A drain that cannot finish must cost nothing: the engine keeps its
// accounting, keeps queueing and admitting, and can be reconfigured. This is
// what lets the host fall back to the running version when it tries to
// replace the plugin under load.
func TestDrainThatTimesOutLeavesEngineServing(t *testing.T) {
	e := engine(t, 1, 1, 1000)
	mustAcquire(t, e, "active", "a")
	queued := make(chan error, 1)
	go func() { queued <- e.Acquire(context.Background(), "queued", "a") }()
	waitQueued(t, e, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := e.Drain(ctx); !errors.Is(err, ErrBusy) {
		t.Fatalf("drain under load returned %v, want ErrBusy", err)
	}
	s := e.Snapshot(nil)
	if s.Draining || s.Waiting != 1 || s.Accounts[0].InFlight != 1 || s.Counters.DrainingRejected != 0 {
		t.Fatalf("failed drain disturbed the engine: %+v", s)
	}
	if err := e.Resume(); err != nil {
		t.Fatalf("resume after a failed drain: %v", err)
	}
	// A request arriving after the failed drain is served on a free account.
	mustAcquire(t, e, "later", "b")
	e.Complete("active")
	if err := <-queued; err != nil {
		t.Fatalf("request queued across the failed drain: %v", err)
	}
}

// Requests keep being admitted while a drain waits; none is turned away.
func TestDrainAdmitsRequestsWhileItWaits(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	mustAcquire(t, e, "active", "a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- e.Drain(ctx) }()
	time.Sleep(20 * time.Millisecond)
	mustAcquire(t, e, "during", "b")
	select {
	case err := <-drained:
		t.Fatalf("drain returned %v with requests active", err)
	case <-time.After(30 * time.Millisecond):
	}
	e.Complete("active")
	e.Complete("during")
	if err := <-drained; err != nil {
		t.Fatalf("drain of an idle engine: %v", err)
	}
	if s := e.Snapshot(nil); !s.HandedOver || s.Draining || s.Counters.DrainingRejected != 0 {
		t.Fatalf("unexpected state after drain: %+v", s)
	}
}

// Between a successful drain and the host switching to the replacement the
// host still routes here. Those requests must run: the pick is left to the
// host and admission lets them through without holding a slot, since their
// completions will be reported to the replacement, not to this version.
func TestHandedOverEngineLetsRequestsThroughUnaccounted(t *testing.T) {
	e := engine(t, 1, 0, 1000)
	if err := e.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if id, handled, err := e.Pick(peers, "sel", "s"); err != nil || handled || id != "" {
		t.Fatalf("pick after handover = %q handled=%v err=%v, want it left to the host", id, handled, err)
	}
	// More than the limit of one, on one account, with no queue allowed.
	for i := 0; i < 3; i++ {
		if err := e.AcquirePicked(context.Background(), fmt.Sprintf("r%d", i), "a", ""); err != nil {
			t.Fatalf("request %d after handover: %v", i, err)
		}
	}
	s := e.Snapshot([]string{"a"})
	if s.Accounts[0].InFlight != 0 || s.Waiting != 0 || s.Counters.HandoverPassed != 3 || s.Counters.Admitted != 0 || s.Counters.DrainingRejected != 0 {
		t.Fatalf("handover requests were accounted or rejected: %+v", s)
	}
	// The host fell back to this version after all: it limits again, and
	// holds nothing from the handover.
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, e, "back", "a")
	if err := e.Acquire(context.Background(), "over", "a"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("limit not enforced after resuming: %v", err)
	}
}

// A shutdown is not a handover: nothing more is admitted.
func TestShutdownStillRejects(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	e.BeginDrain()
	if err := e.Acquire(context.Background(), "r", "a"); !errors.Is(err, ErrDraining) {
		t.Fatalf("admission after shutdown returned %v", err)
	}
	if s := e.Snapshot(nil); !s.Draining || s.HandedOver || s.Counters.HandoverPassed != 0 {
		t.Fatalf("unexpected state after shutdown: %+v", s)
	}
}

// A pick whose admission has not arrived yet belongs to a request in flight
// inside the host, so the drain waits for it instead of stranding it.
func TestDrainWaitsForPickedRequest(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	mustChoose(t, e, peers, "sel", "a")
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if err := e.Drain(short); !errors.Is(err, ErrBusy) {
		t.Fatalf("drain with a pending pick returned %v", err)
	}
	if err := e.AcquirePicked(context.Background(), "r", "a", "sel"); err != nil {
		t.Fatalf("picked request rejected after the failed drain: %v", err)
	}
	e.Complete("r")
	if err := e.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A pick that never turns into a request lapses and stops holding a drain.
func TestDrainIgnoresLapsedPick(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	e.reservationTTL = 30 * time.Millisecond
	mustChoose(t, e, peers, "abandoned", "a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := e.Drain(ctx); err != nil {
		t.Fatalf("drain did not finish once the pick lapsed: %v", err)
	}
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

// mustChoose makes one scheduler pick under the host selection ID sel, for a
// request that carries no session identity.
func mustChoose(t *testing.T, e *Engine, candidates []Candidate, sel, want string) {
	t.Helper()
	if id, err := e.Choose(candidates, sel, ""); err != nil || id != want {
		t.Fatalf("chose %q err=%v, want %q", id, err, want)
	}
}

// turn routes one request of a session the way the host does: pick, then
// admit on the picked account. It returns the account the request ran on.
func turn(t *testing.T, e *Engine, candidates []Candidate, id, session string) string {
	t.Helper()
	account, err := e.Choose(candidates, id, session)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AcquirePicked(context.Background(), id, account, id); err != nil {
		t.Fatal(err)
	}
	return account
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
	if _, err := e.Choose(only, "next", ""); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v", err)
	}
	if _, err := e.Choose(nil, "none", ""); !errors.Is(err, ErrQueueFull) {
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

// peers is two interchangeable accounts in one priority tier.
var peers = []Candidate{{ID: "a"}, {ID: "b"}}

func TestAffinityKeepsSessionOnItsAccount(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	// Two sessions start on different accounts rather than both on the first.
	if got := turn(t, e, peers, "s1-1", "s1"); got != "a" {
		t.Fatalf("first session started on %q", got)
	}
	e.Complete("s1-1")
	if got := turn(t, e, peers, "s2-1", "s2"); got != "b" {
		t.Fatalf("second session started on %q", got)
	}
	e.Complete("s2-1")
	// Each later turn returns to its own account, in either order.
	for i := 2; i <= 4; i++ {
		for session, want := range map[string]string{"s2": "b", "s1": "a"} {
			id := fmt.Sprintf("%s-%d", session, i)
			if got := turn(t, e, peers, id, session); got != want {
				t.Fatalf("%s ran on %q, want %q", id, got, want)
			}
			e.Complete(id)
		}
	}
	c := e.Snapshot(nil).Counters
	if c.AffinityNew != 2 || c.AffinityKept != 6 || c.AffinityMoved != 0 {
		t.Fatalf("unexpected affinity counters: %+v", c)
	}
}

// Without affinity every request of a session starts on the first idle
// account; with it, sessions spread and stay put. This is the behaviour the
// option exists to switch off.
func TestAffinityDisabledIgnoresSessions(t *testing.T) {
	c := DefaultConfig()
	c.Defaults = Policy{MaxConcurrency: 2, MaxQueue: 2, WaitTimeoutMS: 1000}
	c.DisableSessionAffinity = true
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for i, session := range []string{"s1", "s2", "s1", "s2"} {
		id := fmt.Sprintf("r%d", i)
		if got := turn(t, e, peers, id, session); got != "a" {
			t.Fatalf("%s ran on %q", id, got)
		}
		e.Complete(id)
	}
	s := e.Snapshot(nil)
	if s.Counters.AffinityNew+s.Counters.AffinityKept+s.Counters.AffinityMoved+s.Counters.AffinityNone != 0 {
		t.Fatalf("affinity counted while disabled: %+v", s.Counters)
	}
}

// A session never waits behind its own account while another has room: it
// moves, and its following turns stay where it moved to.
func TestAffinityMovesWhenBoundAccountIsFull(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	if got := turn(t, e, peers, "s1-1", "s1"); got != "a" {
		t.Fatalf("session started on %q", got)
	}
	// The first turn is still running when the session sends another.
	if got := turn(t, e, peers, "s1-2", "s1"); got != "b" {
		t.Fatalf("second turn ran on %q, want the free account", got)
	}
	e.Complete("s1-1")
	e.Complete("s1-2")
	if got := turn(t, e, peers, "s1-3", "s1"); got != "b" {
		t.Fatalf("session did not stay where it moved: %q", got)
	}
	c := e.Snapshot(nil).Counters
	if c.AffinityMoved != 1 || c.AffinityKept != 1 || c.Enqueued != 0 {
		t.Fatalf("unexpected counters: %+v", c)
	}
}

// With every account full the session waits on its own account, not on an
// equally loaded stranger.
func TestAffinityQueuesOnBoundAccountWhenAllAreFull(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	if got := turn(t, e, peers, "s1-1", "s1"); got != "a" {
		t.Fatalf("s1 started on %q", got)
	}
	if got := turn(t, e, peers, "s2-1", "s2"); got != "b" {
		t.Fatalf("s2 started on %q", got)
	}
	for session, want := range map[string]string{"s2": "b", "s1": "a"} {
		if got, err := e.Choose(peers, session+"-2", session); err != nil || got != want {
			t.Fatalf("%s would wait on %q err=%v, want %q", session, got, err, want)
		}
	}
}

// Affinity outranks priority while the bound account has room, as it does in
// the host: a session that overflowed keeps its account after the top tier
// frees up, and new sessions go back to the top tier.
func TestAffinityOutranksPriorityUntilBoundAccountIsFull(t *testing.T) {
	e := tieredEngine(t, "fallback")
	if got := turn(t, e, tiers, "s1-1", "s1"); got != "team" {
		t.Fatalf("s1 started on %q", got)
	}
	if got := turn(t, e, tiers, "s2-1", "s2"); got != "prolite" {
		t.Fatalf("s2 did not overflow: %q", got)
	}
	e.Complete("s1-1")
	e.Complete("s2-1")
	if got := turn(t, e, tiers, "s2-2", "s2"); got != "prolite" {
		t.Fatalf("overflowed session left its account: %q", got)
	}
	if got := turn(t, e, tiers, "s3-1", "s3"); got != "team" {
		t.Fatalf("new session started on %q, want the top tier", got)
	}
	s := e.Snapshot(nil)
	if s.Counters.PriorityOverflow != 1 {
		t.Fatalf("a kept session was reported as an overflow: %+v", s.Counters)
	}
}

// In queue mode a new session never leaves the top tier, but a session
// already bound below it is still honoured, as the host keeps a binding
// when a higher-priority credential recovers.
func TestAffinityInQueueModeDoesNotOverflowNewSessions(t *testing.T) {
	e := tieredEngine(t, "queue")
	// Team is unavailable, so the host offers only the lower tier.
	if got := turn(t, e, tiers[1:], "s1-1", "s1"); got != "prolite" {
		t.Fatalf("s1 started on %q", got)
	}
	e.Complete("s1-1")
	// Team is back: the bound session stays, a new one takes the top tier.
	if got := turn(t, e, tiers, "s1-2", "s1"); got != "prolite" {
		t.Fatalf("bound session left its account: %q", got)
	}
	if got := turn(t, e, tiers, "s2-1", "s2"); got != "team" {
		t.Fatalf("new session started on %q", got)
	}
	// With Team now full, a further new session waits there rather than
	// overflowing to the idle lower tier.
	if got, err := e.Choose(tiers, "s3-1", "s3"); err != nil || got != "team" {
		t.Fatalf("queue mode sent a new session to %q err=%v", got, err)
	}
	if c := e.Snapshot(nil).Counters; c.PriorityOverflow != 0 {
		t.Fatalf("queue mode reported an overflow: %+v", c)
	}
}

// A binding only applies while its account is offered; a cooled-down or
// removed account must not hold the session back.
func TestAffinityIgnoresBindingToAccountNotOffered(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	if got := turn(t, e, peers, "s1-1", "s1"); got != "a" {
		t.Fatalf("session started on %q", got)
	}
	e.Complete("s1-1")
	if got := turn(t, e, peers[1:], "s1-2", "s1"); got != "b" {
		t.Fatalf("session ran on %q with its account not offered", got)
	}
	e.Complete("s1-2")
	if got := turn(t, e, peers, "s1-3", "s1"); got != "b" {
		t.Fatalf("session did not follow its latest request: %q", got)
	}
}

func TestAffinityBindingLapsesAfterIdleTTL(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	turn(t, e, peers, "s1-1", "s1")
	e.Complete("s1-1")
	if got := turn(t, e, peers, "s2-1", "s2"); got != "b" {
		t.Fatalf("s2 started on %q", got)
	}
	e.Complete("s2-1")
	// s2 goes idle for longer than the TTL; s1 stays in use.
	e.mu.Lock()
	b := e.sessions["s2"]
	b.expires = time.Now().Add(-time.Second)
	e.sessions["s2"] = b
	e.mu.Unlock()
	if got := turn(t, e, peers, "s2-2", "s2"); got != "b" {
		t.Fatalf("returning session ran on %q, want the account with fewer sessions", got)
	}
	c := e.Snapshot(nil).Counters
	if c.AffinityNew != 3 || c.AffinityKept != 0 {
		t.Fatalf("lapsed binding was treated as live: %+v", c)
	}
}

func TestAffinityRequestsWithoutSessionAreNotBound(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("r%d", i)
		if got := turn(t, e, peers, id, ""); got != "a" {
			t.Fatalf("%s ran on %q", id, got)
		}
		e.Complete(id)
	}
	e.mu.Lock()
	n := len(e.sessions)
	e.mu.Unlock()
	if c := e.Snapshot(nil).Counters; n != 0 || c.AffinityNone != 3 {
		t.Fatalf("sessions=%d counters=%+v", n, c)
	}
}

func TestAffinityBindingsAreBounded(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	for i := 0; i < maxSessions+50; i++ {
		if _, err := e.Choose(peers, "", fmt.Sprintf("s%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	n := len(e.sessions)
	_, newest := e.sessions[fmt.Sprintf("s%d", maxSessions+49)]
	e.mu.Unlock()
	if n != maxSessions || !newest {
		t.Fatalf("sessions=%d newest kept=%v", n, newest)
	}
}

// Turning affinity off drops the bindings, so turning it back on starts clean.
func TestAffinityDisablingForgetsBindings(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	turn(t, e, peers, "s1-1", "s1")
	e.Complete("s1-1")
	c := e.cfg.Clone()
	c.DisableSessionAffinity = true
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	n := len(e.sessions)
	e.mu.Unlock()
	if n != 0 {
		t.Fatalf("bindings kept after disabling: %d", n)
	}
}

// The admission event names how affinity decided, without the session itself.
func TestAffinityDecisionIsRecordedOnAdmission(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	turn(t, e, peers, "s1-1", "private-session-id")
	e.Complete("s1-1")
	turn(t, e, peers, "s1-2", "private-session-id")
	var details []string
	for _, event := range e.Snapshot(nil).RecentEvents {
		if event.Kind == "admitted" {
			details = append(details, event.Detail)
		}
		if strings.Contains(event.Detail, "private-session-id") {
			t.Fatalf("raw session identity leaked into an event: %+v", event)
		}
	}
	want := []string{"immediate; affinity=new session=" + tag("private-session-id"), "immediate; affinity=kept session=" + tag("private-session-id")}
	if fmt.Sprint(details) != fmt.Sprint(want) {
		t.Fatalf("admission details=%q want %q", details, want)
	}
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

func admissionDetails(e *Engine) []string {
	var details []string
	for _, event := range e.Snapshot(nil).RecentEvents {
		if event.Kind == "admitted" || event.Kind == "enqueued" {
			details = append(details, event.Kind+": "+event.Detail)
		}
	}
	return details
}

// Every move of a session names what kept it off the account it was bound
// to, so the diagnostics can tell a full account from a cooled-down one.
func TestAffinityMoveReasonIsRecorded(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	if got := turn(t, e, peers, "r1", "s"); got != "a" {
		t.Fatalf("session started on %q", got)
	}
	// Bound account still busy with the first turn.
	if got := turn(t, e, peers, "r2", "s"); got != "b" {
		t.Fatalf("second turn ran on %q", got)
	}
	e.Complete("r1")
	e.Complete("r2")
	// Bound account (now b) is not among the accounts the host offers.
	if got := turn(t, e, peers[:1], "r3", "s"); got != "a" {
		t.Fatalf("third turn ran on %q", got)
	}
	e.Complete("r3")
	// Bound account (now a) is paused.
	c := e.cfg.Clone()
	c.Accounts["a"] = Policy{MaxConcurrency: 1, MaxQueue: 2, WaitTimeoutMS: 1000, Paused: true}
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	if got := turn(t, e, peers, "r4", "s"); got != "b" {
		t.Fatalf("fourth turn ran on %q", got)
	}
	got := e.Snapshot(nil).Counters
	if got.AffinityMoved != 3 || got.AffinityMovedFull != 1 || got.AffinityMovedNotOffered != 1 || got.AffinityMovedPaused != 1 {
		t.Fatalf("move reasons: %+v", got)
	}
	session := " session=" + tag("s")
	want := []string{
		"admitted: immediate; affinity=new" + session,
		"admitted: immediate; affinity=moved reason=full" + session,
		"admitted: immediate; affinity=moved reason=not_offered" + session,
		"admitted: immediate; affinity=moved reason=paused" + session,
	}
	if details := admissionDetails(e); fmt.Sprint(details) != fmt.Sprint(want) {
		t.Fatalf("details=%q\nwant   %q", details, want)
	}
}

// A queued request is flagged, once, when another account it was offered
// frees up while it is still waiting behind its own.
func TestQueuedRequestNotesAnotherOfferedAccountFreeingUp(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	route(t, e, peers, "r1", "a")
	route(t, e, peers, "r2", "b")
	mustChoose(t, e, peers, "r3", "a")
	done := make(chan error, 1)
	go func() { done <- e.AcquirePicked(context.Background(), "r3", "a", "r3") }()
	waitQueued(t, e, 1)
	if c := e.Snapshot(nil).Counters; c.QueuedOtherFree != 0 {
		t.Fatalf("flagged while every offered account was full: %+v", c)
	}
	e.Complete("r2")
	if c := e.Snapshot(nil).Counters; c.QueuedOtherFree != 1 {
		t.Fatalf("free offered account not noticed: %+v", c)
	}
	// Further changes do not count the same request again.
	mustAcquire(t, e, "r4", "b")
	e.Complete("r4")
	e.Complete("r1")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c := e.Snapshot(nil).Counters; c.QueuedOtherFree != 1 {
		t.Fatalf("request counted more than once: %+v", c)
	}
	details := admissionDetails(e)
	if want := "enqueued: waiting for account slot; affinity=none; other_free=0"; details[2] != want {
		t.Fatalf("enqueue detail %q, want %q", details[2], want)
	}
	if last, want := details[len(details)-1], "admitted: released from queue; other_free=yes"; last != want {
		t.Fatalf("release detail %q, want %q", last, want)
	}
}

// With a single offered account there is nowhere else to go, so a wait
// there is never reported as avoidable.
func TestQueuedRequestWithOneOfferedAccountIsNotFlagged(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	only := peers[:1]
	route(t, e, only, "r1", "a")
	mustAcquire(t, e, "elsewhere", "b")
	mustChoose(t, e, only, "r2", "a")
	done := make(chan error, 1)
	go func() { done <- e.AcquirePicked(context.Background(), "r2", "a", "r2") }()
	waitQueued(t, e, 1)
	e.Complete("elsewhere")
	e.Complete("r1")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c := e.Snapshot(nil).Counters; c.QueuedOtherFree != 0 {
		t.Fatalf("flagged with no alternative offered: %+v", c)
	}
	details := admissionDetails(e)
	if last, want := details[len(details)-1], "admitted: released from queue; other_free=no"; last != want {
		t.Fatalf("release detail %q, want %q", last, want)
	}
}

// With host selection the plugin declines every pick, keeps no bindings or
// reservations, and still enforces the limit on whichever account arrives.
func TestHostSelectionLeavesThePickToTheHost(t *testing.T) {
	c := DefaultConfig()
	c.Defaults = Policy{MaxConcurrency: 1, MaxQueue: 2, WaitTimeoutMS: 1000}
	c.HostSelection = true
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		id, handled, err := e.Pick(peers, fmt.Sprintf("sel%d", i), "s")
		if err != nil || handled || id != "" {
			t.Fatalf("pick = %q handled=%v err=%v, want it left to the host", id, handled, err)
		}
	}
	e.mu.Lock()
	sessions, reserved := len(e.sessions), len(e.reserved)
	e.mu.Unlock()
	got := e.Snapshot(nil).Counters
	if sessions != 0 || reserved != 0 || got.HostPicks != 3 || got.AffinityNew+got.AffinityKept+got.AffinityMoved+got.AffinityNone != 0 {
		t.Fatalf("sessions=%d reserved=%d counters=%+v", sessions, reserved, got)
	}
	// The host put both requests on the same account; the limit still holds.
	if err := e.AcquirePicked(context.Background(), "r1", "a", "sel0"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.AcquirePicked(context.Background(), "r2", "a", "sel1") }()
	waitQueued(t, e, 1)
	// The other account the host offered is idle the whole time.
	if c := e.Snapshot(nil).Counters; c.QueuedOtherFree != 1 {
		t.Fatalf("wait behind a busy account with another idle not reported: %+v", c)
	}
	e.Complete("r1")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Switching to host selection forgets the plugin's own bindings and picks,
// so switching back starts clean.
func TestSwitchingToHostSelectionForgetsBindingsAndPicks(t *testing.T) {
	e := engine(t, 2, 2, 1000)
	turn(t, e, peers, "r1", "s")
	e.Complete("r1")
	if _, err := e.Choose(peers, "pending", ""); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	sessionsBefore, reservedBefore := len(e.sessions), len(e.reserved)
	e.mu.Unlock()
	if sessionsBefore != 1 || reservedBefore != 1 {
		t.Fatalf("setup: sessions=%d reserved=%d, want one of each", sessionsBefore, reservedBefore)
	}
	c := e.cfg.Clone()
	c.HostSelection = true
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	sessions, reserved := len(e.sessions), len(e.reserved)
	e.mu.Unlock()
	if sessions != 0 || reserved != 0 {
		t.Fatalf("kept after switching: sessions=%d reserved=%d", sessions, reserved)
	}
	c.HostSelection = false
	if err := e.Update(c); err != nil {
		t.Fatal(err)
	}
	if id, handled, err := e.Pick(peers, "back", "s"); err != nil || !handled || id == "" {
		t.Fatalf("pick after switching back = %q handled=%v err=%v", id, handled, err)
	}
}
