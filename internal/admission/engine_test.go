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

func TestSchedulerQueuesFullAccountAndHonorsPin(t *testing.T) {
	e := engine(t, 1, 2, 1000)
	mustAcquire(t, e, "active", "a")
	if id, err := e.Choose([]string{"a", "b"}, ""); err != nil || id != "b" {
		t.Fatalf("%s %v", id, err)
	}
	if id, err := e.Choose([]string{"a", "b"}, "a"); err != nil || id != "a" {
		t.Fatalf("%s %v", id, err)
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
