package memory_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gjob"
	"github.com/maxid/gjob/memory"
)

func execution(id string) gjob.Execution {
	return gjob.Execution{
		ID:          id,
		JobName:     "report",
		TriggerType: gjob.TriggerCron,
		ScheduledAt: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
		Attempt:     1,
	}
}

// The de-duplication point: a second claim for the same execution id returns the
// existing receipt and reports Claimed=false.
func TestClaimIsIdempotent(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()

	first, claimed, err := store.Claim(ctx, execution("e1"), time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	second, claimedAgain, err := store.Claim(ctx, execution("e1"), time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimedAgain {
		t.Fatal("a second claim must report Claimed=false")
	}
	if second.FencingToken != first.FencingToken {
		t.Fatalf("fencing token changed: %d -> %d", first.FencingToken, second.FencingToken)
	}
}

// Old workers cannot overwrite a newer attempt: the fencing token decides.
func TestCompleteRejectsStaleFencing(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()

	receipt, _, _ := store.Claim(ctx, execution("e1"), time.Minute)
	if err := store.Start(ctx, "e1", receipt.FencingToken); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := store.Complete(ctx, "e1", receipt.FencingToken+1, gjob.StateSucceeded, nil, ""); !errors.Is(err, gjob.ErrStaleAttempt) {
		t.Fatalf("err = %v, want ErrStaleAttempt", err)
	}
	if err := store.Complete(ctx, "e1", receipt.FencingToken, gjob.StateSucceeded, &gjob.Result{Code: "ok"}, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// A terminal receipt does not move again.
	if err := store.Complete(ctx, "e1", receipt.FencingToken, gjob.StateFailed, nil, "late"); !errors.Is(err, gjob.ErrInvalidState) {
		t.Fatalf("err = %v, want ErrInvalidState", err)
	}
}

func TestHeartbeatExtendsAndChecksOwnership(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()

	receipt, _, _ := store.Claim(ctx, execution("e1"), time.Minute)
	until := time.Now().Add(5 * time.Minute)
	if err := store.Heartbeat(ctx, "e1", receipt.FencingToken, until); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	updated, _, _ := store.Get(ctx, "e1")
	if !updated.LeaseUntil.Equal(until) {
		t.Fatalf("lease = %s, want %s", updated.LeaseUntil, until)
	}

	if err := store.Heartbeat(ctx, "e1", receipt.FencingToken+9, until); !errors.Is(err, gjob.ErrStaleAttempt) {
		t.Fatalf("err = %v, want ErrStaleAttempt", err)
	}
	if err := store.Heartbeat(ctx, "missing", 1, until); !errors.Is(err, gjob.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// An expired lease becomes LOST, in a bounded batch.
func TestRecoverExpired(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	store := memory.NewStore().WithClock(func() time.Time { return now })
	ctx := context.Background()

	for _, id := range []string{"e1", "e2", "e3"} {
		if _, _, err := store.Claim(ctx, execution(id), time.Minute); err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
	}

	// Nothing expired yet.
	recovered, err := store.RecoverExpired(ctx, now, 10)
	if err != nil || len(recovered) != 0 {
		t.Fatalf("recovered = %d, %v", len(recovered), err)
	}

	// One batch of at most two, then the rest.
	later := now.Add(2 * time.Minute)
	first, _ := store.RecoverExpired(ctx, later, 2)
	if len(first) != 2 {
		t.Fatalf("first batch = %d, want 2", len(first))
	}
	second, _ := store.RecoverExpired(ctx, later, 2)
	if len(second) != 1 {
		t.Fatalf("second batch = %d, want 1", len(second))
	}
	for _, receipt := range append(first, second...) {
		if receipt.State != gjob.StateLost {
			t.Fatalf("%s state = %s, want LOST", receipt.ExecutionID, receipt.State)
		}
	}
}

// The dispatcher runs an execution once and records success; a second submission with
// the same id is claimed by nobody.
func TestDispatcherRunsAndRecords(t *testing.T) {
	store := memory.NewStore()
	registry := gjob.NewRegistry()

	var runs atomic.Int64
	if err := registry.Register("report", gjob.ExecutorFunc(func(context.Context, gjob.Execution) error {
		runs.Add(1)
		return nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Seal()

	dispatcher, err := gjob.NewDispatcher(gjob.DispatcherConfig{
		Workers: 2, Queue: 4, Lease: time.Minute, HeartbeatInterval: 10 * time.Second, Node: "n1",
	}, registry, store)
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	dispatcher.Start()

	if err := dispatcher.Submit(execution("e1")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Submitted twice: the second claim finds the receipt and does nothing.
	if err := dispatcher.Submit(execution("e1")); err != nil {
		t.Fatalf("second submit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dispatcher.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if runs.Load() != 1 {
		t.Fatalf("executor ran %d times, want 1", runs.Load())
	}
	receipt, ok, _ := store.Get(context.Background(), "e1")
	if !ok || receipt.State != gjob.StateSucceeded {
		t.Fatalf("receipt = %+v", receipt)
	}
	if receipt.Result == nil || receipt.Result.Node != "n1" {
		t.Fatalf("result = %+v", receipt.Result)
	}
}

func TestDispatcherQueueIsBounded(t *testing.T) {
	store := memory.NewStore()
	registry := gjob.NewRegistry()

	block := make(chan struct{})
	defer close(block)
	_ = registry.Register("slow", gjob.ExecutorFunc(func(ctx context.Context, _ gjob.Execution) error {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil
	}))
	registry.Seal()

	dispatcher, err := gjob.NewDispatcher(gjob.DispatcherConfig{
		Workers: 1, Queue: 1, Lease: time.Minute, HeartbeatInterval: 10 * time.Second,
	}, registry, store)
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	dispatcher.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = dispatcher.Stop(ctx)
	}()

	slow := execution("e1")
	slow.JobName = "slow"
	if err := dispatcher.Submit(slow); err != nil {
		t.Fatalf("first submit: %v", err)
	}

	// Wait until the worker actually picked it up, otherwise the "running" slot is
	// still empty and the queue bound is measured wrong.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if receipt, ok, _ := store.Get(context.Background(), "e1"); ok && receipt.State == gjob.StateRunning {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// One running + one queued fills the bound; the next submission is deferred.
	second := execution("e2")
	second.JobName = "slow"
	if err := dispatcher.Submit(second); err != nil {
		t.Fatalf("second submit: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		third := execution("e3")
		third.JobName = "slow"
		if err := dispatcher.Submit(third); errors.Is(err, gjob.ErrQueueFull) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the queue never reported full")
}

// A failing executor records a failure, and the receipt keeps the message bounded.
func TestDispatcherRecordsFailure(t *testing.T) {
	store := memory.NewStore()
	registry := gjob.NewRegistry()
	_ = registry.Register("broken", gjob.ExecutorFunc(func(context.Context, gjob.Execution) error {
		return errors.New("boom")
	}))
	registry.Seal()

	dispatcher, _ := gjob.NewDispatcher(gjob.DispatcherConfig{
		Workers: 1, Queue: 2, Lease: time.Minute, HeartbeatInterval: 10 * time.Second,
	}, registry, store)
	dispatcher.Start()

	exec := execution("e1")
	exec.JobName = "broken"
	if err := dispatcher.Submit(exec); err != nil {
		t.Fatalf("submit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = dispatcher.Stop(ctx)

	receipt, _, _ := store.Get(context.Background(), "e1")
	if receipt.State != gjob.StateFailed {
		t.Fatalf("state = %s, want FAILED", receipt.State)
	}
	if receipt.Error == "" {
		t.Fatal("failure message not recorded")
	}
}

func TestRegistryRules(t *testing.T) {
	registry := gjob.NewRegistry()
	executor := gjob.ExecutorFunc(func(context.Context, gjob.Execution) error { return nil })

	if err := registry.Register("", executor); !errors.Is(err, gjob.ErrPermanent) {
		t.Fatalf("empty name err = %v", err)
	}
	if err := registry.Register("a", nil); !errors.Is(err, gjob.ErrPermanent) {
		t.Fatalf("nil executor err = %v", err)
	}
	if err := registry.Register("a", executor); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.Register("a", executor); !errors.Is(err, gjob.ErrAlreadyRegistered) {
		t.Fatalf("duplicate err = %v", err)
	}

	registry.Seal()
	if err := registry.Register("b", executor); !errors.Is(err, gjob.ErrPermanent) {
		t.Fatalf("sealed err = %v", err)
	}
	if _, ok := registry.Resolve("a"); !ok {
		t.Fatal("registered executor not resolvable")
	}
}

var _ = sync.Mutex{}
