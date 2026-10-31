package redisstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gjob"
	"github.com/maxid/gjob/redisstore"
)

func newStore(t *testing.T) (*redisstore.Store, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, err := redisstore.New(client)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	return store, server
}

func execution(id string) gjob.Execution {
	return gjob.Execution{
		ID:          id,
		JobName:     "report",
		TriggerType: gjob.TriggerCron,
		ScheduledAt: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
		Attempt:     1,
	}
}

// Two nodes computing the same execution id: the first claim wins and the second one
// sees the existing receipt.
func TestClaimIsAtomicAcrossNodes(t *testing.T) {
	store, _ := newStore(t)
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

func TestCompleteChecksFencing(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	receipt, _, _ := store.Claim(ctx, execution("e1"), time.Minute)
	if err := store.Start(ctx, "e1", receipt.FencingToken); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := store.Complete(ctx, "e1", receipt.FencingToken+1, gjob.StateSucceeded, &gjob.Result{Code: "ok"}, ""); !errors.Is(err, gjob.ErrStaleAttempt) {
		t.Fatalf("err = %v, want ErrStaleAttempt", err)
	}

	result := &gjob.Result{Code: "ok", Node: "n1", StartedAt: time.Now(), FinishedAt: time.Now()}
	if err := store.Complete(ctx, "e1", receipt.FencingToken, gjob.StateSucceeded, result, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	stored, ok, _ := store.Get(ctx, "e1")
	if !ok || stored.State != gjob.StateSucceeded {
		t.Fatalf("receipt = %+v", stored)
	}
	if stored.Result == nil || stored.Result.Node != "n1" {
		t.Fatalf("result = %+v", stored.Result)
	}

	// Unknown execution.
	if err := store.Complete(ctx, "missing", 1, gjob.StateSucceeded, nil, ""); !errors.Is(err, gjob.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestHeartbeat(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	receipt, _, _ := store.Claim(ctx, execution("e1"), time.Minute)
	until := time.Now().Add(5 * time.Minute)
	if err := store.Heartbeat(ctx, "e1", receipt.FencingToken, until); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	stored, _, _ := store.Get(ctx, "e1")
	if !stored.LeaseUntil.Equal(until) {
		t.Fatalf("lease = %s, want %s", stored.LeaseUntil, until)
	}
	if err := store.Heartbeat(ctx, "e1", receipt.FencingToken+1, until); !errors.Is(err, gjob.ErrStaleAttempt) {
		t.Fatalf("err = %v, want ErrStaleAttempt", err)
	}
}

// An expired lease is recovered as LOST, in a bounded batch.
func TestRecoverExpired(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	for _, id := range []string{"e1", "e2", "e3"} {
		if _, _, err := store.Claim(ctx, execution(id), time.Minute); err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
	}

	// Nothing is expired yet.
	recovered, err := store.RecoverExpired(ctx, time.Now(), 10)
	if err != nil || len(recovered) != 0 {
		t.Fatalf("recovered = %d, %v", len(recovered), err)
	}

	// Two minutes later the one-minute leases are stale; the batch is capped at two.
	later := time.Now().Add(2 * time.Minute)
	first, err := store.RecoverExpired(ctx, later, 2)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("first batch = %d, want 2", len(first))
	}
	second, _ := store.RecoverExpired(ctx, later, 2)
	if len(second) != 1 {
		t.Fatalf("second batch = %d, want 1", len(second))
	}
	for _, receipt := range first {
		if receipt.State != gjob.StateLost {
			t.Fatalf("%s state = %s", receipt.ExecutionID, receipt.State)
		}
	}
}

func TestNamespacePrefixesReceiptsAndFencing(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := redisstore.New(client, redisstore.WithNamespace("app"))
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := store.Claim(context.Background(), execution("e1"), time.Minute); err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	keys := map[string]bool{}
	for _, key := range server.Keys() {
		keys[key] = true
	}
	for _, want := range []string{"app:job:e1", "app:job:lease", "app:job:fencing"} {
		if !keys[want] {
			t.Fatalf("missing key %q in %v", want, server.Keys())
		}
	}
}

func TestNewRequiresClient(t *testing.T) {
	if _, err := redisstore.New(nil); !errors.Is(err, gjob.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}
