//go:build integration

package mysqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqlcontainer "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/maxid/gjob"
	"github.com/maxid/gjob/mysqlstore"
)

// Real MySQL only: a mock would happily accept an INSERT that conflicts in reality,
// and the whole point of this store is the conflict behaviour.
func startMySQL(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()

	dsn := os.Getenv("GJOB_TEST_MYSQL_DSN")
	if dsn == "" {
		container, err := mysqlcontainer.Run(ctx, image(),
			mysqlcontainer.WithDatabase("gjob"),
			mysqlcontainer.WithUsername("test"),
			mysqlcontainer.WithPassword("test"),
		)
		if err != nil {
			t.Fatalf("mysql container: %v", err)
		}
		t.Cleanup(func() { _ = container.Terminate(context.Background()) })

		host, _ := container.Host(ctx)
		port, _ := container.MappedPort(ctx, "3306/tcp")
		dsn = fmt.Sprintf("test:test@tcp(%s:%s)/gjob?multiStatements=true&parseTime=true", host, port.Port())
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS gjob_receipt`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE gjob_receipt (
		execution_id VARCHAR(191) NOT NULL,
		job_name VARCHAR(191) NOT NULL,
		trigger_type VARCHAR(32) NOT NULL,
		scheduled_at DATETIME(3) NOT NULL,
		attempt INT NOT NULL DEFAULT 1,
		state VARCHAR(16) NOT NULL,
		fencing_token BIGINT UNSIGNED NOT NULL,
		lease_until DATETIME(3) NOT NULL,
		updated_at DATETIME(3) NOT NULL,
		result JSON NULL,
		error VARCHAR(1024) NOT NULL DEFAULT '',
		PRIMARY KEY (execution_id),
		KEY idx_lease (lease_until)
	) ENGINE=InnoDB`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return db
}

func image() string {
	if v := os.Getenv("GJOB_TEST_MYSQL_IMAGE"); v != "" {
		return v
	}
	return "mysql:8.4.7"
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

func TestClaimIsAtomic(t *testing.T) {
	store := mysqlstore.New(startMySQL(t))
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
	store := mysqlstore.New(startMySQL(t))
	ctx := context.Background()

	receipt, _, _ := store.Claim(ctx, execution("e1"), time.Minute)
	if err := store.Start(ctx, "e1", receipt.FencingToken); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := store.Complete(ctx, "e1", receipt.FencingToken+1, gjob.StateSucceeded, nil, ""); !errors.Is(err, gjob.ErrStaleAttempt) {
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

	// A terminal receipt is final.
	if err := store.Complete(ctx, "e1", receipt.FencingToken, gjob.StateFailed, nil, "late"); !errors.Is(err, gjob.ErrInvalidState) {
		t.Fatalf("err = %v, want ErrInvalidState", err)
	}
}

func TestHeartbeat(t *testing.T) {
	store := mysqlstore.New(startMySQL(t))
	ctx := context.Background()

	receipt, _, _ := store.Claim(ctx, execution("e1"), time.Minute)
	until := time.Now().Add(5 * time.Minute)
	if err := store.Heartbeat(ctx, "e1", receipt.FencingToken, until); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	stored, _, _ := store.Get(ctx, "e1")
	if stored.LeaseUntil.Sub(until).Abs() > time.Second {
		t.Fatalf("lease = %s, want %s", stored.LeaseUntil, until)
	}

	if err := store.Heartbeat(ctx, "e1", receipt.FencingToken+1, until); !errors.Is(err, gjob.ErrStaleAttempt) {
		t.Fatalf("err = %v, want ErrStaleAttempt", err)
	}
}

func TestRecoverExpired(t *testing.T) {
	store := mysqlstore.New(startMySQL(t))
	ctx := context.Background()

	for _, id := range []string{"e1", "e2", "e3"} {
		if _, _, err := store.Claim(ctx, execution(id), time.Minute); err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
	}

	recovered, err := store.RecoverExpired(ctx, time.Now(), 10)
	if err != nil || len(recovered) != 0 {
		t.Fatalf("recovered = %d, %v", len(recovered), err)
	}

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
	for _, receipt := range append(first, second...) {
		if receipt.State != gjob.StateLost {
			t.Fatalf("%s state = %s", receipt.ExecutionID, receipt.State)
		}
	}
}

// The scheduler and the store together: two nodes, one execution.
func TestTwoNodesClaimTheSameSlotOnce(t *testing.T) {
	db := startMySQL(t)
	storeA := mysqlstore.New(db)
	storeB := mysqlstore.New(db)
	ctx := context.Background()

	one, claimedA, err := storeA.Claim(ctx, execution("slot-1200"), time.Minute)
	if err != nil {
		t.Fatalf("claim a: %v", err)
	}
	_, claimedB, err := storeB.Claim(ctx, execution("slot-1200"), time.Minute)
	if err != nil {
		t.Fatalf("claim b: %v", err)
	}

	if !claimedA || claimedB {
		t.Fatalf("claimedA=%v claimedB=%v, want exactly one winner", claimedA, claimedB)
	}
	if one.ExecutionID != "slot-1200" {
		t.Fatalf("receipt = %+v", one)
	}
}
