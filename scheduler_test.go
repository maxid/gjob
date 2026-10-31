package gjob_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gjob"
	"github.com/maxid/gjob/memory"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

type collector struct {
	mu         sync.Mutex
	executions []gjob.Execution
	err        error
}

func (c *collector) submit(execution gjob.Execution) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.executions = append(c.executions, execution)
	return nil
}

func (c *collector) all() []gjob.Execution {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]gjob.Execution(nil), c.executions...)
}

func newScheduler(t *testing.T, job gjob.Job, clk *clock) (*gjob.Scheduler, *memory.Store, *collector) {
	t.Helper()
	store := memory.NewStore().WithClock(clk.Now)
	target := &collector{}
	scheduler, err := gjob.NewScheduler(gjob.SchedulerConfig{
		Tick: time.Minute, Lease: time.Minute, MaxTriggersPerTick: 10,
	}, []gjob.Job{job}, store, target.submit)
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	scheduler.WithSchedulerClock(clk.Now)
	return scheduler, store, target
}

// runTicks drives the scheduler without the wall clock. The first scan only seeds the
// cursors, mirroring a process start.
func runTicks(t *testing.T, scheduler *gjob.Scheduler, ticks int, clk *clock, step time.Duration) {
	t.Helper()
	ctx := context.Background()
	scheduler.RunOnce(ctx) // seed
	for i := 0; i < ticks; i++ {
		clk.Advance(step)
		scheduler.RunOnce(ctx)
	}
}

func TestSchedulerFiresOnSchedule(t *testing.T) {
	clk := &clock{now: time.Date(2026, 10, 31, 11, 58, 0, 0, time.UTC)}
	job := gjob.Job{Name: "report", Cron: "0 12 * * *", Enabled: true, Misfire: gjob.MisfireBackfill}
	scheduler, _, target := newScheduler(t, job, clk)

	runTicks(t, scheduler, 5, clk, time.Minute)

	executions := target.all()
	if len(executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(executions))
	}
	if got := executions[0].ScheduledAt; !got.Equal(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("scheduledAt = %s", got)
	}
	if executions[0].ID != gjob.ExecutionID("report", executions[0].ScheduledAt) {
		t.Fatalf("id = %q, not derived from the slot", executions[0].ID)
	}
}

// Two schedulers (as on two nodes) compute the same execution id; only one claim wins.
func TestSchedulerClaimsAreDeDuplicated(t *testing.T) {
	clk := &clock{now: time.Date(2026, 10, 31, 11, 58, 0, 0, time.UTC)}
	job := gjob.Job{Name: "report", Cron: "0 12 * * *", Enabled: true, Misfire: gjob.MisfireBackfill}

	store := memory.NewStore().WithClock(clk.Now)
	nodeA, nodeB := &collector{}, &collector{}
	cfg := gjob.SchedulerConfig{Tick: time.Minute, Lease: time.Minute, MaxTriggersPerTick: 10}

	first, _ := gjob.NewScheduler(cfg, []gjob.Job{job}, store, nodeA.submit)
	second, _ := gjob.NewScheduler(cfg, []gjob.Job{job}, store, nodeB.submit)
	first.WithSchedulerClock(clk.Now)
	second.WithSchedulerClock(clk.Now)

	first.RunOnce(context.Background()) // seed both
	second.RunOnce(context.Background())
	for i := 0; i < 5; i++ {
		clk.Advance(time.Minute)
		first.RunOnce(context.Background())
		second.RunOnce(context.Background())
	}

	total := len(nodeA.all()) + len(nodeB.all())
	if total != 1 {
		t.Fatalf("both nodes executed %d times, want exactly 1", total)
	}
}

// Skip records nothing for a missed slot; RunOnce collapses several missed slots into
// one execution.
func TestMisfirePolicies(t *testing.T) {
	start := time.Date(2026, 10, 31, 11, 0, 0, 0, time.UTC)

	t.Run("skip", func(t *testing.T) {
		clk := &clock{now: start}
		job := gjob.Job{Name: "hourly", Cron: "0 * * * *", Enabled: true, Misfire: gjob.MisfireSkip}
		scheduler, _, target := newScheduler(t, job, clk)

		// Jump three hours: three slots were missed.
		scheduler.RunOnce(context.Background()) // seed
		clk.Advance(3*time.Hour + time.Minute)
		scheduler.RunOnce(context.Background())

		// 12:00 and 13:00 were missed and are skipped; 14:00 is only one minute old at
		// 14:01, which is within the tick and therefore on time.
		executions := target.all()
		if len(executions) != 1 {
			t.Fatalf("skip produced %d executions, want 1 (the on-time slot)", len(executions))
		}
		if want := time.Date(2026, 10, 31, 14, 0, 0, 0, time.UTC); !executions[0].ScheduledAt.Equal(want) {
			t.Fatalf("scheduledAt = %s, want %s", executions[0].ScheduledAt, want)
		}
	})

	t.Run("run-once", func(t *testing.T) {
		clk := &clock{now: start}
		job := gjob.Job{Name: "hourly", Cron: "0 * * * *", Enabled: true, Misfire: gjob.MisfireRunOnce}
		scheduler, _, target := newScheduler(t, job, clk)

		scheduler.RunOnce(context.Background()) // seed
		clk.Advance(3*time.Hour + time.Minute)
		scheduler.RunOnce(context.Background())

		if got := len(target.all()); got != 1 {
			t.Fatalf("run-once produced %d executions, want 1", got)
		}
	})

	t.Run("backfill is bounded", func(t *testing.T) {
		clk := &clock{now: start}
		job := gjob.Job{Name: "hourly", Cron: "0 * * * *", Enabled: true, Misfire: gjob.MisfireBackfill}
		scheduler, _, target := newScheduler(t, job, clk)

		scheduler.RunOnce(context.Background()) // seed
		clk.Advance(3*time.Hour + time.Minute)
		scheduler.RunOnce(context.Background())

		// Three missed slots fall inside the tick budget of 10.
		if got := len(target.all()); got != 3 {
			t.Fatalf("backfill produced %d executions, want 3", got)
		}
	})
}

// The budget caps a backlog, so one scan cannot dispatch thousands of executions.
func TestTickBudget(t *testing.T) {
	clk := &clock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
	job := gjob.Job{Name: "minutely", Cron: "* * * * *", Enabled: true, Misfire: gjob.MisfireBackfill}

	store := memory.NewStore().WithClock(clk.Now)
	target := &collector{}
	scheduler, err := gjob.NewScheduler(gjob.SchedulerConfig{
		Tick: time.Minute, Lease: time.Minute, MaxTriggersPerTick: 3,
	}, []gjob.Job{job}, store, target.submit)
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	scheduler.WithSchedulerClock(clk.Now)

	scheduler.RunOnce(context.Background()) // seed
	clk.Advance(10 * time.Minute)
	scheduler.RunOnce(context.Background())

	if got := len(target.all()); got != 3 {
		t.Fatalf("dispatched %d, want the budget of 3", got)
	}
}

func TestDisabledJobDoesNotFire(t *testing.T) {
	clk := &clock{now: time.Date(2026, 10, 31, 11, 58, 0, 0, time.UTC)}
	job := gjob.Job{Name: "report", Cron: "* * * * *", Enabled: false, Misfire: gjob.MisfireBackfill}
	scheduler, _, target := newScheduler(t, job, clk)

	runTicks(t, scheduler, 3, clk, time.Minute)
	if got := len(target.all()); got != 0 {
		t.Fatalf("disabled job produced %d executions", got)
	}
}

func TestSchedulerRejectsBadJobs(t *testing.T) {
	store := memory.NewStore()
	submit := func(gjob.Execution) error { return nil }

	if _, err := gjob.NewScheduler(gjob.SchedulerConfig{Tick: time.Second}, []gjob.Job{{Name: "x"}}, store, submit); err == nil {
		t.Fatal("job without a cron expression must fail")
	}
	if _, err := gjob.NewScheduler(gjob.SchedulerConfig{Tick: time.Second}, []gjob.Job{
		{Name: "x", Cron: "bad"},
	}, store, submit); err == nil {
		t.Fatal("invalid cron must fail")
	}
	if _, err := gjob.NewScheduler(gjob.SchedulerConfig{Tick: time.Second}, []gjob.Job{
		{Name: "x", Cron: "* * * * *"}, {Name: "x", Cron: "* * * * *"},
	}, store, submit); err == nil {
		t.Fatal("duplicate job names must fail")
	}
	if _, err := gjob.NewScheduler(gjob.SchedulerConfig{Tick: time.Second}, nil, nil, submit); err == nil {
		t.Fatal("missing receipt store must fail")
	}
	if _, err := gjob.NewScheduler(gjob.SchedulerConfig{Tick: time.Second}, nil, store, nil); err == nil {
		t.Fatal("missing submit function must fail")
	}
}
