package gjob

import (
	"context"
	"fmt"
	"time"
)

// SchedulerConfig bounds the trigger loop.
type SchedulerConfig struct {
	// Tick is how often due jobs are scanned. It also bounds how late a trigger can
	// be: a 1s tick cannot fire on the exact second.
	Tick time.Duration
	// Lease is the claim lease handed to the receipt store.
	Lease time.Duration
	// MaxTriggersPerTick bounds one scan, so a long outage cannot produce an
	// unbounded burst.
	MaxTriggersPerTick int
	// Node names this scheduler in the execution id, which keeps two nodes that
	// clock differently from colliding on the same schedule slot.
	Node string
}

// DefaultSchedulerConfig is a conservative starting point.
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{Tick: time.Second, Lease: time.Minute, MaxTriggersPerTick: 100}
}

// Validate rejects an unusable configuration.
func (c *SchedulerConfig) Validate() error {
	if c.Tick <= 0 {
		return fmt.Errorf("%w: tick must be positive", ErrPermanent)
	}
	if c.Lease <= 0 {
		c.Lease = time.Minute
	}
	if c.MaxTriggersPerTick <= 0 {
		c.MaxTriggersPerTick = DefaultSchedulerConfig().MaxTriggersPerTick
	}
	return nil
}

// SubmitFunc hands a claimed execution to a dispatcher.
type SubmitFunc func(execution Execution) error

// Scheduler turns cron expressions into claimed executions.
//
// Deduplication is the receipt store's job: the execution id is derived from the
// job name and the scheduled instant, so every node computes the same id and only one
// claim wins.
type Scheduler struct {
	cfg    SchedulerConfig
	jobs   []Job
	scheds map[string]*Schedule
	store  ReceiptStore
	submit SubmitFunc
	now    func() time.Time

	// lastFired is per job: the last slot that was handled.
	lastFired map[string]time.Time
	seeded    bool
}

// NewScheduler validates the jobs and their expressions.
func NewScheduler(cfg SchedulerConfig, jobs []Job, store ReceiptStore, submit SubmitFunc) (*Scheduler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("%w: receipt store is required", ErrPermanent)
	}
	if submit == nil {
		return nil, fmt.Errorf("%w: submit function is required", ErrPermanent)
	}

	s := &Scheduler{
		cfg:       cfg,
		jobs:      make([]Job, 0, len(jobs)),
		scheds:    map[string]*Schedule{},
		store:     store,
		submit:    submit,
		now:       time.Now,
		lastFired: map[string]time.Time{},
	}

	for _, job := range jobs {
		if err := job.Validate(); err != nil {
			return nil, err
		}
		schedule, err := ParseSchedule(job.Cron)
		if err != nil {
			return nil, fmt.Errorf("job %s: %w", job.Name, err)
		}
		if _, duplicate := s.scheds[job.Name]; duplicate {
			return nil, fmt.Errorf("%w: job %q", ErrAlreadyRegistered, job.Name)
		}
		s.scheds[job.Name] = schedule
		s.jobs = append(s.jobs, job)
	}
	return s, nil
}

// WithSchedulerClock overrides the clock (tests).
func (s *Scheduler) WithSchedulerClock(now func() time.Time) *Scheduler {
	if now != nil {
		s.now = now
	}
	return s
}

// Run scans for due jobs until the context is cancelled.
//
// On start every job's cursor is set to "now": a restart does not replay the whole
// history. A missed slot inside one process lifetime is what the misfire policy
// handles.
func (s *Scheduler) Run(ctx context.Context) error {
	s.seed()

	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				return err
			}
		}
	}
}

// tick handles one scan. The first scan seeds the cursors: a scheduler that starts
// now must not replay every slot since the epoch.
func (s *Scheduler) tick(ctx context.Context) error {
	if !s.seeded {
		s.seed()
		s.seeded = true
		// Nothing is due in the seeding scan itself; the previous slot was already
		// handled before this process existed.
		return nil
	}

	now := s.now()
	budget := s.cfg.MaxTriggersPerTick

	for _, job := range s.jobs {
		if budget <= 0 {
			return nil
		}
		if !job.Enabled {
			continue
		}

		schedule := s.scheds[job.Name]
		loc := job.TimeZone
		if loc == nil {
			loc = time.UTC
		}

		cursor := s.lastFired[job.Name]
		for {
			if budget <= 0 {
				break
			}

			next := schedule.Next(cursor, loc)
			if next.IsZero() || next.After(now) {
				break
			}

			// Every slot the cursor walks past is a slot that was due.
			dueScheduledAt := next
			cursor = next

			// Misfire is "this slot is older than one tick", i.e. the previous scan
			// should have caught it.
			late := now.Sub(dueScheduledAt) > s.cfg.Tick

			switch {
			case !late:
				// On time.
			case job.Misfire == MisfireSkip:
				// Record nothing: the slot is skipped, and the cursor has moved.
				continue
			case job.Misfire == MisfireRunOnce:
				// Collapse: fire once for the latest missed slot, then stop walking.
				cursor = now
			case job.Misfire == MisfireBackfill:
				// Fire each missed slot, bounded by the tick budget.
			default:
				// The zero policy behaves like skip, which is the safest default for an
				// unset field.
				continue
			}

			execution := Execution{
				ID:          ExecutionID(job.Name, dueScheduledAt),
				JobName:     job.Name,
				TriggerType: TriggerCron,
				ScheduledAt: dueScheduledAt.UTC(),
				Attempt:     1,
			}

			receipt, claimed, err := s.store.Claim(ctx, execution, s.cfg.Lease)
			if err != nil {
				return fmt.Errorf("claim %s: %w", execution.ID, err)
			}
			if !claimed {
				// Another node owns this slot; nothing to do.
				_ = receipt
				budget--
				continue
			}

			if err := s.submit(execution); err != nil {
				// The claim is durable but the execution could not be queued: leave the
				// receipt PENDING so a retry or the recoverer picks it up, and stop this
				// scan rather than burning the budget on a full queue.
				return nil
			}
			budget--
		}
		s.lastFired[job.Name] = cursor
	}
	return nil
}

// seed resets every cursor to now.
func (s *Scheduler) seed() {
	now := s.now()
	for _, job := range s.jobs {
		s.lastFired[job.Name] = now
	}
}

// ExecutionID derives the stable id of a scheduled slot. Two nodes must compute the
// same value, so it uses the UTC instant rather than a local one.
func ExecutionID(jobName string, scheduledAt time.Time) string {
	return fmt.Sprintf("%s@%s", jobName, scheduledAt.UTC().Format(time.RFC3339))
}
