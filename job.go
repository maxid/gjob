// Package gjob schedules and runs jobs in a cluster.
//
// It answers three questions: when to trigger, which node executes, and how far the
// execution got. It deliberately does not own task scripts, permissions, dashboards
// or business compensation.
//
// The default promise is **at least once**. A distributed lock only reduces
// concurrency; preventing duplicate side effects needs a unique execution id, a
// fenced lease and an idempotent executor. A deployment whose external systems are
// not idempotent must accept that risk or supply compensation.
package gjob

import (
	"encoding/json"
	"fmt"
	"time"
)

// TriggerType records why an execution exists.
type TriggerType string

const (
	TriggerCron   TriggerType = "cron"
	TriggerEvent  TriggerType = "event"
	TriggerManual TriggerType = "manual"
)

// MisfirePolicy decides what happens to a schedule that was missed.
type MisfirePolicy string

const (
	// MisfireSkip records the missed slot and does not run it.
	MisfireSkip MisfirePolicy = "skip"
	// MisfireRunOnce collapses several missed slots into one execution.
	MisfireRunOnce MisfirePolicy = "run-once"
	// MisfireBackfill runs every missed slot. Only for jobs the business approved:
	// a long outage otherwise produces a burst of executions.
	MisfireBackfill MisfirePolicy = "backfill"
)

// MaxParametersBytes bounds the stored trigger payload.
const MaxParametersBytes = 64 << 10

// RetryPolicy paces a failed execution.
type RetryPolicy struct {
	// MaxAttempts includes the first run. 1 means no retry.
	MaxAttempts int
	Initial     time.Duration
	Max         time.Duration
	// Jitter is the fraction of the delay randomised away, e.g. 0.2 for ±20%.
	Jitter float64
}

// Job is one schedulable unit.
type Job struct {
	Name string
	// Cron is a standard 5-field expression (minute hour day-of-month month
	// day-of-week). A 6-field expression with seconds is rejected unless
	// WithSeconds is configured, so two nodes cannot disagree about the format.
	Cron string
	// TimeZone is the zone the expression is evaluated in. Nil means UTC.
	TimeZone *time.Location
	Enabled  bool
	Misfire  MisfirePolicy
	Timeout  time.Duration
	Retry    RetryPolicy
	// Concurrency bounds overlapping executions of this job. Zero means one.
	Concurrency int
}

// Validate rejects a job the scheduler cannot honour.
func (j Job) Validate() error {
	if j.Name == "" {
		return fmt.Errorf("%w: job name is required", ErrPermanent)
	}
	if j.Cron == "" {
		return fmt.Errorf("%w: job %s: cron expression is required", ErrPermanent, j.Name)
	}
	if j.Concurrency < 0 {
		return fmt.Errorf("%w: job %s: concurrency must not be negative", ErrPermanent, j.Name)
	}
	if j.Timeout < 0 {
		return fmt.Errorf("%w: job %s: timeout must not be negative", ErrPermanent, j.Name)
	}
	switch j.Misfire {
	case "", MisfireSkip, MisfireRunOnce, MisfireBackfill:
	default:
		return fmt.Errorf("%w: job %s: unknown misfire policy %q", ErrPermanent, j.Name, j.Misfire)
	}
	if j.Retry.MaxAttempts < 0 {
		return fmt.Errorf("%w: job %s: retry attempts must not be negative", ErrPermanent, j.Name)
	}
	return nil
}

// Execution is one attempt at a job.
type Execution struct {
	// ID is stable for one logical trigger: retries keep it and raise Attempt.
	ID          string
	JobName     string
	TriggerType TriggerType
	ScheduledAt time.Time
	Attempt     int
	Parameters  json.RawMessage
	// FencingToken comes from the lease and is checked on every write, so a late
	// worker from an older attempt cannot overwrite a newer result.
	FencingToken uint64
}

// Validate rejects an execution that cannot be stored.
func (e Execution) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("%w: execution id is required", ErrPermanent)
	}
	if e.JobName == "" {
		return fmt.Errorf("%w: job name is required", ErrPermanent)
	}
	if len(e.Parameters) > MaxParametersBytes {
		return fmt.Errorf("%w: parameters are %d bytes, limit %d", ErrPermanent, len(e.Parameters), MaxParametersBytes)
	}
	if len(e.Parameters) > 0 && !json.Valid(e.Parameters) {
		return fmt.Errorf("%w: parameters are not valid JSON", ErrPermanent)
	}
	switch e.TriggerType {
	case TriggerCron, TriggerEvent, TriggerManual:
	default:
		return fmt.Errorf("%w: unknown trigger type %q", ErrPermanent, e.TriggerType)
	}
	return nil
}

// Result is what an execution left behind. It is bounded on purpose: the receipt
// stores a short summary, never the payload of a failure.
type Result struct {
	StartedAt  time.Time
	FinishedAt time.Time
	// Code is a stable, low-cardinality label.
	Code string
	// Message is a redacted summary.
	Message string
	// Node identifies the worker.
	Node string
}

// Duration reports the wall time of the attempt.
func (r Result) Duration() time.Duration { return r.FinishedAt.Sub(r.StartedAt) }
