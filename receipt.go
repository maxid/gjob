package gjob

import (
	"context"
	"fmt"
	"time"
)

// State is the lifecycle position of an execution.
type State string

const (
	StatePending   State = "PENDING"
	StateRunning   State = "RUNNING"
	StateSucceeded State = "SUCCEEDED"
	StateFailed    State = "FAILED"
	StateRetryWait State = "RETRY_WAIT"
	StateCancelled State = "CANCELLED"
	// StateLost means the lease expired: the worker may still be running somewhere,
	// which is why the fencing token, not the clock, decides who may write.
	StateLost State = "LOST"
)

// Terminal reports whether no further transition is expected.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// Receipt is the durable record of an execution.
type Receipt struct {
	ExecutionID  string
	JobName      string
	TriggerType  TriggerType
	ScheduledAt  time.Time
	Attempt      int
	State        State
	FencingToken uint64
	// LeaseUntil is when the current holder must have heartbeated.
	LeaseUntil time.Time
	UpdatedAt  time.Time
	Result     *Result
	// Error is a redacted summary; a payload must never be stored here.
	Error string
}

// ReceiptStore persists execution state. Every method is atomic: the store is what
// makes "only one node starts this execution" true.
//
// A claim that finds an existing execution must return it rather than overwrite it,
// with Claimed=false. That is the de-duplication point for cron slots, repeated bus
// events and retries.
type ReceiptStore interface {
	// Claim creates the receipt if absent, or returns the existing one.
	Claim(ctx context.Context, execution Execution, lease time.Duration) (Receipt, bool, error)
	// Start moves a claimed receipt to RUNNING. Without it a long execution would
	// look PENDING to the recoverer while it is actually running.
	Start(ctx context.Context, executionID string, fencingToken uint64) error
	// Heartbeat extends the lease. It fails with ErrLeaseLost when the token no
	// longer matches.
	Heartbeat(ctx context.Context, executionID string, fencingToken uint64, until time.Time) error
	// Complete records a terminal or retry state, checking the fencing token.
	Complete(ctx context.Context, executionID string, fencingToken uint64, state State, result *Result, failure string) error
	// Get reads one receipt.
	Get(ctx context.Context, executionID string) (Receipt, bool, error)
	// RecoverExpired moves executions whose lease expired to LOST, in bounded
	// batches so several nodes cannot stampede.
	RecoverExpired(ctx context.Context, now time.Time, limit int) ([]Receipt, error)
}

// allowTransition reports whether a state change is legal. Keeping it in one place
// means a driver cannot invent its own state machine.
func allowTransition(from, to State) bool {
	switch from {
	case StatePending:
		switch to {
		case StateRunning, StateRetryWait, StateCancelled, StateFailed, StateLost:
			return true
		}
	case StateRunning:
		switch to {
		case StateSucceeded, StateFailed, StateRetryWait, StateCancelled, StateLost:
			return true
		}
	case StateRetryWait:
		switch to {
		case StateRunning, StateCancelled, StateLost, StateFailed:
			return true
		}
	case StateLost:
		switch to {
		case StateRetryWait, StateFailed, StateCancelled:
			return true
		}
	case StateSucceeded, StateFailed, StateCancelled:
		return false
	}
	return false
}

// checkTransition returns an error for an illegal move.
func checkTransition(from, to State) error {
	if !allowTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidState, from, to)
	}
	return nil
}
