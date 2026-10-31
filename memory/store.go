// Package memory provides in-process implementations of gjob's ports.
//
// It is a contract reference and a single-process driver, not a distributed store:
// two processes using it share nothing, so it cannot prove cluster-wide
// de-duplication.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/maxid/gjob"
)

// Store is an in-memory gjob.ReceiptStore.
type Store struct {
	mu       sync.Mutex
	receipts map[string]gjob.Receipt
	// fencing hands out a monotonically increasing token per claim.
	fencing uint64
	now     func() time.Time
}

// NewStore creates an empty store.
func NewStore() *Store {
	return &Store{receipts: map[string]gjob.Receipt{}, now: time.Now}
}

// WithClock overrides the clock (tests).
func (s *Store) WithClock(now func() time.Time) *Store {
	s.now = now
	return s
}

// Claim implements gjob.ReceiptStore.
//
// The existing receipt wins: a second claim for the same execution id never
// overwrites it, which is how cron slots and repeated bus events are de-duplicated.
func (s *Store) Claim(_ context.Context, execution gjob.Execution, lease time.Duration) (gjob.Receipt, bool, error) {
	if err := execution.Validate(); err != nil {
		return gjob.Receipt{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if existing, ok := s.receipts[execution.ID]; ok {
		return existing, false, nil
	}

	s.fencing++
	receipt := gjob.Receipt{
		ExecutionID:  execution.ID,
		JobName:      execution.JobName,
		TriggerType:  execution.TriggerType,
		ScheduledAt:  execution.ScheduledAt,
		Attempt:      execution.Attempt,
		State:        gjob.StatePending,
		FencingToken: s.fencing,
		LeaseUntil:   now.Add(lease),
		UpdatedAt:    now,
	}
	s.receipts[execution.ID] = receipt
	return receipt, true, nil
}

// Start implements gjob.ReceiptStore.
func (s *Store) Start(_ context.Context, executionID string, fencingToken uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	receipt, ok := s.receipts[executionID]
	if !ok {
		return gjob.ErrNotFound
	}
	if receipt.FencingToken != fencingToken {
		return gjob.ErrStaleAttempt
	}
	if receipt.State != gjob.StatePending && receipt.State != gjob.StateRetryWait {
		return fmt.Errorf("%w: %s -> RUNNING from %s", gjob.ErrInvalidState, executionID, receipt.State)
	}

	receipt.State = gjob.StateRunning
	receipt.UpdatedAt = s.now()
	s.receipts[executionID] = receipt
	return nil
}

// Heartbeat implements gjob.ReceiptStore.
func (s *Store) Heartbeat(_ context.Context, executionID string, fencingToken uint64, until time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	receipt, ok := s.receipts[executionID]
	if !ok {
		return gjob.ErrNotFound
	}
	if receipt.FencingToken != fencingToken {
		return gjob.ErrStaleAttempt
	}
	if receipt.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", gjob.ErrInvalidState, executionID, receipt.State)
	}

	receipt.LeaseUntil = until
	receipt.UpdatedAt = s.now()
	s.receipts[executionID] = receipt
	return nil
}

// Complete implements gjob.ReceiptStore.
func (s *Store) Complete(_ context.Context, executionID string, fencingToken uint64, state gjob.State, result *gjob.Result, failure string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	receipt, ok := s.receipts[executionID]
	if !ok {
		return gjob.ErrNotFound
	}
	// A late worker from an older attempt must not overwrite a newer one.
	if receipt.FencingToken != fencingToken {
		return gjob.ErrStaleAttempt
	}
	// A terminal receipt is final: a late duplicate must not rewrite the outcome.
	if receipt.State.Terminal() || !allowTransition(receipt.State, state) {
		return fmt.Errorf("%w: %s -> %s", gjob.ErrInvalidState, receipt.State, state)
	}

	receipt.State = state
	receipt.Result = result
	receipt.Error = failure
	receipt.UpdatedAt = s.now()
	s.receipts[executionID] = receipt
	return nil
}

// Get implements gjob.ReceiptStore.
func (s *Store) Get(_ context.Context, executionID string) (gjob.Receipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	receipt, ok := s.receipts[executionID]
	return receipt, ok, nil
}

// RecoverExpired implements gjob.ReceiptStore.
//
// It marks executions whose lease expired as LOST, in a bounded batch, so several
// nodes cannot stampede the recovery.
func (s *Store) RecoverExpired(_ context.Context, now time.Time, limit int) ([]gjob.Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = 100
	}

	var expired []gjob.Receipt
	ids := make([]string, 0, len(s.receipts))
	for id := range s.receipts {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		receipt := s.receipts[id]
		if receipt.State.Terminal() || receipt.State == gjob.StateLost {
			continue
		}
		if receipt.LeaseUntil.After(now) {
			continue
		}
		receipt.State = gjob.StateLost
		receipt.UpdatedAt = now
		receipt.Error = "lease expired"
		s.receipts[id] = receipt
		expired = append(expired, receipt)
		if len(expired) >= limit {
			break
		}
	}
	return expired, nil
}

// All returns every receipt, for assertions.
func (s *Store) All() []gjob.Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]gjob.Receipt, 0, len(s.receipts))
	for _, receipt := range s.receipts {
		out = append(out, receipt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecutionID < out[j].ExecutionID })
	return out
}

// allowTransition mirrors the library's state machine; the store cannot import the
// unexported helper, so it restates it and the contract test asserts the same rules.
func allowTransition(from, to gjob.State) bool {
	switch from {
	case gjob.StatePending:
		switch to {
		case gjob.StateRunning, gjob.StateRetryWait, gjob.StateCancelled, gjob.StateFailed, gjob.StateLost:
			return true
		}
	case gjob.StateRunning:
		switch to {
		case gjob.StateSucceeded, gjob.StateFailed, gjob.StateRetryWait, gjob.StateCancelled, gjob.StateLost:
			return true
		}
	case gjob.StateRetryWait:
		switch to {
		case gjob.StateRunning, gjob.StateCancelled, gjob.StateLost, gjob.StateFailed:
			return true
		}
	case gjob.StateLost:
		switch to {
		case gjob.StateRetryWait, gjob.StateFailed, gjob.StateCancelled:
			return true
		}
	}
	return false
}
