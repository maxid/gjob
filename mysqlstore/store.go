// Package mysqlstore implements gjob.ReceiptStore in MySQL.
//
// The store is the de-duplication point of the scheduler: a claim is an INSERT that
// either wins or conflicts, never a read-then-write. Fencing is checked on every
// update, so a worker whose lease expired cannot overwrite the newer attempt.
package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/maxid/gjob"
)

// mysqlDuplicateEntry is ER_DUP_ENTRY.
const mysqlDuplicateEntry = 1062

// Store is a MySQL-backed receipt store.
type Store struct {
	db  *sql.DB
	now func() time.Time
	// retention is unused by the queries; it documents the operator's cleanup
	// expectation and is applied by the pruning statement in README.
	retention time.Duration
}

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// Open connects to MySQL.
//
// The DSN is normalised before opening: `parseTime` so DATETIME columns scan into
// time.Time, and `clientFoundRows` so an UPDATE that matched a row but changed
// nothing still reports one row — otherwise a heartbeat that rewrites the same lease
// would look like a lost lease.
func Open(dsn string) (*Store, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: parse dsn: %w", gjob.ErrPermanent, err)
	}
	cfg.ParseTime = true
	cfg.ClientFoundRows = true

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("gjob/mysql: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("gjob/mysql: ping: %w", err)
	}
	return New(db), nil
}

// New wraps an existing pool.
func New(db *sql.DB, opts ...Option) *Store {
	s := &Store{db: db, now: time.Now, retention: 7 * 24 * time.Hour}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Close releases the pool.
func (s *Store) Close() error { return s.db.Close() }

// Claim implements gjob.ReceiptStore.
//
// The INSERT is the atomic step: a duplicate key means another node already owns the
// execution, and the existing receipt is returned instead of overwritten.
func (s *Store) Claim(ctx context.Context, execution gjob.Execution, lease time.Duration) (gjob.Receipt, bool, error) {
	if err := execution.Validate(); err != nil {
		return gjob.Receipt{}, false, err
	}

	now := s.now()
	receipt := gjob.Receipt{
		ExecutionID:  execution.ID,
		JobName:      execution.JobName,
		TriggerType:  execution.TriggerType,
		ScheduledAt:  execution.ScheduledAt,
		Attempt:      execution.Attempt,
		State:        gjob.StatePending,
		FencingToken: uint64(now.UnixNano()),
		LeaseUntil:   now.Add(lease),
		UpdatedAt:    now,
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO gjob_receipt
		 (execution_id, job_name, trigger_type, scheduled_at, attempt, state, fencing_token, lease_until, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		receipt.ExecutionID, receipt.JobName, string(receipt.TriggerType), receipt.ScheduledAt,
		receipt.Attempt, string(receipt.State), receipt.FencingToken, receipt.LeaseUntil, receipt.UpdatedAt)
	if err == nil {
		return receipt, true, nil
	}

	if !isDuplicate(err) {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/mysql: claim: %w", err)
	}

	existing, ok, getErr := s.Get(ctx, execution.ID)
	if getErr != nil {
		return gjob.Receipt{}, false, getErr
	}
	if !ok {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/mysql: claim conflicted but the receipt is missing")
	}
	return existing, false, nil
}

// Start implements gjob.ReceiptStore.
func (s *Store) Start(ctx context.Context, executionID string, fencingToken uint64) error {
	return s.transition(ctx, executionID, fencingToken, gjob.StateRunning, nil, "")
}

// Heartbeat implements gjob.ReceiptStore.
func (s *Store) Heartbeat(ctx context.Context, executionID string, fencingToken uint64, until time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE gjob_receipt SET lease_until = ?, updated_at = ?
		 WHERE execution_id = ? AND fencing_token = ?`,
		until, s.now(), executionID, fencingToken)
	if err != nil {
		return fmt.Errorf("gjob/mysql: heartbeat: %w", err)
	}
	// Zero affected rows with found-rows semantics means no row matched: either it
	// does not exist or the fencing token moved on.
	return s.explain(ctx, executionID, fencingToken)
}

// Complete implements gjob.ReceiptStore.
func (s *Store) Complete(ctx context.Context, executionID string, fencingToken uint64, state gjob.State, result *gjob.Result, failure string) error {
	return s.transition(ctx, executionID, fencingToken, state, result, failure)
}

func (s *Store) transition(ctx context.Context, executionID string, fencingToken uint64, state gjob.State, result *gjob.Result, failure string) error {
	payload := sql.NullString{}
	if result != nil {
		raw, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("gjob/mysql: encode result: %w", err)
		}
		payload = sql.NullString{String: string(raw), Valid: true}
	}

	current, ok, err := s.Get(ctx, executionID)
	if err != nil {
		return err
	}
	if !ok {
		return gjob.ErrNotFound
	}
	if current.FencingToken != fencingToken {
		return gjob.ErrStaleAttempt
	}
	if current.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", gjob.ErrInvalidState, executionID, current.State)
	}

	_, err = s.db.ExecContext(ctx,
		`UPDATE gjob_receipt SET state = ?, result = ?, error = ?, updated_at = ?
		 WHERE execution_id = ? AND fencing_token = ?`,
		string(state), payload, truncate(failure, 1024), s.now(), executionID, fencingToken)
	if err != nil {
		return fmt.Errorf("gjob/mysql: transition: %w", err)
	}
	return s.explain(ctx, executionID, fencingToken)
}

// Get implements gjob.ReceiptStore.
func (s *Store) Get(ctx context.Context, executionID string) (gjob.Receipt, bool, error) {
	var (
		receipt   gjob.Receipt
		trigger   string
		state     string
		resultRaw sql.NullString
		failure   string
	)

	err := s.db.QueryRowContext(ctx,
		`SELECT execution_id, job_name, trigger_type, scheduled_at, attempt, state,
		        fencing_token, lease_until, updated_at, result, error
		 FROM gjob_receipt WHERE execution_id = ?`, executionID).
		Scan(&receipt.ExecutionID, &receipt.JobName, &trigger, &receipt.ScheduledAt, &receipt.Attempt,
			&state, &receipt.FencingToken, &receipt.LeaseUntil, &receipt.UpdatedAt, &resultRaw, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return gjob.Receipt{}, false, nil
	}
	if err != nil {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/mysql: get: %w", err)
	}

	receipt.TriggerType = gjob.TriggerType(trigger)
	receipt.State = gjob.State(state)
	receipt.Error = failure
	if resultRaw.Valid && resultRaw.String != "" {
		var result gjob.Result
		if err := json.Unmarshal([]byte(resultRaw.String), &result); err == nil {
			receipt.Result = &result
		}
	}
	return receipt, true, nil
}

// RecoverExpired implements gjob.ReceiptStore.
//
// The lease index makes this a bounded range scan; the batch limit keeps several
// nodes from stampeding the same rows.
func (s *Store) RecoverExpired(ctx context.Context, now time.Time, limit int) ([]gjob.Receipt, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT execution_id, fencing_token FROM gjob_receipt
		 WHERE lease_until <= ? AND state NOT IN (?, ?, ?, ?)
		 ORDER BY lease_until LIMIT ?`,
		now, string(gjob.StateSucceeded), string(gjob.StateFailed),
		string(gjob.StateCancelled), string(gjob.StateLost), limit)
	if err != nil {
		return nil, fmt.Errorf("gjob/mysql: recover scan: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		id      string
		fencing uint64
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.fencing); err != nil {
			return nil, fmt.Errorf("gjob/mysql: recover scan: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gjob/mysql: recover scan: %w", err)
	}

	var recovered []gjob.Receipt
	for _, c := range candidates {
		if err := s.transition(ctx, c.id, c.fencing, gjob.StateLost, nil, "lease expired"); err != nil {
			// Another node won the recovery; that is the expected race, not a failure.
			if errors.Is(err, gjob.ErrStaleAttempt) || errors.Is(err, gjob.ErrInvalidState) || errors.Is(err, gjob.ErrNotFound) {
				continue
			}
			return recovered, err
		}
		receipt, ok, err := s.Get(ctx, c.id)
		if err != nil {
			return recovered, err
		}
		if ok {
			recovered = append(recovered, receipt)
		}
	}
	return recovered, nil
}

// explain resolves why an update matched nothing: a missing receipt, a moved fencing
// token, or (with found-rows disabled) an update that changed nothing.
func (s *Store) explain(ctx context.Context, executionID string, fencingToken uint64) error {
	receipt, ok, err := s.Get(ctx, executionID)
	if err != nil {
		return err
	}
	if !ok {
		return gjob.ErrNotFound
	}
	if receipt.FencingToken != fencingToken {
		return gjob.ErrStaleAttempt
	}
	return nil
}

func isDuplicate(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlDuplicateEntry
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}
