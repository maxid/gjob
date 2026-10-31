// Package redisstore implements gjob.ReceiptStore on Redis.
//
// Every transition is a single atomic command or Lua script, because the whole
// point of the store is that two nodes cannot both start the same execution.
package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gjob"
)

// claimScript stores the receipt only if the key is absent, returning 1 when it
// created it. SETNX + EXPIRE in one script keeps a crash from leaving a key without
// a TTL.
const claimScript = `
if redis.call("exists", KEYS[1]) == 1 then return 0 end
redis.call("set", KEYS[1], ARGV[1])
redis.call("expire", KEYS[1], ARGV[2])
redis.call("zadd", KEYS[2], ARGV[3], ARGV[4])
return 1
`

// heartbeatScript extends the lease only for the current fencing token.
const heartbeatScript = `
local raw = redis.call("get", KEYS[1])
if not raw then return -1 end
local receipt = cjson.decode(raw)
if tonumber(receipt.FencingToken) ~= tonumber(ARGV[1]) then return -2 end
receipt.LeaseUntil = ARGV[2]
receipt.UpdatedAt = ARGV[3]
redis.call("set", KEYS[1], cjson.encode(receipt), "keepttl")
redis.call("zadd", KEYS[2], ARGV[4], ARGV[5])
return 1
`

// completeScript writes the terminal or retry state, checking the fencing token.
const completeScript = `
local raw = redis.call("get", KEYS[1])
if not raw then return -1 end
local receipt = cjson.decode(raw)
if tonumber(receipt.FencingToken) ~= tonumber(ARGV[1]) then return -2 end
receipt.State = ARGV[2]
receipt.Error = ARGV[3]
receipt.Result = cjson.decode(ARGV[4])
receipt.UpdatedAt = ARGV[5]
redis.call("set", KEYS[1], cjson.encode(receipt), "keepttl")
redis.call("zrem", KEYS[2], ARGV[6])
return 1
`

// Store is a Redis-backed receipt store.
type Store struct {
	client goredis.UniversalClient
	prefix string
	fence  string
	now    func() time.Time
	ttl    time.Duration
}

// Option configures a Store.
type Option func(*Store)

// WithPrefix sets the key prefix. It also moves the fencing counter under the
// same prefix so two applications sharing Redis cannot mix receipt namespaces.
func WithPrefix(prefix string) Option {
	return func(s *Store) {
		if prefix != "" {
			s.prefix = prefix
			s.fence = prefix + "fencing"
		}
	}
}

// WithNamespace derives the receipt and fencing keys from a plain namespace:
// "<namespace>:job:<execution>" and "<namespace>:job:fencing". It is the
// explicit alternative to WithPrefix for applications that share Redis.
func WithNamespace(namespace string) Option {
	return func(s *Store) {
		namespace = strings.Trim(namespace, ":")
		if namespace == "" {
			return
		}
		s.prefix = namespace + ":job:"
		s.fence = namespace + ":job:fencing"
	}
}

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// WithRetention bounds how long a finished receipt lives. Receipts are diagnostics,
// not the source of business state, so they expire.
func WithRetention(ttl time.Duration) Option {
	return func(s *Store) {
		if ttl > 0 {
			s.ttl = ttl
		}
	}
}

// New creates a store over the given client.
func New(client goredis.UniversalClient, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gjob.ErrPermanent)
	}
	s := &Store{
		client: client,
		prefix: "gjob:receipt:",
		fence:  "gjob:fencing",
		now:    time.Now,
		ttl:    7 * 24 * time.Hour,
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Claim implements gjob.ReceiptStore.
func (s *Store) Claim(ctx context.Context, execution gjob.Execution, lease time.Duration) (gjob.Receipt, bool, error) {
	if err := execution.Validate(); err != nil {
		return gjob.Receipt{}, false, err
	}

	fencing, err := s.client.Incr(ctx, s.fence).Result()
	if err != nil {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/redis: fencing: %w", err)
	}

	now := s.now()
	receipt := gjob.Receipt{
		ExecutionID:  execution.ID,
		JobName:      execution.JobName,
		TriggerType:  execution.TriggerType,
		ScheduledAt:  execution.ScheduledAt,
		Attempt:      execution.Attempt,
		State:        gjob.StatePending,
		FencingToken: uint64(fencing),
		LeaseUntil:   now.Add(lease),
		UpdatedAt:    now,
	}

	raw, err := json.Marshal(receipt)
	if err != nil {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/redis: encode receipt: %w", err)
	}

	created, err := s.client.Eval(ctx, claimScript, []string{s.key(execution.ID), s.leaseKey()},
		string(raw), int(s.ttl.Seconds()), receipt.LeaseUntil.Unix(), execution.ID).Int64()
	if err != nil {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/redis: claim: %w", err)
	}
	if created != 0 {
		return receipt, true, nil
	}

	// Someone else owns it: return the existing receipt so the caller can see why.
	existing, ok, err := s.Get(ctx, execution.ID)
	if err != nil {
		return gjob.Receipt{}, false, err
	}
	if !ok {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/redis: claim lost but no receipt exists")
	}
	return existing, false, nil
}

// Start implements gjob.ReceiptStore.
func (s *Store) Start(ctx context.Context, executionID string, fencingToken uint64) error {
	return s.completeWith(ctx, executionID, fencingToken, gjob.StateRunning, nil, "")
}

// Heartbeat implements gjob.ReceiptStore.
func (s *Store) Heartbeat(ctx context.Context, executionID string, fencingToken uint64, until time.Time) error {
	code, err := s.client.Eval(ctx, heartbeatScript, []string{s.key(executionID), s.leaseKey()},
		fencingToken, until.Format(time.RFC3339Nano), s.now().Format(time.RFC3339Nano),
		until.Unix(), executionID).Int64()
	if err != nil {
		return fmt.Errorf("gjob/redis: heartbeat: %w", err)
	}
	return classifyCode(code, executionID)
}

// Complete implements gjob.ReceiptStore.
func (s *Store) Complete(ctx context.Context, executionID string, fencingToken uint64, state gjob.State, result *gjob.Result, failure string) error {
	return s.completeWith(ctx, executionID, fencingToken, state, result, failure)
}

func (s *Store) completeWith(ctx context.Context, executionID string, fencingToken uint64, state gjob.State, result *gjob.Result, failure string) error {
	payload := "null"
	if result != nil {
		raw, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("gjob/redis: encode result: %w", err)
		}
		payload = string(raw)
	}

	code, err := s.client.Eval(ctx, completeScript, []string{s.key(executionID), s.leaseKey()},
		fencingToken, string(state), failure, payload,
		s.now().Format(time.RFC3339Nano), executionID).Int64()
	if err != nil {
		return fmt.Errorf("gjob/redis: complete: %w", err)
	}
	return classifyCode(code, executionID)
}

// Get implements gjob.ReceiptStore.
func (s *Store) Get(ctx context.Context, executionID string) (gjob.Receipt, bool, error) {
	raw, err := s.client.Get(ctx, s.key(executionID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return gjob.Receipt{}, false, nil
	}
	if err != nil {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/redis: get: %w", err)
	}

	var receipt gjob.Receipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return gjob.Receipt{}, false, fmt.Errorf("gjob/redis: decode receipt: %w", err)
	}
	return receipt, true, nil
}

// RecoverExpired implements gjob.ReceiptStore.
//
// The lease index is a sorted set scored by expiry, so recovery reads only the
// expired slice instead of scanning every receipt.
func (s *Store) RecoverExpired(ctx context.Context, now time.Time, limit int) ([]gjob.Receipt, error) {
	if limit <= 0 {
		limit = 100
	}

	ids, err := s.client.ZRangeByScore(ctx, s.leaseKey(), &goredis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.Unix(), 10), Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("gjob/redis: recover scan: %w", err)
	}

	var recovered []gjob.Receipt
	for _, id := range ids {
		receipt, ok, err := s.Get(ctx, id)
		if err != nil {
			return recovered, err
		}
		if !ok || receipt.State.Terminal() || receipt.State == gjob.StateLost {
			// Stale index entry: drop it so the scan stays short.
			s.client.ZRem(ctx, s.leaseKey(), id)
			continue
		}

		if err := s.Complete(ctx, id, receipt.FencingToken, gjob.StateLost, nil, "lease expired"); err != nil {
			// A concurrent writer moved it first; that is fine.
			if errors.Is(err, gjob.ErrStaleAttempt) || errors.Is(err, gjob.ErrInvalidState) {
				continue
			}
			return recovered, err
		}
		receipt.State = gjob.StateLost
		receipt.Error = "lease expired"
		recovered = append(recovered, receipt)
	}
	return recovered, nil
}

// classifyCode maps a script return code to the library error.
func classifyCode(code int64, executionID string) error {
	switch code {
	case 1:
		return nil
	case -1:
		return gjob.ErrNotFound
	case -2:
		return gjob.ErrStaleAttempt
	default:
		return fmt.Errorf("gjob/redis: unexpected script result %d for %s", code, executionID)
	}
}

func (s *Store) key(executionID string) string { return s.prefix + executionID }

func (s *Store) leaseKey() string { return s.prefix + "lease" }
