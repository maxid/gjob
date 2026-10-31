package gjob

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DispatcherConfig bounds local execution.
type DispatcherConfig struct {
	// Workers bounds concurrent executions across all jobs.
	Workers int
	// Queue bounds pending executions. A full queue is reported as ErrQueueFull: the
	// trigger is deferred, never silently dropped and never reported as success.
	Queue int
	// ShutdownGrace is how long Stop waits for running executions.
	ShutdownGrace time.Duration
	// Lease is the initial execution lease; heartbeats extend it.
	Lease time.Duration
	// HeartbeatInterval must be well below Lease.
	HeartbeatInterval time.Duration
	// Node identifies this worker in receipts.
	Node string
}

// DefaultDispatcherConfig is a conservative starting point.
func DefaultDispatcherConfig() DispatcherConfig {
	return DispatcherConfig{
		Workers:           4,
		Queue:             64,
		ShutdownGrace:     30 * time.Second,
		Lease:             time.Minute,
		HeartbeatInterval: 20 * time.Second,
	}
}

// Validate rejects a configuration that cannot keep its promises.
func (c *DispatcherConfig) Validate() error {
	if c.Workers <= 0 {
		return fmt.Errorf("%w: workers must be positive", ErrPermanent)
	}
	if c.Queue < 0 {
		return fmt.Errorf("%w: queue must not be negative", ErrPermanent)
	}
	if c.Lease <= 0 {
		return fmt.Errorf("%w: lease must be positive", ErrPermanent)
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.Lease / 3
	}
	if c.HeartbeatInterval >= c.Lease {
		return fmt.Errorf("%w: heartbeat %s must be shorter than lease %s", ErrPermanent, c.HeartbeatInterval, c.Lease)
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = DefaultDispatcherConfig().ShutdownGrace
	}
	return nil
}

// Dispatcher runs executions with a bounded worker pool and a bounded queue.
type Dispatcher struct {
	cfg      DispatcherConfig
	registry *Registry
	receipts ReceiptStore
	metrics  Metrics
	now      func() time.Time

	queue  chan Execution
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
	drain  sync.WaitGroup
}

// NewDispatcher creates a dispatcher. Call Start to launch the workers.
func NewDispatcher(cfg DispatcherConfig, registry *Registry, receipts ReceiptStore, opts ...DispatcherOption) (*Dispatcher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, fmt.Errorf("%w: registry is required", ErrPermanent)
	}
	if receipts == nil {
		return nil, fmt.Errorf("%w: receipt store is required", ErrPermanent)
	}

	d := &Dispatcher{
		cfg:      cfg,
		registry: registry,
		receipts: receipts,
		now:      time.Now,
		queue:    make(chan Execution, cfg.Queue),
	}
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// DispatcherOption configures a Dispatcher.
type DispatcherOption func(*Dispatcher)

// WithDispatcherMetrics installs a metrics sink.
func WithDispatcherMetrics(metrics Metrics) DispatcherOption {
	return func(d *Dispatcher) { d.metrics = metrics }
}

// WithDispatcherClock overrides the clock (tests).
func WithDispatcherClock(now func() time.Time) DispatcherOption {
	return func(d *Dispatcher) {
		if now != nil {
			d.now = now
		}
	}
}

// Start launches the workers.
func (d *Dispatcher) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	for i := 0; i < d.cfg.Workers; i++ {
		d.wg.Add(1)
		go d.worker()
	}
}

// Submit queues an execution. A full queue returns ErrQueueFull; the caller keeps the
// receipt PENDING so the trigger can be retried instead of lost.
func (d *Dispatcher) Submit(execution Execution) error {
	if err := execution.Validate(); err != nil {
		return err
	}

	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return ErrClosed
	}

	d.drain.Add(1)
	select {
	case d.queue <- execution:
		return nil
	default:
		d.drain.Done()
		if d.metrics != nil {
			d.metrics.TriggerDeferred(execution.JobName, "queue_full")
		}
		return ErrQueueFull
	}
}

// Stop stops accepting work, waits up to the grace period, then cancels the rest.
//
// Executions still running at the deadline have their context cancelled; their
// receipts stay RUNNING until the lease expires and the recoverer marks them LOST.
func (d *Dispatcher) Stop(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	close(d.queue)
	d.mu.Unlock()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// worker executes submissions in order until the queue closes.
func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for execution := range d.queue {
		d.run(execution)
		d.drain.Done()
	}
}

// run performs one execution: claim, execute, heartbeat, complete.
func (d *Dispatcher) run(execution Execution) {
	executor, ok := d.registry.Resolve(execution.JobName)
	if !ok {
		// Nothing to run: record the failure instead of losing the slot.
		_, _, _ = d.receipts.Claim(context.Background(), execution, d.cfg.Lease)
		_ = d.receipts.Complete(context.Background(), execution.ID, 0, StateFailed, nil, "executor not registered")
		return
	}

	ctx := context.Background()
	claimCtx, cancelClaim := context.WithCancel(ctx)
	defer cancelClaim()

	receipt, claimed, err := d.receipts.Claim(claimCtx, execution, d.cfg.Lease)
	if err != nil || !claimed {
		// Another node owns this execution, or the store failed; either way this
		// worker must not run it.
		return
	}
	if d.metrics != nil {
		d.metrics.TriggerClaimed(execution.JobName, execution.TriggerType)
	}

	// Record RUNNING before the work starts: a long execution must not look PENDING to
	// the recoverer.
	if err := d.receipts.Start(claimCtx, receipt.ExecutionID, receipt.FencingToken); err != nil {
		return
	}

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat keeps the lease alive; a lost lease cancels the work so it stops
	// producing side effects.
	stopHeartbeat := make(chan struct{})
	go d.heartbeat(execCtx, cancel, receipt, stopHeartbeat)
	defer close(stopHeartbeat)

	started := d.now()
	err = executor.Execute(execCtx, Execution{
		ID:           execution.ID,
		JobName:      execution.JobName,
		TriggerType:  execution.TriggerType,
		ScheduledAt:  execution.ScheduledAt,
		Attempt:      execution.Attempt,
		Parameters:   execution.Parameters,
		FencingToken: receipt.FencingToken,
	})
	finished := d.now()

	result := &Result{
		StartedAt:  started,
		FinishedAt: finished,
		Node:       d.cfg.Node,
	}

	state := StateSucceeded
	failure := ""
	if err != nil {
		state = StateFailed
		failure = truncate(err.Error(), 512)
		result.Code = "error"
	}

	_ = d.receipts.Complete(ctx, execution.ID, receipt.FencingToken, state, result, failure)
	if d.metrics != nil {
		d.metrics.ExecutionFinished(execution.JobName, state, finished.Sub(started))
	}
}

// heartbeat renews the lease until stop closes.
func (d *Dispatcher) heartbeat(ctx context.Context, cancel context.CancelFunc, receipt Receipt, stop <-chan struct{}) {
	ticker := time.NewTicker(d.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			until := d.now().Add(d.cfg.Lease)
			if err := d.receipts.Heartbeat(ctx, receipt.ExecutionID, receipt.FencingToken, until); err != nil {
				// The lease is gone: stop the work rather than let it keep writing.
				if d.metrics != nil {
					d.metrics.LeaseLost(receipt.JobName)
				}
				cancel()
				return
			}
		}
	}
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

// Metrics receives neutral counters.
type Metrics interface {
	TriggerClaimed(job string, trigger TriggerType)
	TriggerDeferred(job string, reason string)
	ExecutionFinished(job string, state State, duration time.Duration)
	LeaseLost(job string)
}
