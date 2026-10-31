package gjob

import (
	"context"
	"fmt"
	"sync"
)

// Executor runs one execution. It must honour ctx cancellation, because that is how
// a timeout or a lost lease reaches the work.
//
// Returning nil means the work is done; the framework still cannot undo partial side
// effects, which is why a lease/lost attempt is reported as a failure rather than
// assumed harmless.
type Executor interface {
	Execute(ctx context.Context, execution Execution) error
}

// ExecutorFunc adapts a function.
type ExecutorFunc func(ctx context.Context, execution Execution) error

// Execute implements Executor.
func (f ExecutorFunc) Execute(ctx context.Context, execution Execution) error {
	return f(ctx, execution)
}

// Registry maps job names to executors. It is filled before Start and read-only
// afterwards: replacing an executor while executions are running would make a
// receipt describe code that no longer exists.
type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
	sealed    bool
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{executors: map[string]Executor{}}
}

// Register adds an executor. Duplicate names, empty names and nil executors are
// configuration errors, not silent overwrites.
func (r *Registry) Register(name string, executor Executor) error {
	if name == "" {
		return fmt.Errorf("%w: executor name is required", ErrPermanent)
	}
	if executor == nil {
		return fmt.Errorf("%w: executor %q is nil", ErrPermanent, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.sealed {
		return fmt.Errorf("%w: registry is sealed", ErrPermanent)
	}
	if _, exists := r.executors[name]; exists {
		return fmt.Errorf("%w: %q", ErrAlreadyRegistered, name)
	}
	r.executors[name] = executor
	return nil
}

// Seal makes the registry read-only. Called once before the scheduler starts.
func (r *Registry) Seal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
}

// Resolve looks up an executor.
func (r *Registry) Resolve(name string) (Executor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[name]
	return executor, ok
}

// Names lists the registered job names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.executors))
	for name := range r.executors {
		names = append(names, name)
	}
	return names
}
