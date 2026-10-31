package gjob

import "errors"

// The stable errors. Callers match on these rather than on driver text.
var (
	// ErrClosed means the scheduler or dispatcher was stopped.
	ErrClosed = errors.New("gjob: closed")
	// ErrPermanent means retrying cannot help.
	ErrPermanent = errors.New("gjob: permanent error")
	// ErrQueueFull means the dispatcher's bounded queue is full. The trigger must be
	// recorded as deferred rather than reported as success.
	ErrQueueFull = errors.New("gjob: queue full")
	// ErrNotFound means the job name is not registered.
	ErrNotFound = errors.New("gjob: not found")
	// ErrLeaseLost means the lease expired or was taken over; the executor must stop
	// producing external side effects.
	ErrLeaseLost = errors.New("gjob: lease lost")
	// ErrStaleAttempt means a write arrived with an outdated fencing token.
	ErrStaleAttempt = errors.New("gjob: stale attempt")
	// ErrAlreadyRegistered means a job name was registered twice.
	ErrAlreadyRegistered = errors.New("gjob: already registered")
	// ErrInvalidState means a receipt transition is not allowed from the current
	// state.
	ErrInvalidState = errors.New("gjob: invalid state transition")
)
