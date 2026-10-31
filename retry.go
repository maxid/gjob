package gjob

import (
	"math/rand"
	"time"
)

// Backoff computes the delay before the next attempt.
//
// The jitter is symmetric: several workers failing at the same instant must not all
// retry at the same instant.
func Backoff(policy RetryPolicy, attempt int, rng *rand.Rand) time.Duration {
	if policy.Initial <= 0 {
		policy.Initial = time.Second
	}
	if policy.Max <= 0 {
		policy.Max = 5 * time.Minute
	}
	if attempt < 1 {
		attempt = 1
	}

	delay := float64(policy.Initial)
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= float64(policy.Max) {
			delay = float64(policy.Max)
			break
		}
	}

	if policy.Jitter > 0 && rng != nil {
		// ±Jitter around the base delay.
		spread := delay * policy.Jitter
		delay += (rng.Float64()*2 - 1) * spread
	}
	if delay < 0 {
		delay = 0
	}
	if delay > float64(policy.Max) {
		delay = float64(policy.Max)
	}
	return time.Duration(delay)
}

// ShouldRetry reports whether another attempt is allowed.
func ShouldRetry(policy RetryPolicy, attempt int) bool {
	if policy.MaxAttempts <= 0 {
		return false
	}
	return attempt < policy.MaxAttempts
}
