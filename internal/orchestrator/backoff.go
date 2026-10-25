package orchestrator

import (
	"context"
	"time"
)

// Bounded backoff for the loops' transient-error retries: the wait doubles
// per consecutive failure and stops growing at BackoffCap, so a Redis blip
// costs one short wait while a Redis outage keeps the process alive —
// retrying — instead of crash-looping it back to an empty run table
// (AUD-007).
const (
	// BackoffBase is the first retry's wait.
	BackoffBase = 100 * time.Millisecond

	// BackoffCap bounds every retry wait.
	BackoffCap = 2 * time.Second

	// maxBackoffShift is the doubling step at which the clamp first
	// applies (BackoffBase << 4 = 1.6s is the last doubling below the
	// cap; every attempt past it returns BackoffCap).
	maxBackoffShift = 16
)

// BackoffDelay returns the wait before the attempt-th consecutive retry:
// doubling until BackoffCap, then flat.
func BackoffDelay(attempt int) time.Duration {
	shift := attempt
	if shift > maxBackoffShift {
		shift = maxBackoffShift
	}
	d := BackoffBase << uint(shift)
	if d > BackoffCap || d <= 0 { // the <= 0 arm guards an overflowed shift
		return BackoffCap
	}
	return d
}

// BackoffSleep waits out the attempt-th retry, returning early with the
// context's error when cancellation lands first.
func BackoffSleep(ctx context.Context, attempt int) error {
	timer := time.NewTimer(BackoffDelay(attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
