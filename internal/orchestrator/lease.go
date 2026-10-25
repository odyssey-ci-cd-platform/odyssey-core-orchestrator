package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// The single-instance lease (AUD-007): the orchestrator's run table is in
// memory, so two live instances would each sequence only the runs they
// started while one consumer name feeds them both — split brain. The lease
// is a Redis key held with SET NX PX and renewed by compare-and-expire; a
// second instance's AcquireLease fails fast instead of half-serving. v1
// keeps the constant consumer name deliberately: the lease, not the name,
// is what enforces single-instance operation.
const (
	// LeaseKey is the Redis key holding the orchestrator's lease.
	LeaseKey = "odyssey:orchestrator-lease"

	// LeaseTTL is how long an unrenewed lease stays held. Comfortably
	// above several renewal intervals, like the contract's claim-renewal
	// parameters on the dispatcher side.
	LeaseTTL = 15 * time.Second

	// LeaseRenewEvery is the renewal tick: an instance that cannot renew
	// for a full TTL has lost the lease.
	LeaseRenewEvery = 5 * time.Second
)

// ErrLeaseHeld reports that another instance holds the lease: this process
// must not start.
var ErrLeaseHeld = errors.New("another orchestrator instance holds the lease")

// renewScript extends the lease only when the stored token is still this
// holder's; the same compare shape drives Release.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// releaseScript deletes the lease only when it is still this holder's.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// Lease is a held single-instance lease. Acquire it once at startup and
// renew it on the LeaseRenewEvery tick.
type Lease struct {
	client redis.UniversalClient
	token  string
}

// AcquireLease takes the single-instance lease, failing with ErrLeaseHeld
// when another instance holds it and surfacing Redis errors (an
// unreachable Redis cannot be leased) so the caller can fail fast.
func AcquireLease(ctx context.Context, client redis.UniversalClient) (*Lease, error) {
	token := leaseToken()
	ok, err := client.SetNX(ctx, LeaseKey, token, LeaseTTL).Result()
	if err != nil {
		return nil, fmt.Errorf("acquiring orchestrator lease: %w", err)
	}
	if !ok {
		return nil, ErrLeaseHeld
	}
	return &Lease{client: client, token: token}, nil
}

// Token is the lease's random holder identity.
func (l *Lease) Token() string { return l.token }

// Renew extends the lease if it is still this holder's. A false held (with
// a nil error) means the key expired or changed hands: this instance is no
// longer the single orchestrator and must say so. Transport errors surface.
func (l *Lease) Renew(ctx context.Context) (bool, error) {
	res, err := renewScript.Run(ctx, l.client, []string{LeaseKey}, l.token, LeaseTTL.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("renewing orchestrator lease: %w", err)
	}
	return res == 1, nil
}

// Release gives the lease up (graceful shutdown), deleting the key only
// when this holder still owns it. Best-effort: a release failure on a
// dying process only costs the TTL's remaining seconds.
func (l *Lease) Release(ctx context.Context) {
	_ = releaseScript.Run(ctx, l.client, []string{LeaseKey}, l.token).Err()
}

// leaseToken mints the lease's random holder identity; the same shape as
// the run id's, because both are opaque 128-bit hex nonces.
func leaseToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}
