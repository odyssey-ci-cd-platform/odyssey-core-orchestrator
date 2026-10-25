package orchestrator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: v1 runs one orchestrator process, and nothing used to
// enforce it — a second replica would split the consumer groups across
// machines that each know only their own runs (AUD-007). A Redis lease
// (SET NX PX + compare-and-renew) makes the second instance fail fast at
// startup (issue gh-24).

func newLeaseRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func TestLeaseAcquireBlocksSecondInstance(t *testing.T) {
	mr, client := newLeaseRedis(t)
	ctx := context.Background()

	first, err := orchestrator.AcquireLease(ctx, client)
	if err != nil {
		t.Fatalf("AcquireLease() error = %v, want the lease taken", err)
	}
	if first.Token() == "" {
		t.Error("Token() is empty, want a random holder token")
	}
	if ttl := mr.TTL(orchestrator.LeaseKey); ttl <= 0 || ttl > orchestrator.LeaseTTL {
		t.Errorf("lease TTL = %v, want (0, %v]", ttl, orchestrator.LeaseTTL)
	}

	second, err := orchestrator.AcquireLease(ctx, client)
	if !errors.Is(err, orchestrator.ErrLeaseHeld) {
		t.Fatalf("second AcquireLease() error = %v, want ErrLeaseHeld", err)
	}
	if second != nil {
		t.Errorf("second AcquireLease() lease = %v, want nil", second)
	}
}

func TestLeaseRenewExtendsOnlyForHolder(t *testing.T) {
	mr, client := newLeaseRedis(t)
	ctx := context.Background()

	lease, err := orchestrator.AcquireLease(ctx, client)
	if err != nil {
		t.Fatalf("AcquireLease() error = %v", err)
	}

	// Half the TTL burns down; a renewal hands the full TTL back.
	mr.FastForward(orchestrator.LeaseTTL / 2)
	held, err := lease.Renew(ctx)
	if err != nil || !held {
		t.Fatalf("Renew() = (%v, %v), want (true, nil)", held, err)
	}
	if ttl := mr.TTL(orchestrator.LeaseKey); ttl <= orchestrator.LeaseTTL/2 {
		t.Errorf("TTL after renewal = %v, want the full %v handed back", ttl, orchestrator.LeaseTTL)
	}

	// A key this lease does not own is never renewed.
	if _, err := client.Set(ctx, orchestrator.LeaseKey, "someone-else", orchestrator.LeaseTTL).Result(); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	held, err = lease.Renew(ctx)
	if err != nil {
		t.Fatalf("Renew() error = %v, want a clean not-held report", err)
	}
	if held {
		t.Error("Renew() = true for a key holding another token, want false")
	}
}

func TestLeaseExpiresAndFreesTheKey(t *testing.T) {
	mr, client := newLeaseRedis(t)
	ctx := context.Background()

	lease, err := orchestrator.AcquireLease(ctx, client)
	if err != nil {
		t.Fatalf("AcquireLease() error = %v", err)
	}
	mr.FastForward(orchestrator.LeaseTTL + time.Second)
	held, err := lease.Renew(ctx)
	if err != nil {
		t.Fatalf("Renew() error = %v, want a clean not-held report", err)
	}
	if held {
		t.Error("Renew() = true after the lease expired, want false")
	}
	if _, err := orchestrator.AcquireLease(ctx, client); err != nil {
		t.Fatalf("AcquireLease() after expiry error = %v, want the lease takeable", err)
	}
}

func TestLeaseReleaseDeletesOnlyOwnKey(t *testing.T) {
	mr, client := newLeaseRedis(t)
	ctx := context.Background()

	lease, err := orchestrator.AcquireLease(ctx, client)
	if err != nil {
		t.Fatalf("AcquireLease() error = %v", err)
	}
	lease.Release(ctx)
	if mr.Exists(orchestrator.LeaseKey) {
		t.Error("lease key survives Release(), want it deleted")
	}

	if _, err := client.Set(ctx, orchestrator.LeaseKey, "someone-else", orchestrator.LeaseTTL).Result(); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	lease.Release(ctx) // token mismatch: someone-else's key must survive
	if !mr.Exists(orchestrator.LeaseKey) {
		t.Error("Release() deleted a key holding another token, want it kept")
	}
}
