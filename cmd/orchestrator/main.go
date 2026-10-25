// The orchestrator binary: env configuration, a structured logger through
// the package's Logger seams, one signal context driving both loops, and a
// clean shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// runner is what main drives until cancellation — both loops satisfy it.
type runner interface {
	Run(ctx context.Context) error
}

func main() {
	if err := run(os.Getenv, os.Stderr); err != nil {
		os.Exit(1)
	}
}

// renewLease keeps the single-instance lease held on its tick. A lost
// lease does not kill the process — in-memory runs would die with it —
// but it is logged and recorded so health can report it (the v1 limit
// RUNBOOK documents).
func renewLease(ctx context.Context, lease *orchestrator.Lease, wasLost func() bool, setLost func(bool), log *slog.Logger) {
	ticker := time.NewTicker(orchestrator.LeaseRenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			held, err := lease.Renew(ctx)
			if err != nil {
				log.Error("lease renewal failed", "err", err)
				continue
			}
			if held {
				setLost(false)
				continue
			}
			if !wasLost() {
				log.Error("orchestrator lease lost: the key expired or changed hands; serving degraded until restarted")
			}
			setLost(true)
		}
	}
}

// run wires the service and blocks until a signal or the first loop
// failure, then shuts the rest down. A signal exits nil; a failed loop
// exits non-zero after cancelling its sibling.
func run(getenv func(string) string, stderr io.Writer) error {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	cfg, err := LoadConfig(getenv)
	if err != nil {
		log.Error("invalid configuration", "err", err)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	// Single-instance lease (AUD-007): without it, a second replica would
	// split the consumer groups across processes that each know only their
	// own runs. Fail fast — including on an unreachable Redis — and let
	// the deployment's restart policy retry, rather than half-serve.
	lease, err := orchestrator.AcquireLease(ctx, rdb)
	if err != nil {
		log.Error("cannot take the orchestrator lease", "err", err)
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lease.Release(releaseCtx) // the signal context is already cancelled by now
	}()
	var leaseLost atomic.Bool
	go renewLease(ctx, lease, leaseLost.Load, func(b bool) { leaseLost.Store(b) }, log)

	publisher := orchestrator.NewPublisher(rdb)
	publisher.MaxLen = cfg.JobsMaxLen
	publisher.MaxDeadLen = cfg.DeadMaxLen
	machine := orchestrator.NewMachine(orchestrator.NewEmitter(rdb), publisher, log)
	queueLoop := orchestrator.NewLoop(rdb, machine, publisher, log)
	eventLoop := orchestrator.NewEventLoop(rdb, machine, log)

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, r := range []struct {
		name string
		proc runner
	}{
		{"pipeline queue loop", queueLoop},
		{"event loop", eventLoop},
	} {
		wg.Add(1)
		go func(name string, proc runner) {
			defer wg.Done()
			if err := proc.Run(ctx); err != nil {
				errs <- fmt.Errorf("%s: %w", name, err)
			}
		}(r.name, r.proc)
	}

	var runErr error
	select {
	case runErr = <-errs:
		log.Error("loop failed; shutting down", "err", runErr)
		stop() // cancel the sibling loop
	case <-ctx.Done():
		log.Info("signal received; shutting down")
	}
	wg.Wait()
	return runErr
}
