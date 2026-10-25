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
	"syscall"

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
