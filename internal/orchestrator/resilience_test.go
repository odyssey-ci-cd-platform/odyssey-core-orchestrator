package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the loops survive transient Redis errors around their
// reads with bounded backoff instead of returning from Run — a process
// restart loses every in-memory run (ADR 0001, decision 6), which is
// exactly the failure AUD-007 flags — while a decision error still exits
// so the entry stays pending for the next start's recovery (gh-20), and
// cancellation still returns nil (issue gh-24).

// loopRunner is what both loops expose: serve until cancellation.
type loopRunner interface {
	Run(ctx context.Context) error
}

func startLoopAgainst(t *testing.T, proc loopRunner) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- proc.Run(ctx) }()
	return done, cancel
}

// hasNotReturned fails the test if Run returned within the window.
func hasNotReturned(t *testing.T, done <-chan error, window time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Run() returned early with %v, want it to keep retrying through the transient error", err)
	case <-time.After(window):
	}
}

func TestLoopRetriesTransientReadErrors(t *testing.T) {
	j := &journal{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	loop := orchestrator.NewLoop(client, machine, nil, nil)
	loop.Block = 25 * time.Millisecond
	done, cancel := startLoopAgainst(t, loop)

	// Redis erroring at the wire: the loop retries, it does not return.
	mr.SetError("MINIREDIS-DOWN")
	hasNotReturned(t, done, 400*time.Millisecond)

	// The blip clears; the loop resumes consuming.
	mr.SetError("")
	seed(t, client, validEnvelope)
	waitFor(t, j, 2) // started + first stage
	waitAcked(t, client, orchestrator.StreamPipelineRequests)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func TestEventLoopRetriesTransientReadErrors(t *testing.T) {
	j := &journal{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	// An in-flight run the bus outcome can resolve; the sinks are fakes,
	// so the machine's own writes never touch Redis.
	req, err := orchestrator.DecodeRunRequest([]byte(validEnvelope))
	if err != nil {
		t.Fatalf("DecodeRunRequest() error = %v", err)
	}
	assignments, err := orchestrator.Segment(req, "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := machine.Start(context.Background(), "run-1", req.Pipeline, assignments); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	eventLoop := orchestrator.NewEventLoop(client, machine, nil)
	eventLoop.Block = 25 * time.Millisecond
	done, cancel := startLoopAgainst(t, eventLoop)

	mr.SetError("MINIREDIS-DOWN")
	hasNotReturned(t, done, 400*time.Millisecond)

	mr.SetError("")
	finished, err := json.Marshal(orchestrator.Event{
		Type:       orchestrator.EventJobFinished,
		OccurredAt: time.Now().UTC(),
		RunID:      "run-1",
		Pipeline:   req.Pipeline,
		Job:        "compile",
		Payload:    map[string]string{"status": orchestrator.StatusPassed},
	})
	if err != nil {
		t.Fatalf("marshal job.finished: %v", err)
	}
	seedEvent(t, client, string(finished))
	waitFor(t, j, 2) // started (from Start) + the released second stage

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func TestBackoffDelayIsBounded(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1600 * time.Millisecond},
		{5, orchestrator.BackoffCap},
		{9, orchestrator.BackoffCap},
		{1000, orchestrator.BackoffCap},
	}
	for _, c := range cases {
		if got := orchestrator.BackoffDelay(c.attempt); got != c.want {
			t.Errorf("BackoffDelay(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffSleepReturnsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := orchestrator.BackoffSleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("BackoffSleep(cancelled ctx) error = %v, want context.Canceled", err)
	}
}
