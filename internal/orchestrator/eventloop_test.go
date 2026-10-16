package orchestrator_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the orchestrator consumes job.finished from the event
// bus — alongside the bus's other consumers — and drives the machine with
// it; events that are malformed, unknown, or unsequenceable at this
// boundary are skipped and acknowledged (issue gh-16; contract §§ Streams,
// Event emission responsibilities).

// startLoops runs both loops against one machine over miniredis, returning
// the client and two stop functions.
func startLoops(t *testing.T, j *journal) (*redis.Client, func()) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	loop := orchestrator.NewLoop(client, machine)
	loop.Block = 25 * time.Millisecond
	eventLoop := orchestrator.NewEventLoop(client, machine)
	eventLoop.Block = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	requestsDone := make(chan error, 1)
	eventsDone := make(chan error, 1)
	go func() { requestsDone <- loop.Run(ctx) }()
	go func() { eventsDone <- eventLoop.Run(ctx) }()
	return client, func() {
		cancel()
		for _, done := range []<-chan error{requestsDone, eventsDone} {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("loop error = %v, want nil on cancellation", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("a loop did not return after cancellation")
			}
		}
	}
}

// seedEvent adds one bus entry carrying body as its single envelope field.
func seedEvent(t *testing.T, client *redis.Client, body string) {
	t.Helper()
	err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: orchestrator.StreamEvents,
		Values: map[string]any{"envelope": body},
	}).Err()
	if err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}
}

func TestEventLoopDrivesMachineFromTheBus(t *testing.T) {
	j := &journal{}
	client, stop := startLoops(t, j)

	seed(t, client, validEnvelope)
	waitFor(t, j, 2) // started + first stage
	id := runIDOf(j.snapshot()[1])

	body, err := json.Marshal(orchestrator.Event{
		Type:       orchestrator.EventJobFinished,
		OccurredAt: time.Now().UTC(),
		RunID:      id,
		Pipeline:   "demo",
		Job:        "compile",
		Payload:    map[string]string{"status": orchestrator.StatusPassed},
	})
	if err != nil {
		t.Fatalf("marshal job.finished: %v", err)
	}
	seedEvent(t, client, string(body))

	waitFor(t, j, 3) // stage 2 released through the bus
	waitAcked(t, client, orchestrator.StreamPipelineRequests)
	waitAcked(t, client, orchestrator.StreamEvents)
	stop()

	if got := j.snapshot()[2]; got != "assign:"+id+":test/test" {
		t.Errorf("third entry = %q, want stage 2 released under run id %q", got, id)
	}
}

func TestEventLoopServesOnlySequenceableEvents(t *testing.T) {
	j := &journal{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	if err := machine.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	eventLoop := orchestrator.NewEventLoop(client, machine)
	eventLoop.Block = 25 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	eventsDone := make(chan error, 1)
	go func() { eventsDone <- eventLoop.Run(ctx) }()

	// Poison the stream ahead of the one sequenceable event: malformed
	// JSON, another producer's event type, a non-terminal status slug.
	seedEvent(t, client, "not json {{")
	unknown, err := json.Marshal(orchestrator.Event{
		Type: "step.finished", OccurredAt: time.Now().UTC(),
		RunID: "run-1", Job: "compile", Step: "compile",
		Payload: map[string]string{"status": orchestrator.StatusPassed},
	})
	if err != nil {
		t.Fatalf("marshal step.finished: %v", err)
	}
	seedEvent(t, client, string(unknown))
	badStatus, err := json.Marshal(orchestrator.Event{
		Type: orchestrator.EventJobFinished, OccurredAt: time.Now().UTC(),
		RunID: "run-1", Job: "compile",
		Payload: map[string]string{"status": "running"},
	})
	if err != nil {
		t.Fatalf("marshal non-terminal job.finished: %v", err)
	}
	seedEvent(t, client, string(badStatus))

	good, err := json.Marshal(orchestrator.Event{
		Type: orchestrator.EventJobFinished, OccurredAt: time.Now().UTC(),
		RunID: "run-1", Job: "compile",
		Payload: map[string]string{"status": orchestrator.StatusPassed},
	})
	if err != nil {
		t.Fatalf("marshal job.finished: %v", err)
	}
	seedEvent(t, client, string(good))

	waitFor(t, j, 3)
	waitAcked(t, client, orchestrator.StreamEvents)
	cancel()
	select {
	case err := <-eventsDone:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}

	want := []string{"started:run-1", "assign:run-1:build/compile", "finished:run-1:passed"}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (only the sequenceable event reached the machine)", got, want)
	}
}
