package orchestrator_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
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

	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	loop := orchestrator.NewLoop(client, machine, nil, nil)
	loop.Block = 25 * time.Millisecond
	eventLoop := orchestrator.NewEventLoop(client, machine, nil)
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
	log := &fakeLogger{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, log)
	if err := machine.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	eventLoop := orchestrator.NewEventLoop(client, machine, log)
	eventLoop.Block = 25 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	eventsDone := make(chan error, 1)
	go func() { eventsDone <- eventLoop.Run(ctx) }()
	waitGroup(t, client, orchestrator.StreamEvents)

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
	// Exactly one entry here is malformed — the poisoned JSON — so one
	// decode trace; the foreign type and non-terminal slug are normal
	// traffic and stay silent by design.
	gotLogs := log.snapshot()
	if len(gotLogs) != 1 || !strings.HasPrefix(gotLogs[0], "warn:event envelope failed to decode err=") {
		t.Errorf("log lines = %v, want exactly the poisoned entry's decode trace (by-design skips stay silent)", gotLogs)
	}
}

// waitGroup fails the test unless the stream's consumer group exists —
// the event loop creates its group at `$`, so seeding must wait until the
// group's start id is pinned, or the first entries predate it.
func waitGroup(t *testing.T, client *redis.Client, stream string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		groups, err := client.XInfoGroups(context.Background(), stream).Result()
		if err == nil && len(groups) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("consumer group on %s never appeared", stream)
}

// AUD-005: malformed bus entries leave a trace before they are skipped —
// logged at warn, acknowledged, and never mistaken for the valid event
// behind them.
func TestEventLoopLogsMalformedEntries(t *testing.T) {
	j := &journal{}
	log := &fakeLogger{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	machine := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, log)
	if err := machine.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	eventLoop := orchestrator.NewEventLoop(client, machine, log)
	eventLoop.Block = 25 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	eventsDone := make(chan error, 1)
	go func() { eventsDone <- eventLoop.Run(ctx) }()
	waitGroup(t, client, orchestrator.StreamEvents)

	// No envelope field at all, undecodable JSON, an outcome without its
	// coordinates, an outcome without a status — then a non-terminal slug,
	// a foreign type, and the one valid job.finished.
	if err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: orchestrator.StreamEvents,
		Values: map[string]any{"other": "not an envelope"},
	}).Err(); err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}
	seedEvent(t, client, "not json {{")
	seedEvent(t, client, mustJSON(t, orchestrator.Event{
		Type: orchestrator.EventJobFinished, OccurredAt: time.Now().UTC(),
		Pipeline: "demo", Job: "compile",
		Payload: map[string]string{"status": orchestrator.StatusPassed},
	}))
	seedEvent(t, client, mustJSON(t, orchestrator.Event{
		Type: orchestrator.EventJobFinished, OccurredAt: time.Now().UTC(),
		RunID: "run-1", Pipeline: "demo", Job: "compile",
	}))
	seedEvent(t, client, mustJSON(t, orchestrator.Event{
		Type: orchestrator.EventJobFinished, OccurredAt: time.Now().UTC(),
		RunID: "run-1", Pipeline: "demo", Job: "compile",
		Payload: map[string]string{"status": "running"},
	}))
	seedEvent(t, client, mustJSON(t, orchestrator.Event{
		Type: "step.finished", OccurredAt: time.Now().UTC(),
		RunID: "run-1", Pipeline: "demo", Job: "compile", Step: "compile",
		Payload: map[string]string{"status": orchestrator.StatusPassed},
	}))
	seedEvent(t, client, mustJSON(t, orchestrator.Event{
		Type: orchestrator.EventJobFinished, OccurredAt: time.Now().UTC(),
		RunID: "run-1", Pipeline: "demo", Job: "compile",
		Payload: map[string]string{"status": orchestrator.StatusPassed},
	}))

	waitFor(t, j, 3) // started + first stage + the valid event's finish
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

	// The decode line carries the JSON decoder's message after err= —
	// its exact wording is not behavior, so match by prefix.
	wantLogs := []string{
		"warn:event entry has no envelope field",
		"warn:event envelope failed to decode err=",
		"warn:job.finished outcome is missing coordinates run_id= job=compile",
		"warn:job.finished outcome carries no status run_id=run-1 job=compile",
	}
	gotLogs := log.snapshot()
	if len(gotLogs) != len(wantLogs) {
		t.Fatalf("log lines = %v, want %d (each malformed entry traced, by-design skips silent)", gotLogs, len(wantLogs))
	}
	for i, want := range wantLogs {
		if !strings.HasPrefix(gotLogs[i], want) {
			t.Errorf("log line %d = %q, want prefix %q", i, gotLogs[i], want)
		}
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, []string{"started:run-1", "assign:run-1:build/compile", "finished:run-1:passed"}) {
		t.Errorf("journal = %v, want the valid event to finish run-1 passed", got)
	}
}

// mustJSON marshals ev or fails the test.
func mustJSON(t *testing.T, ev orchestrator.Event) string {
	t.Helper()
	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return string(body)
}
