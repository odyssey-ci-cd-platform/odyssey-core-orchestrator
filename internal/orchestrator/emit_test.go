package orchestrator_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the orchestrator emits pipeline.started on the event
// bus in the dispatcher repo's Event envelope shape, with no job, step, or
// payload for this event (dispatcher contract § Event emission
// responsibilities; issue gh-14).

func newTestEmitter(t *testing.T) (*orchestrator.Emitter, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return orchestrator.NewEmitter(client), client
}

func TestEmitWritesEventEnvelope(t *testing.T) {
	emitter, client := newTestEmitter(t)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	started := orchestrator.Event{
		Type:       orchestrator.EventPipelineStarted,
		OccurredAt: at,
		RunID:      "run-1",
		Pipeline:   "demo",
	}

	if err := emitter.Publish(context.Background(), started); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}

	entries, err := client.XRange(context.Background(), orchestrator.StreamEvents, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream has %d entries, want 1", len(entries))
	}
	envelope, ok := entries[0].Values["envelope"].(string)
	if !ok {
		t.Fatalf("entry values = %v, want an envelope string", entries[0].Values)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(envelope), &doc); err != nil {
		t.Fatalf("envelope is not JSON: %v\n%s", err, envelope)
	}
	if doc["type"] != orchestrator.EventPipelineStarted {
		t.Errorf("type = %v, want %q", doc["type"], orchestrator.EventPipelineStarted)
	}
	if doc["run_id"] != "run-1" || doc["pipeline"] != "demo" {
		t.Errorf("run_id/pipeline = %v/%v, want run-1/demo", doc["run_id"], doc["pipeline"])
	}
	stamped, err := time.Parse(time.RFC3339Nano, doc["occurred_at"].(string))
	if err != nil {
		t.Fatalf("occurred_at %v is not RFC3339: %v", doc["occurred_at"], err)
	}
	if !stamped.Equal(at) {
		t.Errorf("occurred_at = %v, want %v (round-trips through the wire)", stamped, at)
	}
	// pipeline.started carries no job, step, or payload: the contract pins
	// this event's payload to none, and the parent-row consumer keys on the
	// absence being meaningful.
	for _, absent := range []string{"job", "step", "payload"} {
		if _, ok := doc[absent]; ok {
			t.Errorf("envelope carries %q: %s", absent, envelope)
		}
	}
}

// The contract pins each event's envelope fields (§ Event emission
// responsibilities): every event names its type, run, and pipeline; the two
// events this repo emits carry exactly their pinned fields — pipeline.started
// with no job, step, or payload; pipeline.finished with no job or step and a
// worst-stage status payload (AUD-015).
func TestEventValidatePinsContractFields(t *testing.T) {
	cases := []struct {
		name string
		ev   orchestrator.Event
		want string // error substring; empty means valid
	}{
		{"valid started", orchestrator.Event{
			Type: orchestrator.EventPipelineStarted, RunID: "run-1", Pipeline: "demo",
		}, ""},
		{"valid finished", orchestrator.Event{
			Type: orchestrator.EventPipelineFinished, RunID: "run-1", Pipeline: "demo",
			Payload: map[string]string{"status": orchestrator.StatusPassed},
		}, ""},
		{"started without run id", orchestrator.Event{
			Type: orchestrator.EventPipelineStarted, Pipeline: "demo",
		}, "run_id must not be empty"},
		{"finished without pipeline", orchestrator.Event{
			Type: orchestrator.EventPipelineFinished, RunID: "run-1",
			Payload: map[string]string{"status": orchestrator.StatusPassed},
		}, "pipeline must not be empty"},
		{"started with payload", orchestrator.Event{
			Type: orchestrator.EventPipelineStarted, RunID: "run-1", Pipeline: "demo",
			Payload: map[string]string{"stage": "build"},
		}, "carries no payload"},
		{"started with job", orchestrator.Event{
			Type: orchestrator.EventPipelineStarted, RunID: "run-1", Pipeline: "demo",
			Job: "compile",
		}, "carries no job or step"},
		{"finished with step", orchestrator.Event{
			Type: orchestrator.EventPipelineFinished, RunID: "run-1", Pipeline: "demo",
			Step:    "compile",
			Payload: map[string]string{"status": orchestrator.StatusPassed},
		}, "carries no job or step"},
		{"finished with unknown status", orchestrator.Event{
			Type: orchestrator.EventPipelineFinished, RunID: "run-1", Pipeline: "demo",
			Payload: map[string]string{"status": "detonated"},
		}, "passed, failed, or errored"},
		{"finished without status", orchestrator.Event{
			Type: orchestrator.EventPipelineFinished, RunID: "run-1", Pipeline: "demo",
		}, "passed, failed, or errored"},
		{"foreign event type", orchestrator.Event{
			Type: orchestrator.EventJobFinished, RunID: "run-1", Pipeline: "demo",
			Job: "compile",
		}, "unknown event type"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ev.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

// The degradation policy is the platform's (dispatcher repo ADR 0001): the
// bus never being reachable must not stop execution — but a marshaling
// failure means the event is silently lost, so the emitter still surfaces
// it as an error for the caller's policy.
func TestEmitRejectsUnnamedEvent(t *testing.T) {
	emitter, client := newTestEmitter(t)

	err := emitter.Publish(context.Background(), orchestrator.Event{RunID: "run-1"})
	if err == nil {
		t.Fatal("Publish() error = nil, want refusal for an unnamed event")
	}
	if !strings.Contains(err.Error(), "type must not be empty") {
		t.Errorf("error = %q, want missing-type wording", err)
	}
	if entries, _ := client.XRange(context.Background(), orchestrator.StreamEvents, "-", "+").Result(); len(entries) != 0 {
		t.Errorf("stream has %d entries, want 0 (refused, not enqueued)", len(entries))
	}
}
