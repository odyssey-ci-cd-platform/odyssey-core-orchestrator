package orchestrator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the orchestrator publishes assignments to the job
// queue one entry per job, in the contract's § Job assignment shape, and
// refuses invalid assignments at the wire (dispatcher contract §§ Streams,
// Job assignment; issue gh-14).

func newTestPublisher(t *testing.T) (*orchestrator.Publisher, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return orchestrator.NewPublisher(client), client
}

// segmentedAssignments derives real assignments from the valid run request
// envelope, so publish tests exercise the actual segmentation output.
func segmentedAssignments(t *testing.T) []orchestrator.Assignment {
	t.Helper()
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	return assignments
}

func jobQueueEntries(t *testing.T, client *redis.Client) []string {
	t.Helper()
	entries, err := client.XRange(context.Background(), orchestrator.StreamJobs, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}
	envelopes := make([]string, 0, len(entries))
	for _, entry := range entries {
		body, ok := entry.Values["envelope"].(string)
		if !ok {
			t.Fatalf("entry values = %v, want an envelope string", entry.Values)
		}
		envelopes = append(envelopes, body)
	}
	return envelopes
}

func TestPublishWritesOneEntryPerAssignment(t *testing.T) {
	pub, client := newTestPublisher(t)
	assignments := segmentedAssignments(t)

	for _, a := range assignments {
		if err := pub.Publish(context.Background(), a); err != nil {
			t.Fatalf("Publish(%s/%s) error = %v, want nil", a.Stage, a.Job, err)
		}
	}

	envelopes := jobQueueEntries(t, client)
	if len(envelopes) != len(assignments) {
		t.Fatalf("stream has %d entries, want %d (one per assignment)", len(envelopes), len(assignments))
	}
	for i, a := range assignments {
		var got orchestrator.Assignment
		if err := json.Unmarshal([]byte(envelopes[i]), &got); err != nil {
			t.Fatalf("envelope %d is not assignment JSON: %v\n%s", i, err, envelopes[i])
		}
		if !reflect.DeepEqual(got, a) {
			t.Errorf("envelope %d = %+v, want %+v (wire round-trip)", i, got, a)
		}
	}
}

// The door holds at the wire: an assignment violating the contract's field
// rules is refused, and the queue stays empty.
func TestPublishRefusesInvalidAssignment(t *testing.T) {
	cases := []struct {
		name string
		bend func(a *orchestrator.Assignment)
	}{
		{"empty run id", func(a *orchestrator.Assignment) { a.RunID = "" }},
		{"empty stage", func(a *orchestrator.Assignment) { a.Stage = "" }},
		{"empty job name", func(a *orchestrator.Assignment) { a.Job = "" }},
		{"attempt below one", func(a *orchestrator.Assignment) { a.Attempt = 0 }},
		{"missing source commit", func(a *orchestrator.Assignment) { a.Source.Commit = "" }},
		{"job spec without image", func(a *orchestrator.Assignment) { a.JobSpec.Image = "" }},
		{"job spec without steps", func(a *orchestrator.Assignment) { a.JobSpec.Steps = nil }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			pub, client := newTestPublisher(t)
			assignments := segmentedAssignments(t)
			tt.bend(&assignments[0])

			err := pub.Publish(context.Background(), assignments[0])
			if err == nil {
				t.Fatal("Publish() error = nil, want refusal")
			}
			if !strings.Contains(err.Error(), "refusing to publish") {
				t.Errorf("error = %q, want refusal wording", err)
			}
			if envelopes := jobQueueEntries(t, client); len(envelopes) != 0 {
				t.Errorf("stream has %d entries, want 0 (refused, not enqueued)", len(envelopes))
			}
		})
	}
}

// mustCompact compacts s into canonical wire JSON — the marshaler embeds
// RawMessage compacted, insignificant whitespace apart.
func mustCompact(t *testing.T, s string) json.RawMessage {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(s)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	return buf.Bytes()
}

// The dead-letter wrap rides the wire in the contract's shape — reason
// plus the original, embedded as `request` when the envelope was JSON and
// as `raw` when it was not (dispatcher contract § Run request). Note:
// written after PublishDeadLetter shipped; the dead-letter behavior
// itself went red-first in TestLoopDeadLettersPoisonEntries.
func TestPublishDeadLetterWritesWrap(t *testing.T) {
	cases := []struct {
		name string
		want orchestrator.DeadLetter
	}{
		{
			"validation wraps the original envelope",
			orchestrator.DeadLetter{Reason: orchestrator.DeadReasonValidation, Request: mustCompact(t, validEnvelope)},
		},
		{
			"decode wraps the raw bytes",
			orchestrator.DeadLetter{Reason: orchestrator.DeadReasonDecode, Raw: "not json {{"},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			pub, client := newTestPublisher(t)

			if err := pub.PublishDeadLetter(context.Background(), tt.want); err != nil {
				t.Fatalf("PublishDeadLetter() error = %v, want nil", err)
			}

			entries, err := client.XRange(context.Background(), orchestrator.StreamPipelineRequestsDead, "-", "+").Result()
			if err != nil {
				t.Fatalf("XRange() error = %v", err)
			}
			if len(entries) != 1 {
				t.Fatalf("stream has %d entries, want 1", len(entries))
			}
			body, ok := entries[0].Values["envelope"].(string)
			if !ok {
				t.Fatalf("entry values = %v, want an envelope string", entries[0].Values)
			}
			var got orchestrator.DeadLetter
			if err := json.Unmarshal([]byte(body), &got); err != nil {
				t.Fatalf("envelope is not dead-letter JSON: %v\n%s", err, body)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("wrap = %+v, want %+v (wire round-trip)", got, tt.want)
			}
		})
	}
}
