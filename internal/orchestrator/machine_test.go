package orchestrator_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the machine starts a run with pipeline.started and its
// first stage only, holding later stages until job.finished events arrive
// (ADR 0001: the orchestrator owns the stage state machine; issue gh-16).

func TestStartPublishesOnlyFirstStage(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}

	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"started:run-1", "assign:run-1:build/compile"}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (started, first stage, nothing else)", got, want)
	}
}
