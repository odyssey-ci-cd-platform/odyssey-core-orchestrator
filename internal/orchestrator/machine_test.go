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

// Completing every job of the current stage Passed releases the next stage
// under the same run id (ADR 0001: releases stage N+1 on success).
func TestMachineReleasesNextStageWhenStagePasses(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"assign:run-1:test/test",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (stage 2 released under the same run id)", got, want)
	}
}

// singleStageAssignments segments a one-stage one-job variant of the valid
// envelope under runID.
func singleStageAssignments(t *testing.T, runID string) []orchestrator.Assignment {
	t.Helper()
	req := mustDecode(t, validEnvelope)
	req.Config.Stages = []string{"build"}
	delete(req.Config.Jobs, "test")
	assignments, err := orchestrator.Segment(req, runID)
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	return assignments
}

// The last stage resolving Passed ends the run: pipeline.finished carries
// the run id and payload status passed (contract § Event emission
// responsibilities).
func TestMachineFinishesSingleStageRun(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"finished:run-1:passed",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (run finished passed)", got, want)
	}
}

// Concurrent runs sequence independently: an event for one run never
// releases or finishes another.
func TestMachineSequencesRunsIndependently(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start(run-1) error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-2", "demo", singleStageAssignments(t, "run-2")); err != nil {
		t.Fatalf("Start(run-2) error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-2", "compile", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	got := j.snapshot()
	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"started:run-2",
		"assign:run-2:build/compile",
		"finished:run-2:passed",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (run-1 untouched by run-2's outcome)", got, want)
	}
}
