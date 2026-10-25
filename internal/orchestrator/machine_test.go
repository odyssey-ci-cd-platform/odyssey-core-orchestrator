package orchestrator_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
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

// twoJobFirstStageAssignments segments a variant of the valid envelope
// whose first stage carries a second job, under runID.
func twoJobFirstStageAssignments(t *testing.T, runID string) []orchestrator.Assignment {
	t.Helper()
	req := mustDecode(t, validEnvelope)
	req.Config.Jobs["lint"] = pipeline.Job{
		Stage: "build",
		Image: "alpine",
		Steps: []pipeline.Step{{Name: "lint", Run: "make lint"}},
	}
	assignments, err := orchestrator.Segment(req, runID)
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	return assignments
}

// Fail-fast (gh-82): a stage that resolves Failed ends the run —
// pipeline.finished carries `failed`, and no later stage is released.
func TestMachineEndsRunOnFailedStage(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusFailed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"finished:run-1:failed",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (run ended failed, stage 2 never released)", got, want)
	}
}

// The decision waits for full stage resolution: a Failed job does not end
// the run while a sibling job of the stage is still pending (the engine
// awaits all jobs of the stage), and a sibling's Passed cannot heal the
// stage's worst status.
func TestMachineWaitsForFullStageResolution(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusFailed); err != nil {
		t.Fatalf("JobFinished(compile) error = %v, want nil", err)
	}
	if got := j.snapshot(); len(got) != 3 {
		t.Fatalf("journal = %v, want no decision while lint is still pending", got)
	}

	if err := m.JobFinished(context.Background(), "run-1", "lint", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished(lint) error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"assign:run-1:build/lint",
		"finished:run-1:failed",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (stage resolved, run ended failed)", got, want)
	}
}

// AUD-010 ranking: a stage with both Failed and Errored outcomes ends the
// run with `errored`, whichever order the outcomes arrive in.
func TestMachineReportsErroredAsWorst(t *testing.T) {
	cases := []struct {
		name   string
		first  string
		second string
	}{
		{"failed then errored", orchestrator.StatusFailed, orchestrator.StatusErrored},
		{"errored then failed", orchestrator.StatusErrored, orchestrator.StatusFailed},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			j := &journal{}
			m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
			if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
				t.Fatalf("Start() error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "compile", tt.first); err != nil {
				t.Fatalf("JobFinished(compile) error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "lint", tt.second); err != nil {
				t.Fatalf("JobFinished(lint) error = %v, want nil", err)
			}

			want := []string{
				"started:run-1",
				"assign:run-1:build/compile",
				"assign:run-1:build/lint",
				"finished:run-1:errored",
			}
			if got := j.snapshot(); !reflect.DeepEqual(got, want) {
				t.Errorf("journal = %v, want %v (errored outranks failed)", got, want)
			}
		})
	}
}

// AUD-015: a slug the machine cannot classify ranks worst — worse than
// errored, never as success — in either arrival order, and the run reports
// the contract's errored slug (pipeline.finished's payload status is pinned
// to passed/failed/errored; contract § Event emission responsibilities).
func TestMachineRanksUnknownSlugWorst(t *testing.T) {
	cases := []struct {
		name   string
		first  string
		second string
	}{
		{"unknown then passed", "detonated", orchestrator.StatusPassed},
		{"unknown then failed", "detonated", orchestrator.StatusFailed},
		{"unknown then errored", "detonated", orchestrator.StatusErrored},
		{"passed then unknown", orchestrator.StatusPassed, "detonated"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			j := &journal{}
			m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
			if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
				t.Fatalf("Start() error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "compile", tt.first); err != nil {
				t.Fatalf("JobFinished(compile) error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "lint", tt.second); err != nil {
				t.Fatalf("JobFinished(lint) error = %v, want nil", err)
			}

			want := []string{
				"started:run-1",
				"assign:run-1:build/compile",
				"assign:run-1:build/lint",
				"finished:run-1:errored",
			}
			if got := j.snapshot(); !reflect.DeepEqual(got, want) {
				t.Errorf("journal = %v, want %v (unclassifiable slug outranks every known slug, reported as errored)", got, want)
			}
		})
	}
}

// AUD-015: the started emission precedes registration — a pipeline.started
// emission failure aborts the start: the error surfaces (the queue entry
// stays pending), no assignment is published, and the run is not tracked.
func TestStartAbortsWhenStartedEmitFails(t *testing.T) {
	j := &journal{failEmit: true}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})

	err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1"))
	if err == nil {
		t.Fatal("Start() error = nil, want the pipeline.started emission failure to abort the start")
	}
	for _, e := range j.snapshot() {
		if strings.HasPrefix(e, "assign:") {
			t.Errorf("journal has %q, want no assignment published for an aborted start", e)
		}
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}
	for _, e := range j.snapshot() {
		if strings.HasPrefix(e, "assign:") {
			t.Errorf("journal has %q after a later outcome, want the aborted run untracked", e)
		}
	}
}

// After a run ends — or for a run the machine never knew — job outcomes
// change nothing: no duplicate finishes, no releases.
func TestMachineIgnoresOutcomesForUntrackedRuns(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	before := j.snapshot()
	_ = m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed)
	_ = m.JobFinished(context.Background(), "run-9", "compile", orchestrator.StatusPassed)

	if got := j.snapshot(); !reflect.DeepEqual(got, before) {
		t.Errorf("journal = %v, want unchanged (%v)", got, before)
	}
}

// Emission failure on pipeline.finished must not corrupt state: the run is
// already over, so later outcomes for it change nothing.
func TestMachineEndsRunEvenWhenFinishedEmitFails(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j})
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	// The started emission succeeded; only the finished emission fails
	// (since AUD-015 a failing started emission aborts the start itself).
	j.failEmit = true
	if err := m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	before := j.snapshot()
	_ = m.JobFinished(context.Background(), "run-1", "compile", orchestrator.StatusPassed)

	if got := j.snapshot(); !reflect.DeepEqual(got, before) {
		t.Errorf("journal = %v, want unchanged (%v) — the run ended despite the emission failure", got, before)
	}
}
