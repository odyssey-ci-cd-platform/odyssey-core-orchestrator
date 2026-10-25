package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the one stateful component in the platform must be
// observable — in-flight runs and stage-release latency, at minimum
// (issue gh-24; AUD-013).

// slowAssignments makes each assignment publish take the given duration,
// so a recorded latency is distinguishable from zero.
type slowAssignments struct {
	j *journal
	d time.Duration
}

func (s slowAssignments) Publish(_ context.Context, a orchestrator.Assignment) error {
	s.j.record("assign:" + a.RunID + ":" + a.Stage + "/" + a.Job)
	time.Sleep(s.d)
	return nil
}

func TestInFlightCountsTrackedRuns(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if n := m.InFlightRuns(); n != 0 {
		t.Fatalf("InFlightRuns() = %d on a fresh machine, want 0", n)
	}

	a1, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	a2, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-2")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", a1); err != nil {
		t.Fatalf("Start(run-1) error = %v", err)
	}
	if err := m.Start(context.Background(), "run-2", "demo", a2); err != nil {
		t.Fatalf("Start(run-2) error = %v", err)
	}
	if n := m.InFlightRuns(); n != 2 {
		t.Errorf("InFlightRuns() = %d with two started runs, want 2", n)
	}

	// A single-stage run resolves on its one job's outcome.
	single := singleStageAssignments(t, "run-3")
	if err := m.Start(context.Background(), "run-3", "demo", single); err != nil {
		t.Fatalf("Start(run-3) error = %v", err)
	}
	if err := m.JobFinished(context.Background(), "run-3", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v", err)
	}
	if n := m.InFlightRuns(); n != 2 {
		t.Errorf("InFlightRuns() = %d after run-3 finished, want 2", n)
	}
}

func TestLastReleaseLatencyIsRecorded(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, slowAssignments{j: j, d: 5 * time.Millisecond}, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if d := m.LastReleaseLatency(); d != 0 {
		t.Fatalf("LastReleaseLatency() = %v before any release, want 0", d)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v", err)
	}
	if d := m.LastReleaseLatency(); d < 5*time.Millisecond {
		t.Errorf("LastReleaseLatency() = %v, want at least the fake sink's 5ms", d)
	}
}

// A release publish failure is a decision error, not a recorded release:
// the latency must not update (the last successful release is what
// metrics should show).
func TestLastReleaseLatencyUnchangedOnFailedRelease(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, &flakyAssignments{j: j, failStage: "test"}, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err == nil {
		t.Fatalf("JobFinished() error = nil, want the release publish failure")
	}
	if d := m.LastReleaseLatency(); d != 0 {
		t.Errorf("LastReleaseLatency() = %v after a failed release, want 0", d)
	}
}
