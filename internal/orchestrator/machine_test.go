package orchestrator_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// Requirement trace: the machine starts a run with pipeline.started and its
// first stage only, holding later stages until job.finished events arrive
// (ADR 0001: the orchestrator owns the stage state machine; issue gh-16).

func TestStartPublishesOnlyFirstStage(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start(run-1) error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-2", "demo", singleStageAssignments(t, "run-2")); err != nil {
		t.Fatalf("Start(run-2) error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-2", "compile", 1, orchestrator.StatusPassed); err != nil {
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusFailed); err != nil {
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusFailed); err != nil {
		t.Fatalf("JobFinished(compile) error = %v, want nil", err)
	}
	if got := j.snapshot(); len(got) != 3 {
		t.Fatalf("journal = %v, want no decision while lint is still pending", got)
	}

	if err := m.JobFinished(context.Background(), "run-1", "lint", 1, orchestrator.StatusPassed); err != nil {
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
			m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
			if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
				t.Fatalf("Start() error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "compile", 1, tt.first); err != nil {
				t.Fatalf("JobFinished(compile) error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "lint", 1, tt.second); err != nil {
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
			m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
			if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
				t.Fatalf("Start() error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "compile", 1, tt.first); err != nil {
				t.Fatalf("JobFinished(compile) error = %v, want nil", err)
			}
			if err := m.JobFinished(context.Background(), "run-1", "lint", 1, tt.second); err != nil {
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)

	err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1"))
	if err == nil {
		t.Fatal("Start() error = nil, want the pipeline.started emission failure to abort the start")
	}
	for _, e := range j.snapshot() {
		if strings.HasPrefix(e, "assign:") {
			t.Errorf("journal has %q, want no assignment published for an aborted start", e)
		}
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
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
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	before := j.snapshot()
	_ = m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed)
	_ = m.JobFinished(context.Background(), "run-9", "compile", 1, orchestrator.StatusPassed)

	if got := j.snapshot(); !reflect.DeepEqual(got, before) {
		t.Errorf("journal = %v, want unchanged (%v)", got, before)
	}
}

// A nil logger degrades to discarding: the degraded paths never panic and
// never fail the run.
func TestMachineToleratesNilLogger(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	want := []string{"started:run-1", "assign:run-1:build/compile", "finished:run-1:passed"}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (the discard logger changes nothing)", got, want)
	}
}

// AUD-005: a degraded emission leaves a trace through the logger seam —
// visible with the run id, without failing the run (the platform's
// degradation policy, dispatcher repo ADR 0001). Since AUD-015 a failing
// pipeline.started aborts the start instead of degrading — that abort is
// traced too, and the finished emission keeps the degrade policy.
func TestMachineLogsDegradedEmissions(t *testing.T) {
	j := &journal{failEmit: true}
	log := &fakeLogger{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, log)
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err == nil {
		t.Fatal("Start() error = nil, want the started-emission abort")
	}
	hasLog(t, log, "warn:pipeline.started emission failed", "run_id=run-1")

	j.failEmit = false
	if err := m.Start(context.Background(), "run-2", "demo", singleStageAssignments(t, "run-2")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	j.failEmit = true
	if err := m.JobFinished(context.Background(), "run-2", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}
	hasLog(t, log, "warn:pipeline.finished emission failed", "run_id=run-2")
}

// Emission failure on pipeline.finished must not corrupt state: the run is
// already over, so later outcomes for it change nothing.
func TestMachineEndsRunEvenWhenFinishedEmitFails(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	// The started emission succeeded; only the finished emission fails
	// (since AUD-015 a failing started emission aborts the start itself).
	j.failEmit = true
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	before := j.snapshot()
	_ = m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed)

	if got := j.snapshot(); !reflect.DeepEqual(got, before) {
		t.Errorf("journal = %v, want unchanged (%v) — the run ended despite the emission failure", got, before)
	}
}

// flakyAssignments fails every publish for one stage name — the transient
// release failure AUD-001 reproduces with. The test clears failStage to
// model the failure clearing.
type flakyAssignments struct {
	j         *journal
	failStage string // publishes for this stage fail; empty means none fail
}

func (f *flakyAssignments) Publish(_ context.Context, a orchestrator.Assignment) error {
	if a.Stage == f.failStage {
		return errors.New("redis down")
	}
	f.j.record("assign:" + a.RunID + ":" + a.Stage + "/" + a.Job)
	return nil
}

// AUD-001: a stage-release publish failure must not strand the run. The
// failing publish surfaces the error with the run state rolled back — the
// triggering job stays pending — so the outcome's unacknowledged entry,
// redelivered once the failure clears, re-applies the outcome and the
// release goes through; the run then finishes normally.
func TestMachineRetriesReleaseAfterFailedPublish(t *testing.T) {
	j := &journal{}
	sink := &flakyAssignments{j: j, failStage: "test"}
	m := orchestrator.NewMachine(fakeEvents{j}, sink, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err == nil {
		t.Fatal("JobFinished() error = nil, want the release publish failure surfaced")
	}

	// The transient failure clears; the event loop redelivers the still-
	// unacknowledged outcome.
	sink.failStage = ""
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("redelivered JobFinished() error = %v, want the release retried to success", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "test", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished(test) error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"assign:run-1:test/test",
		"finished:run-1:passed",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (release retried after the failure cleared, run finished)", got, want)
	}
}

// AUD-004: a reclaimed job re-executes under the same run id with an
// incremented attempt (the pending contract revision, gh-25). The machine
// accepts a terminal outcome whose attempt exceeds the recorded one for a
// job still pending — the reclaim re-execution's outcome replaces the lost
// first attempt — and ignores an attempt below the recorded one. Attempt 1
// on a pending job stays the normal path.
func TestMachineAcceptsReclaimAttemptOutcome(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	// An attempt below the recorded one cannot resolve the job.
	if err := m.JobFinished(context.Background(), "run-1", "compile", 0, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished(attempt 0) error = %v, want nil", err)
	}
	if got := j.snapshot(); len(got) != 2 {
		t.Fatalf("journal = %v, want the sub-recorded attempt ignored", got)
	}

	// The reclaim re-execution's outcome — attempt 2, the first attempt's
	// outcome lost — replaces it: stage 2 releases under the same run id.
	if err := m.JobFinished(context.Background(), "run-1", "compile", 2, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished(attempt 2) error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"assign:run-1:test/test",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (attempt 2 outcome replaced the lost first attempt)", got, want)
	}
}

// fakeClock is the reaper's injectable time source (AUD-003): tests
// advance it past a run's deadline without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// AUD-003: a run whose outcome never arrives cannot hang forever. Once the
// default deadline passes, the reaper resolves the run as errored —
// pipeline.finished carries status errored, the stage-2 release never
// happens — evicts it, and later outcomes for the run change nothing.
func TestMachineReapsRunPastDeadline(t *testing.T) {
	j := &journal{}
	clock := &fakeClock{now: time.Now()}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	m.Clock = clock.Now
	if err := m.Start(context.Background(), "run-1", "demo", twoJobFirstStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	clock.Advance(2 * time.Hour) // past NewMachine's default deadline
	m.Reap(context.Background())

	if err := m.JobFinished(context.Background(), "run-1", "lint", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("late JobFinished() error = %v, want nil", err)
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"assign:run-1:build/lint",
		"finished:run-1:errored",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (deadline resolved the lost-outcome run errored, then evicted it)", got, want)
	}
}

// The deadline is machine-level and configurable: a shorter deadline reaps
// a run the default would still keep, and a scan inside the deadline
// leaves the run untouched.
func TestMachineDeadlineIsConfigurable(t *testing.T) {
	j := &journal{}
	clock := &fakeClock{now: time.Now()}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	m.Deadline = 30 * time.Minute
	m.Clock = clock.Now
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	clock.Advance(29 * time.Minute)
	m.Reap(context.Background())
	if got := j.snapshot(); len(got) != 2 {
		t.Fatalf("journal = %v, want the run untouched inside its deadline", got)
	}

	clock.Advance(16 * time.Minute) // 45m: past the 30m deadline
	m.Reap(context.Background())

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"finished:run-1:errored",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (the 30m deadline resolved the run errored)", got, want)
	}
}

// Degradation policy holds at the deadline too: a failed pipeline.finished
// emission is logged and the eviction stands — the resolved run cannot be
// stranded back into the table.
func TestMachineDeadlineEvictionSurvivesEmitFailure(t *testing.T) {
	j := &journal{}
	log := &fakeLogger{}
	clock := &fakeClock{now: time.Now()}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, log)
	m.Clock = clock.Now
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	j.failEmit = true

	clock.Advance(2 * time.Hour)
	m.Reap(context.Background())

	hasLog(t, log, "warn:run deadline exceeded", "run_id=run-1")
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("late JobFinished() error = %v, want nil", err)
	}
	if got := j.snapshot(); len(got) != 3 {
		t.Errorf("journal = %v, want the run evicted despite the emission failure", got)
	}
}

// The reaper scans on a ticker: a run past its deadline is resolved while
// RunReaper serves, and RunReaper returns nil on cancellation.
func TestMachineRunReaperScansOnTicker(t *testing.T) {
	j := &journal{}
	clock := &fakeClock{now: time.Now()}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	m.ReapInterval = 25 * time.Millisecond
	m.Clock = clock.Now
	if err := m.Start(context.Background(), "run-1", "demo", singleStageAssignments(t, "run-1")); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.RunReaper(ctx) }()

	clock.Advance(2 * time.Hour)
	deadline := time.Now().Add(2 * time.Second)
	for len(j.snapshot()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunReaper() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunReaper() did not return after cancellation")
	}

	want := []string{
		"started:run-1",
		"assign:run-1:build/compile",
		"finished:run-1:errored",
	}
	if got := j.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("journal = %v, want %v (the ticker-driven reaper resolved the run errored)", got, want)
	}
}

// The ack-lost ordering stays exactly-once: a release that already
// published is not re-published by a redelivered outcome — the job is no
// longer pending, so the duplicate is ignored.
func TestMachineReleaseIsNotDuplicatedByRedelivery(t *testing.T) {
	j := &journal{}
	m := orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil)
	assignments, err := orchestrator.Segment(mustDecode(t, validEnvelope), "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v, want nil", err)
	}
	if err := m.Start(context.Background(), "run-1", "demo", assignments); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if err := m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed); err != nil {
		t.Fatalf("JobFinished() error = %v, want nil", err)
	}

	before := j.snapshot()
	_ = m.JobFinished(context.Background(), "run-1", "compile", 1, orchestrator.StatusPassed)

	if got := j.snapshot(); !reflect.DeepEqual(got, before) {
		t.Errorf("journal = %v, want unchanged (%v) — no duplicate release for the redelivered outcome", got, before)
	}
}
