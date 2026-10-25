package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Job outcome slugs (contract § Event emission responsibilities: the
// dispatcher's Status values). The machine sequences on these.
const (
	StatusPassed  = "passed"
	StatusFailed  = "failed"
	StatusErrored = "errored"
)

// Machine sequences a run's stages: the stage state machine ADR 0001 gives
// this repo ownership of. It is the one stateful component in the platform
// — a run table keyed by run id, guarded by a mutex because the two queue
// loops drive it concurrently. v1 keeps state in memory only: a restart
// loses in-flight runs, and events for unknown runs are ignored (ADR 0001,
// decision 6; reconciliation from the results DB is v2).
type Machine struct {
	mu          sync.Mutex
	events      EventSink
	assignments AssignmentSink
	log         Logger
	runs        map[string]*run

	// Deadline is a run's maximum lifetime, measured from Start to
	// resolution. A run whose outcome has not arrived when the deadline
	// passes resolves as errored and is evicted (Reap, AUD-003): the event
	// bus is lossy and never replayed, so the deadline is the bound on a
	// lost job.finished. Zero or negative disables deadline reaping.
	// NewMachine sets a generous default; a deployment config may override
	// it.
	Deadline time.Duration

	// ReapInterval is how often RunReaper scans the run table. Zero or
	// negative leaves reaping to explicit Reap calls. NewMachine sets a
	// default; a deployment config may override it.
	ReapInterval time.Duration

	// Clock is the reaper's time source; NewMachine sets time.Now. Tests
	// inject a fake to advance time without sleeping.
	Clock func() time.Time
}

// run is one in-flight pipeline run: its stages grouped in declared order,
// the index of the next stage to release, the current stage's unfinished
// job names, the worst job outcome resolved so far, and when the run
// started (the reaper's deadline anchor).
type run struct {
	pipeline  string
	stages    [][]Assignment
	next      int
	pending   map[string]bool
	worst     string
	startedAt time.Time
}

// NewMachine returns a Machine emitting through events, publishing
// assignments through assignments, and logging degraded paths through log
// (a nil log discards).
func NewMachine(events EventSink, assignments AssignmentSink, log Logger) *Machine {
	if log == nil {
		log = discardLogger{}
	}
	return &Machine{
		events:       events,
		assignments:  assignments,
		log:          log,
		runs:         make(map[string]*run),
		Deadline:     time.Hour,
		ReapInterval: 30 * time.Second,
		Clock:        time.Now,
	}
}

// Start hands off one segmented run: pipeline.started first — the results
// DB's parent-row invariant — then the first stage only. Later stages wait
// on JobFinished. The run registers only after its started event is on the
// bus (AUD-015): an emission failure aborts the start — the error returns,
// nothing is tracked, no job is published — because a tracked run whose
// parent row can never exist is exactly the stranding the invariant
// forbids. A stage-1 publish failure rolls the run back out of the table.
func (m *Machine) Start(ctx context.Context, runID, pipeline string, assignments []Assignment) error {
	stages := groupByStage(assignments)
	if len(stages) == 0 {
		return nil // a validated request always has a job; nothing to sequence
	}

	if err := m.events.Publish(ctx, Event{
		Type:       EventPipelineStarted,
		OccurredAt: time.Now(),
		RunID:      runID,
		Pipeline:   pipeline,
	}); err != nil {
		// Abort, but leave a trace: the queue entry stays pending and the
		// error surfaces, yet nothing downstream sees why without this.
		m.log.Warn("pipeline.started emission failed", "run_id", runID, "err", err)
		return fmt.Errorf("emitting pipeline.started for %s: %w", runID, err)
	}

	r := &run{
		pipeline:  pipeline,
		stages:    stages,
		next:      1,
		pending:   jobNames(stages[0]),
		worst:     StatusPassed,
		startedAt: m.Clock(),
	}
	m.mu.Lock()
	m.runs[runID] = r
	m.mu.Unlock()

	if err := m.publishStage(ctx, r.stages[0], runID); err != nil {
		m.mu.Lock()
		delete(m.runs, runID)
		m.mu.Unlock()
		return err
	}
	return nil
}

// publishStage publishes one stage's assignments under the run id.
func (m *Machine) publishStage(ctx context.Context, stage []Assignment, runID string) error {
	for _, a := range stage {
		if err := m.assignments.Publish(ctx, a); err != nil {
			return err
		}
	}
	return nil
}

// Reap resolves every tracked run whose deadline has passed: it ends the
// run with pipeline.finished carrying errored and evicts it (AUD-003) —
// a lost or delayed job.finished delays a run's resolution but cannot
// hang it. Eviction commits before the emission, so an emission failure
// (which degrades, never fails) cannot strand the resolved run back into
// the table. A deadline of zero or less disables reaping.
func (m *Machine) Reap(ctx context.Context) {
	if m.Deadline <= 0 {
		return
	}
	type expired struct {
		id, pipeline string
	}
	var overdue []expired

	m.mu.Lock()
	now := m.Clock()
	for id, r := range m.runs {
		if now.Sub(r.startedAt) >= m.Deadline {
			overdue = append(overdue, expired{id: id, pipeline: r.pipeline})
			delete(m.runs, id)
		}
	}
	m.mu.Unlock()

	for _, e := range overdue {
		m.log.Warn("run deadline exceeded", "run_id", e.id, "deadline", m.Deadline.String())
		m.emitFinished(ctx, e.id, e.pipeline, StatusErrored)
	}
}

// RunReaper scans the run table every ReapInterval until ctx is cancelled
// (returning nil), applying the deadline (Reap). It serves beside the two
// queue loops; a non-positive interval returns immediately, leaving
// reaping to explicit Reap calls.
func (m *Machine) RunReaper(ctx context.Context) error {
	if m.ReapInterval <= 0 {
		return nil
	}
	ticker := time.NewTicker(m.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.Reap(ctx)
		}
	}
}

// groupByStage splits a stage-ordered assignment slice into per-stage
// groups. Segment orders assignments by declared stage, so grouping is
// consecutive.
func groupByStage(assignments []Assignment) [][]Assignment {
	stages := make([][]Assignment, 0, 1)
	for _, a := range assignments {
		if n := len(stages); n > 0 && stages[n-1][0].Stage == a.Stage {
			stages[n-1] = append(stages[n-1], a)
		} else {
			stages = append(stages, []Assignment{a})
		}
	}
	return stages
}

// jobNames extracts a stage's job names into a set.
func jobNames(stage []Assignment) map[string]bool {
	names := make(map[string]bool, len(stage))
	for _, a := range stage {
		names[a.Job] = true
	}
	return names
}

// JobFinished applies one dispatcher job outcome to its run. The job
// leaves the current stage's pending set; the decision waits for full
// stage resolution, matching the engine's recorded behavior (gh-82,
// AUD-015): a stage that resolves all-Passed releases the next stage
// under the same run id, or finishes the run with pipeline.finished
// carrying `passed` when it was the last; a stage resolving Failed or
// Errored ends the run with the run's worst status (AUD-010 ranking) and
// no further stages. Unknown run ids and duplicate or stray jobs are
// ignored — after a restart lost in-flight state there is nothing to
// sequence.
//
// A stage release publishes before it commits: when the publish fails,
// the advance is rolled back — the triggering job returns to the pending
// set — and the error surfaces, so the outcome's unacknowledged entry is
// redelivered by the event loop's start recovery (XREADGROUP 0) and
// re-applies the outcome instead of finding the job already consumed
// (AUD-001). A release that failed midway may re-publish assignments the
// first attempt already enqueued: the job queue is at-least-once within
// a recovering stage. A release that already succeeded stays idempotent:
// the outcome's job is no longer pending, so a redelivered entry changes
// nothing. Emission failures degrade.
func (m *Machine) JobFinished(ctx context.Context, runID, job, status string) error {
	var (
		pipeline string
		finished string
		release  []Assignment
	)

	m.mu.Lock()
	r, ok := m.runs[runID]
	if !ok || !r.pending[job] {
		m.mu.Unlock()
		return nil
	}
	prevWorst := r.worst
	delete(r.pending, job)
	r.worst = worstStatus(r.worst, status)
	if len(r.pending) == 0 {
		pipeline = r.pipeline
		if r.worst == StatusPassed {
			if r.next < len(r.stages) {
				release = r.stages[r.next]
				r.next++
				r.pending = jobNames(release)
			} else {
				finished = StatusPassed
				delete(m.runs, runID)
			}
		} else {
			finished = runStatus(r.worst)
			delete(m.runs, runID)
		}
	}
	m.mu.Unlock()

	if finished != "" {
		m.emitFinished(ctx, runID, pipeline, finished)
		return nil
	}
	if release == nil {
		return nil
	}
	if err := m.publishStage(ctx, release, runID); err != nil {
		m.mu.Lock()
		if r, ok := m.runs[runID]; ok {
			// The stage never published; unwind the advance so the
			// outcome can be re-applied. The decision emptied the
			// pending set, so restoring it is the triggering job alone.
			r.next--
			r.pending = map[string]bool{job: true}
			r.worst = prevWorst
		}
		m.mu.Unlock()
		return err
	}
	return nil
}

// statusRank orders the job outcome slugs the machine sequences on,
// following the dispatcher's ranking (AUD-010): Errored is worst of the
// known slugs, then Failed, then Passed. A slug the machine cannot
// classify ranks worse than Errored (AUD-015) — an unreadable outcome
// must never be read as success.
func statusRank(s string) int {
	switch s {
	case StatusErrored:
		return 2
	case StatusFailed:
		return 1
	case StatusPassed:
		return 0
	}
	return 3
}

// runStatus maps the run's worst outcome onto the contract's
// pipeline.finished payload slugs (passed/failed/errored): an
// unclassifiable worst reports as Errored.
func runStatus(worst string) string {
	if statusRank(worst) > statusRank(StatusErrored) {
		return StatusErrored
	}
	return worst
}

// worstStatus returns whichever of a, b ranks worse per statusRank.
func worstStatus(a, b string) string {
	if statusRank(b) > statusRank(a) {
		return b
	}
	return a
}

// emitFinished emits pipeline.finished for a resolved run. Degradation
// policy applies: the run is already over, and emission failure must not
// fail anything.
func (m *Machine) emitFinished(ctx context.Context, runID, pipeline, status string) {
	if err := m.events.Publish(ctx, Event{
		Type:       EventPipelineFinished,
		OccurredAt: time.Now(),
		RunID:      runID,
		Pipeline:   pipeline,
		Payload:    map[string]string{"status": status},
	}); err != nil {
		// Degradation: never fail a run over emission — but leave a trace.
		m.log.Warn("pipeline.finished emission failed", "run_id", runID, "err", err)
	}
}
