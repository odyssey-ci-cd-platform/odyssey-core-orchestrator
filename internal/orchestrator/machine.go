package orchestrator

import (
	"context"
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
	runs        map[string]*run
}

// run is one in-flight pipeline run: its stages grouped in declared order,
// the index of the next stage to release, the current stage's unfinished
// job names, and the worst job outcome resolved so far.
type run struct {
	pipeline string
	stages   [][]Assignment
	next     int
	pending  map[string]bool
	worst    string
}

// NewMachine returns a Machine emitting through events and publishing
// assignments through assignments.
func NewMachine(events EventSink, assignments AssignmentSink) *Machine {
	return &Machine{events: events, assignments: assignments, runs: make(map[string]*run)}
}

// Start hands off one segmented run: pipeline.started first — the results
// DB's parent-row invariant — then the first stage only. Later stages wait
// on JobFinished. A stage-1 publish failure returns the error and leaves
// nothing behind; emission failures degrade per the platform's policy
// (dispatcher repo ADR 0001) and never fail the start.
func (m *Machine) Start(ctx context.Context, runID, pipeline string, assignments []Assignment) error {
	stages := groupByStage(assignments)
	if len(stages) == 0 {
		return nil // a validated request always has a job; nothing to sequence
	}

	r := &run{
		pipeline: pipeline,
		stages:   stages,
		next:     1,
		pending:  jobNames(stages[0]),
		worst:    StatusPassed,
	}
	m.mu.Lock()
	m.runs[runID] = r
	m.mu.Unlock()

	if err := m.events.Publish(ctx, Event{
		Type:       EventPipelineStarted,
		OccurredAt: time.Now(),
		RunID:      runID,
		Pipeline:   pipeline,
	}); err != nil {
		// Degradation: never fail a run over emission. v1 has no logger;
		// process bootstrap wires one.
		_ = err
	}

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
