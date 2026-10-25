package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// StreamEvents is the event bus all lifecycle events land on (contract
	// § Streams).
	StreamEvents = "odyssey:events"

	// maxStreamLen bounds retention via approximate XADD trimming — the
	// same trim the dispatcher repo's publisher applies (its ADR 0001):
	// the results DB owns history, nothing replays from the bus.
	maxStreamLen = 100000
)

// EventPipelineStarted is emitted before any job of the run is published:
// the results DB's parent-row invariant depends on that ordering (contract
// § Event emission responsibilities).
const EventPipelineStarted = "pipeline.started"

// EventPipelineFinished is emitted when the state machine resolves the
// run; its payload carries the run's worst status (contract § Event
// emission responsibilities).
const EventPipelineFinished = "pipeline.finished"

// EventJobFinished is the dispatcher's job outcome event; the machine
// sequences stages on it (contract § Event emission responsibilities).
const EventJobFinished = "job.finished"

// Event is this repo's emit-side envelope, matching the dispatcher repo's
// internal/domain Event JSON field for field — the bus is a shared
// boundary, so both producers write the same shape and consumers cannot
// tell them apart.
type Event struct {
	Type       string            `json:"type"`
	OccurredAt time.Time         `json:"occurred_at"`
	RunID      string            `json:"run_id,omitempty"`
	Pipeline   string            `json:"pipeline"`
	Job        string            `json:"job,omitempty"`
	Step       string            `json:"step,omitempty"`
	Payload    map[string]string `json:"payload,omitempty"`
}

// Emitter writes lifecycle events onto the event bus.
type Emitter struct {
	client redis.UniversalClient
}

// NewEmitter returns an Emitter issuing XADDs through client.
func NewEmitter(client redis.UniversalClient) *Emitter {
	return &Emitter{client: client}
}

// Validate enforces the contract's per-event envelope rules (§ Event
// emission responsibilities): every event names its type, run, and pipeline;
// this emitter publishes this repo's two lifecycle events only, and each
// carries exactly its pinned fields — pipeline.started with no job, step,
// or payload; pipeline.finished with no job or step and a worst-stage
// status payload (AUD-015).
func (e Event) Validate() error {
	var errs []error
	if e.Type == "" {
		errs = append(errs, errors.New("type must not be empty"))
	}
	if e.RunID == "" {
		errs = append(errs, errors.New("run_id must not be empty"))
	}
	if e.Pipeline == "" {
		errs = append(errs, errors.New("pipeline must not be empty"))
	}
	switch e.Type {
	case EventPipelineStarted:
		if e.Job != "" || e.Step != "" {
			errs = append(errs, errors.New("pipeline.started carries no job or step"))
		}
		if len(e.Payload) != 0 {
			errs = append(errs, errors.New("pipeline.started carries no payload"))
		}
	case EventPipelineFinished:
		if e.Job != "" || e.Step != "" {
			errs = append(errs, errors.New("pipeline.finished carries no job or step"))
		}
		switch e.Payload["status"] {
		case StatusPassed, StatusFailed, StatusErrored:
		default:
			errs = append(errs, fmt.Errorf("pipeline.finished payload must carry status passed, failed, or errored, got %q", e.Payload["status"]))
		}
	default:
		if e.Type != "" {
			errs = append(errs, fmt.Errorf("unknown event type %q: this emitter publishes pipeline lifecycle events only", e.Type))
		}
	}
	return errors.Join(errs...)
}

// Publish writes one event as the stream entry's single envelope field.
// Degradation policy is the caller's: emission failures never fail a run
// (the platform's policy, dispatcher repo ADR 0001) — the error exists so
// the caller can log it.
func (e *Emitter) Publish(ctx context.Context, ev Event) error {
	if err := ev.Validate(); err != nil {
		return fmt.Errorf("refusing to publish invalid event: %w", err)
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return e.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamEvents,
		MaxLen: maxStreamLen,
		Approx: true,
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}
