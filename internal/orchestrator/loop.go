package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// StreamPipelineRequests is the queue run requests are consumed from
	// (contract § Streams).
	StreamPipelineRequests = "odyssey:pipeline-requests"

	// group is the pipeline queue's sole consumer group and consumer name.
	// v1 runs one orchestrator process; ack-on-decision means an
	// acknowledged run is never re-run, and an unacknowledged entry of a
	// dead process stays pending (visible via XPENDING) rather than
	// re-running under a second run id — v1 loses in-flight runs by
	// design (ADR 0001).
	group = "orchestrator"
)

// EventSink is where the loop emits lifecycle events. The consumer package
// owns the seam; *Emitter satisfies it.
type EventSink interface {
	Publish(ctx context.Context, e Event) error
}

// AssignmentSink is where the loop publishes job assignments. The consumer
// package owns the seam; *Publisher satisfies it.
type AssignmentSink interface {
	Publish(ctx context.Context, a Assignment) error
}

// Loop consumes run requests from the pipeline queue and hands each off as
// pipeline.started followed by its stage-ordered assignments. It holds no
// state between entries: every decision is per entry, and the queue's
// consumer group carries the read position.
type Loop struct {
	client      redis.UniversalClient
	events      EventSink
	assignments AssignmentSink

	// Block is the XREADGROUP block interval; NewLoop sets a default. A
	// deployment config may override it.
	Block time.Duration
}

// NewLoop returns a Loop reading entries through client, emitting through
// events and publishing through assignments.
func NewLoop(client redis.UniversalClient, events EventSink, assignments AssignmentSink) *Loop {
	return &Loop{client: client, events: events, assignments: assignments, Block: time.Second}
}

// Run serves the pipeline queue until ctx is cancelled. It returns nil on
// cancellation and the first infrastructure error otherwise; a poison
// entry is neither — it is skipped, so one bad queue entry cannot stop the
// runs behind it.
func (l *Loop) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	err := l.client.XGroupCreateMkStream(ctx, StreamPipelineRequests, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("ensuring consumer group: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		read, err := l.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: group,
			Streams:  []string{StreamPipelineRequests, ">"},
			Count:    1,
			Block:    l.Block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return nil // the read died with the shutdown, not the queue
			}
			return fmt.Errorf("reading pipeline queue: %w", err)
		}
		for _, stream := range read {
			for _, msg := range stream.Messages {
				if err := l.handOff(ctx, msg.Values["envelope"]); err != nil {
					return err // the entry stays pending; only decided entries are acknowledged
				}
				if err := l.client.XAck(ctx, StreamPipelineRequests, group, msg.ID).Err(); err != nil {
					return fmt.Errorf("acknowledging entry: %w", err)
				}
			}
		}
	}
}

// handOff turns one queue entry into pipeline.started plus assignments.
// An entry that fails to decode or to re-validate at this trust boundary
// is poison: nothing is published for it and the caller acknowledges it.
// An emission failure degrades per the platform's policy (dispatcher repo
// ADR 0001); a publish failure aborts the entry unacknowledged.
func (l *Loop) handOff(ctx context.Context, envelope any) error {
	body, ok := envelope.(string)
	if !ok {
		return nil // no envelope field: nothing to validate, nothing to publish
	}
	req, err := DecodeRunRequest([]byte(body))
	if err != nil {
		return nil
	}
	assignments, err := Segment(req, newRunID())
	if err != nil {
		return nil
	}

	// pipeline.started precedes every assignment of the run: the results
	// DB's parent-row invariant depends on that ordering (contract § Event
	// emission responsibilities).
	if err := l.events.Publish(ctx, Event{
		Type:       EventPipelineStarted,
		OccurredAt: time.Now(),
		RunID:      assignments[0].RunID,
		Pipeline:   req.Pipeline,
	}); err != nil {
		// Degradation: never fail a run over emission. v1 has no logger;
		// process bootstrap wires one.
		_ = err
	}

	for _, a := range assignments {
		if err := l.assignments.Publish(ctx, a); err != nil {
			return fmt.Errorf("publishing assignment %s/%s for run %s: %w", a.Stage, a.Job, a.RunID, err)
		}
	}
	return nil
}

// newRunID mints a fresh opaque run id per run attempt (contract § Job
// assignment). An entropy failure yields the empty id, which Segment
// refuses: the entry is skipped rather than published under a reused id.
func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}
