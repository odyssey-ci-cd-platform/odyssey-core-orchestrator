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

// EventSink is where lifecycle events are emitted. The consumer package
// owns the seam; *Emitter satisfies it.
type EventSink interface {
	Publish(ctx context.Context, e Event) error
}

// AssignmentSink is where job assignments are published. The consumer
// package owns the seam; *Publisher satisfies it.
type AssignmentSink interface {
	Publish(ctx context.Context, a Assignment) error
}

// Loop consumes run requests from the pipeline queue and hands each off to
// the machine, which sequences the run's stages. It holds no run state:
// every entry is one decode-validate-segment, and the queue's consumer
// group carries the read position.
type Loop struct {
	client  redis.UniversalClient
	machine *Machine

	// Block is the XREADGROUP block interval; NewLoop sets a default. A
	// deployment config may override it.
	Block time.Duration
}

// NewLoop returns a Loop reading entries through client and handing runs
// off to machine.
func NewLoop(client redis.UniversalClient, machine *Machine) *Loop {
	return &Loop{client: client, machine: machine, Block: time.Second}
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

// handOff turns one queue entry into a machine start. An entry that fails
// to decode or to re-validate at this trust boundary is poison: nothing is
// published for it and the caller acknowledges it. A stage-1 publish
// failure aborts the entry unacknowledged; emission failures degrade
// inside the machine.
func (l *Loop) handOff(ctx context.Context, envelope any) error {
	body, ok := envelope.(string)
	if !ok {
		return nil // no envelope field: nothing to validate, nothing to publish
	}
	req, err := DecodeRunRequest([]byte(body))
	if err != nil {
		return nil
	}
	runID := newRunID()
	assignments, err := Segment(req, runID)
	if err != nil {
		return nil
	}
	if err := l.machine.Start(ctx, runID, req.Pipeline, assignments); err != nil {
		return err
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
