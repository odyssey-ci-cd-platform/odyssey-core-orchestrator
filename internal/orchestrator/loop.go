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
	// acknowledged run is never re-run. An entry left unacknowledged — a
	// hand-off that failed mid-publish, or a dead process — is recovered on
	// the next start: the loop re-reads the group's pending entries and
	// hands each off again, under a fresh run id for the request queue (the
	// at-least-once cost of recovery; v1 still loses in-flight runs, ADR
	// 0001).
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

// recoverPending re-reads the consumer group's own pending entries
// (XREADGROUP with ID 0) and runs each through apply, acknowledging the
// entries that decide cleanly. Both loops call it once on start, so an
// entry left unacknowledged — a hand-off that failed mid-publish, or a
// dead process — is retried instead of stranding the run behind it
// (AUD-001). An apply that fails again leaves the entry pending and
// aborts the loop; the next start retries it.
func recoverPending(ctx context.Context, client redis.UniversalClient, stream string, apply func(context.Context, any) error) error {
	for {
		read, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: group,
			Streams:  []string{stream, "0"},
			Count:    64,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil // no unacknowledged entries
			}
			if ctx.Err() != nil {
				return nil // the read died with the shutdown, not the stream
			}
			return fmt.Errorf("recovering pending entries on %s: %w", stream, err)
		}
		for _, s := range read {
			for _, msg := range s.Messages {
				if err := apply(ctx, msg.Values["envelope"]); err != nil {
					return err // stays pending; the next start retries it
				}
				if err := client.XAck(ctx, stream, group, msg.ID).Err(); err != nil {
					return fmt.Errorf("acknowledging recovered entry on %s: %w", stream, err)
				}
			}
		}
		if len(read) == 0 || len(read[0].Messages) < 64 {
			return nil
		}
	}
}

// Run serves the pipeline queue until ctx is cancelled. It returns nil on
// cancellation and the first infrastructure error otherwise; a poison
// entry is neither — it is skipped, so one bad queue entry cannot stop the
// runs behind it. On start the loop first recovers its group's own
// unacknowledged entries (recoverPending).
func (l *Loop) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	err := l.client.XGroupCreateMkStream(ctx, StreamPipelineRequests, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("ensuring consumer group: %w", err)
	}
	if err := recoverPending(ctx, l.client, StreamPipelineRequests, l.handOff); err != nil {
		return err
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
					return err // the entry stays pending; the next start's recovery retries it
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
