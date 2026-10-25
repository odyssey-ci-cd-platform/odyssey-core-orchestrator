package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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

// DeadLetterSink is where poison run requests land. The consumer package
// owns the seam; *Publisher satisfies it. A Loop requires one: a nil sink
// would turn every poison entry back into a silent drop.
type DeadLetterSink interface {
	PublishDeadLetter(ctx context.Context, d DeadLetter) error
}

// Logger is where degraded paths leave a trace: emission failures, poison
// entries, failed dead-letter writes. The consumer package owns the seam;
// *slog.Logger satisfies it.
type Logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// discardLogger drops every line — the nil-logger default, so a caller
// without logging wiring degrades instead of panicking.
type discardLogger struct{}

func (discardLogger) Warn(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}

// Loop consumes run requests from the pipeline queue and hands each off to
// the machine, which sequences the run's stages. It holds no run state:
// every entry is one decode-validate-segment, and the queue's consumer
// group carries the read position.
type Loop struct {
	client  redis.UniversalClient
	machine *Machine
	dead    DeadLetterSink
	log     Logger

	// Block is the XREADGROUP block interval; NewLoop sets a default. A
	// deployment config may override it.
	Block time.Duration
}

// NewLoop returns a Loop reading entries through client, handing runs off
// to machine, dead-lettering poison entries through dead, and logging
// degraded paths through log (a nil log discards).
func NewLoop(client redis.UniversalClient, machine *Machine, dead DeadLetterSink, log Logger) *Loop {
	if log == nil {
		log = discardLogger{}
	}
	return &Loop{client: client, machine: machine, dead: dead, log: log, Block: time.Second}
}

// Run serves the pipeline queue until ctx is cancelled. It returns nil on
// cancellation and the first infrastructure error otherwise; a poison
// entry is neither — it is dead-lettered with a reason, so one bad queue
// entry cannot stop the runs behind it.
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
// to decode or to re-validate at this trust boundary is poison: it is
// dead-lettered with a reason — never silently dropped (contract § Run
// request) — and the caller acknowledges it. A stage-1 publish failure
// aborts the entry unacknowledged; emission failures degrade inside the
// machine.
func (l *Loop) handOff(ctx context.Context, envelope any) error {
	body, ok := envelope.(string)
	if !ok {
		l.log.Warn("run request entry has no envelope field")
		return l.deadLetter(ctx, DeadLetter{Reason: DeadReasonDecode})
	}
	req, err := DecodeRunRequest([]byte(body))
	if err != nil {
		l.log.Warn("run request failed to decode", "err", err)
		return l.deadLetter(ctx, DeadLetter{Reason: DeadReasonDecode, Raw: body})
	}
	runID := newRunID()
	assignments, err := Segment(req, runID)
	if err != nil {
		l.log.Warn("run request failed re-validation", "err", err)
		return l.deadLetter(ctx, DeadLetter{Reason: DeadReasonValidation, Request: json.RawMessage(body)})
	}
	if err := l.machine.Start(ctx, runID, req.Pipeline, assignments); err != nil {
		return err
	}
	return nil
}

// deadLetter publishes one poison wrap. A failed dead-letter write is an
// infrastructure failure like any other: the error surfaces so the entry
// stays unacknowledged — a trace that cannot be written is not dropped.
func (l *Loop) deadLetter(ctx context.Context, d DeadLetter) error {
	if err := l.dead.PublishDeadLetter(ctx, d); err != nil {
		l.log.Error("run request dead-letter write failed", "reason", d.Reason, "err", err)
		return fmt.Errorf("dead-lettering run request: %w", err)
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
