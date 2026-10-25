package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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

// LoopStatus records a loop's most recent serve error for the health
// endpoint (AUD-013): a transient error the loop retried through never
// reaches the caller, so this is the only trace besides the log line.
type LoopStatus struct {
	mu  sync.Mutex
	err string
	at  time.Time
}

// Record notes the loop's most recent serve error and when it was seen.
func (s *LoopStatus) Record(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err.Error()
	s.at = time.Now()
}

// LastError returns the recorded error and when it was seen; empty and
// zero when the loop has served cleanly.
func (s *LoopStatus) LastError() (string, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err, s.at
}

// Loop consumes run requests from the pipeline queue and hands each off to
// the machine, which sequences the run's stages. It holds no run state:
// every entry is one decode-validate-segment, and the queue's consumer
// group carries the read position.
type Loop struct {
	client  redis.UniversalClient
	machine *Machine
	dead    DeadLetterSink
	log     Logger

	// Status records the loop's last serve error for health (AUD-013);
	// NewLoop provides a fresh one.
	Status *LoopStatus

	// Block is the XREADGROUP block interval; NewLoop sets a default. A
	// deployment config may override it.
	Block time.Duration
}

// decisionError marks an error from applying a queue decision — a hand-off
// or stage release that failed midway, or the ack that commits it. The
// loop must exit so the entry stays pending and the next start's recovery
// re-applies it (gh-20); retrying a decision in place would re-decide a
// half-applied entry against a still-populated run table — a duplicate run
// — where exiting lets the process's restart recover it cleanly. Read
// errors are the opposite: retrying them in place preserves in-memory run
// state a process restart would lose (AUD-007).
type decisionError struct{ err error }

func (e *decisionError) Error() string { return e.err.Error() }
func (e *decisionError) Unwrap() error { return e.err }

// NewLoop returns a Loop reading entries through client, handing runs off
// to machine, dead-lettering poison entries through dead, and logging
// degraded paths through log (a nil log discards).
func NewLoop(client redis.UniversalClient, machine *Machine, dead DeadLetterSink, log Logger) *Loop {
	if log == nil {
		log = discardLogger{}
	}
	return &Loop{client: client, machine: machine, dead: dead, log: log, Status: &LoopStatus{}, Block: time.Second}
}

// recoverPending re-reads the consumer group's own pending entries
// (XREADGROUP with ID 0) and runs each through apply, acknowledging the
// entries that decide cleanly. Both loops call it once per serve, so an
// entry left unacknowledged — a hand-off that failed mid-publish, or a
// dead process — is retried instead of stranding the run behind it
// (AUD-001). An apply that fails again leaves the entry pending and
// surfaces as a decision error; the next start retries it.
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
					return &decisionError{fmt.Errorf("re-applying pending entry on %s: %w", stream, err)} // stays pending; the next start retries it
				}
				if err := client.XAck(ctx, stream, group, msg.ID).Err(); err != nil {
					return &decisionError{fmt.Errorf("acknowledging recovered entry on %s: %w", stream, err)}
				}
			}
		}
		if len(read) == 0 || len(read[0].Messages) < 64 {
			return nil
		}
	}
}

// Run serves the pipeline queue until ctx is cancelled, retrying transient
// Redis errors around the reads with bounded backoff — a process exit
// costs every in-memory run (ADR 0001, decision 6), so a blip must not
// (AUD-007). A decision error still returns: the entry stays pending and
// the next start's recovery re-applies it (gh-20). Cancellation returns
// nil. A poison entry is neither — it is dead-lettered with a reason, so
// one bad queue entry cannot stop the runs behind it. Each retry re-runs
// the serve sequence below (ensure group, recover pending, read).
func (l *Loop) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil
		}
		err := l.serve(ctx)
		if err == nil {
			return nil
		}
		l.Status.Record(err)
		var decision *decisionError
		if errors.As(err, &decision) {
			return err
		}
		l.log.Warn("pipeline queue error; retrying", "err", err, "attempt", attempt)
		if err := BackoffSleep(ctx, attempt); err != nil {
			return nil
		}
	}
}

// serve runs one connected stretch of the pipeline queue: ensure the
// consumer group, recover the group's own pending entries, then read new
// entries until a cancellation, a read error, or a decision error.
func (l *Loop) serve(ctx context.Context) error {
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
					return &decisionError{err} // the entry stays pending; the next start's recovery retries it
				}
				if err := l.client.XAck(ctx, StreamPipelineRequests, group, msg.ID).Err(); err != nil {
					return &decisionError{fmt.Errorf("acknowledging entry: %w", err)}
				}
			}
		}
	}
}

// handOff turns one queue entry into a machine start. An entry that fails
// to decode or to re-validate at this trust boundary is poison: it is
// dead-lettered with a reason — never silently dropped (contract § Run
// request) — and the caller acknowledges it. A stage-1 publish failure —
// and, since AUD-015, a pipeline.started emission failure — aborts the
// entry unacknowledged; emission failures degrade inside the machine.
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
