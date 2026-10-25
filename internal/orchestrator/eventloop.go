package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// EventLoop consumes lifecycle events from the event bus and drives the
// machine with dispatcher job outcomes. The orchestrator is one of several
// bus consumers (contract § Streams); its consumer group keeps the others
// untouched.
type EventLoop struct {
	client  redis.UniversalClient
	machine *Machine
	log     Logger

	// Block is the XREADGROUP block interval; NewEventLoop sets a default.
	// A deployment config may override it.
	Block time.Duration
}

// NewEventLoop returns an EventLoop reading events through client and
// driving machine, logging malformed entries through log (a nil log
// discards).
func NewEventLoop(client redis.UniversalClient, machine *Machine, log Logger) *EventLoop {
	if log == nil {
		log = discardLogger{}
	}
	return &EventLoop{client: client, machine: machine, log: log, Block: time.Second}
}

// Run serves the event bus until ctx is cancelled. The consumer group
// starts at `$`: the bus keeps other producers' history, and runs cannot
// predate this process — run ids are minted here — so replaying that
// history would only feed the machine events for runs it never started.
// On start the loop first recovers its group's own unacknowledged entries
// (recoverPending), so an outcome left pending by a failed stage release
// is re-applied.
func (l *EventLoop) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	err := l.client.XGroupCreateMkStream(ctx, StreamEvents, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("ensuring event consumer group: %w", err)
	}
	if err := recoverPending(ctx, l.client, StreamEvents, l.apply); err != nil {
		return err
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		read, err := l.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: group,
			Streams:  []string{StreamEvents, ">"},
			Count:    1,
			Block:    l.Block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return nil // the read died with the shutdown, not the bus
			}
			return fmt.Errorf("reading event bus: %w", err)
		}
		for _, stream := range read {
			for _, msg := range stream.Messages {
				if err := l.apply(ctx, msg.Values["envelope"]); err != nil {
					return err // the entry stays pending; the next start's recovery re-applies it
				}
				if err := l.client.XAck(ctx, StreamEvents, group, msg.ID).Err(); err != nil {
					return fmt.Errorf("acknowledging event: %w", err)
				}
			}
		}
	}
}

// apply feeds one bus entry to the machine when it is a job outcome this
// boundary can sequence. Malformed entries — no envelope, undecodable
// JSON, an outcome without its coordinates or status — are logged and
// acknowledged; other producers' event types and outcomes without
// terminal status slugs are normal traffic on a shared bus and are
// acknowledged silently. A machine error is a stage-release publish
// failure — the entry stays pending and is re-applied by the next start's
// recovery read (AUD-001).
func (l *EventLoop) apply(ctx context.Context, envelope any) error {
	body, ok := envelope.(string)
	if !ok {
		l.log.Warn("event entry has no envelope field")
		return nil
	}
	var ev Event
	if err := json.Unmarshal([]byte(body), &ev); err != nil {
		l.log.Warn("event envelope failed to decode", "err", err)
		return nil
	}
	if ev.Type != EventJobFinished {
		return nil
	}
	if ev.RunID == "" || ev.Job == "" {
		l.log.Warn("job.finished outcome is missing coordinates", "run_id", ev.RunID, "job", ev.Job)
		return nil // an outcome without its coordinates is unsequenceable
	}
	status, ok := ev.Payload["status"]
	if !ok {
		l.log.Warn("job.finished outcome carries no status", "run_id", ev.RunID, "job", ev.Job)
		return nil
	}
	switch status {
	case StatusPassed, StatusFailed, StatusErrored:
	default:
		return nil // not a terminal slug this machine sequences on
	}
	return l.machine.JobFinished(ctx, ev.RunID, ev.Job, status)
}
