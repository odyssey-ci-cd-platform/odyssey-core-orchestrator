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

	// Block is the XREADGROUP block interval; NewEventLoop sets a default.
	// A deployment config may override it.
	Block time.Duration
}

// NewEventLoop returns an EventLoop reading events through client and
// driving machine.
func NewEventLoop(client redis.UniversalClient, machine *Machine) *EventLoop {
	return &EventLoop{client: client, machine: machine, Block: time.Second}
}

// Run serves the event bus until ctx is cancelled. The consumer group
// starts at `$`: the bus keeps other producers' history, and runs cannot
// predate this process — run ids are minted here — so replaying that
// history would only feed the machine events for runs it never started.
func (l *EventLoop) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	err := l.client.XGroupCreateMkStream(ctx, StreamEvents, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("ensuring event consumer group: %w", err)
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
					return err // the entry stays pending; only decided events are acknowledged
				}
				if err := l.client.XAck(ctx, StreamEvents, group, msg.ID).Err(); err != nil {
					return fmt.Errorf("acknowledging event: %w", err)
				}
			}
		}
	}
}

// apply feeds one bus entry to the machine when it is a job outcome this
// boundary can sequence, and skips it otherwise: malformed envelopes,
// other producers' event types, and outcomes without terminal status
// slugs are acknowledged and forgotten. A machine error is a stage-release
// publish failure — the entry stays pending.
func (l *EventLoop) apply(ctx context.Context, envelope any) error {
	body, ok := envelope.(string)
	if !ok {
		return nil
	}
	var ev Event
	if err := json.Unmarshal([]byte(body), &ev); err != nil {
		return nil
	}
	if ev.Type != EventJobFinished {
		return nil
	}
	if ev.RunID == "" || ev.Job == "" {
		return nil // an outcome without its coordinates is unsequenceable
	}
	status, ok := ev.Payload["status"]
	if !ok {
		return nil
	}
	switch status {
	case StatusPassed, StatusFailed, StatusErrored:
	default:
		return nil // not a terminal slug this machine sequences on
	}
	return l.machine.JobFinished(ctx, ev.RunID, ev.Job, status)
}
