package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the orchestrator consumes run requests from the
// pipeline queue and hands each off as pipeline.started followed by its
// stage-ordered assignments, skipping poison entries, degrading on
// emission failures, and acknowledging decided entries (issue gh-14;
// dispatcher contract §§ Streams, Event emission responsibilities).

// journal records the hand-off sequence, so tests can assert ordering and
// correlation across the two sinks without inspecting Redis timing.
type journal struct {
	mu       sync.Mutex
	entries  []string
	failEmit bool
}

func (j *journal) record(s string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, s)
}

func (j *journal) snapshot() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

type fakeEvents struct{ j *journal }

func (f fakeEvents) Publish(_ context.Context, e orchestrator.Event) error {
	switch e.Type {
	case orchestrator.EventPipelineStarted:
		f.j.record("started:" + e.RunID)
	case orchestrator.EventPipelineFinished:
		f.j.record("finished:" + e.RunID + ":" + e.Payload["status"])
	}
	if f.j.failEmit {
		return errors.New("bus unreachable")
	}
	return nil
}

type fakeAssignments struct{ j *journal }

func (f fakeAssignments) Publish(_ context.Context, a orchestrator.Assignment) error {
	f.j.record("assign:" + a.RunID + ":" + a.Stage + "/" + a.Job)
	return nil
}

// fakeLogger records the degraded-path log lines as "level:msg k=v …"
// strings, so tests assert what a degraded path made visible.
type fakeLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *fakeLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args) }
func (l *fakeLogger) Error(msg string, args ...any) { l.record("error", msg, args) }

func (l *fakeLogger) record(level, msg string, args []any) {
	line := level + ":" + msg
	for i := 0; i+1 < len(args); i += 2 {
		line += fmt.Sprintf(" %v=%v", args[i], args[i+1])
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, line)
}

func (l *fakeLogger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// fakeDeadLetters records dead-letter wraps, or fails when err is set —
// attempts count the failed ones too.
type fakeDeadLetters struct {
	mu       sync.Mutex
	letter   []orchestrator.DeadLetter
	attempts int
	err      error
}

func (f *fakeDeadLetters) PublishDeadLetter(_ context.Context, d orchestrator.DeadLetter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.err != nil {
		return f.err
	}
	f.letter = append(f.letter, d)
	return nil
}

func (f *fakeDeadLetters) attempted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts > 0
}

func (f *fakeDeadLetters) snapshot() []orchestrator.DeadLetter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]orchestrator.DeadLetter(nil), f.letter...)
}

// hasLog fails the test unless some recorded line contains every fragment.
func hasLog(t *testing.T, l *fakeLogger, fragments ...string) {
	t.Helper()
	for _, line := range l.snapshot() {
		all := true
		for _, f := range fragments {
			if !strings.Contains(line, f) {
				all = false
				break
			}
		}
		if all {
			return
		}
	}
	t.Errorf("no log line contains %v; lines = %v", fragments, l.snapshot())
}

func newTestLoop(t *testing.T, j *journal, dead *fakeDeadLetters, logs *fakeLogger) (*redis.Client, context.CancelFunc, <-chan error) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	var deadSink orchestrator.DeadLetterSink
	if dead != nil {
		deadSink = dead
	}
	var lg orchestrator.Logger
	if logs != nil {
		lg = logs
	}
	loop := orchestrator.NewLoop(client, orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil), deadSink, lg)
	loop.Block = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()
	return client, cancel, done
}

// seed adds one queue entry carrying envelope as its single envelope field.
func seed(t *testing.T, client *redis.Client, envelope string) {
	t.Helper()
	err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: orchestrator.StreamPipelineRequests,
		Values: map[string]any{"envelope": envelope},
	}).Err()
	if err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}
}

func waitFor(t *testing.T, j *journal, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(j.snapshot()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("journal has %d entries, want %d within deadline:\n%v", len(j.snapshot()), n, j.snapshot())
}

func waitAcked(t *testing.T, client *redis.Client, stream string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(context.Background(), stream, "orchestrator").Result()
		if err != nil {
			t.Fatalf("XPending() error = %v", err)
		}
		if pending.Count == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("entries on %s still pending after deadline, want all acknowledged", stream)
}

func stopLoop(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

// startedIDs returns the run ids in journal order that a hand-off emitted
// as pipeline.started.
func startedIDs(entries []string) []string {
	var ids []string
	for _, e := range entries {
		if len(e) > len("started:") && e[:len("started:")] == "started:" {
			ids = append(ids, e[len("started:"):])
		}
	}
	return ids
}

func TestLoopHandsOffValidRun(t *testing.T) {
	j := &journal{}
	client, cancel, done := newTestLoop(t, j, nil, nil)
	seed(t, client, validEnvelope)

	waitFor(t, j, 2) // one started + the first stage only
	waitAcked(t, client, orchestrator.StreamPipelineRequests)
	stopLoop(t, cancel, done)

	entries := j.snapshot()
	if len(entries) != 2 {
		t.Fatalf("journal = %v, want started plus the first stage only (later stages wait on job.finished)", entries)
	}
	// The run id comes from the assignment entry; the started event must
	// carry the same id and precede it.
	id := runIDOf(entries[1])
	if entries[0] != "started:"+id {
		t.Errorf("first entry = %q, want the started event under run id %q", entries[0], id)
	}
	if entries[1] != "assign:"+id+":build/compile" {
		t.Errorf("assignment = %q, want the first stage's build/compile under run id %q", entries[1], id)
	}
	ids := startedIDs(entries)
	if len(ids) != 1 || ids[0] == "" {
		t.Errorf("started ids = %v, want one non-empty fresh run id", ids)
	}
}

// runIDOf extracts the run id from an "assign:<id>:stage/job" journal
// entry.
func runIDOf(assignEntry string) string {
	rest := assignEntry[len("assign:"):]
	return rest[:strings.IndexByte(rest, ':')]
}

func TestLoopAssignsFreshRunIDPerRun(t *testing.T) {
	j := &journal{}
	client, cancel, done := newTestLoop(t, j, nil, nil)
	seed(t, client, validEnvelope)
	seed(t, client, validEnvelope)

	waitFor(t, j, 4) // two runs, one started + first stage each
	waitAcked(t, client, orchestrator.StreamPipelineRequests)
	stopLoop(t, cancel, done)

	ids := startedIDs(j.snapshot())
	if len(ids) != 2 {
		t.Fatalf("started ids = %v, want one per run", ids)
	}
	if ids[0] == ids[1] {
		t.Errorf("both runs handed off under run id %q, want a fresh id per run", ids[0])
	}
}

func TestLoopSkipsPoisonEntries(t *testing.T) {
	cases := []struct {
		name   string
		poison string
	}{
		{"undecodable", "not json {{"},
		{"fails re-validation", strings.Replace(validEnvelope,
			"8f3a1c4d5e6f708192a3b4c5d6e7f8091a2b3c4d", "shortsha", 1)},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			j := &journal{}
			client, cancel, done := newTestLoop(t, j, &fakeDeadLetters{}, &fakeLogger{})
			seed(t, client, tt.poison)
			seed(t, client, validEnvelope)

			waitFor(t, j, 2) // only the valid run's hand-off
			waitAcked(t, client, orchestrator.StreamPipelineRequests)
			stopLoop(t, cancel, done)

			ids := startedIDs(j.snapshot())
			if len(ids) != 1 {
				t.Errorf("started ids = %v, want exactly the valid run (poison produced nothing)", ids)
			}
		})
	}
}

// AUD-015: the started emission precedes registration — an emission
// failure aborts the hand-off, the loop returns the error, and the queue
// entry stays pending, unacknowledged. This rewrites the old
// degrade-and-hand-off pin: degradation was exactly the stranding AUD-015
// names — a tracked run whose parent row can never exist.
func TestLoopLeavesEntryPendingWhenStartedEmitFails(t *testing.T) {
	j := &journal{failEmit: true}
	client, cancel, done := newTestLoop(t, j, nil, nil)
	seed(t, client, validEnvelope)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() error = nil, want the started-emission failure to abort the hand-off")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after the emission failure")
	}
	cancel()

	for _, e := range j.snapshot() {
		if strings.HasPrefix(e, "assign:") {
			t.Errorf("journal has %q, want no assignment for an aborted hand-off", e)
		}
	}
	pending, err := client.XPending(context.Background(), orchestrator.StreamPipelineRequests, "orchestrator").Result()
	if err != nil {
		t.Fatalf("XPending() error = %v", err)
	}
	if pending.Count != 1 {
		t.Errorf("%d entries pending, want the aborted entry left pending", pending.Count)
	}
}

// AUD-001: a stage-1 publish failure must not strand the run request. The
// loop exits with the error and the entry stays pending, nothing
// published; a restarted loop re-reads the group's own pending entries
// (XREADGROUP 0) and re-hands the entry off under a fresh run id, then
// acknowledges it.
func TestLoopRecoversUnacknowledgedRunAfterRestart(t *testing.T) {
	j := &journal{}
	sink := &flakyAssignments{j: j, failStage: "build"}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	machine := orchestrator.NewMachine(fakeEvents{j}, sink, nil)
	loop := orchestrator.NewLoop(client, machine, &fakeDeadLetters{}, nil)
	loop.Block = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	seed(t, client, validEnvelope)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() error = nil, want the stage-1 publish failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after the failed hand-off")
	}
	pending, err := client.XPending(context.Background(), orchestrator.StreamPipelineRequests, "orchestrator").Result()
	if err != nil {
		t.Fatalf("XPending() error = %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending request entries = %d, want 1", pending.Count)
	}
	if got := len(j.snapshot()); got != 1 {
		t.Fatalf("journal = %v, want only the aborted attempt's started event", j.snapshot())
	}

	// The transient failure clears with the restart: recovery re-hands the
	// entry off under a fresh run id.
	sink.failStage = ""
	recovered := make(chan error, 1)
	restarted := orchestrator.NewLoop(client, machine, &fakeDeadLetters{}, nil)
	restarted.Block = 25 * time.Millisecond
	go func() { recovered <- restarted.Run(ctx) }()

	waitFor(t, j, 3) // started (aborted) + started (retry) + first stage
	waitAcked(t, client, orchestrator.StreamPipelineRequests)
	cancel()
	select {
	case err := <-recovered:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}

	entries := j.snapshot()
	if len(entries) != 3 {
		t.Fatalf("journal = %v, want the aborted start, the retried start, and the first stage", entries)
	}
	aborted := strings.TrimPrefix(entries[0], "started:")
	retried := strings.TrimPrefix(entries[1], "started:")
	if entries[1] == entries[0] || retried == "" {
		t.Errorf("second entry = %q, want a fresh run id for the recovered hand-off", entries[1])
	}
	if entries[2] != "assign:"+retried+":build/compile" {
		t.Errorf("third entry = %q, want the first stage under the recovered run id %q", entries[2], retried)
	}
	if aborted == retried {
		t.Errorf("both attempts ran under run id %q, want a fresh id per hand-off", aborted)
	}
}

// AUD-005: a poison run request is dead-lettered with a reason — never
// silently dropped (contract § Run request) — and logged, while the runs
// behind it still hand off.
func TestLoopDeadLettersPoisonEntries(t *testing.T) {
	validationPoison := strings.Replace(validEnvelope,
		"8f3a1c4d5e6f708192a3b4c5d6e7f8091a2b3c4d", "shortsha", 1)
	cases := []struct {
		name       string
		noEnvelope bool
		poison     string
		want       orchestrator.DeadLetter
		logLine    string
	}{
		{
			name:    "undecodable",
			poison:  "not json {{",
			want:    orchestrator.DeadLetter{Reason: orchestrator.DeadReasonDecode, Raw: "not json {{"},
			logLine: "warn:run request failed to decode",
		},
		{
			name:    "fails re-validation",
			poison:  validationPoison,
			want:    orchestrator.DeadLetter{Reason: orchestrator.DeadReasonValidation, Request: json.RawMessage(validationPoison)},
			logLine: "warn:run request failed re-validation",
		},
		{
			name:       "no envelope field",
			noEnvelope: true,
			want:       orchestrator.DeadLetter{Reason: orchestrator.DeadReasonDecode},
			logLine:    "warn:run request entry has no envelope field",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			j := &journal{}
			dead := &fakeDeadLetters{}
			log := &fakeLogger{}
			client, cancel, done := newTestLoop(t, j, dead, log)
			if tt.noEnvelope {
				if err := client.XAdd(context.Background(), &redis.XAddArgs{
					Stream: orchestrator.StreamPipelineRequests,
					Values: map[string]any{"other": "not an envelope"},
				}).Err(); err != nil {
					t.Fatalf("XAdd() error = %v", err)
				}
			} else {
				seed(t, client, tt.poison)
			}
			seed(t, client, validEnvelope)

			waitFor(t, j, 2) // only the valid run's hand-off
			waitAcked(t, client, orchestrator.StreamPipelineRequests)
			stopLoop(t, cancel, done)

			got := dead.snapshot()
			if len(got) != 1 || !reflect.DeepEqual(got[0], tt.want) {
				t.Errorf("dead letters = %+v, want [%+v]", got, tt.want)
			}
			hasLog(t, log, tt.logLine)
			ids := startedIDs(j.snapshot())
			if len(ids) != 1 {
				t.Errorf("started ids = %v, want exactly the valid run (poison produced no run)", ids)
			}
		})
	}
}

// A trace that cannot be written is not dropped either: a failed
// dead-letter write leaves the entry unacknowledged, surfaces the error,
// and logs it.
func TestLoopKeepsEntryPendingWhenDeadLetterFails(t *testing.T) {
	j := &journal{}
	dead := &fakeDeadLetters{err: errors.New("dead stream unwritable")}
	log := &fakeLogger{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	loop := orchestrator.NewLoop(client, orchestrator.NewMachine(fakeEvents{j}, fakeAssignments{j}, nil), dead, log)
	loop.Block = 25 * time.Millisecond
	seed(t, client, "not json {{")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !dead.attempted() {
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "dead-lettering run request") {
			t.Fatalf("Run() error = %v, want the dead-letter failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after the dead-letter failure")
	}

	pending, err := client.XPending(context.Background(), orchestrator.StreamPipelineRequests, "orchestrator").Result()
	if err != nil {
		t.Fatalf("XPending() error = %v", err)
	}
	if pending.Count != 1 {
		t.Errorf("pending count = %d, want 1 (the poison entry stays unacknowledged)", pending.Count)
	}
	hasLog(t, log, "error:run request dead-letter write failed")
}

func TestLoopStopsWhenContextCancelledBeforeStart(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	loop := orchestrator.NewLoop(client, orchestrator.NewMachine(fakeEvents{&journal{}}, fakeAssignments{&journal{}}, nil), nil, nil)
	loop.Block = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := loop.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil when cancelled before start", err)
	}
}
