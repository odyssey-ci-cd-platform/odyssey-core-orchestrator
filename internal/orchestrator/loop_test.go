package orchestrator_test

import (
	"context"
	"errors"
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
	f.j.record("started:" + e.RunID)
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

func newTestLoop(t *testing.T, j *journal) (*orchestrator.Loop, *redis.Client, context.CancelFunc, <-chan error) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	loop := orchestrator.NewLoop(client, fakeEvents{j}, fakeAssignments{j})
	loop.Block = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()
	return loop, client, cancel, done
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

func waitAcked(t *testing.T, client *redis.Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(context.Background(), orchestrator.StreamPipelineRequests, "orchestrator").Result()
		if err != nil {
			t.Fatalf("XPending() error = %v", err)
		}
		if pending.Count == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("entries still pending after deadline, want all acknowledged")
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
	_, client, cancel, done := newTestLoop(t, j)
	seed(t, client, validEnvelope)

	waitFor(t, j, 3) // one started + two assignments
	waitAcked(t, client)
	stopLoop(t, cancel, done)

	entries := j.snapshot()
	if len(entries) != 3 {
		t.Fatalf("journal = %v, want exactly one run's hand-off", entries)
	}
	// The run id comes from the assignment entries; the started event must
	// carry the same id and precede both.
	id := runIDOf(entries[1])
	if entries[0] != "started:"+id {
		t.Errorf("first entry = %q, want the started event under run id %q", entries[0], id)
	}
	if entries[1] != "assign:"+id+":build/compile" || entries[2] != "assign:"+id+":test/test" {
		t.Errorf("assignments = %v, want stage-ordered build/compile then test/test under run id %q", entries[1:], id)
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
	_, client, cancel, done := newTestLoop(t, j)
	seed(t, client, validEnvelope)
	seed(t, client, validEnvelope)

	waitFor(t, j, 6) // two runs, one started + two assignments each
	waitAcked(t, client)
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
			_, client, cancel, done := newTestLoop(t, j)
			seed(t, client, tt.poison)
			seed(t, client, validEnvelope)

			waitFor(t, j, 3) // only the valid run's hand-off
			waitAcked(t, client)
			stopLoop(t, cancel, done)

			ids := startedIDs(j.snapshot())
			if len(ids) != 1 {
				t.Errorf("started ids = %v, want exactly the valid run (poison produced nothing)", ids)
			}
		})
	}
}

// The platform's degradation policy (dispatcher repo ADR 0001): emission
// failures never fail a run — the assignments still hand off.
func TestLoopPublishesAssignmentsWhenEmitFails(t *testing.T) {
	j := &journal{failEmit: true}
	_, client, cancel, done := newTestLoop(t, j)
	seed(t, client, validEnvelope)

	waitFor(t, j, 2) // the two assignments, no usable started event
	waitAcked(t, client)
	stopLoop(t, cancel, done)

	assigns := 0
	for _, e := range j.snapshot() {
		if len(e) > len("assign:") && e[:len("assign:")] == "assign:" {
			assigns++
		}
	}
	if assigns != 2 {
		t.Errorf("%d assignments handed off despite the emit failure, want 2", assigns)
	}
}

func TestLoopStopsWhenContextCancelledBeforeStart(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	loop := orchestrator.NewLoop(client, fakeEvents{&journal{}}, fakeAssignments{&journal{}})
	loop.Block = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := loop.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil when cancelled before start", err)
	}
}
