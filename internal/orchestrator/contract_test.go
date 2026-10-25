package orchestrator_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the wire matches the contract exactly — the
// contract's JSON examples are golden fixtures, and a marshalled shape
// that carries any key the example does not have (or loses one it does)
// fails the suite (dispatcher contract §§ Run request, Job assignment,
// Event emission responsibilities; issue gh-22, audit AUD-006).
//
// Fixtures live at the repository root so the composer and dispatcher
// repositories can test against the same files.

// compact renders JSON without insignificant whitespace, so byte equality
// pins key sets, key order, names, and values — not fixture indentation.
func compact(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("json.Compact() error = %v\n%s", err, b)
	}
	return buf.Bytes()
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return b
}

// roundTrip decodes data into v, marshals it back, and requires the
// compacted bytes to equal the input — the exactness pin: an unexpected
// key in either direction fails.
func roundTrip(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\n%s", err, data)
	}
	back, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	want := compact(t, data)
	got := compact(t, back)
	if !bytes.Equal(got, want) {
		t.Errorf("marshalled shape does not match the contract fixture:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestAssignmentMatchesContractFixture(t *testing.T) {
	var a orchestrator.Assignment
	roundTrip(t, fixture(t, "assignment.json"), &a)
	if err := a.Validate(); err != nil {
		t.Errorf("the contract's own assignment example fails its field rules: %v", err)
	}
}

// The run-request example's config placeholder is filled with the §
// Resolved pipeline configuration example.
func TestRunRequestMatchesContractFixture(t *testing.T) {
	var req orchestrator.RunRequest
	roundTrip(t, fixture(t, "run-request.json"), &req)
	if err := req.Validate(); err != nil {
		t.Errorf("the contract's own run-request example fails its field rules: %v", err)
	}
}

// The envelope's field list is pinned by the contract (§ Event emission
// responsibilities) and mirrored by the dispatcher repo's internal/domain
// Event; the fixture carries every field so omitempty cannot hide drift.
// The example is a dispatcher's job.finished: shape is pinned here, while
// Event.Validate is this emitter's policy — it refuses event types this
// repo does not emit, so it does not apply to dispatcher-side examples.
func TestEventEnvelopeMatchesContractFixture(t *testing.T) {
	var ev orchestrator.Event
	roundTrip(t, fixture(t, "event-envelope.json"), &ev)
}
