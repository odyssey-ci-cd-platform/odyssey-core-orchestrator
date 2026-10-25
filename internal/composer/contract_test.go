package composer_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/composer"
)

// Requirement trace: what the composer publishes is byte-identical to
// what the orchestrator consumes — the composer's run request round-trips
// the same golden fixture the orchestrator's decode is pinned against
// (dispatcher contract § Run request; issue gh-22, audit AUD-006).

func compact(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("json.Compact() error = %v\n%s", err, b)
	}
	return buf.Bytes()
}

func TestRunRequestMatchesContractFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "run-request.json"))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	var req composer.RunRequest
	if err := json.Unmarshal(data, &req); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\n%s", err, data)
	}
	if err := req.Validate(); err != nil {
		t.Errorf("the contract's own run-request example fails its field rules: %v", err)
	}

	back, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if want, got := compact(t, data), compact(t, back); !bytes.Equal(got, want) {
		t.Errorf("marshalled shape does not match the contract fixture:\ngot:  %s\nwant: %s", got, want)
	}
}
