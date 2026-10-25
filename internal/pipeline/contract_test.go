package pipeline_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// Requirement trace: the resolved configuration on the wire matches the
// contract's § Resolved pipeline configuration example exactly — the
// example is a golden fixture, and a marshalled shape that carries any
// key the example does not have (or loses one it does) fails the suite
// (issue gh-22, audit AUD-006).

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

func TestConfigMatchesContractFixture(t *testing.T) {
	data := fixture(t, "config.json")

	var cfg pipeline.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\n%s", err, data)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("the contract's own config example fails its field rules: %v", err)
	}

	back, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if want, got := compact(t, data), compact(t, back); !bytes.Equal(got, want) {
		t.Errorf("marshalled shape does not match the contract fixture:\ngot:  %s\nwant: %s", got, want)
	}
}
