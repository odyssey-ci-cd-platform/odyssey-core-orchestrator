package orchestrator_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: the orchestrator accepts only contract-shaped run
// requests, re-validating at its trust boundary, and turns validated runs
// into stage-ordered assignments (dispatcher contract §§ Run request, Job
// assignment; issue gh-11).

const validEnvelope = `{
  "trigger_id": "hook:42",
  "repo": "org/repo",
  "commit": "8f3a1c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
  "pipeline": "demo",
  "config": {
    "pipeline": "demo",
    "stages": ["build", "test"],
    "jobs": {
      "compile": {
        "stage": "build",
        "image": "alpine",
        "setup": ["apk add make"],
        "env": { "KEY": "value" },
        "steps": [{ "name": "compile", "run": "make", "timeout_ms": 300 }]
      },
      "test": {
        "stage": "test",
        "image": "golang",
        "steps": [{ "name": "unit", "run": "go test ./..." }]
      }
    }
  }
}`

func mustDecode(t *testing.T, envelope string) orchestrator.RunRequest {
	t.Helper()
	req, err := orchestrator.DecodeRunRequest([]byte(envelope))
	if err != nil {
		t.Fatalf("DecodeRunRequest() error = %v, want nil", err)
	}
	return req
}

func TestDecodeRunRequest(t *testing.T) {
	req := mustDecode(t, validEnvelope)

	if req.TriggerID != "hook:42" || req.Repo != "org/repo" || req.Pipeline != "demo" {
		t.Errorf("decoded = %+v, want trigger hook:42, org/repo, demo", req)
	}
	if len(req.Config.Stages) != 2 || req.Config.Stages[0] != "build" {
		t.Errorf("stages = %v, want [build test]", req.Config.Stages)
	}
	if _, ok := req.Config.Jobs["test"]; !ok {
		t.Errorf("decoded config has no test job: %+v", req.Config.Jobs)
	}
}

// The contract evolves additively: consumers ignore unknown fields, so a
// producer ahead of this repo must not break decoding.
func TestDecodeIgnoresUnknownFields(t *testing.T) {
	envelope := strings.Replace(validEnvelope,
		`"trigger_id": "hook:42",`,
		`"trigger_id": "hook:42", "future_field": {"nested": true},`, 1)
	if _, err := orchestrator.DecodeRunRequest([]byte(envelope)); err != nil {
		t.Fatalf("DecodeRunRequest() error = %v, want nil (unknown fields ignored)", err)
	}
}

func TestDecodeRejectsMalformedJSON(t *testing.T) {
	_, err := orchestrator.DecodeRunRequest([]byte("not json {"))
	if err == nil {
		t.Fatal("DecodeRunRequest() error = nil, want parse error")
	}
	if !strings.Contains(err.Error(), "invalid run request envelope") {
		t.Errorf("error = %q, want envelope wording", err)
	}
}

func TestValidateRejectsEnvelopeViolations(t *testing.T) {
	tests := []struct {
		name  string
		mutot func(*orchestrator.RunRequest)
		want  string
	}{
		{
			name:  "empty trigger id",
			mutot: func(r *orchestrator.RunRequest) { r.TriggerID = "" },
			want:  "trigger_id must not be empty",
		},
		{
			name:  "repo without separator",
			mutot: func(r *orchestrator.RunRequest) { r.Repo = "justname" },
			want:  "repo must be owner/repo",
		},
		{
			name:  "short commit",
			mutot: func(r *orchestrator.RunRequest) { r.Commit = "8f3a1c" },
			want:  "commit must be a full 40-hex sha",
		},
		{
			name:  "empty pipeline name",
			mutot: func(r *orchestrator.RunRequest) { r.Pipeline = "" },
			want:  "pipeline must not be empty",
		},
		{
			name: "structurally invalid config",
			mutot: func(r *orchestrator.RunRequest) {
				bad := r.Config.Jobs["test"]
				bad.Stage = "nope"
				r.Config.Jobs["test"] = bad
			},
			want: "is not declared",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := mustDecode(t, validEnvelope)
			tt.mutot(&req)
			err := req.Validate()
			if err == nil {
				t.Fatalf("Validate() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Validate() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestSegment(t *testing.T) {
	req := mustDecode(t, validEnvelope)

	assignments, err := orchestrator.Segment(req, "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("Segment() produced %d assignments, want 2", len(assignments))
	}

	// Stage order is the config's declared order: build first, then test.
	if assignments[0].Stage != "build" || assignments[0].Job != "compile" {
		t.Errorf("first assignment = %s/%s, want build/compile", assignments[0].Stage, assignments[0].Job)
	}
	if assignments[1].Stage != "test" || assignments[1].Job != "test" {
		t.Errorf("second assignment = %s/%s, want test/test", assignments[1].Stage, assignments[1].Job)
	}
	for i, a := range assignments {
		if a.RunID != "run-1" || a.Pipeline != "demo" || a.Attempt != 1 {
			t.Errorf("assignment %d = %+v, want run run-1, demo, attempt 1", i, a)
		}
		if a.Source.Repo != "org/repo" || a.Source.Commit != "8f3a1c4d5e6f708192a3b4c5d6e7f8091a2b3c4d" {
			t.Errorf("assignment %d source = %+v, want the request's source", i, a.Source)
		}
	}

	// The job spec mirrors the resolved config job exactly.
	compile := assignments[0].JobSpec
	if compile.Image != "alpine" || len(compile.Setup) != 1 || compile.Env["KEY"] != "value" {
		t.Errorf("compile job spec = %+v, want alpine, one setup command, KEY=value", compile)
	}
	if len(compile.Steps) != 1 || compile.Steps[0].TimeoutMS != 300 {
		t.Errorf("compile steps = %+v, want one step with timeout_ms 300", compile.Steps)
	}
}

// Assignments marshal to exactly the contract's § Job assignment shape —
// the dispatcher validates this at its trust boundary, so no key may drift.
func TestSegmentMarshalsContractShape(t *testing.T) {
	req := mustDecode(t, validEnvelope)

	assignments, err := orchestrator.Segment(req, "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	raw, err := json.Marshal(assignments[0])
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("assignment is not JSON: %v\n%s", err, raw)
	}
	for _, key := range []string{"run_id", "pipeline", "stage", "job", "attempt", "source", "job_spec"} {
		if _, ok := doc[key]; !ok {
			t.Errorf("assignment missing %q: %s", key, raw)
		}
	}
	source, ok := doc["source"].(map[string]any)
	if !ok {
		t.Fatalf("source is not an object: %s", raw)
	}
	for _, key := range []string{"repo", "commit"} {
		if _, ok := source[key]; !ok {
			t.Errorf("source missing %q: %s", key, raw)
		}
	}
	jobSpec, ok := doc["job_spec"].(map[string]any)
	if !ok {
		t.Fatalf("job_spec is not an object: %s", raw)
	}
	for _, key := range []string{"image", "steps"} {
		if _, ok := jobSpec[key]; !ok {
			t.Errorf("job_spec missing %q: %s", key, raw)
		}
	}
}

// Segment order is deterministic: declared stage order, jobs sorted by
// name within a stage (the dispatcher's AUD-008 convention) — two runs
// over the same input produce identical assignment lists.
func TestSegmentIsDeterministic(t *testing.T) {
	// Declared stage order deliberately differs from alphabetical, and job
	// names within a stage are deliberately unsorted in the envelope.
	envelope := `{
  "trigger_id": "hook:42",
  "repo": "org/repo",
  "commit": "8f3a1c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
  "pipeline": "demo",
  "config": {
    "pipeline": "demo",
    "stages": ["test", "build"],
    "jobs": {
      "zcompile": { "stage": "build", "image": "alpine", "steps": [{ "name": "c", "run": "make" }] },
      "atest": { "stage": "test", "image": "golang", "steps": [{ "name": "t", "run": "go vet" }] },
      "mtest": { "stage": "test", "image": "golang", "steps": [{ "name": "t", "run": "go test" }] }
    }
  }
}`
	req := mustDecode(t, envelope)

	first, err := orchestrator.Segment(req, "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}
	second, err := orchestrator.Segment(req, "run-1")
	if err != nil {
		t.Fatalf("Segment() error = %v", err)
	}

	want := []struct{ stage, job string }{
		{"test", "atest"},
		{"test", "mtest"},
		{"build", "zcompile"},
	}
	if len(first) != len(want) {
		t.Fatalf("Segment() produced %d assignments, want %d", len(first), len(want))
	}
	for i, w := range want {
		if first[i].Stage != w.stage || first[i].Job != w.job {
			t.Errorf("assignment %d = %s/%s, want %s/%s", i, first[i].Stage, first[i].Job, w.stage, w.job)
		}
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("Segment() is not deterministic: %+v vs %+v", first, second)
	}
}

func TestSegmentRefusesInvalidRequest(t *testing.T) {
	req := mustDecode(t, validEnvelope)
	req.Repo = "justname"

	assignments, err := orchestrator.Segment(req, "run-1")
	if err == nil {
		t.Fatal("Segment() error = nil, want refusal")
	}
	if !strings.Contains(err.Error(), "refusing to segment") {
		t.Errorf("error = %q, want refusal wording", err)
	}
	if assignments != nil {
		t.Errorf("Segment() returned assignments on refusal: %+v", assignments)
	}
}

func TestSegmentRefusesEmptyRunID(t *testing.T) {
	req := mustDecode(t, validEnvelope)

	if _, err := orchestrator.Segment(req, ""); err == nil {
		t.Fatal("Segment() error = nil, want run_id rejection")
	}
}
