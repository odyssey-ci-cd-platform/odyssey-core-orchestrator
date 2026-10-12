package pipeline_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// Requirement trace: the composer validates pipeline.toml at the door and
// attaches the contract's resolved configuration (dispatcher contract,
// § Resolved pipeline configuration; issue gh-5).

const validMinimal = `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`

const validFull = `
[pipeline]
name = "demo"
stages = ["build", "test"]

[jobs.compile]
stage = "build"
image = "alpine"
setup = ["apk add git", "apk add make"]
env = { KEY = "value" }

[[jobs.compile.steps]]
name = "compile"
run = "make"
timeout = 300

[jobs.test]
stage = "test"
image = "golang"

[[jobs.test.steps]]
name = "unit"
run = "go test ./..."
`

func mustParse(t *testing.T, data string) pipeline.Config {
	t.Helper()
	cfg, err := pipeline.Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse() error = %v, want nil", err)
	}
	return cfg
}

func TestParseMinimal(t *testing.T) {
	cfg := mustParse(t, validMinimal)

	if cfg.Pipeline != "demo" {
		t.Errorf("Pipeline = %q, want %q", cfg.Pipeline, "demo")
	}
	if len(cfg.Stages) != 1 || cfg.Stages[0] != "build" {
		t.Errorf("Stages = %v, want [build]", cfg.Stages)
	}
	job, ok := cfg.Jobs["compile"]
	if !ok {
		t.Fatalf("Jobs has no %q key", "compile")
	}
	if job.Stage != "build" || job.Image != "alpine" {
		t.Errorf("job = %+v, want stage build, image alpine", job)
	}
	if len(job.Steps) != 1 || job.Steps[0].Name != "compile" || job.Steps[0].Run != "make" {
		t.Errorf("steps = %+v, want one step compile/make", job.Steps)
	}
	if job.Steps[0].TimeoutMS != 0 {
		t.Errorf("TimeoutMS = %d, want 0 (no timeout)", job.Steps[0].TimeoutMS)
	}
}

func TestParseMapsTimeoutToMilliseconds(t *testing.T) {
	cfg := mustParse(t, validFull)

	if got := cfg.Jobs["compile"].Steps[0].TimeoutMS; got != 300 {
		t.Errorf("TimeoutMS = %d, want 300 (TOML timeout is milliseconds)", got)
	}
}

// The resolved configuration must marshal to exactly the contract's wire
// shape: pipeline/stages/jobs at the top, timeout_ms on steps, setup and env
// carried when present.
func TestParseResolvesContractJSONShape(t *testing.T) {
	cfg := mustParse(t, validFull)

	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var doc struct {
		Pipeline string   `json:"pipeline"`
		Stages   []string `json:"stages"`
		Jobs     map[string]struct {
			Stage string            `json:"stage"`
			Image string            `json:"image"`
			Setup []string          `json:"setup"`
			Env   map[string]string `json:"env"`
			Steps []struct {
				Name      string `json:"name"`
				Run       string `json:"run"`
				TimeoutMS int    `json:"timeout_ms"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshaling resolved config: %v\njson: %s", err, raw)
	}
	if doc.Pipeline != "demo" || len(doc.Stages) != 2 {
		t.Errorf("top level = %q, stages %v, want demo + two stages", doc.Pipeline, doc.Stages)
	}
	compile, ok := doc.Jobs["compile"]
	if !ok {
		t.Fatalf("resolved jobs has no compile job; json: %s", raw)
	}
	if len(compile.Setup) != 2 || compile.Env["KEY"] != "value" {
		t.Errorf("compile setup = %v, env = %v, want two setup commands and KEY=value", compile.Setup, compile.Env)
	}
	if len(compile.Steps) != 1 || compile.Steps[0].TimeoutMS != 300 {
		t.Errorf("compile steps = %+v, want one step with timeout_ms 300", compile.Steps)
	}

	// A job without setup and env must omit both keys rather than emit
	// empty containers.
	minimal := mustParse(t, validMinimal)
	rawMinimal, err := json.Marshal(minimal)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(rawMinimal), `"setup"`) || strings.Contains(string(rawMinimal), `"env"`) {
		t.Errorf("minimal job json carries empty setup/env: %s", rawMinimal)
	}
}

func TestParseValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		toml string
		want string
	}{
		{
			name: "no stages",
			toml: `
[pipeline]
name = "demo"
stages = []

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: "at least one stage",
		},
		{
			name: "duplicate stage",
			toml: `
[pipeline]
name = "demo"
stages = ["build", "build"]

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: "duplicate stage",
		},
		{
			name: "no jobs",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]
`,
			want: "at least one job",
		},
		{
			name: "job with empty stage",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = ""
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: `job "compile": stage must not be empty`,
		},
		{
			name: "job with undeclared stage",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "test"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: `"test" is not declared`,
		},
		{
			name: "declared stage without jobs",
			toml: `
[pipeline]
name = "demo"
stages = ["build", "test"]

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: `stage "test" has no jobs`,
		},
		{
			name: "missing image",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: `job "compile": image must not be empty`,
		},
		{
			name: "no steps",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine"
`,
			want: `job "compile": must define at least one step`,
		},
		{
			name: "step without name",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
run = "make"
`,
			want: "name must not be empty",
		},
		{
			name: "step without run",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
`,
			want: "run must not be empty",
		},
		{
			name: "negative timeout",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
timeout = -1
`,
			want: "timeout must not be negative",
		},
		{
			name: "empty setup entry",
			toml: `
[pipeline]
name = "demo"
stages = ["build"]

[jobs.compile]
stage = "build"
image = "alpine"
setup = ["apk add git", ""]

[[jobs.compile.steps]]
name = "compile"
run = "make"
`,
			want: `setup entry 2 must not be empty`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pipeline.Parse([]byte(tt.toml))
			if err == nil {
				t.Fatalf("Parse() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Parse() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

// The orchestrator re-validates decoded configs at its trust boundary —
// the rule set must hold on a constructed Config, not only on TOML bytes
// (gh-11; contract § Resolved pipeline configuration).
func TestConfigValidateDirect(t *testing.T) {
	valid := pipeline.Config{
		Pipeline: "demo",
		Stages:   []string{"build"},
		Jobs: map[string]pipeline.Job{
			"compile": {Stage: "build", Image: "alpine", Steps: []pipeline.Step{{Name: "compile", Run: "make"}}},
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}

	bad := valid
	bad.Jobs = map[string]pipeline.Job{
		"compile": {Stage: "nope", Image: "alpine", Steps: []pipeline.Step{{Name: "compile", Run: "make", TimeoutMS: -1}}},
	}
	err := bad.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want violations")
	}
	for _, want := range []string{"is not declared", "must not be negative"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() error = %q, want it to contain %q", err, want)
		}
	}
}

// The contract requires both boundaries to reject the same pipelines; the
// composer reports every violation in one pass, not the first one.
func TestParseReportsAllViolationsTogether(t *testing.T) {
	invalid := `
[pipeline]
name = "demo"
stages = ["build", "build"]

[jobs.compile]
stage = "test"
image = ""

[[jobs.compile.steps]]
run = "make"
`
	_, err := pipeline.Parse([]byte(invalid))
	if err == nil {
		t.Fatal("Parse() error = nil, want violations")
	}
	for _, want := range []string{"duplicate stage", `"test" is not declared`, "image must not be empty", "name must not be empty"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

// Parity with the dispatcher repo's unknown-key rejection (gh-68): a
// pipeline.toml with keys outside the schema is invalid, not silently
// tolerated.
func TestParseRejectsUnknownKeys(t *testing.T) {
	unknown := `
[pipeline]
name = "demo"
stages = ["build"]
parallelism = 4

[jobs.compile]
stage = "build"
image = "alpine"

[[jobs.compile.steps]]
name = "compile"
run = "make"
`
	_, err := pipeline.Parse([]byte(unknown))
	if err == nil {
		t.Fatal("Parse() error = nil, want unknown-key rejection")
	}
	if !strings.Contains(err.Error(), "parallelism") {
		t.Errorf("error %q does not name the unknown key", err)
	}
}

func TestParseRejectsMalformedTOML(t *testing.T) {
	if _, err := pipeline.Parse([]byte("this is not toml [")); err == nil {
		t.Fatal("Parse() error = nil, want parse error")
	}
}
