package composer_test

import (
	"strings"
	"testing"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/composer"
	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// Requirement trace: the contract pins the run request's field rules
// (§ Run request) — the composer enforces them before anything reaches the
// queue (issue gh-7).

func validRequest() composer.RunRequest {
	return composer.RunRequest{
		TriggerID: "hook:42",
		Origin:    composer.OriginHook,
		Repo:      "org/repo",
		Commit:    "8f3a1c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
		Pipeline:  "demo",
		Config: pipeline.Config{
			Pipeline: "demo",
			Stages:   []string{"build"},
			Jobs: map[string]pipeline.Job{
				"compile": {
					Stage: "build",
					Image: "alpine",
					Steps: []pipeline.Step{{Name: "compile", Run: "make"}},
				},
			},
		},
	}
}

func TestValidateAcceptsContractShape(t *testing.T) {
	if err := validRequest().Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidateRejectsContractViolations(t *testing.T) {
	tests := []struct {
		name  string
		mutot func(*composer.RunRequest)
		want  string
	}{
		{
			name:  "empty trigger id",
			mutot: func(r *composer.RunRequest) { r.TriggerID = "" },
			want:  "trigger_id must not be empty",
		},
		{
			name:  "unknown origin",
			mutot: func(r *composer.RunRequest) { r.Origin = "cron" },
			want:  `origin must be one of hook, ui, ci, got "cron"`,
		},
		{
			name:  "empty origin",
			mutot: func(r *composer.RunRequest) { r.Origin = "" },
			want:  "origin must be one of",
		},
		{
			name:  "repo without separator",
			mutot: func(r *composer.RunRequest) { r.Repo = "justname" },
			want:  "repo must be owner/repo",
		},
		{
			name:  "repo with two separators",
			mutot: func(r *composer.RunRequest) { r.Repo = "org/group/repo" },
			want:  "repo must be owner/repo",
		},
		{
			name:  "repo with empty owner",
			mutot: func(r *composer.RunRequest) { r.Repo = "/repo" },
			want:  "repo must be owner/repo",
		},
		{
			name:  "repo with whitespace",
			mutot: func(r *composer.RunRequest) { r.Repo = "or g/repo" },
			want:  "repo must be owner/repo",
		},
		{
			name:  "short commit",
			mutot: func(r *composer.RunRequest) { r.Commit = "8f3a1c4" },
			want:  "commit must be a full 40-hex sha",
		},
		{
			name:  "non-hex commit",
			mutot: func(r *composer.RunRequest) { r.Commit = strings.Repeat("g", 40) },
			want:  "commit must be a full 40-hex sha",
		},
		{
			name:  "empty pipeline name",
			mutot: func(r *composer.RunRequest) { r.Pipeline = "" },
			want:  "pipeline must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
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

// The contract pins 40 hex digits, not lowercase ones: a UI that copies a
// sha from elsewhere may carry uppercase hex.
func TestValidateAcceptsUppercaseSha(t *testing.T) {
	req := validRequest()
	req.Commit = strings.ToUpper(req.Commit)
	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

// All envelope violations surface together, in one error.
func TestValidateReportsAllViolationsTogether(t *testing.T) {
	req := validRequest()
	req.TriggerID = ""
	req.Origin = "cron"
	req.Commit = "short"
	err := req.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want violations")
	}
	for _, want := range []string{"trigger_id must not be empty", "origin must be one of", "commit must be a full 40-hex sha"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}
