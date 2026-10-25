// Package orchestrator turns contract-valid run requests into stage-ordered
// job assignments (dispatcher contract §§ Run request, Job assignment). It
// owns its consumer-side run-request type outright: the queue is untrusted
// input, so everything is re-validated here regardless of what the composer
// published. This slice is pure — bytes in, values out; the queue loop is
// the next work item.
package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// RunRequest is the orchestrator's own decode of a contract run request
// envelope. Unknown fields are ignored by decoding (contract: additive
// evolution), so a producer ahead of this repo cannot break it.
type RunRequest struct {
	TriggerID string          `json:"trigger_id"`
	Repo      string          `json:"repo"`
	Commit    string          `json:"commit"`
	Pipeline  string          `json:"pipeline"`
	Config    pipeline.Config `json:"config"`
}

// DecodeRunRequest parses a queue envelope into a RunRequest. Decoding only
// — call Validate before acting on the result.
func DecodeRunRequest(data []byte) (RunRequest, error) {
	var req RunRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return RunRequest{}, fmt.Errorf("invalid run request envelope: %w", err)
	}
	return req, nil
}

// Validate enforces the contract's envelope field rules at this trust
// boundary, re-validating the attached config with the shared rule set.
// Every violation is reported in one joined error.
func (r RunRequest) Validate() error {
	var errs []error

	if r.TriggerID == "" {
		errs = append(errs, errors.New("trigger_id must not be empty"))
	}
	owner, name, ok := strings.Cut(r.Repo, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		errs = append(errs, errors.New("repo must be owner/repo"))
	}
	if !isFullSHA(r.Commit) {
		errs = append(errs, errors.New("commit must be a full 40-hex sha"))
	}
	if r.Pipeline == "" {
		errs = append(errs, errors.New("pipeline must not be empty"))
	}
	if r.Pipeline != r.Config.Pipeline {
		errs = append(errs, fmt.Errorf("pipeline %q does not match config.pipeline %q", r.Pipeline, r.Config.Pipeline))
	}
	if err := r.Config.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// isFullSHA reports whether s is a full 40-character hex sha.
func isFullSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

// Source is what the dispatcher must provision before executing: a shallow
// fetch of Repo at Commit.
type Source struct {
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
}

// JobSpec is a job's execution spec on the assignment wire (contract §
// Job assignment): the spec alone, because the job's name and stage ride
// the envelope — a dispatcher reconstructs its domain.Job from both.
type JobSpec struct {
	Image string            `json:"image"`
	Setup []string          `json:"setup,omitempty"`
	Env   map[string]string `json:"env,omitempty"`
	Steps []pipeline.Step   `json:"steps"`
}

// Assignment is one job assignment in the contract's § Job assignment shape:
// run context plus job spec, never outcomes.
type Assignment struct {
	RunID    string  `json:"run_id"`
	Pipeline string  `json:"pipeline"`
	Stage    string  `json:"stage"`
	Job      string  `json:"job"`
	Attempt  int     `json:"attempt"`
	Source   Source  `json:"source"`
	JobSpec  JobSpec `json:"job_spec"`
}

// Segment turns a validated run request plus a run id into one assignment
// per job, in declared stage order with job names sorted within a stage
// (the dispatcher's AUD-008 determinism convention). Every assignment
// carries attempt 1 and the request's source. The caller must emit
// pipeline.started before publishing any assignment of the run — the
// results DB's parent-row invariant depends on that ordering.
func Segment(req RunRequest, runID string) ([]Assignment, error) {
	if runID == "" {
		return nil, errors.New("run_id must not be empty")
	}
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("refusing to segment: %w", err)
	}

	src := Source{Repo: req.Repo, Commit: req.Commit}
	var assignments []Assignment
	for _, stage := range req.Config.Stages {
		names := make([]string, 0, len(req.Config.Jobs))
		for name, job := range req.Config.Jobs {
			if job.Stage == stage {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			job := req.Config.Jobs[name]
			assignments = append(assignments, Assignment{
				RunID:    runID,
				Pipeline: req.Pipeline,
				Stage:    stage,
				Job:      name,
				Attempt:  1,
				Source:   src,
				JobSpec:  JobSpec{Image: job.Image, Setup: job.Setup, Env: job.Env, Steps: job.Steps},
			})
		}
	}
	return assignments, nil
}
