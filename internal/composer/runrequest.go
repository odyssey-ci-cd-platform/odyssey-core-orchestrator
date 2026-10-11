// Package composer turns origin triggers into validated run requests and
// publishes them onto the pipeline-requests queue. It is fire-and-forget by
// design (ADR 0001): nothing here tracks what happens to a run afterwards.
package composer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// Origin names where a trigger came from (contract § Run request).
type Origin string

const (
	OriginHook Origin = "hook"
	OriginUI   Origin = "ui"
	OriginCI   Origin = "ci"
)

// RunRequest is the message a validated trigger becomes: the contract's
// § Run request shape with the resolved pipeline configuration attached.
type RunRequest struct {
	TriggerID string          `json:"trigger_id"`
	Origin    Origin          `json:"origin"`
	Repo      string          `json:"repo"`
	Commit    string          `json:"commit"`
	Pipeline  string          `json:"pipeline"`
	Config    pipeline.Config `json:"config"`
}

// Validate enforces the contract's envelope field rules. The resolved
// configuration's own validity is pipeline.Parse's job: it was checked when
// the config was attached, and it travels as structured data, not text.
func (r RunRequest) Validate() error {
	var errs []error

	if r.TriggerID == "" {
		errs = append(errs, errors.New("trigger_id must not be empty"))
	}
	switch r.Origin {
	case OriginHook, OriginUI, OriginCI:
	default:
		errs = append(errs, fmt.Errorf("origin must be one of hook, ui, ci, got %q", r.Origin))
	}
	if !validRepo(r.Repo) {
		errs = append(errs, fmt.Errorf("repo must be owner/repo, got %q", r.Repo))
	}
	if !validCommit(r.Commit) {
		errs = append(errs, fmt.Errorf("commit must be a full 40-hex sha, got %q", r.Commit))
	}
	if r.Pipeline == "" {
		errs = append(errs, errors.New("pipeline must not be empty"))
	}
	return errors.Join(errs...)
}

// validRepo accepts an owner/repo slug: exactly one separator, no
// whitespace, neither side empty.
func validRepo(repo string) bool {
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || owner == "" || name == "" {
		return false
	}
	return !strings.ContainsAny(repo, " \t\n\r") && !strings.Contains(name, "/")
}

// validCommit accepts a full sha: 40 hex digits, case-insensitive — a UI
// copying a sha from elsewhere may carry uppercase hex.
func validCommit(commit string) bool {
	if len(commit) != 40 {
		return false
	}
	return !strings.ContainsFunc(commit, func(r rune) bool {
		return !('0' <= r && r <= '9' || 'a' <= r && r <= 'f' || 'A' <= r && r <= 'F')
	})
}
