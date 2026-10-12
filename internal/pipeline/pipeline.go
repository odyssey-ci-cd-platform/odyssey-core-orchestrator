// Package pipeline loads and validates .odyssey/pipeline.toml into the
// resolved configuration pinned by the dispatcher contract (§ Resolved
// pipeline configuration). Parse is the door: it decodes, rejects unknown
// keys, and validates every rule before returning, so a returned Config is
// always publishable as-is. The composer is the only component that sees
// TOML; everything downstream of it consumes the resolved shape.
package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
)

// Step is one step in the resolved configuration. pipeline.toml spells the
// timeout `timeout`, the contract's wire shape spells it `timeout_ms`:
// integer milliseconds, 0 = no timeout, negative invalid.
type Step struct {
	Name      string `toml:"name" json:"name"`
	Run       string `toml:"run" json:"run"`
	TimeoutMS int    `toml:"timeout" json:"timeout_ms"`
}

// Job is one job in the resolved configuration.
type Job struct {
	Stage string            `toml:"stage" json:"stage"`
	Image string            `toml:"image" json:"image"`
	Setup []string          `toml:"setup" json:"setup,omitempty"`
	Env   map[string]string `toml:"env" json:"env,omitempty"`
	Steps []Step            `toml:"steps" json:"steps"`
}

// Config is the resolved pipeline configuration a run request carries.
type Config struct {
	Pipeline string         `json:"pipeline"`
	Stages   []string       `json:"stages"`
	Jobs     map[string]Job `json:"jobs"`
}

// tomlRoot mirrors pipeline.toml's layout: a [pipeline] table plus one
// [jobs.<name>] table per job. Its shape is the dispatcher repo's
// internal/config schema; the two must not drift.
type tomlRoot struct {
	Pipeline struct {
		Name   string   `toml:"name"`
		Stages []string `toml:"stages"`
	} `toml:"pipeline"`
	Jobs map[string]Job `toml:"jobs"`
}

// Parse decodes and validates pipeline.toml bytes into the resolved
// configuration. Every violation is reported in one joined error, so a
// fix-one-retry loop never happens.
func Parse(data []byte) (Config, error) {
	var root tomlRoot
	md, err := toml.NewDecoder(bytes.NewReader(data)).Decode(&root)
	if err != nil {
		return Config{}, fmt.Errorf("invalid pipeline.toml: %w", err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, 0, len(undecoded))
		for _, key := range undecoded {
			keys = append(keys, key.String())
		}
		return Config{}, fmt.Errorf("pipeline.toml has unknown keys: %s", strings.Join(keys, ", "))
	}
	cfg := Config{
		Pipeline: root.Pipeline.Name,
		Stages:   root.Pipeline.Stages,
		Jobs:     root.Jobs,
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid pipeline config: %w", err)
	}
	return cfg, nil
}

// Validate enforces the contract's rules on the resolved configuration —
// the same set as the dispatcher repo's internal/config Validate, so what
// passes here passes there. It runs on the decoded shape, so any component
// receiving a Config over the wire can re-check it without a TOML parser.
func (c Config) Validate() error {
	var errs []error

	if len(c.Stages) == 0 {
		errs = append(errs, errors.New("pipeline must declare at least one stage"))
	}
	seen := make(map[string]bool)
	for _, stage := range c.Stages {
		if seen[stage] {
			errs = append(errs, fmt.Errorf("duplicate stage name: %q", stage))
		}
		seen[stage] = true
	}

	if len(c.Jobs) == 0 {
		errs = append(errs, errors.New("pipeline must define at least one job"))
	}
	jobsPerStage := make(map[string]int)
	for name, job := range c.Jobs {
		jobsPerStage[job.Stage]++
		if job.Stage == "" {
			errs = append(errs, fmt.Errorf("job %q: stage must not be empty", name))
		} else if !seen[job.Stage] {
			errs = append(errs, fmt.Errorf("job %q: stage %q is not declared in pipeline.stages", name, job.Stage))
		}
		if job.Image == "" {
			errs = append(errs, fmt.Errorf("job %q: image must not be empty", name))
		}
		for i, setup := range job.Setup {
			if setup == "" {
				errs = append(errs, fmt.Errorf("job %q: setup entry %d must not be empty", name, i+1))
			}
		}
		if len(job.Steps) == 0 {
			errs = append(errs, fmt.Errorf("job %q: must define at least one step", name))
		}
		for i, step := range job.Steps {
			if step.Name == "" {
				errs = append(errs, fmt.Errorf("job %q: step %d: name must not be empty", name, i+1))
			}
			if step.Run == "" {
				errs = append(errs, fmt.Errorf("job %q: step %d (%q): run must not be empty", name, i+1, step.Name))
			}
			if step.TimeoutMS < 0 {
				errs = append(errs, fmt.Errorf("job %q: step %d (%q): timeout must not be negative (milliseconds)", name, i+1, step.Name))
			}
		}
	}

	for _, stage := range c.Stages {
		if jobsPerStage[stage] == 0 {
			errs = append(errs, fmt.Errorf("stage %q has no jobs", stage))
		}
	}
	return errors.Join(errs...)
}
