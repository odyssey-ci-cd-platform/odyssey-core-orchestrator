// Command orchestrator runs the cluster-side orchestrator as a deployable
// service: it consumes run requests, sequences pipeline stages, and emits
// lifecycle events (README § Architecture). Configuration is environmental
// — this file parses it; main.go wires it.
package main

import (
	"fmt"
	"net"
	"strconv"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Config is the binary's deployment configuration. Stream names are not
// configurable: the dispatcher contract (§ Streams) pins them by agreement,
// so an env override could silently detach this service from its peers.
// The queue trims and the listen addresses are the deployment's to set.
type Config struct {
	// RedisAddr dials the queue/bus Redis (host:port).
	RedisAddr string
	// HealthAddr is the health/metrics HTTP listen address (host:port).
	HealthAddr string
	// JobsMaxLen trims odyssey:jobs (contract § Delivery parameters:
	// v1 default 100,000, overridable by deployment config).
	JobsMaxLen int64
	// DeadMaxLen trims odyssey:pipeline-requests-dead (contract default
	// 1,000, overridable by deployment config).
	DeadMaxLen int64
}

// LoadConfig parses the environment through getenv. Unset variables take
// the contract's v1 defaults; every violation is reported in one joined
// error, so a misconfigured deployment fails at startup with the full list.
func LoadConfig(getenv func(string) string) (Config, error) {
	// Trims default to the contract's v1 delivery parameters, named in
	// the package that applies them, so cmd cannot drift from the wire.
	cfg := Config{
		RedisAddr:  envDefault(getenv, "ODYSSEY_REDIS_ADDR", "localhost:6379"),
		HealthAddr: envDefault(getenv, "ODYSSEY_HEALTH_ADDR", "127.0.0.1:8080"),
		JobsMaxLen: orchestrator.MaxStreamLen,
		DeadMaxLen: orchestrator.MaxDeadLen,
	}

	var errs []error
	for name, dst := range map[string]*string{
		"ODYSSEY_REDIS_ADDR":  &cfg.RedisAddr,
		"ODYSSEY_HEALTH_ADDR": &cfg.HealthAddr,
	} {
		if _, _, err := net.SplitHostPort(*dst); err != nil {
			errs = append(errs, fmt.Errorf("%s must be host:port, got %q", name, *dst))
		}
	}
	for name, dst := range map[string]*int64{
		"ODYSSEY_JOBS_MAXLEN": &cfg.JobsMaxLen,
		"ODYSSEY_DEAD_MAXLEN": &cfg.DeadMaxLen,
	} {
		v := getenv(name)
		if v == "" {
			continue // unset: keep the contract default
		}
		n, err := parsePositive(name, v)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		*dst = n
	}
	return cfg, joinErrs(errs)
}

// envDefault returns getenv(name), or def when unset or empty.
func envDefault(getenv func(string) string, name, def string) string {
	if v := getenv(name); v != "" {
		return v
	}
	return def
}

// parsePositive parses a strictly positive integer trim bound.
func parsePositive(name, v string) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, v)
	}
	return n, nil
}

// joinErrs reports every collected violation as one error, or nil.
func joinErrs(errs []error) error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	}
	joined := errs[0]
	for _, err := range errs[1:] {
		joined = fmt.Errorf("%v; %w", joined, err)
	}
	return joined
}
