package main

import (
	"strings"
	"testing"
)

// Requirement trace: the orchestrator runs as a deployable service with
// env-based configuration — the Redis address, the health listen address,
// and the delivery parameters the contract marks deployment-overridable
// (the queue MAXLEN trims) parse from the environment with contract-shape
// defaults, and contradictory values are rejected at parse time, not
// silently (issue gh-24; AUD-002; dispatcher contract § Delivery
// parameters; AGENTS.md design principles).
func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want localhost:6379", cfg.RedisAddr)
	}
	if cfg.HealthAddr != "127.0.0.1:8080" {
		t.Errorf("HealthAddr = %q, want 127.0.0.1:8080", cfg.HealthAddr)
	}
	if cfg.JobsMaxLen != 100000 {
		t.Errorf("JobsMaxLen = %d, want 100000 (contract default)", cfg.JobsMaxLen)
	}
	if cfg.DeadMaxLen != 1000 {
		t.Errorf("DeadMaxLen = %d, want 1000 (contract default)", cfg.DeadMaxLen)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	env := map[string]string{
		"ODYSSEY_REDIS_ADDR":  "redis.internal:6380",
		"ODYSSEY_HEALTH_ADDR": "0.0.0.0:9090",
		"ODYSSEY_JOBS_MAXLEN": "500",
		"ODYSSEY_DEAD_MAXLEN": "10",
	}
	cfg, err := LoadConfig(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg.RedisAddr != "redis.internal:6380" {
		t.Errorf("RedisAddr = %q, want redis.internal:6380", cfg.RedisAddr)
	}
	if cfg.HealthAddr != "0.0.0.0:9090" {
		t.Errorf("HealthAddr = %q, want 0.0.0.0:9090", cfg.HealthAddr)
	}
	if cfg.JobsMaxLen != 500 {
		t.Errorf("JobsMaxLen = %d, want 500", cfg.JobsMaxLen)
	}
	if cfg.DeadMaxLen != 10 {
		t.Errorf("DeadMaxLen = %d, want 10", cfg.DeadMaxLen)
	}
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"non-numeric jobs trim":  {"ODYSSEY_JOBS_MAXLEN": "lots"},
		"non-numeric dead trim":  {"ODYSSEY_DEAD_MAXLEN": "-1"},
		"zero jobs trim":         {"ODYSSEY_JOBS_MAXLEN": "0"},
		"redis addr missing pos": {"ODYSSEY_REDIS_ADDR": "redis.internal"},
		"health addr missing po": {"ODYSSEY_HEALTH_ADDR": "localhost"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConfig(func(key string) string { return env[key] })
			if err == nil {
				t.Fatalf("LoadConfig() error = nil, want rejection")
			}
			if strings.TrimSpace(err.Error()) == "" {
				t.Errorf("error message is empty, want the violated rule")
			}
		})
	}
}
