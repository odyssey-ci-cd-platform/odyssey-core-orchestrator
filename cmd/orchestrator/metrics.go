package main

import (
	"context"
	"expvar"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// registerMetrics publishes the orchestrator's operational numbers as
// expvars for the /debug/vars endpoint (AUD-013): in-flight runs, pending
// entries per consumer group, and the last stage-release latency. Stdlib
// expvar only — prometheus is not a dependency of this module. Queue-lag
// style counts scrape Redis at exposition time; a scrape during a Redis
// blip reports the error string instead of a stale number.
func registerMetrics(m *orchestrator.Machine, rdb redis.UniversalClient) {
	expvar.Publish("orchestrator_inflight_runs", expvar.Func(func() any {
		return m.InFlightRuns()
	}))
	expvar.Publish("orchestrator_last_stage_release_ms", expvar.Func(func() any {
		return float64(m.LastReleaseLatency().Microseconds()) / 1000
	}))

	for _, g := range []struct {
		metric string
		stream string
	}{
		{"orchestrator_pending_pipeline_requests", orchestrator.StreamPipelineRequests},
		{"orchestrator_pending_events", orchestrator.StreamEvents},
	} {
		stream := g.stream
		expvar.Publish(g.metric, expvar.Func(func() any {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			pending, err := rdb.XPending(ctx, stream, "orchestrator").Result()
			if err != nil {
				return "unavailable: " + err.Error()
			}
			return pending.Count
		}))
	}
}
