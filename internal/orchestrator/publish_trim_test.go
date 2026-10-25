package orchestrator_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

// Requirement trace: the queue trims are deployment-overridable delivery
// parameters (issue gh-24; dispatcher contract § Delivery parameters —
// "The deployment config overrides them"), so a deployment's smaller
// MAXLEN must reach the XADD wire, not just a struct field.

func validAssignment() orchestrator.Assignment {
	return orchestrator.Assignment{
		RunID:    "abc123",
		Pipeline: "demo",
		Stage:    "build",
		Job:      "compile",
		Attempt:  1,
		Source:   orchestrator.Source{Repo: "org/repo", Commit: "8f3a1c8f3a1c8f3a1c8f3a1c8f3a1c8f3a1c8f3a"},
		JobSpec: orchestrator.JobSpec{
			Image: "alpine",
			Steps: []pipeline.Step{{Name: "compile", Run: "make", TimeoutMS: 1000}},
		},
	}
}

func xlen(t *testing.T, client *redis.Client, stream string) int64 {
	t.Helper()
	n, err := client.XLen(context.Background(), stream).Result()
	if err != nil {
		t.Fatalf("XLen(%s) error = %v", stream, err)
	}
	return n
}

func TestPublisherHonorsDeploymentTrims(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	p := orchestrator.NewPublisher(client)
	p.MaxLen = 2
	p.MaxDeadLen = 2

	for i := 0; i < 3; i++ {
		if err := p.Publish(context.Background(), validAssignment()); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		if err := p.PublishDeadLetter(context.Background(), orchestrator.DeadLetter{Reason: orchestrator.DeadReasonDecode}); err != nil {
			t.Fatalf("PublishDeadLetter() error = %v", err)
		}
	}
	if got := xlen(t, client, orchestrator.StreamJobs); got != 2 {
		t.Errorf("XLen(%s) = %d, want 2 (deployment trim)", orchestrator.StreamJobs, got)
	}
	if got := xlen(t, client, orchestrator.StreamPipelineRequestsDead); got != 2 {
		t.Errorf("XLen(%s) = %d, want 2 (deployment trim)", orchestrator.StreamPipelineRequestsDead, got)
	}
}

func TestPublisherDefaultsToContractTrims(t *testing.T) {
	p := orchestrator.NewPublisher(nil)
	if p.MaxLen != orchestrator.MaxStreamLen {
		t.Errorf("MaxLen = %d, want the contract default %d", p.MaxLen, orchestrator.MaxStreamLen)
	}
	if p.MaxDeadLen != orchestrator.MaxDeadLen {
		t.Errorf("MaxDeadLen = %d, want the contract default %d", p.MaxDeadLen, orchestrator.MaxDeadLen)
	}
}
