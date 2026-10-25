package composer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/composer"
	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

func newTestRedis(t *testing.T) (*composer.Publisher, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return composer.NewPublisher(client), client
}

// The entry shape mirrors the dispatcher bus convention: one JSON body
// under the envelope field, on the contract's stream.
func TestPublishWritesEnvelope(t *testing.T) {
	pub, client := newTestRedis(t)

	if err := pub.Publish(context.Background(), validRequest()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	entries, err := client.XRange(context.Background(), composer.StreamPipelineRequests, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream has %d entries, want 1", len(entries))
	}
	envelope, ok := entries[0].Values["envelope"].(string)
	if !ok {
		t.Fatalf("entry values = %v, want an envelope string", entries[0].Values)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(envelope), &doc); err != nil {
		t.Fatalf("envelope is not JSON: %v\n%s", err, envelope)
	}
	for _, key := range []string{"trigger_id", "repo", "commit", "pipeline", "config"} {
		if _, ok := doc[key]; !ok {
			t.Errorf("envelope missing %q: %s", key, envelope)
		}
	}
	// The orchestrator is origin-blind (gh-9): intake provenance never
	// crosses the queue — the composer service owns it.
	if _, ok := doc["origin"]; ok {
		t.Errorf("envelope carries origin: %s", envelope)
	}
	config, ok := doc["config"].(map[string]any)
	if !ok {
		t.Fatalf("config is not an object: %s", envelope)
	}
	if _, ok := config["jobs"]; !ok {
		t.Errorf("config missing jobs: %s", envelope)
	}
}

// The door holds at the wire: an invalid request is refused, the queue
// stays empty.
func TestPublishRefusesInvalidRequest(t *testing.T) {
	pub, client := newTestRedis(t)

	req := validRequest()
	req.Commit = "short"
	err := pub.Publish(context.Background(), req)
	if err == nil {
		t.Fatal("Publish() error = nil, want refusal")
	}
	if !strings.Contains(err.Error(), "refusing to publish") {
		t.Errorf("error = %q, want refusal wording", err)
	}
	n, err := client.XLen(context.Background(), composer.StreamPipelineRequests).Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if n != 0 {
		t.Errorf("stream has %d entries, want 0", n)
	}
}

// The door holds at the wire for the attached config too: an envelope
// wrapped around an invalid config is refused, never enqueued (AUD-006).
func TestPublishRefusesInvalidConfig(t *testing.T) {
	pub, client := newTestRedis(t)

	req := validRequest()
	req.Config.Jobs["compile"] = pipeline.Job{Stage: "build"} // no image, no steps
	err := pub.Publish(context.Background(), req)
	if err == nil {
		t.Fatal("Publish() error = nil, want refusal for an invalid config")
	}
	if !strings.Contains(err.Error(), "refusing to publish") {
		t.Errorf("error = %q, want refusal wording", err)
	}
	n, err := client.XLen(context.Background(), composer.StreamPipelineRequests).Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if n != 0 {
		t.Errorf("stream has %d entries, want 0", n)
	}
}

// Retention is work-not-history: approximate MAXLEN trimming at the
// contract's delivery parameter.
func TestPublishTrimsToMaxLen(t *testing.T) {
	pub, client := newTestRedis(t)
	ctx := context.Background()

	for i := 0; i <= composer.MaxStreamLen; i++ {
		req := validRequest()
		req.TriggerID = fmt.Sprintf("hook:%d", i)
		if err := pub.Publish(ctx, req); err != nil {
			t.Fatalf("Publish() error = %v at %d", err, i)
		}
	}
	n, err := client.XLen(ctx, composer.StreamPipelineRequests).Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if n > composer.MaxStreamLen {
		t.Errorf("stream length = %d, want <= %d (approximate MAXLEN)", n, composer.MaxStreamLen)
	}
}
