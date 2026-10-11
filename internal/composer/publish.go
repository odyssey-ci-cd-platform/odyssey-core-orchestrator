package composer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	// StreamPipelineRequests is the queue run requests publish to
	// (contract § Streams).
	StreamPipelineRequests = "odyssey:pipeline-requests"

	// MaxStreamLen bounds retention via approximate XADD trimming
	// (contract § Delivery parameters): the queue carries work, not
	// history. A deployment config may override the default.
	MaxStreamLen = 10000
)

// Publisher writes run requests onto the pipeline-requests queue.
type Publisher struct {
	client redis.UniversalClient
}

// NewPublisher returns a Publisher issuing XADDs through client.
func NewPublisher(client redis.UniversalClient) *Publisher {
	return &Publisher{client: client}
}

// Publish writes one run request as the stream entry's single envelope
// field — the dispatcher repo's bus convention, mirrored here so both
// sides of this repo's internal queue read the same entry shape. The door
// holds at the wire: a request that fails validation is refused, never
// enqueued.
func (p *Publisher) Publish(ctx context.Context, req RunRequest) error {
	if err := req.Validate(); err != nil {
		return fmt.Errorf("refusing to publish invalid run request: %w", err)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamPipelineRequests,
		MaxLen: MaxStreamLen,
		Approx: true,
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}
