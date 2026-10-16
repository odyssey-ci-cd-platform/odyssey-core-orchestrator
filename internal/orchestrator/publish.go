package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	// StreamJobs is the queue job assignments publish to (contract
	// § Streams).
	StreamJobs = "odyssey:jobs"

	// MaxStreamLen bounds retention via approximate XADD trimming
	// (contract § Delivery parameters): the queue carries work, not
	// history. A deployment config may override the default.
	MaxStreamLen = 100000
)

// Publisher writes job assignments onto the job queue.
type Publisher struct {
	client redis.UniversalClient
}

// NewPublisher returns a Publisher issuing XADDs through client.
func NewPublisher(client redis.UniversalClient) *Publisher {
	return &Publisher{client: client}
}

// Validate enforces the contract's § Job assignment field rules — the same
// set the dispatcher re-checks at its boundary — so what this repo publishes
// never dies there. Every violation is reported in one joined error.
func (a Assignment) Validate() error {
	var errs []error

	if a.RunID == "" {
		errs = append(errs, errors.New("run_id must not be empty"))
	}
	if a.Pipeline == "" {
		errs = append(errs, errors.New("pipeline must not be empty"))
	}
	if a.Stage == "" {
		errs = append(errs, errors.New("stage must not be empty"))
	}
	if a.Job == "" {
		errs = append(errs, errors.New("job must not be empty"))
	}
	if a.Attempt < 1 {
		errs = append(errs, errors.New("attempt must be at least 1"))
	}
	if a.Source.Repo == "" {
		errs = append(errs, errors.New("source.repo must not be empty"))
	}
	if a.Source.Commit == "" {
		errs = append(errs, errors.New("source.commit must not be empty"))
	}
	if err := a.JobSpec.Validate(a.Job); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Publish writes one assignment as the stream entry's single envelope
// field — the dispatcher bus convention, so both sides of the queue read
// the same entry shape. The door holds at the wire: an assignment that
// fails the contract's field rules is refused, never enqueued.
func (p *Publisher) Publish(ctx context.Context, a Assignment) error {
	if err := a.Validate(); err != nil {
		return fmt.Errorf("refusing to publish invalid assignment: %w", err)
	}
	body, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamJobs,
		MaxLen: MaxStreamLen,
		Approx: true,
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}
