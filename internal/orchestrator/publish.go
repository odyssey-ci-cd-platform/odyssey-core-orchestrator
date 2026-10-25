package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/pipeline"
)

const (
	// StreamJobs is the queue job assignments publish to (contract
	// § Streams).
	StreamJobs = "odyssey:jobs"

	// StreamPipelineRequestsDead is where poison run requests land for
	// human inspection (contract § Streams): a discarded request is a
	// vanished user run — never a silent one.
	StreamPipelineRequestsDead = "odyssey:pipeline-requests-dead"

	// MaxStreamLen bounds retention via approximate XADD trimming
	// (contract § Delivery parameters): the queue carries work, not
	// history. A deployment config may override the default.
	MaxStreamLen = 100000

	// MaxDeadLen bounds retention on the dead stream (contract § Delivery
	// parameters): an inspection queue, small by design. A deployment
	// config may override the default.
	MaxDeadLen = 1000
)

// Dead-letter reasons (contract § Run request: the dead-letter wrap's
// reason field, mirroring odyssey:jobs-dead's).
const (
	// DeadReasonDecode marks an entry whose run request could not be
	// read: no envelope field, or bytes that are not JSON.
	DeadReasonDecode = "decode"

	// DeadReasonValidation marks a decodable request that failed
	// re-validation at this trust boundary.
	DeadReasonValidation = "validation"
)

// DeadLetter is the dead-letter wrap for a discarded run request,
// mirroring odyssey:jobs-dead: a reason plus the original. Request embeds
// the original envelope when it was decodable JSON; Raw carries the
// original entry's bytes when it was not; an entry with no envelope field
// carries the reason alone.
type DeadLetter struct {
	Reason  string          `json:"reason"`
	Request json.RawMessage `json:"request,omitempty"`
	Raw     string          `json:"raw,omitempty"`
}

// Publisher writes job assignments onto the job queue.
type Publisher struct {
	client redis.UniversalClient

	// MaxLen trims the job queue and MaxDeadLen the dead-letter queue
	// via approximate XADD trimming; NewPublisher sets the contract's
	// v1 defaults. A deployment config may override both.
	MaxLen     int64
	MaxDeadLen int64
}

// NewPublisher returns a Publisher issuing XADDs through client.
func NewPublisher(client redis.UniversalClient) *Publisher {
	return &Publisher{client: client, MaxLen: MaxStreamLen, MaxDeadLen: MaxDeadLen}
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
	// The spec rides without its stage (contract § Job assignment); the
	// envelope's stage is re-attached so the shared config rule set applies
	// unchanged.
	spec := pipeline.Job{Stage: a.Stage, Image: a.JobSpec.Image, Setup: a.JobSpec.Setup, Env: a.JobSpec.Env, Steps: a.JobSpec.Steps}
	if err := spec.Validate(a.Job); err != nil {
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
		MaxLen: p.MaxLen,
		Approx: true,
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}

// PublishDeadLetter writes one dead-letter wrap as the stream entry's
// single envelope field — the same bus convention as every other queue,
// bounded at the contract's inspection-queue trim default.
func (p *Publisher) PublishDeadLetter(ctx context.Context, d DeadLetter) error {
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamPipelineRequestsDead,
		MaxLen: p.MaxDeadLen,
		Approx: true,
		Values: map[string]any{"envelope": string(body)},
	}).Err()
}
