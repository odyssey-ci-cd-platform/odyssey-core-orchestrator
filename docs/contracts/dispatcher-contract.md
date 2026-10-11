# Dispatcher contract

- **Status:** DRAFT — the field-by-field pass with the dispatcher repo is pending (both repos must agree before either codes against it)
- **Binding for:** every message this repo publishes or consumes across the queue boundary

Two queues and one event bus bind this repo to the dispatcher repo. The event envelope itself is already specified in the dispatcher repo (`internal/domain`, `Event`); this record fixes the queue messages and each side's emission responsibilities. Fields evolve additively only — consumers ignore unknown fields, producers never repurpose existing ones.

## Streams

| Stream | Publisher | Consumer | Semantics |
|---|---|---|---|
| `odyssey:pipeline-requests` | composer | orchestrator | run requests; one pipeline run each |
| `odyssey:jobs` | orchestrator | dispatchers (shared consumer group `dispatchers`) | job assignments; ack on job completion |
| `odyssey:events` | orchestrator + dispatchers | orchestrator, results-recorder, consumer services | lifecycle events, existing envelope |

Trim policy: both queues carry work, not history — approximate `MAXLEN` trimming (workload-sized, documented in the deployment config). The event bus keeps its own trim from the dispatcher repo's ADR 0001.

## Run request (composer → orchestrator)

Published on `odyssey:pipeline-requests` after the composer has validated the trigger.

```json
{
  "trigger_id": "hook:42",
  "origin": "hook | ui | ci",
  "repo": "org/repo",
  "commit": "full sha",
  "pipeline": "name from .odyssey/pipeline.toml",
  "config": "<resolved pipeline.toml, attached by the composer — open question ADR 0001 §Open question>"
}
```

Validation is the composer's job and happens before publish: a malformed pipeline configuration never reaches this stream.

## Job assignment (orchestrator → dispatchers)

Published on `odyssey:jobs`, one message per job. Carries run context and the job spec — **never outcomes**.

```json
{
  "run_id": "from the orchestrator's pipeline.started event",
  "pipeline": "demo",
  "stage": "build",
  "job": "compile",
  "attempt": 1,
  "source": { "repo": "org/repo", "commit": "full sha" },
  "job_spec": {
    "image": "alpine",
    "env": { "KEY": "value" },
    "steps": [{ "name": "compile", "run": "make", "timeout_ms": 300000 }]
  }
}
```

The dispatcher validates this at its trust boundary (a queue message is untrusted input): run id, pipeline, stage, job non-empty; exactly one image; at least one step. Invalid assignments dead-letter, they are never executed.

## Event emission responsibilities

| Event | Emitter | Notes |
|---|---|---|
| `pipeline.started` | orchestrator | **before** publishing any job of the run — results DB parent-row invariant |
| `pipeline.finished` | orchestrator | when the stage state machine resolves |
| `job.started` / `job.finished` | dispatcher | payload carries `status`; `job.started` also `stage` |
| `step.started` / `step.finished` | dispatcher | `step.finished` payload: `status`, `exit_code`, `duration_ms` — never stdout/stderr |

Envelope shape and payload discipline: see the dispatcher repo's `internal/domain` (`Event`) — the envelope is the shared contract and evolves additively only.

## Delivery semantics

- Dispatchers form one consumer group; each dispatcher reads only while it has a free runner slot (capacity awareness).
- Ack happens **on job completion**, not on read. A dispatcher renews its pending entry (`XCLAIM ... JUSTID`) on an interval while its runner is alive, so long jobs are not reclaimed.
- A dead dispatcher's entries are claimed by peers after the idle threshold and the job re-executes from scratch; the abandoned attempt is visible in the results DB as a run stuck `pending`.
- Exhausted attempts dead-letter; poison assignments are inspected, never silently dropped.

## Parameters to fix in the field-by-field pass

- claim-renewal interval and idle threshold (must exceed sane job duration; renewal makes false reclaims rare)
- `MAXLEN` per queue
- attempt limits before dead-letter
