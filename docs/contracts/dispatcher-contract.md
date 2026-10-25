# Dispatcher contract

- **Status:** Agreed (2026-10-12) — field-by-field pass complete; the pass was made against the dispatcher repo's `internal/config` and `internal/domain` as they exist on `main`, and agreement is recorded on odyssey-ci-cd-platform/dispatcher issue 35.
- **Revision (2026-10-12, gh-9):** `origin` removed from the run request. The composer is a separate service (its own repo planned; services consolidate into a monorepo later): it consumes origin triggers and owns intake provenance — the orchestrator is origin-blind and never sees which surface produced a run. Nothing consumed the field; removal is safe. trigger_id stays as the composer's correlation/dedup handle.
- **Binding for:** every message this repo publishes or consumes across the queue boundary.

Two queues and one event bus bind this repo to the dispatcher repo. The event envelope itself is specified in the dispatcher repo (`internal/domain`, `Event`); this record fixes the queue messages and each side's emission responsibilities. Fields evolve additively only — consumers ignore unknown fields, producers never repurpose existing ones. The JSON examples below are mirrored as golden fixtures under this repository's `testdata/`, pinned by exact round-trip tests (an unexpected key fails), and the composer and dispatcher repositories test their own wire code against the same files.

## Streams

| Stream | Publisher | Consumer | Semantics |
|---|---|---|---|
| `odyssey:pipeline-requests` | composer | orchestrator | run requests; one pipeline run each |
| `odyssey:pipeline-requests-dead` | orchestrator | human inspection | dead-lettered run requests, wrapped with a reason; never silently dropped |
| `odyssey:jobs` | orchestrator | dispatchers (shared consumer group `dispatchers`) | job assignments; ack on job completion |
| `odyssey:jobs-dead` | dispatchers | human inspection | dead-lettered assignments, wrapped with a reason; never silently dropped |
| `odyssey:events` | orchestrator + dispatchers | orchestrator, results-recorder, consumer services | lifecycle events, existing envelope |

Trim policy: both queues carry work, not history — `MAXLEN` trimming (defaults below, overridable by deployment config). The event bus keeps its own trim from the dispatcher repo's ADR 0001.

## Run request (composer → orchestrator)

Published on `odyssey:pipeline-requests` after the composer service has validated the trigger. The composer is a separate service (its own repo planned); this message binds it to this repo's orchestrator — the dispatcher never sees it. It is in this contract because the job assignment below is derived from it, and because both sides of the queue boundary validate at their trust boundary.

```json
{
  "trigger_id": "hook:42",
  "repo": "org/repo",
  "commit": "8f3a1c…full 40-hex sha",
  "pipeline": "demo",
  "config": { "…resolved pipeline, schema below" }
}
```

Field rules — all enforced by the composer before publish, re-checked by the orchestrator at consume:

- `trigger_id` — non-empty string, opaque; the composer's correlation/dedup handle for the trigger that produced the run (e.g. `hook:42`). The orchestrator never acts on it.
- `repo` — `org/repo` slug as the dispatcher's source provisioning will fetch it. No full URLs, no protocol prefixes.
- `commit` — full 40-hex sha. Short shas rejected: a run must be reproducible from its record.
- `pipeline` — pipeline name from `[pipeline]` in `.odyssey/pipeline.toml`. It must equal `config.pipeline`; a request where the two disagree is rejected at both boundaries.
- `config` — the resolved pipeline configuration as JSON (schema below), attached by the composer.

### Dead-letter at the orchestrator's trust boundary

A queue entry is untrusted input, and the composer is fire-and-forget: an entry the orchestrator cannot act on is dead-lettered, never silently dropped.
The orchestrator dead-letters an entry that carries no `envelope` field, whose envelope is not valid JSON, or whose run request fails re-validation at its trust boundary.
Dead-letter entries on `odyssey:pipeline-requests-dead` wrap the original, mirroring `odyssey:jobs-dead`: `{ "reason": "decode" | "validation", "request": { …original envelope JSON… } }`, with the original entry's bytes in a `raw` string instead of `request` when the envelope is not decodable JSON; an entry with no `envelope` field carries the reason alone.

### Resolved pipeline configuration (`config`)

ADR 0001's open question is settled here: the composer fetches the repository shallowly at the run's commit, loads and validates `.odyssey/pipeline.toml`, and attaches the resolved configuration. The orchestrator segments from this object and never fetches; the dispatcher keeps parsing pipeline.toml nowhere — it executes assignments. The shape mirrors the dispatcher's `internal/config`/`internal/domain` one-to-one so either side can validate it structurally.

```json
{
  "pipeline": "demo",
  "stages": ["build", "test"],
  "jobs": {
    "compile": {
      "stage": "build",
      "image": "alpine",
      "setup": ["apk add git"],
      "env": { "KEY": "value" },
      "steps": [{ "name": "compile", "run": "make", "timeout_ms": 300000 }]
    },
    "unit": {
      "stage": "test",
      "image": "golang",
      "steps": [{ "name": "unit", "run": "go test ./...", "timeout_ms": 600000 }]
    }
  }
}
```

- `stages` — ordered; order is execution order; no duplicates; every stage has at least one job.
- `jobs` — keyed by job name; each job's `stage` references a declared stage.
- `setup` — optional array of shell commands run in order before the steps; a setup failure makes the job `errored` (mirrors the dispatcher's `SetupErr`).
- `env` — string-to-string map; values are literal strings (secret interpolation is not this contract's concern).
- `steps[].timeout_ms` — integer milliseconds; `0` means no timeout; negative values are invalid. The TOML key is `timeout`; the composer maps it to this explicit-units wire name.

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
    "setup": ["apk add git"],
    "env": { "KEY": "value" },
    "steps": [{ "name": "compile", "run": "make", "timeout_ms": 300000 }]
  }
}
```

- `run_id` — opaque string, generated by this repo's orchestrator, fresh per run attempt. A job re-executed after a dead dispatcher's reclaim runs under a new `run_id`; the results DB records the abandoned attempt as `pending` with no `finished_at`.
- `attempt` — 1-based count of executions of this assignment across reclaims. This repo always publishes `1`; the dispatcher that reclaims a pending entry past the idle threshold increments it on the re-executed run.
- `source` — what the dispatcher must provision before executing: shallow fetch of `repo` at `commit`.
- `job_spec` — exactly one job from the run request's resolved `config`. Job name and stage ride the assignment envelope, not the spec; a dispatcher reconstructs its `domain.Job` as `{Name: job, Image, Setup, Env, Steps}` from both.

### Validation at the dispatcher's trust boundary

A queue message is untrusted input. The dispatcher rejects — dead-letters, never executes — an assignment that fails any of:

- envelope: `run_id`, `pipeline`, `stage`, `job` non-empty; `attempt` ≥ 1; `source.repo` and `source.commit` non-empty.
- `job_spec`: `image` non-empty; at least one step; per step, `name` and `run` non-empty and `timeout_ms` ≥ 0; every `setup` entry, when present, non-empty.

These are the same rules `internal/config`'s `Validate` applies to pipeline.toml's job table, so both sides reject the same pipelines: what the composer accepts at the door, the dispatcher accepts at the wire, and nothing invalid survives either boundary.

Dead-letter entries on `odyssey:jobs-dead` wrap the original message: `{ "reason": "validation" | "attempts", "assignment": { …original assignment JSON… } }`.

## Event emission responsibilities

| Event | Emitter | Envelope fields | Payload |
|---|---|---|---|
| `pipeline.started` | orchestrator (this repo) | `run_id`, `pipeline` | none — **before** publishing any job of the run: results DB parent-row invariant |
| `pipeline.finished` | orchestrator (this repo) | `run_id`, `pipeline` | `status` — worst stage status: `passed` / `failed` / `errored` |
| `job.started` | dispatcher | `run_id`, `pipeline`, `job` | `stage` |
| `job.finished` | dispatcher | `run_id`, `pipeline`, `job` | `status` |
| `step.started` | dispatcher | `run_id`, `pipeline`, `job`, `step` | none |
| `step.finished` | dispatcher | `run_id`, `pipeline`, `job`, `step` | `status`, `exit_code`, `duration_ms` — never stdout/stderr |

The envelope is the dispatcher repo's `internal/domain` `Event` JSON (`type`, `occurred_at`, `run_id`, `pipeline`, `job`, `step`, `payload`) and evolves additively only there. `status` values are that module's `Status` slugs (`pending`, `running`, `passed`, `failed`, `skipped`, `errored`). Fail-fast is this repo's state-machine rule: the first `failed` or `errored` stage ends the run and no further stages are published.

## Delivery semantics

- Dispatchers form one consumer group (`dispatchers`); each dispatcher reads only while it has a free runner slot (capacity awareness).
- Ack happens **on job completion**, not on read. A dispatcher renews its pending entry (`XCLAIM … JUSTID`) on the renewal interval while its runner is alive, so long jobs are not reclaimed.
- A dead dispatcher's entries are claimed by peers after the idle threshold and the job re-executes from scratch under a new `run_id` and an incremented `attempt`; the abandoned attempt is visible in the results DB as a run stuck `pending`.
- Assignments that fail validation dead-letter immediately. Assignments whose `attempt` exceeds the attempt limit dead-letter with reason `attempts` — a poison assignment that kills its dispatcher is caught by the reclaim loop at most `attempt-limit` times before it lands in the dead letter for inspection.

## Delivery parameters (v1 defaults, overridable by deployment config)

| Parameter | v1 default | Rationale |
|---|---|---|
| Claim-renewal interval | 30s | far below sane job duration; renewal makes false reclaims rare |
| Idle threshold | 5m | comfortably above several missed renewals; reclaim means dead dispatcher |
| Attempt limit | 3 | bounds the reclaim loop for poison assignments |
| `MAXLEN`, `odyssey:pipeline-requests` | 10,000 | work, not history; sized well past any plausible backlog spike |
| `MAXLEN`, `odyssey:jobs` | 100,000 | one entry per job; sized for bursty stages |
| `MAXLEN`, `odyssey:jobs-dead` | 1,000 | inspection queue; small by design |
| `MAXLEN`, `odyssey:pipeline-requests-dead` | 1,000 | inspection queue; small by design |

These are defaults for a system with no load data yet. The deployment config overrides them; revisiting them from measured data is the trigger, not taste.
