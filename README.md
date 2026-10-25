# odyssey-core-orchestrator

The cluster-side orchestrator for the Odyssey CI/CD platform: it turns pipeline runs into job assignments and owns the stage state machine.

Not a fork of the execution engine — this repo contains no runner management and executes nothing. It is one service:

- **orchestrator** — consumes run requests, re-validates them at its trust boundary, segments the pipeline into jobs, publishes assignments to the job queue, and consumes `job.finished` events to sequence stages (fail-fast: the first Failed or Errored stage ends the run) and emit `pipeline.*` lifecycle events.

The intake half — the **composer** — is a separate service with its own planned repo: it consumes origin triggers (repo hook, UI, CI), validates the pipeline configuration at the door, and publishes run requests onto the pipeline queue. Services live in separate repos today (small integration tests, exact per-repo style, low PR volume) and consolidate into a monorepo later.

The execution side lives in the **dispatcher** repo (`odyssey-ci-cd-platform/dispatcher`). What this repo asserts about it is the contract's obligation, not observed behavior: a conforming dispatcher consumes assignments from `odyssey:jobs`, executes each job in an ephemeral runner container (one job each), and emits `job.*`/`step.*` events (dispatcher contract §§ Job assignment, Event emission responsibilities). This README scopes to this repo; everything outside it (composer, triggers, dispatcher, results-recorder, consumer services) appears only as a boundary.

## Architecture

```mermaid
flowchart LR
    subgraph repo["this repo"]
        orch["orchestrator
        re-validates at the boundary
        segments pipeline into jobs
        stage state machine · fail-fast"]
        jq(("odyssey:jobs"))
    end

    composer["composer service
    consumes origin triggers, shapes them
    into run requests · fire-and-forget
    (own repo, planned)"]
    pq(("odyssey:pipeline-requests"))
    dispatch["dispatcher repo
    consumes assignments, runs jobs,
    emits job.* / step.* events"]
    bus(("odyssey:events"))
    downstream["elsewhere: results-recorder
    → results DB; consumer services"]

    composer -- "run request
    (no origin: intake provenance
    is composer-owned)" --> pq
    pq -- "consume" --> orch
    orch -- "1. pipeline.started" --> bus
    orch -- "2. job assignments:
    run ctx + job spec, NO outcomes" --> jq
    jq -- "consume" --> dispatch
    bus -- "3. job.finished
    (releases next stage)" --> orch
    bus --> downstream
```

### Reading notes

- The numbered ordering on the orchestrator's edges is load-bearing: `pipeline.started` is emitted **before** jobs are published, so the results DB's parent-row invariant holds across distributed producers.
- Assignments carry run context + job spec, **never outcome blobs** — the results DB is the single source of truth for outcomes. The results-recorder writes it; building that recorder is the dispatcher repo's obligation under its ADR 0002 (the recorder is the results DB's single writer), not a fact this repo verifies.
- The orchestrator is the one stateful component in the platform (the price of job-grain). v1: a restart loses in-flight runs (ADR 0001); v2: reconciliation rebuilds state from the results DB.
- No registry and no queue-to-queue dispatcher service are built here: the consumer group is the dispatcher, and capacity gating, claim renewal, and release-on-death are dispatcher-repo mechanics — the contract records the boundary semantics only.

## Vocabulary

| Name | Thing |
|---|---|
| composer | separate intake service (own repo planned): consumes origin triggers, shapes them into run requests, publishes to the pipeline queue |
| orchestrator | this repo: re-validates run requests, segmentation + stage state machine |
| dispatcher | node daemon in the dispatcher repo: consume assignments, manage runners |
| runner | ephemeral docker container executing exactly one job |
| event bus | `odyssey:events` Redis Stream — the only outcome channel |
| results DB | embedded SQLite, one central writer, the queryable system of record |

## Non-goals

- No intake transport — consuming origins (hook, UI, CI) and shaping them into run requests is the composer service's job, in its own repo.
- No runner management, container handling, or execution — see the dispatcher repo.
- No per-test analytics, visualization, or notification — consumer services' territory.
- No priorities, quotas, dedup, or commit-status push — deferred until the pipeline above works end to end (one issue each when queued).

## Documents

- [ADR 0001: cluster orchestration](docs/adr/0001-cluster-orchestration.md)
- [Dispatcher contract](docs/contracts/dispatcher-contract.md) — the message schemas binding this repo to the composer service (run requests) and the dispatcher repo (assignments, events)
- Design discussion: odyssey-ci-cd-platform/dispatcher issue 35
