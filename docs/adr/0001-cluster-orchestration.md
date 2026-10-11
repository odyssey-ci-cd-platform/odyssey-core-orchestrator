# ADR 0001: Cluster orchestration

- **Status:** Accepted (2026-10-11) — pending sign-off recorded in odyssey-ci-cd-platform/dispatcher issue 35
- **Decides:** odyssey-ci-cd-platform/dispatcher issue 35 — orchestrator scheduling across a runner cluster
- **Feeds:** the intelligence layer's prerequisites (testAnalysisService et al. need runs distributed and recorded)

## Context

gh-35's acceptance criteria: jobs are distributed across multiple runner nodes, with basic capacity/availability scheduling. The execution side (dispatcher repo) spawns one ephemeral container per job; the question this record closes is who sequences a pipeline's stages once jobs leave the process that used to run them all in order.

Constraints that shape the decision:

- **Fail-fast semantics already exist** (gh-82, AUD-015): the first Failed or Errored stage ends the run, jobs within a stage run concurrently. Whatever sequences stages in the cluster must preserve exactly this behavior, not invent new semantics.
- **The orchestrator must be reachable without a registry**: runners are ephemeral; a component that must know every node's address is a component that owns a registry.
- **Outcomes have exactly one channel**: the event bus (ADR 0001 in the dispatcher repo, two-sink split). Anything that needs to know what happened subscribes or reads the results DB.
- **Source provisioning is real**: a job on node N needs the repository at the run's commit on node N's disk. Nothing in the old in-process world needed this.

## Options considered

### Pipeline-grain assignment — rejected for gh-35, kept as a deployment mode of one

The whole pipeline goes to one dispatcher, which sequences stages itself (today's in-process `internal/orchestrator` logic). Trivial to build, but it never distributes the jobs *of one run* — a stage with ten parallel jobs and a one-slot cluster queues seven of them behind one node while nine nodes sit idle. It fails gh-35's "jobs distributed across multiple runner nodes" as written.

### Central dispatcher service between the queues — rejected

A queue-to-queue router that picks targets. It was drawn in the first architecture sketch and deleted: the consumer group already *is* the dispatcher. A capacity-gated consumer reads only when it has a free slot, group semantics give at-most-one claim, and pending-entry claim (the gh-76 machinery in the dispatcher repo) gives release-on-death for free. A separate service re-implements all three with a deploy story attached.

### Agent/runner registry with heartbeats — rejected

A Redis hash of live runners, refreshed by heartbeats, consulted for "pick an available agent." Every fact it holds exists cheaper elsewhere: liveness is `XINFO CONSUMERS` on the group; capacity is the consumer not reading when its slots are full; the heartbeat survives in minimized form as claim renewal (the dispatcher re-`XCLAIM`s its own pending entry `JUSTID` while its runner is alive, so slow runs are not mistaken for dead ones). A registry is a TTL to babysit.

### Job-grain with a stateful orchestrator — ✅ chosen

The orchestrator owns the stage state machine. It publishes stage N's jobs, counts completions from `job.finished` events, releases stage N+1 on success, and ends the run on the first failed stage. One component holds the coordination; every runner stays dumb, cheap, and disposable.

## Decision

**Job-grain distribution with a stateful central orchestrator**, bound as follows:

1. **Segmentation:** the orchestrator resolves the pipeline configuration (attached to the run request by the composer — see the open question below) and publishes one assignment per job, stage by stage.
2. **The feedback edge is binding:** the orchestrator consumes `job.finished` from the event bus. It is stateful by design; fire-and-forget applies to the composer only.
3. **Event emission split:** the orchestrator emits `pipeline.started` **before** publishing any job of the run, and `pipeline.finished` when the state machine resolves. The dispatcher emits `job.started`/`job.finished` and `step.*` for its own jobs. The ordering keeps the results DB's parent-row invariant intact across repos.
4. **Assignments carry run context + job spec, never outcomes:** run id, pipeline, stage, job name, attempt, source reference, job definition. Rejected alternative: embedding previous-stage results in messages — run-global counting cannot live in a per-message blob, the blob is stale by construction, and it duplicates the results DB as a second source of truth.
5. **Failure modes are split by ownership:** a runner container dying mid-job is *seen by its dispatcher* (it spawned the child), which retries or releases per policy; a dispatcher dying leaves its entries pending, another dispatcher's claim sweep picks them up, and the run re-executes from scratch under a new run id — the results DB records the abandoned attempt as `pending` with no `finished_at`, which is honest history. Re-running a pipeline is normal CI retry semantics; pipelines with non-idempotent side effects need guardrails, documented per pipeline.
6. **Restart story:** v1 documents that an orchestrator restart loses in-flight runs (state is in memory; the bus is lossy and never replayed). v2 reconciles at startup from the results DB — runs with no `finished_at` and their job rows — using the system of record instead of inventing a parallel one.
7. **Source provisioning:** the runner path performs a shallow fetch of the repository at the run's commit before executing its job. v1 has no cache; a fetched-source cache is a follow-up when fetch time shows up in the profile.
8. **Capacity:** a dispatcher's capacity is its concurrent-runner limit; capacity awareness is "do not read from the queue while full." Cluster elasticity is replica count.

### Open question — settled in the contract pass (2026-10-12)

Who fetches the repository for segmentation: the composer (clone once at intake, validate, attach resolved config to the run request — one fetch, validation at the door) or the orchestrator (fetch at segmentation time). Leaning was: composer attaches; both sides must validate what they receive regardless.

**Settled: the composer fetches and attaches.** The run request carries the resolved pipeline configuration as JSON pinned field-by-field in the dispatcher contract (§ Resolved pipeline configuration); the orchestrator segments from that object and never fetches. Recorded in the contract's field-by-field pass, agreed with the dispatcher repo on issue 35.

## When to revisit

- Sustained queue pressure or fair-share needs → priorities/quotas on the run queue (deferred, one issue each).
- Runs that must survive orchestrator restarts before v2 reconciliation lands → raise the priority of decision point 6's v2.
- Placement constraints (pinned runners, hardware classes) → that is the registry coming back through the wall; design it then.

## References

- Dispatcher repo: ADR 0001 (event bus, two-sink split), ADR 0002 (results DB, central recorder under clustering), ADR 0004 (no local mode)
- Design discussion: odyssey-ci-cd-platform/dispatcher issue 35 and the session of 2026-10-11 that produced the diagram this record formalizes
- Woodpecker CI agent model (per-node agent, per-job containers) — the closest surveyed analog
