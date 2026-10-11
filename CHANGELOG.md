# Changelog

Notable changes to odyssey-core-orchestrator. Latest changes go at the top, under `[Unreleased]`, until a version is tagged.

## [Unreleased]

### Added
- Process scaffolding: README with the architecture diagram and vocabulary, AGENTS.md conventions, RUNBOOK, docs-check gate, CI and issue/PR automation workflows, ship/merge scripts.
- ADR 0001: cluster orchestration — job-grain distribution, stateful stage state machine, feedback edge, failure modes, restart story.
- Dispatcher contract draft: streams, run request, job assignment, event emission responsibilities, delivery semantics.

### Changed
- Scoped the README to this repo: the architecture diagram keeps only composer and orchestrator inside the repo boundary, with triggers, the dispatcher repo, the event bus, and downstream consumers as boundary nodes, and the reading notes restate dispatcher-side mechanics as boundary references. Cross-references now name `odyssey-ci-cd-platform/dispatcher` (the current location of the repo formerly named `odyssey-core-agent`).
- Recorded the branch-protection setup in RUNBOOK: protection on `main` applied at repository creation (required `check` status, admins enforced, no required approvals).
- Untracked `gates.json` and ignored it: it is machine-local harness configuration (its `allowPaths` carries home-directory paths), not repo content. It remains in early history from the bootstrap commit; removal from published history was not attempted.
- Dispatcher contract frozen by the field-by-field pass (gh-3): every run-request and assignment field pinned with type, semantics, and units; the resolved pipeline configuration schema added (composer fetches and attaches — ADR 0001's open question settled); `setup` added to the job spec so setup commands survive the boundary (a setup failure is job-level `errored`, mirroring the dispatcher's `SetupErr`); step timeouts pinned as `timeout_ms` with `0` = no timeout; dispatcher-side validation enumerated to match `internal/config`'s `Validate`; per-event payload fields pinned from the dispatcher's emission code; dead-letter stream `odyssey:jobs-dead` named; delivery parameters fixed as v1 defaults (30s renewal, 5m idle threshold, attempt limit 3, queue MAXLENs).
