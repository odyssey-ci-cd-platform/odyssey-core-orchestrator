# Changelog

Notable changes to odyssey-core-orchestrator. Latest changes go at the top, under `[Unreleased]`, until a version is tagged.

## [Unreleased]

### Added
- Process scaffolding: README with the architecture diagram and vocabulary, AGENTS.md conventions, RUNBOOK, docs-check gate, CI and issue/PR automation workflows, ship/merge scripts.
- ADR 0001: cluster orchestration — job-grain distribution, stateful stage state machine, feedback edge, failure modes, restart story.
- Dispatcher contract draft: streams, run request, job assignment, event emission responsibilities, delivery semantics.

### Changed
- Scoped the README to this repo: the architecture diagram keeps only composer and orchestrator inside the repo boundary, with triggers, the dispatcher repo, the event bus, and downstream consumers as boundary nodes, and the reading notes restate dispatcher-side mechanics as boundary references. Cross-references now name `odyssey-ci-cd-platform/dispatcher` (the current location of the repo formerly named `odyssey-core-agent`).
