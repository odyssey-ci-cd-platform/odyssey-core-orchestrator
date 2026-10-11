# odyssey-core-orchestrator

The cluster-side orchestrator for the Odyssey CI/CD platform: intake (composer) plus the stage state machine (orchestrator). **Read `README.md` first** for the architecture and vocabulary — it and the ADRs are maintained design documents, not stale docs. Don't duplicate their content here; this file is operational notes only.

## Commands

- Gate: `make check` — the single gate every change must leave green: the docs check plus the Go gate (`go vet`, `go test ./...`, `go mod tidy -diff`, gofmt). CI runs both.
- Docs check: `.agents/bin/check-docs.sh` — catches mid-sentence hard wraps in changed Markdown files.

## Workflow

- Multi-commit work needs a GitHub issue first (template: Work item). An issue describes the work needed — the outcome or business requirement — never the code change. One-commit tasks get no issue.
- A workflow posts the solution template as the first issue comment automatically; the first actionable is filling it in — edit the posted comment, keep every header, N/A where not applicable.
- Branch per work item: `feature/<issue-number>`, or issueless `type/slug`. All changes land through PRs — main is protected (required `check` status, no direct pushes).
- Tests come before changes: the intent test is red first, then the change turns it green.
- One-shot PR flow: `scripts/ship-pr.sh <branch> <title> [body-file]` (push, open PR, watch CI), `scripts/merge-pr.sh <branch>` (merge, delete branch, sync main).
- Intermediate session state lives in `STATE.md` (repo root, gitignored, never committed).
- Operational procedures and the recorded baseline live in `RUNBOOK.md`; update it in the same commit that changes what a procedure does.

## Design principles (from the README and the sibling dispatcher repo, worth restating)

- Minimal abstractions; functions over classes when no state is needed.
- Direct, slightly repetitive call sites over an abstraction that doesn't earn its weight.
- Schema fields minimal and orthogonal; reject contradictory combinations at validation/parse time, not silently.
- The composer is fire-and-forget; the orchestrator is stateful by design (ADR 0001) — do not blur that line to make either simpler.
- Assignments carry run context + job spec, never outcomes; the results DB is the only outcome store.

## Conventions

- Branches: `feature/<issue-number>`. Commits: `gh-<issue-number>: <summary>`, imperative mood.
- Service seams take injected interfaces with test fakes: the consumer package owns the small interface, fakes mirror real signatures.
- Guard clauses and negative checks over nesting; flatten before adding a level.
- Names and doc comments say exactly what a thing does, at its own level of abstraction.
- Message schemas live in `docs/contracts/` and evolve additively only; both sides of the queue boundary validate at their trust boundary.

## Documentation rules

- In Markdown files (README, docs/, CHANGELOG, etc.), never hard-wrap a line mid-sentence. Break lines only at sentence boundaries; one sentence per line is the norm.
- Long lines are fine. Markdown soft-wraps in every editor and renders identically regardless of where the source line breaks. Wrapping at ~80 columns midsentence produces noisy diffs and broken search.
- Exceptions where line length is structural: code blocks, tables, HTML/frontmatter.
- Update CHANGELOG.md on every change that lands on main, in the same PR, under `[Unreleased]`.
- Before committing docs changes, run `.agents/bin/check-docs.sh`.

## Gotchas

- The Go gate is live (gh-5): `make check` runs the docs check and `go vet`/`go test`/`go mod tidy -diff`/gofmt; a green gate means both. This reverses the scaffolding-era warning that the gate was docs-only.
- The dispatcher contract in `docs/contracts/` is frozen (gh-3, agreed with the dispatcher repo on its issue 35); it evolves additively only — new fields, never repurposed ones. This reverses the scaffolding-era DRAFT warning.
