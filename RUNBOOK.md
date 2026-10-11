# RUNBOOK

Operational procedures, incidents, and the recorded baseline. Update in the same commit that changes what a procedure does.

## 1. Procedures

### 1.1 Canonical loop

1. File an issue (Work item template) for multi-commit work; one-commit tasks need none.
2. Branch `feature/<issue-number>`; fill in the auto-posted solution-template comment.
3. Intent test red first, smallest change to green, `make check` green.
4. `scripts/ship-pr.sh <branch> <title> [body-file]`, then `scripts/merge-pr.sh <branch>`.

### 1.2 Gate baseline

- Recorded baseline: `make check` green on the scaffold commit (docs check only — no Go code exists yet).
- When the Go gate lands (composer/orchestrator code), record `go version` and re-baseline here.

### 1.3 Docs check

Run `.agents/bin/check-docs.sh` before committing Markdown changes; CI runs it against `origin/main`. It cannot prove changelog coverage — that stays a discipline.

## 2. Maintenance tasks

- When Go code lands: add `go vet` / `go test ./...` / `go mod tidy -diff` / gofmt to the Makefile and CI, and update this section.
- When the GitHub repository is created: enable branch protection (required `check` status, no direct pushes) before the first PR merges.
