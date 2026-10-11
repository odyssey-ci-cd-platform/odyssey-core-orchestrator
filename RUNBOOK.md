# RUNBOOK

Operational procedures, incidents, and the recorded baseline. Update in the same commit that changes what a procedure does.

## 1. Procedures

### 1.1 Canonical loop

1. File an issue (Work item template) for multi-commit work; one-commit tasks need none.
2. Branch `feature/<issue-number>`; fill in the auto-posted solution-template comment.
3. Intent test red first, smallest change to green, `make check` green.
4. `scripts/ship-pr.sh <branch> <title> [body-file]`, then `scripts/merge-pr.sh <branch>`.

### 1.2 Gate baseline

- Recorded baseline: `make check` green on the gh-5 composer-door commit — docs check plus the Go gate (vet, test, tidy -diff, gofmt).
- Toolchain at baseline: local `go1.26.8`; `go.mod` pins `go 1.26.4` (the dispatcher repo's floor).

### 1.3 Docs check

Run `.agents/bin/check-docs.sh` before committing Markdown changes; CI runs it against `origin/main`. It cannot prove changelog coverage — that stays a discipline.

## 2. Maintenance tasks

- The Go gate (vet / test / tidy -diff / gofmt) is in the Makefile and CI since gh-5; toolchain bumps go through `go.mod`, not this file.
- Branch protection on `main` is enabled (required `check` status, strict; admins enforced; stale-review dismissal on; no required approvals — solo merges). It was applied at repository creation, after the bootstrap commit; every later change lands through a PR.
