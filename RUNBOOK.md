# RUNBOOK

Operational procedures, incidents, and the recorded baseline. Update in the same commit that changes what a procedure does.

## 1. Procedures

### 1.1 Canonical loop

1. File an issue (Work item template) for multi-commit work; one-commit tasks need none.
2. Branch `feature/<issue-number>`; fill in the auto-posted solution-template comment.
3. Intent test red first, smallest change to green, `make check` green.
4. `scripts/ship-pr.sh <branch> <title> [body-file]`, then `scripts/merge-pr.sh <branch>`.

### 1.2 Gate baseline

- Recorded baseline: `make check` green — docs check plus the Go gate (vet, `go test -race` since gh-23, tidy -diff, gofmt).
- Toolchain at baseline: local `go1.26.8` (auto-upgrades per GOTOOLCHAIN); `go.mod` pins `go 1.26.10` — raised from the dispatcher repo's 1.26.4 floor when govulncheck (gh-23) flagged reachable stdlib fixes at 1.26.5–1.26.10 and CI builds with the go.mod floor.

### 1.3 Docs check

Run `.agents/bin/check-docs.sh` before committing Markdown changes; CI runs it against `origin/main`. It cannot prove changelog coverage — that stays a discipline.

## 2. Maintenance tasks

- Run-deadline defaults (gh-25, AUD-003): a run whose outcome has not arrived resolves as `errored` at a per-run deadline of **1h** (set in `NewMachine`, overridable at construction; zero or less disables); the reaper scans every **30s** (`ReapInterval`, non-positive disables the ticker). Both are deployment-relevant defaults sized before any load data — revisit them from measured run durations, not taste.
- The Go gate (vet / `go test -race` since gh-23 / tidy -diff / gofmt) is in the Makefile and CI since gh-5; toolchain bumps go through `go.mod`, not this file.
- Branch protection on `main` is enabled (required `check` status, strict; admins enforced; stale-review dismissal on; no required approvals — solo merges). It was applied at repository creation, after the bootstrap commit; every later change lands through a PR.
