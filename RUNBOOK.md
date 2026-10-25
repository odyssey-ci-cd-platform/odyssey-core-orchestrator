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

### 1.4 Running the orchestrator

- Build: `go build ./cmd/orchestrator` (with `GOTOOLCHAIN=auto` — go.mod pins the 1.26.10 floor).
- Required env: `ODYSSEY_REDIS_ADDR` (default `localhost:6379`) — the queue/bus Redis, `host:port`.
- Optional env: `ODYSSEY_HEALTH_ADDR` (default `127.0.0.1:8080`, the health/metrics listen address), `ODYSSEY_JOBS_MAXLEN` (default 100,000) and `ODYSSEY_DEAD_MAXLEN` (default 1,000) — the contract's deployment-overridable queue trims.
- Stream names are pinned by the dispatcher contract (§ Streams) and are deliberately not configurable; a bad env value fails startup with the violated rule in one joined error.
- Single instance: the process takes the `odyssey:orchestrator-lease` Redis key (`SET NX PX` 15s, renewed every 5s) before serving; a second instance exits non-zero immediately, and an unreachable Redis also exits non-zero for the deployment's restart policy to retry.
- If the lease is lost mid-run (expired or taken over), the process logs an error and `/healthz` serves 503 while it keeps its in-memory runs — the v1 limit is that it serves degraded until restarted, never two healthy holders.
- Transient Redis errors around the loops' reads retry in process with bounded backoff (100ms doubling to a 2s cap); a failed decision (hand-off, stage release, or its ack) exits the loop so the entry stays pending for the next start's pending-entry recovery.
- Shutdown: SIGINT/SIGTERM cancel both loops and the lease renewer; reads unblock within the 1s block interval, the lease is released, and the exit is clean unless a loop failed first.
- Observability: `/healthz` serves 200 with the lease held and 503 without it, carrying each loop's last serve error and its time; `/debug/vars` carries `orchestrator_inflight_runs`, `orchestrator_pending_pipeline_requests`, `orchestrator_pending_events`, and `orchestrator_last_stage_release_ms`.
- Smoke: `scripts/e2e-orchestrator.sh` runs the whole flow against a live Redis it spawns (needs docker and go) and asserts the lease rejects a second instance.
- Image: `docker build -t orchestrator .` produces the distroless static image; the container takes the same env (see the Dockerfile).

## 2. Maintenance tasks

- Run-deadline defaults (gh-25, AUD-003): a run whose outcome has not arrived resolves as `errored` at a per-run deadline of **1h** (set in `NewMachine`, overridable at construction; zero or less disables); the reaper scans every **30s** (`ReapInterval`, non-positive disables the ticker). Both are deployment-relevant defaults sized before any load data — revisit them from measured run durations, not taste.
- The Go gate (vet / `go test -race` since gh-23 / tidy -diff / gofmt) is in the Makefile and CI since gh-5; toolchain bumps go through `go.mod`, not this file.
- Branch protection on `main` is enabled (required `check` status, strict; admins enforced; stale-review dismissal on; no required approvals — solo merges). It was applied at repository creation, after the bootstrap commit; every later change lands through a PR.
- CODEOWNERS (gh-24 follow-up, AUD-012): `.github/CODEOWNERS` requests the owner's review on every PR, explicitly on `docs/contracts/` and `internal/`. Enforcing it — "require review from Code Owners" — is a one-time branch-protection settings flip, an admin action in the GitHub UI, deliberately not automated.
