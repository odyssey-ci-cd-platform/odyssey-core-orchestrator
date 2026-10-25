#!/usr/bin/env bash
# End-to-end smoke: the orchestrator binary consumes a run request from a
# live Redis, emits pipeline.started, publishes the stage's assignment to
# the job queue, and resolves the run when the dispatcher's job.finished
# arrives on the event bus.
#
# Spawns its own Redis (docker), starts the real orchestrator against it,
# publishes one contract run request (redis-cli), plays the dispatcher
# (reads the assignment from odyssey:jobs, emits its job.finished), then
# asserts pipeline.started, the assignment, and pipeline.finished. Also
# asserts the single-instance lease rejects a second process. Cleans up
# everything it created. Needs: docker, go. Exit 0 = pass.
set -euo pipefail
cd "$(dirname "$0")/.."

# go.mod pins the toolchain floor (gh-23); let the local Go auto-upgrade to it.
export GOTOOLCHAIN=auto

REDIS_PORT=6398
HEALTH_PORT=6397
REDIS_CONTAINER=odyssey-e2e-orchestrator-redis
BIN=./.e2e-orchestrator
LOG=.e2e-orchestrator.log

cleanup() {
	[[ -n "${PID:-}" ]] && kill "$PID" 2>/dev/null || true
	docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
	rm -f "$BIN" "$LOG"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# 1. Live Redis in docker; R talks to it through redis-cli in the container.
docker run -d --rm --name "$REDIS_CONTAINER" -p "${REDIS_PORT}:6379" redis:7-alpine >/dev/null
R() { docker exec "$REDIS_CONTAINER" redis-cli --raw "$@"; }
for _ in $(seq 1 20); do
	R PING 2>/dev/null | grep -q PONG && break
	sleep 0.5
done
R PING | grep -q PONG || fail "redis did not come up"

# 2. Build and start the orchestrator under test, wired to the live Redis.
go build -o "$BIN" ./cmd/orchestrator
ODYSSEY_REDIS_ADDR=localhost:$REDIS_PORT ODYSSEY_HEALTH_ADDR=127.0.0.1:$HEALTH_PORT "$BIN" >"$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 25); do
	grep -q "orchestrator serving" "$LOG" 2>/dev/null && break
	sleep 0.2
done
grep -q "orchestrator serving" "$LOG" || fail "orchestrator did not start (see $LOG)"

# 2b. The single-instance lease rejects a second process (AUD-007).
if ODYSSEY_REDIS_ADDR=localhost:$REDIS_PORT "$BIN" >"$LOG" 2>&1; then
	fail "a second orchestrator instance started while one holds the lease"
fi
grep -q "another orchestrator instance holds the lease" "$LOG" \
	|| fail "second instance failed for the wrong reason (see $LOG)"

# 3. Publish one contract run request (the composer's side of the boundary).
CONFIG='{"pipeline":"e2e","stages":["only"],"jobs":{"hello":{"stage":"only","image":"alpine","steps":[{"name":"hi","run":"echo hi","timeout_ms":60000}]}}}'
REQUEST="{\"trigger_id\":\"e2e:1\",\"repo\":\"org/repo\",\"commit\":\"8f3a1c8f3a1c8f3a1c8f3a1c8f3a1c8f3a1c8f3a\",\"pipeline\":\"e2e\",\"config\":$CONFIG}"
R XADD odyssey:pipeline-requests '*' envelope "$REQUEST" >/dev/null

# 4. pipeline.started precedes any assignment (the parent-row invariant).
deadline=$((SECONDS + 10))
until R XRANGE odyssey:events - + 2>/dev/null | grep -q '"type":"pipeline.started"'; do
	[[ $SECONDS -lt $deadline ]] || fail "no pipeline.started within 10s (see $LOG)"
	sleep 0.2
done

# 5. The assignment lands on the job queue with the run's coordinates.
R XGROUP CREATE odyssey:jobs dispatchers 0 MKSTREAM >/dev/null 2>&1 || true
ASSIGNMENT=""
deadline=$((SECONDS + 10))
until ASSIGNMENT=$(R XREADGROUP GROUP dispatchers e2e-consumer COUNT 1 STREAMS odyssey:jobs '>' 2>/dev/null) && [[ -n "$ASSIGNMENT" ]]; do
	[[ $SECONDS -lt $deadline ]] || fail "no assignment within 10s (see $LOG)"
	sleep 0.2
done
echo "$ASSIGNMENT" | grep -q '"pipeline":"e2e"' || fail "assignment is not for pipeline e2e: $ASSIGNMENT"
echo "$ASSIGNMENT" | grep -q '"stage":"only"' || fail "assignment is not for stage only: $ASSIGNMENT"
echo "$ASSIGNMENT" | grep -q '"job":"hello"' || fail "assignment is not for job hello: $ASSIGNMENT"
RUN_ID=$(echo "$ASSIGNMENT" | grep -o '"run_id":"[^"]*"' | head -1 | cut -d'"' -f4)
[[ -n "$RUN_ID" ]] || fail "assignment carries no run id: $ASSIGNMENT"

# 6. Play the dispatcher's completion: job.finished, passed.
R XADD odyssey:events '*' envelope "{\"type\":\"job.finished\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"run_id\":\"$RUN_ID\",\"pipeline\":\"e2e\",\"job\":\"hello\",\"payload\":{\"status\":\"passed\"}}" >/dev/null

# 7. The run resolves: pipeline.finished with the stage's status.
deadline=$((SECONDS + 10))
until R XRANGE odyssey:events - + 2>/dev/null | grep -q '"type":"pipeline.finished"'; do
	[[ $SECONDS -lt $deadline ]] || fail "no pipeline.finished within 10s (see $LOG)"
	sleep 0.2
done
R XRANGE odyssey:events - + | grep -q '"status":"passed"' || fail "pipeline.finished did not carry passed"

# 8. The queue entry was decided and acknowledged; the bus holds exactly
#    the three lifecycle events of this run (started, job.finished, finished).
PENDING=$(R XPENDING odyssey:pipeline-requests orchestrator | head -1)
[[ "$PENDING" == "0" ]] || fail "pipeline-requests pending = $PENDING, want 0 (acked)"
XLEN=$(R XLEN odyssey:events)
[[ "$XLEN" == "3" ]] || fail "XLEN odyssey:events = $XLEN, want 3"

echo "PASS: run request consumed, pipeline.started emitted, assignment published to odyssey:jobs, run resolved passed on pipeline.finished; lease rejected the second instance"
R XRANGE odyssey:events - + | grep -o '"type":"[^"]*"'
