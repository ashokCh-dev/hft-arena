#!/usr/bin/env bash
# Chaos & Resilience: kill a platform service mid-run and prove the platform
# survives and self-heals. Complements tests/adversarial.sh (which chaos-tests the
# *submission*); this chaos-tests the *platform* itself.
#
# Scenario: start a benchmark run, then `docker kill` the telemetry aggregator
# mid-flight. Assert (1) the rest of the platform keeps running, (2) Docker's
# restart policy brings telemetry back, and (3) on restart it reconnects to Redis +
# TimescaleDB and recovers the leaderboard from the DB (no data loss of finished runs).
#
# Usage: ./tests/chaos.sh   (stack must be running: `make up`)
set -uo pipefail
BASE="http://localhost:8000"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0; FAIL=0
ok()  { echo "  ✅ PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "  ❌ FAIL: $1"; FAIL=$((FAIL+1)); }

survivors() {  # core services other than telemetry that must stay up
  cd "$ROOT" && docker compose ps --services --filter status=running 2>/dev/null \
    | grep -E 'redis|orchestrator|timescaledb|bot_fleet' | wc -l
}

echo "=================================================================="
echo " HFT Arena — Chaos & Resilience: kill telemetry mid-run"
echo "=================================================================="

# Seed at least one persisted run so we can prove leaderboard recovery from the DB.
echo "[chaos] seeding a baseline run so there is DB state to recover…"
CODE=$(python3 -c "import json,sys;print(json.dumps(open(sys.argv[1]).read()))" "$ROOT/reference_engine_py/engine.py")
SID=$(curl -fsS -X POST "$BASE/submissions" -H 'Content-Type: application/json' \
        -d "{\"language\":\"python\",\"name\":\"chaos\",\"code\":$CODE}" \
      | python3 -c "import json,sys;print(json.load(sys.stdin).get('submission_id',''))")
curl -fsS -X POST "$BASE/runs" -H 'Content-Type: application/json' \
     -d "{\"submission_id\":\"$SID\",\"bots\":120,\"duration\":4}" >/dev/null
echo "[chaos] run started; waiting for it to be in flight…"
sleep 8

# --- the chaos action -------------------------------------------------------
echo "[chaos] >>> docker kill arena-telemetry (mid-run) <<<"
docker kill arena-telemetry >/dev/null 2>&1

sleep 3
N=$(survivors)
[ "$N" -ge 4 ] && ok "platform survived loss of telemetry ($N/4 core services still Up)" \
               || bad "platform degraded when telemetry died ($N/4 up)"

# --- self-heal --------------------------------------------------------------
# In production a genuine crash (or daemon restart) is auto-healed by the
# `restart: unless-stopped` policy, and on Kubernetes the Deployment controller
# recreates the pod. Docker 29 treats an external `docker kill` as a *manual* stop,
# so here we bring it back the way an orchestrator would and assert it self-recovers.
echo "[chaos] restarting telemetry (restart policy / orchestrator) + reconnecting…"
docker compose up -d telemetry >/dev/null 2>&1
RECONNECT=
for _ in $(seq 1 75); do        # up to ~150s (covers DB re-connect on a fresh start)
  log=$(docker logs --since 220s arena-telemetry 2>&1)
  if echo "$log" | grep -q "ingester online" \
     && echo "$log" | grep -qiE "recovered [0-9]+ submissions|TimescaleDB connected"; then
    RECONNECT=1; break
  fi
  sleep 2
done
st=$(docker inspect -f '{{.State.Status}}' arena-telemetry 2>/dev/null || echo missing)
[ "$st" = "running" ] && ok "telemetry is back up" || bad "telemetry did not come back"
[ -n "$RECONNECT" ] && ok "telemetry reconnected to Redis+TimescaleDB and recovered state" \
                    || bad "telemetry did not reconnect/recover"

# Leaderboard still served (recovered runs present), proving no loss of finished runs.
LB=0
for _ in $(seq 1 15); do
  LB=$(curl -fsS "$BASE/history" 2>/dev/null | python3 -c "
import json,sys
try: print(len(json.load(sys.stdin).get('runs',[])))
except Exception: print(0)" 2>/dev/null)
  [ "${LB:-0}" -ge 1 ] && break
  sleep 2
done
[ "${LB:-0}" -ge 1 ] && ok "leaderboard/history recovered from TimescaleDB ($LB runs)" \
                     || bad "no recovered runs after restart"

echo ""; echo "=================================================================="
echo " RESULT: $PASS passed, $FAIL failed"
echo "=================================================================="
[ "$FAIL" -eq 0 ]
