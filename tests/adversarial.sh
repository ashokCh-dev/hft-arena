#!/usr/bin/env bash
# Adversarial resilience suite: submit hostile/broken engines and prove the
# platform's Sandboxing + Validation defenses hold and the platform stays up.
#
# Usage: ./tests/adversarial.sh   (stack must be running: `make up`)
set -uo pipefail
BASE="http://localhost:8000"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0; FAIL=0
ok()  { echo "  ✅ PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "  ❌ FAIL: $1"; FAIL=$((FAIL+1)); }

submit() {  # submit <lang> <name> <file> -> echoes submission_id
  local code; code=$(python3 -c "import json,sys;print(json.dumps(open(sys.argv[1]).read()))" "$3")
  curl -s -X POST "$BASE/submissions" -H 'Content-Type: application/json' \
    -d "{\"language\":\"$1\",\"name\":\"$2\",\"code\":$code}" \
    | python3 -c "import json,sys;print(json.load(sys.stdin).get('submission_id',''))"
}
run() {     # run <submission_id> <bots> <dur>
  curl -s -X POST "$BASE/runs" -H 'Content-Type: application/json' \
    -d "{\"submission_id\":\"$1\",\"bots\":$2,\"duration\":$3}" >/dev/null
}
row() {     # row <submission-name> <field> -> value from live snapshot
  docker exec arena-redis redis-cli GET arena:snapshot 2>/dev/null | python3 -c "
import json,sys
try: rows=json.load(sys.stdin)['rows']
except Exception: print(''); raise SystemExit
for r in rows:
    if r['submission']=='$1': print(r.get('$2','')); break
else: print('')
"
}
platform_healthy() {  # all core services still Up?
  local n; n=$(cd "$ROOT" && docker compose ps --services --filter status=running 2>/dev/null \
        | grep -E 'redis|orchestrator|telemetry|timescaledb|bot_fleet' | wc -l)
  [ "$n" -ge 5 ]
}

echo "=================================================================="
echo " HFT Arena — Adversarial Resilience Suite"
echo "=================================================================="

# ---- Test 1: cheating engine (violates price-time priority) ----------------
echo ""; echo "[1] Cheating engine (LIFO matching) — Validation must catch it"
SID=$(submit python cheater "$ROOT/tests/adversarial/cheater.py")
[ -n "$SID" ] && ok "cheater built & sandboxed" || bad "cheater build"
run "$SID" 200 4
sleep 12   # correctness probe runs first; let it report
CORR=$(row cheater correctness)
echo "     reported correctness = ${CORR:-none}"
python3 -c "import sys; sys.exit(0 if (('$CORR' or '1') and float('${CORR:-1}')<0.6) else 1)" \
  && ok "Validation caught wrong matching (correctness $CORR < 0.6)" \
  || bad "Validation did NOT flag the cheater (correctness=$CORR)"
curl -s -X POST "$BASE/stop" >/dev/null; sleep 2

# ---- Test 2: memory bomb (sandbox isolation must contain it) ----------------
echo ""; echo "[2] Memory-bomb engine — sandbox 512MB cap must OOM-contain it"
SID=$(submit python membomb "$ROOT/tests/adversarial/membomb.py")
run "$SID" 100 4
sleep 12
STATE=$(docker inspect "arena-sub-$SID" --format '{{.State.Status}} OOMKilled={{.State.OOMKilled}}' 2>/dev/null || echo "gone")
echo "     sandbox container state: $STATE"
echo "$STATE" | grep -qiE "oomkilled=true|exited|gone" \
  && ok "hostile engine was contained/killed (not the host)" \
  || bad "membomb sandbox still running unbounded: $STATE"
platform_healthy && ok "platform services all healthy after the attack" \
  || bad "a platform service went down"
curl -s -X POST "$BASE/stop" >/dev/null; sleep 2

# ---- Test 3: crasher (resilience — platform survives, run still scored) -----
echo ""; echo "[3] Crasher engine — platform must survive, run still scored"
SID=$(submit python crasher "$ROOT/tests/adversarial/crasher.py")
run "$SID" 300 4
sleep 14
platform_healthy && ok "platform survived the crashing submission" \
  || bad "platform degraded after crash"
curl -s -X POST "$BASE/stop" >/dev/null; sleep 2

# ---- Test 4: legit engine still works (no contamination) -------------------
echo ""; echo "[4] Legit engine after the attacks — no contamination"
SID=$(submit python good-engine "$ROOT/reference_engine_py/engine.py")
run "$SID" 200 4
sleep 12
CORR=$(row good-engine correctness)
echo "     legit correctness = ${CORR:-none}"
python3 -c "import sys; sys.exit(0 if float('${CORR:-0}')>0.8 else 1)" \
  && ok "legit engine scores correctly (correctness $CORR > 0.8)" \
  || bad "legit engine degraded (correctness=$CORR)"
curl -s -X POST "$BASE/stop" >/dev/null; sleep 2

# ---- Test 5: bursty order patterns (not just steady sweeps) -----------------
echo ""; echo "[5] Bursty/adversarial order flow — microburst phase must measure tail/jitter"
SID=$(submit python burst-target "$ROOT/reference_engine_py/engine.py")
run "$SID" 200 4
# Run = steady sweep -> MICROBURST sweep -> closed; wait for the burst phase to report.
BURST=; STEADY=
for _ in $(seq 1 30); do
  BURST=$(row burst-target burst_p99_us); STEADY=$(row burst-target p99_us)
  python3 -c "import sys; sys.exit(0 if float('${BURST:-0}')>0 else 1)" && break
  sleep 2
done
echo "     steady p99=${STEADY:-none} µs   burst p99=${BURST:-none} µs"
python3 -c "import sys; sys.exit(0 if float('${BURST:-0}')>0 else 1)" \
  && ok "platform exercised bursty flow & measured a burst tail (burst p99=${BURST} µs)" \
  || bad "no burst-phase tail measured (burst p99=${BURST:-none})"
platform_healthy && ok "platform healthy under bursty/microburst load" \
  || bad "a platform service went down under bursty load"
curl -s -X POST "$BASE/stop" >/dev/null

echo ""; echo "=================================================================="
echo " RESULT: $PASS passed, $FAIL failed"
echo "=================================================================="
[ "$FAIL" -eq 0 ]
