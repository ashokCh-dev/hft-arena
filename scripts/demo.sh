#!/usr/bin/env bash
# End-to-end demo: submit a reference engine, launch a load run, watch the
# leaderboard. Usage: ./scripts/demo.sh [python|cpp] [bots] [duration]
set -euo pipefail

# DURATION = closed-loop (peak-TPS) phase length; an open-loop offered-load sweep
# (SWEEP_RATES x STEP_SECS, ~20s by default) then runs automatically after it.
LANG_SEL="${1:-python}"
BOTS="${2:-400}"
DURATION="${3:-8}"
BASE="http://localhost:8000"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

case "$LANG_SEL" in
  python) SRC="$ROOT/reference_engine_py/engine.py" ;;
  cpp)    SRC="$ROOT/reference_engine_cpp/engine.cpp" ;;
  *) echo "language must be python or cpp"; exit 1 ;;
esac

echo "==> Submitting $LANG_SEL reference engine ($SRC)"
CODE_JSON=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$SRC")
SUB=$(curl -s -X POST "$BASE/submissions" -H 'Content-Type: application/json' \
      -d "{\"language\":\"$LANG_SEL\",\"name\":\"$LANG_SEL-engine\",\"code\":$CODE_JSON}")
echo "    response: $SUB"
SUBMISSION_ID=$(echo "$SUB" | python3 -c "import json,sys; print(json.load(sys.stdin)['submission_id'])")

echo "==> Launching run: $BOTS bots for ${DURATION}s against submission $SUBMISSION_ID"
RUN=$(curl -s -X POST "$BASE/runs" -H 'Content-Type: application/json' \
      -d "{\"submission_id\":\"$SUBMISSION_ID\",\"bots\":$BOTS,\"duration\":$DURATION}")
echo "    response: $RUN"

echo ""
echo "==> Sandbox container (note the CPU/memory limits):"
sleep 2
docker ps --filter "name=arena-sub-$SUBMISSION_ID" --format \
  'table {{.Names}}\t{{.Status}}\t{{.Image}}' || true

echo ""
echo "==> Open http://localhost:8000 to watch the live leaderboard + latency curve."
echo "    Closed-loop phase (${DURATION}s) then open-loop offered-load sweep…"
timeout "$((DURATION+40))" docker compose logs -f --tail=0 bot_fleet || true
