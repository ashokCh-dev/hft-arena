#!/usr/bin/env bash
# CI smoke test: bring-up assumed running; submit the Python reference engine,
# launch a short load run, and assert the leaderboard gets a correctly-scored row.
set -uo pipefail
BASE="http://localhost:8000"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

say() { echo "[smoke] $*"; }

say "waiting for orchestrator /health…"
for _ in $(seq 1 60); do curl -fsS "$BASE/health" >/dev/null 2>&1 && break; sleep 2; done
curl -fsS "$BASE/health" >/dev/null || { say "orchestrator never came up"; exit 1; }

say "waiting for a bot_fleet worker to be ready…"
for _ in $(seq 1 90); do
  docker compose logs bot_fleet 2>/dev/null | grep -q "ready, BOTS_PER_WORKER" && break
  sleep 2
done

say "submitting Python reference engine…"
CODE=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$ROOT/reference_engine_py/engine.py")
SID=$(curl -fsS -X POST "$BASE/submissions" -H 'Content-Type: application/json' \
        -d "{\"language\":\"python\",\"name\":\"smoke\",\"code\":$CODE}" \
      | python3 -c "import json,sys; print(json.load(sys.stdin).get('submission_id',''))")
[ -n "$SID" ] || { say "submission failed"; exit 1; }
say "submission_id=$SID"

curl -fsS -X POST "$BASE/runs" -H 'Content-Type: application/json' \
     -d "{\"submission_id\":\"$SID\",\"bots\":80,\"duration\":4}" >/dev/null
say "run started; waiting for a scored leaderboard row…"

for _ in $(seq 1 60); do
  OK=$(docker compose exec -T redis redis-cli GET arena:snapshot 2>/dev/null | python3 -c "
import json,sys
s=sys.stdin.read().strip()
if s:
    for r in json.loads(s).get('rows',[]):
        if r['submission']=='smoke' and r['score']>0 and r['correctness']>0.5 and len(r['curve'])>=1:
            print('OK'); break
" 2>/dev/null)
  if [ "$OK" = "OK" ]; then say "PASS — leaderboard scored the submission"; exit 0; fi
  sleep 3
done

say "FAIL — no correctly-scored leaderboard row within timeout"
docker compose logs --tail=60 || true
exit 1
