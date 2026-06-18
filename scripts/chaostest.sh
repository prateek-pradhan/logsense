#!/usr/bin/env bash
# chaostest.sh — proves the consumer loses nothing and duplicates nothing
# even when killed (-9) mid-stream.
#
# Per round: produce a known set of events through the pipeline, kill -9
# the consumer while it's working, restart it, wait for it to drain, then
# reconcile the produced IDs against Mongo. PASS = every event present
# exactly once.
#
# Requires: `docker compose up` running (Redpanda + Mongo).
# Usage: ./scripts/chaostest.sh [rounds]

set -euo pipefail  # exit on error, on unset var, and fail pipes loudly

# --- config -----------------------------------------------------------
ROUNDS="${1:-5}"          # how many crash rounds (default 5)
N=20000                   # events per round
RATE=2000                 # events/sec (so a round takes ~10s)
CRASH_AFTER=2             # seconds into the run before we kill -9
IDS_FILE="/tmp/chaos.ids"
GATEWAY_LOG="/tmp/chaos-gateway.log"
CONSUMER_LOG="/tmp/chaos-consumer.log"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"  # repo root, regardless of cwd
cd "$ROOT"

# --- build the binaries we need (clean PIDs for kill -9) --------------
echo "building binaries..."
go build -o bin/gateway ./cmd/gateway
go build -o bin/consumer ./cmd/consumer
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/reconcile ./cmd/reconcile

# --- start the gateway (shared across all rounds) ---------------------
echo "starting gateway..."
./bin/gateway >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!

# cleanup() runs on ANY exit (normal, error, or Ctrl+C) — kill leftovers.
cleanup() {
  kill "$GATEWAY_PID" 2>/dev/null || true
  [[ -n "${CONSUMER_PID:-}" ]] && kill -9 "$CONSUMER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait until the gateway answers /healthz before producing anything.
for i in {1..20}; do
  if curl -sf http://localhost:8080/healthz >/dev/null; then break; fi
  sleep 0.5
  if [[ $i == 20 ]]; then echo "gateway never became healthy"; exit 1; fi
done
echo "gateway healthy."

# --- run the rounds ---------------------------------------------------
PASSES=0
for round in $(seq 1 "$ROUNDS"); do
  echo ""
  echo "================ ROUND $round / $ROUNDS ================"

  # Start a consumer in the background; remember its PID so we can kill it.
  ./bin/consumer >"$CONSUMER_LOG" 2>&1 &
  CONSUMER_PID=$!

  # Start the load generator in the background, recording every ID it sends.
  ./bin/loadgen -n "$N" -rate "$RATE" -ids-file "$IDS_FILE" >/dev/null 2>&1 &
  LOADGEN_PID=$!

  # Let the consumer get into the middle of the work, then KILL IT HARD.
  sleep "$CRASH_AFTER"
  echo ">>> kill -9 consumer (pid $CONSUMER_PID) mid-stream"
  kill -9 "$CONSUMER_PID" 2>/dev/null || true

  # Restart the consumer immediately (simulating a pod being rescheduled).
  ./bin/consumer >>"$CONSUMER_LOG" 2>&1 &
  CONSUMER_PID=$!
  echo ">>> restarted consumer (pid $CONSUMER_PID)"

  # Wait for the load generator to finish producing.
  wait "$LOADGEN_PID"
  echo ">>> loadgen finished; draining..."

  # Poll reconcile until it passes (drain complete) or we time out.
  # While events are still flowing into Mongo, reconcile FAILS and we wait.
  result="FAIL"
  for attempt in {1..30}; do          # up to ~60s
    if ./bin/reconcile -ids-file "$IDS_FILE" >/tmp/chaos-reconcile.out 2>&1; then
      result="PASS"
      break
    fi
    sleep 2
  done

  # Stop this round's consumer cleanly before the next round.
  kill "$CONSUMER_PID" 2>/dev/null || true
  wait "$CONSUMER_PID" 2>/dev/null || true

  tail -n 2 /tmp/chaos-reconcile.out
  if [[ "$result" == "PASS" ]]; then
    echo ">>> ROUND $round: PASS"
    PASSES=$((PASSES + 1))
  else
    echo ">>> ROUND $round: FAIL (see /tmp/chaos-reconcile.out)"
  fi
done

# --- summary ----------------------------------------------------------
echo ""
echo "================ SUMMARY ================"
echo "PASSED $PASSES / $ROUNDS rounds"
[[ "$PASSES" == "$ROUNDS" ]]  # script exits non-zero if any round failed
