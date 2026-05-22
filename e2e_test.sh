#!/usr/bin/env bash
# End-to-end test: tpd_server (real trace_processor children) <- adapter
# <- HTTP client. Stands up a private tpd daemon over its own socket and
# traces dir (so it never collides with any other running daemon), starts
# the adapter, and drives Bigtrace-shaped HTTP queries through to real
# SQL results.
#
# Nothing in the tpd source tree is modified; we only *run* the prebuilt
# tpd_server / tpd_cli binaries and read trace files.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
TP_BIN="${TP_BIN:-/mnt/agent/perfetto/perfetto/out/release/trace_processor_shell}"
TPD_SERVER="${TPD_SERVER:-/mnt/agent/tpd/build/tpd_server}"
SRC_TRACES="${SRC_TRACES:-/mnt/agent/tpd-data/datasets/big_base}"

WORK="$(mktemp -d /mnt/agent/tmp/bt_adapter_e2e.XXXXXX)"
TRACES_DIR="$WORK/traces"
SESSION_DIR="$WORK/session"
SOCK="$SESSION_DIR/sock"
PORT="${PORT:-5071}"
BASE="http://127.0.0.1:$PORT"
mkdir -p "$TRACES_DIR" "$SESSION_DIR"

TPD_PID=""
ADAPTER_PID=""
cleanup() {
  [ -n "$ADAPTER_PID" ] && kill "$ADAPTER_PID" 2>/dev/null
  [ -n "$TPD_PID" ] && kill "$TPD_PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  echo "--- tpd log ---";     tail -30 "$WORK/tpd.log" 2>/dev/null
  echo "--- adapter log ---"; tail -30 "$WORK/adapter.log" 2>/dev/null
  exit 1
}

# post_query <sql-json-string> <traces-json-array> -> JSON into $WORK/resp.json
post_query() {
  curl -sS -X POST "$BASE/query" -H 'Content-Type: application/json' \
    -d "{\"traces\":$2,\"sql_query\":$1}" -o "$WORK/resp.json" \
    || fail "curl POST /query"
}

echo "== work dir: $WORK"
cp "$SRC_TRACES/cf30.pftrace" "$SRC_TRACES/cf60.pftrace" "$TRACES_DIR/" || fail "copy traces"

echo "== starting tpd_server"
"$TPD_SERVER" \
  --traces-dir="$TRACES_DIR" \
  --session-dir="$SESSION_DIR" \
  --listen="$SOCK" \
  --tp-binary="$TP_BIN" \
  --memory-budget-mb=2048 \
  --daemon-memory-reserve-mb=512 \
  --cpu-workers=2 \
  --alive-cap=8 \
  --http-port=0 \
  >"$WORK/tpd.log" 2>&1 &
TPD_PID=$!

for i in $(seq 1 100); do
  [ -S "$SOCK" ] && break
  kill -0 "$TPD_PID" 2>/dev/null || fail "tpd_server died on startup"
  sleep 0.1
done
[ -S "$SOCK" ] || fail "tpd socket never appeared"
echo "== tpd_server up (pid $TPD_PID)"

echo "== starting adapter on port $PORT"
python3 "$here/adapter.py" --tpd-socket="$SOCK" --listen-port="$PORT" \
  >"$WORK/adapter.log" 2>&1 &
ADAPTER_PID=$!

for i in $(seq 1 100); do
  curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break
  kill -0 "$ADAPTER_PID" 2>/dev/null || fail "adapter died on startup"
  sleep 0.1
done
echo -n "== adapter healthz: "; curl -fsS "$BASE/healthz" || fail "healthz"; echo

# Give the dir-watcher a moment to register the two traces.
for i in $(seq 1 100); do
  n=$(curl -fsS "$BASE/traces" 2>/dev/null \
      | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["traces"]))' 2>/dev/null || echo 0)
  [ "$n" -ge 2 ] && break
  sleep 0.2
done
echo "== /traces:"; curl -fsS "$BASE/traces" | python3 -m json.tool || fail "/traces"

# ----------------------------------------------------------------------------
echo "== [1] count(*) from thread across both traces"
post_query '"select count(*) as n from thread"' '["cf30.pftrace","cf60.pftrace"]'
python3 -m json.tool "$WORK/resp.json"
python3 "$here/_check.py" cross "$WORK/resp.json" || fail "[1] cross-trace count"

echo "== [2] multi-column (varint + string/null cells)"
post_query '"select tid, name from thread order by tid limit 5"' '["cf30.pftrace"]'
python3 -m json.tool "$WORK/resp.json"
python3 "$here/_check.py" multicol "$WORK/resp.json" || fail "[2] multi-column"

echo "== [3] error path: bad SQL surfaces per-trace error"
post_query '"select * from no_such_table"' '["cf30.pftrace"]'
python3 -m json.tool "$WORK/resp.json"
python3 "$here/_check.py" error "$WORK/resp.json" || fail "[3] error path"

echo "== [4] unknown trace address"
post_query '"select 1"' '["does_not_exist.pftrace"]'
python3 -m json.tool "$WORK/resp.json"
python3 "$here/_check.py" unknown "$WORK/resp.json" || fail "[4] unknown trace"

echo
echo "ALL E2E CHECKS PASSED"
