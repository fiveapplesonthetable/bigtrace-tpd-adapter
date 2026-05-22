#!/usr/bin/env bash
# Browser end-to-end: serve the real Bigtrace UI from the adapter, load it in
# Chrome, point its backend at the same origin, and run a query through to tpd.
#
# Requires the Bigtrace UI to be built (run ./build_ui.sh once) and prebuilt
# tpd_server / trace_processor_shell. Nothing in tpd or perfetto source is
# modified — we only run binaries and serve build outputs.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
TP_BIN="${TP_BIN:-/mnt/agent/perfetto/perfetto/out/release/trace_processor_shell}"
TPD_SERVER="${TPD_SERVER:-/mnt/agent/tpd/build/tpd_server}"
SRC_TRACES="${SRC_TRACES:-/mnt/agent/tpd-data/datasets/big_base}"
UI_DIST="${UI_DIST:-/mnt/agent/perfetto/perfetto/out/ui/ui/dist}"
PERFETTO_UI="${PERFETTO_UI:-/mnt/agent/perfetto/perfetto/ui}"
PORT="${PORT:-5071}"
BASE="http://127.0.0.1:$PORT"

[ -f "$UI_DIST/bigtrace.html" ] || { echo "FAIL: $UI_DIST/bigtrace.html missing — run ./build_ui.sh first" >&2; exit 1; }

WORK="$(mktemp -d /mnt/agent/tmp/bt_ui_e2e.XXXXXX)"
mkdir -p "$WORK/traces" "$WORK/session"
TPD_PID=""; ADAPTER_PID=""
cleanup() {
  [ -n "$ADAPTER_PID" ] && kill "$ADAPTER_PID" 2>/dev/null
  [ -n "$TPD_PID" ] && kill "$TPD_PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; tail -20 "$WORK/tpd.log" "$WORK/adapter.log" 2>/dev/null; exit 1; }

cp "$SRC_TRACES/cf30.pftrace" "$SRC_TRACES/cf60.pftrace" "$WORK/traces/" || fail "copy traces"

echo "== starting tpd_server"
"$TPD_SERVER" --traces-dir="$WORK/traces" --session-dir="$WORK/session" \
  --listen="$WORK/session/sock" --tp-binary="$TP_BIN" \
  --memory-budget-mb=2048 --daemon-memory-reserve-mb=512 --cpu-workers=2 \
  --alive-cap=8 --http-port=0 >"$WORK/tpd.log" 2>&1 &
TPD_PID=$!
for i in $(seq 1 100); do [ -S "$WORK/session/sock" ] && break; sleep 0.1; done
[ -S "$WORK/session/sock" ] || fail "tpd socket never appeared"

echo "== starting adapter (serving UI from $UI_DIST)"
python3 "$here/adapter.py" --tpd-socket="$WORK/session/sock" --listen-port="$PORT" \
  --ui-dist="$UI_DIST" >"$WORK/adapter.log" 2>&1 &
ADAPTER_PID=$!
for i in $(seq 1 100); do curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break;
  kill -0 "$ADAPTER_PID" 2>/dev/null || fail "adapter died"; sleep 0.1; done

echo "== driving the Bigtrace UI in Chrome"
OUT_DIR="$WORK" BASE="$BASE" "$PERFETTO_UI/node" "$here/ui_browser_check.mjs" 2>&1 | tee "$WORK/browser.log"
grep -q "UI_E2E_OK" "$WORK/browser.log" || fail "browser check did not pass"

# Keep the screenshots next to this script as evidence.
cp "$WORK/bigtrace_ui.png" "$here/bigtrace_ui.png" 2>/dev/null && \
  echo "== screenshot saved: $here/bigtrace_ui.png"
cp "$WORK/bigtrace_settings.png" "$here/bigtrace_settings.png" 2>/dev/null && \
  echo "== screenshot saved: $here/bigtrace_settings.png"
cp "$WORK/bigtrace_persist.png" "$here/bigtrace_persist.png" 2>/dev/null && \
  echo "== screenshot saved: $here/bigtrace_persist.png"

echo
echo "BIGTRACE UI E2E PASSED"
