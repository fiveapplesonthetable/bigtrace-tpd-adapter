#!/usr/bin/env bash
# Build the Bigtrace UI bundle that the adapter serves (writes only into
# perfetto's out/ — no tracked perfetto source is modified).
#
# Why not just `ui/build --bigtrace`: on this checkout that path aborts at a
# `tsc --project ui/src/bigtrace --noEmit` gate — the bigtrace tsconfig
# (include: ["."]) does not pick up the ambient `declare module '*.grammar'`
# in ui/src/types/virtual-modules.d.ts, so it errors TS2307 before the Vite
# bundling step runs. Vite does NOT type-check (its @lezer/generator plugin
# compiles the .grammar on import), so we run the build for everything else,
# then invoke Vite directly to emit bigtrace_bundle.js + bigtrace.css.
set -euo pipefail
PERFETTO_UI="${PERFETTO_UI:-/mnt/agent/perfetto/perfetto/ui}"
PERFETTO_ROOT="$(cd "$PERFETTO_UI/.." && pwd)"
cd "$PERFETTO_UI"

# UI build deps go stale across branch switches (the dep manifest hash
# changes), and `ui/build` aborts at its stale-deps check before generating
# anything (including ui/src/gen/perfetto_version, which Vite needs). Refresh
# them first; it is a fast no-op when already current.
echo "== tools/install-build-deps --ui =="
"$PERFETTO_ROOT/tools/install-build-deps" --ui

# Sets up node deps, the gen/ + dist_version symlinks, scss, and copies
# bigtrace.html + assets into dist. Aborts at the bigtrace tsc gate (expected),
# hence `|| true` — everything before that gate has already been emitted.
echo "== ui/build --bigtrace (expected to stop at the bigtrace tsc gate) =="
./build --bigtrace || true

echo "== vite build (bigtrace bundle, no tsc gate) =="
BUNDLE=bigtrace ENABLE_BIGTRACE=true ./node node_modules/vite/bin/vite.js \
  build --config vite.config.mjs --logLevel warn

echo "== artifacts =="
ls -la out/dist_version/bigtrace/bigtrace_bundle.js out/dist_version/bigtrace/bigtrace.css
echo
echo "UI dist dir (pass to adapter --ui-dist):"
echo "  $(readlink -f out)/dist"
