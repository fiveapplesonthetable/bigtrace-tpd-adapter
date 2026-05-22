#!/usr/bin/env bash
# Regenerate the Python bindings the adapter needs from the bundled protos.
# The adapter speaks the tpd native control protocol, so it needs
# control.proto (RunQueryRequest/ServerMsg), envelope.proto
# (ResultEnvelope stream) and tpd_query.proto (TpdQueryResult, a
# field-for-field mirror of perfetto QueryResult).
#
# The .proto files in ./protos are vendored copies of the tpd protos so this
# folder is self-contained and does not depend on the tpd checkout. To
# refresh them after a tpd wire change, point TPD_PROTOS at the tpd protos
# dir:
#   TPD_PROTOS=/path/to/tpd/protos ./gen_protos.sh --refresh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
protos_dir="$here/protos"
out="$here/gen"

if [[ "${1:-}" == "--refresh" ]]; then
  src="${TPD_PROTOS:?set TPD_PROTOS to the tpd protos dir to refresh}"
  cp "$src/control.proto" "$src/envelope.proto" "$src/tpd_query.proto" "$protos_dir/"
  echo "refreshed vendored protos from $src"
fi

mkdir -p "$out"
protoc -I "$protos_dir" --python_out="$out" \
  control.proto envelope.proto tpd_query.proto
echo "generated bindings in $out:"
ls -1 "$out"/*.py
