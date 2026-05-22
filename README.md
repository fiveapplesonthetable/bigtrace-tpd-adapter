# bigtrace → tpd adapter

A small HTTP server that lets the Perfetto **Bigtrace UI** (and any Bigtrace
HTTP client) run queries against **tpd**. It takes the HTTP requests the
Bigtrace UI makes and speaks tpd's native protocol on the back; tpd keeps a
warm `trace_processor` per trace, so it is the worker pool.

Nothing in the tpd or perfetto **source** trees is modified — the adapter
talks to a running `tpd_server` over its unix socket and serves the prebuilt
UI from perfetto's `out/`. The folder is self-contained (vendored protos +
generated bindings).

## Quick start: the Bigtrace UI

The Bigtrace UI applies a `connect-src 'self'` CSP, so it can only call a
backend on **its own origin**. The adapter therefore serves both the UI and
the API on one port; set the UI's backend endpoint to `""` (same origin).

```sh
# 0. One-time: build the Bigtrace UI bundle (writes only to perfetto out/).
./build_ui.sh

# 1. A tpd_server must be running with some traces (tpd's job, not ours):
#    tpd_server --traces-dir=DIR --listen=$HOME/.tpd/session/sock \
#               --tp-binary=.../trace_processor_shell

# 2. Start the adapter, serving the UI + API on one origin:
python3 adapter.py --tpd-socket=$HOME/.tpd/session/sock --listen-port=5051 \
        --ui-dist=/mnt/agent/perfetto/perfetto/out/ui/ui/dist

# 3. Open http://localhost:5051/bigtrace.html
#    -> Settings -> set "BigTrace Endpoint" to ''  (empty = same origin) -> reload
#    -> Query (SQL): type SQL, Ctrl+Enter. Rows come back from tpd.
```

`ui_e2e_test.sh` does all of this headlessly and screenshots the result
(`bigtrace_ui.png`): the UI runs `select count(*) from thread` and renders
one row per trace, routed UI → adapter → tpd.

### Endpoints the UI calls (served by the adapter)

| UI request                          | adapter behaviour                                              |
| ----------------------------------- | ------------------------------------------------------------- |
| `POST /execute_bigtrace_query` `{limit, perfetto_sql, settings}` | run the SQL across every tpd trace, merge into one table, return `{columnNames, rows:[{values}]}` with a leading `_trace` provenance column |
| `POST /bigtrace_execution_config`   | `{setting: []}` (no server-side execution filters)            |
| `POST /trace_metadata_settings`     | `{setting: []}` (no metadata-derived filters)                 |
| `GET  /<asset>`                     | static file from `--ui-dist` (bigtrace.html, bundle, css, …)  |

CORS echoes the request `Origin` and sets `Access-Control-Allow-Credentials`
(the UI fetches with `credentials:'include'`, which forbids a `*` origin).

## Also: adapter-native + per-trace API

Independent of the UI, the adapter exposes a Bigtrace-shaped JSON API:

```
POST /query   {"traces": ["cf30.pftrace", ...], "sql_query": "select ..."}
              -> {"responses": [{"trace","columns","rows","error","elapsed_ms",...}]}
GET  /traces  -> {"traces": [{"trace_uuid","trace_path","state","pid"}]}
GET  /healthz -> {"ok": true, ...}
```

`POST /query` mirrors Bigtrace's `BigtraceQueryArgs{traces, sql_query}` ->
per-trace `BigtraceQueryResponse{trace, result}`: one tpd `RunQueryRequest`
per address (1:1 with Bigtrace's one-trace-per-worker model), run
concurrently. 200 when all traces succeed, 207 if any errored (every
per-trace payload still returned). `max_rows` caps rows per trace.

Trace addresses map to tpd trace-path **basename** globs, e.g.
`/bench/cf30.pftrace` → `cf30.pftrace`; `cf*.pftrace` passes through as a
glob. `GET /traces` lists what tpd knows about.

## How it maps to tpd (exact, not lossy)

| Bigtrace                  | tpd                                                          |
| ------------------------- | ----------------------------------------------------------- |
| trace address             | `RunQueryRequest.trace_filter_globs` (matched on basename)  |
| sql_query / perfetto_sql  | `RunQueryRequest.sql`                                        |
| per-trace `QueryResult`   | `ResultEnvelope.chunk.raw_query_result` (verbatim perfetto `QueryResult`) |
| per-trace OK/error        | `ResultEnvelope.footer.status` / `error_message`            |

Wire: connect to tpd's `AF_UNIX` control socket, send a varint-framed
`ClientMsg{run_query}` with the result pipe's write end attached via
`SCM_RIGHTS`, read one framed `ServerMsg`, then drain varint-framed
`ResultEnvelope`s until `QueryComplete` — the same dance `tpd_cli query`
does. Raw `QueryResult` bytes are decoded with the same cell layout
Perfetto's `QueryResultIterator` uses.

## Layout

```
adapter.py          the server (HTTP front, static UI, tpd client, decoder)
build_ui.sh         build the Bigtrace UI bundle (Vite, bypassing a tsc gate)
ui_e2e_test.sh      headless browser e2e: UI -> adapter -> tpd, + screenshot
ui_browser_check.mjs   the Playwright driver used by ui_e2e_test.sh
bigtrace_ui.png     screenshot evidence from the last ui_e2e_test.sh run
e2e_test.sh         API e2e: private tpd_server + adapter, drives JSON queries
_check.py           assertion helper for e2e_test.sh
gen_protos.sh       regenerate gen/ from protos/ (protoc); --refresh re-vendors
protos/             vendored copies of tpd's control/envelope/tpd_query .proto
gen/                generated Python bindings (checked in; runs without a build)
```

## Tests

```sh
./e2e_test.sh      # API path: cross-trace count, multi-column, error, unknown-trace
./ui_e2e_test.sh   # Browser path: loads the real Bigtrace UI, runs a query, screenshots
```

Both stand up a private `tpd_server` (own socket + traces dir, never collides
with another daemon) over two real traces. Paths overridable via
`TPD_SERVER`, `TP_BIN`, `SRC_TRACES`, `UI_DIST`.

## Notes

- `build_ui.sh` runs `ui/build --bigtrace` (which sets up assets + the
  dist) and then invokes Vite directly for the bigtrace bundle. The plain
  `ui/build --bigtrace` aborts at a `tsc --noEmit` gate on `ui/src/bigtrace`
  (its tsconfig `include:["."]` misses the ambient `declare module
  '*.grammar'` in `ui/src/types/virtual-modules.d.ts`); Vite doesn't
  type-check and its lezer plugin compiles the grammar, so it produces the
  bundle regardless. No perfetto source is changed.
- The UI logs a harmless `GET /live_reload 404` (a dev-server-only SSE the
  adapter doesn't implement); it does not affect queries.

## Refreshing protos after a tpd wire change

```sh
TPD_PROTOS=/mnt/agent/tpd/protos ./gen_protos.sh --refresh
```
