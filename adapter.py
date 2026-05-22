#!/usr/bin/env python3
# Copyright (C) 2026 The tpd Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""HTTP front-end that speaks the Bigtrace query shape and routes to tpd.

Bigtrace's client-facing contract is one call: take a list of trace
addresses plus a SQL string, run the SQL on each trace, and stream back
one result per trace (see protos/perfetto/bigtrace/orchestrator.proto:
``BigtraceQueryArgs{traces, sql_query}`` -> ``BigtraceQueryResponse{
trace, result}``). This adapter exposes that contract over plain HTTP and
fulfils it by talking tpd's native unix-socket protocol instead of
sharding across Bigtrace workers: tpd already keeps a warm
trace_processor per trace, so it is the worker pool.

The mapping is exact, not lossy:

  Bigtrace                         tpd
  --------                         ---
  trace address          ->        RunQueryRequest.trace_filter_globs
                                    (matched against trace-path basename)
  sql_query              ->        RunQueryRequest.sql
  per-trace QueryResult  <-        ResultEnvelope.chunk.raw_query_result
                                    (raw perfetto QueryResult, verbatim)

One RunQueryRequest is issued per requested address (a clean 1:1 map,
mirroring Bigtrace's one-trace-per-worker model); the per-trace
RunQuerys run concurrently on a bounded thread pool. The raw
QueryResult bytes tpd forwards are decoded into columns + rows using the
same cell layout Perfetto's QueryResultIterator uses, so the JSON the
adapter returns is faithful to what a Bigtrace client would compute.

HTTP surface:
  POST /query    {"traces": [...], "sql_query": "..."}  -> per-trace rows
  GET  /traces   list traces tpd currently knows about
  GET  /healthz  adapter + tpd liveness

Run `adapter.py --help` for flags.
"""

import argparse
import array
import base64
import collections
import datetime
import itertools
import json
import os
import select
import socket
import sys
import threading
import urllib.parse
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Generated from tpd's protos (see gen_protos.sh). These are tpd's wire
# types; QueryResultChunk.raw_query_result is a verbatim perfetto
# QueryResult, which TpdQueryResult mirrors field-for-field.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))
import control_pb2  # noqa: E402
import envelope_pb2  # noqa: E402
import tpd_query_pb2  # noqa: E402

# ---------------------------------------------------------------------------
# tpd wire protocol (mirrors src/cli_main.cc + src/base/io_util.cc).
#
# A client connects to the AF_UNIX control socket, sends a varint-length-
# framed ClientMsg with the pipe's write end attached via SCM_RIGHTS, reads
# one framed ServerMsg back on the control socket, then drains varint-framed
# ResultEnvelope messages from the pipe until QueryComplete.
# ---------------------------------------------------------------------------


def _encode_varint(n):
    out = bytearray()
    while n >= 0x80:
        out.append(0x80 | (n & 0x7F))
        n >>= 7
    out.append(n)
    return bytes(out)


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError(f"control socket closed: got {len(buf)} of {n} bytes")
        buf += chunk
    return bytes(buf)


def _read_framed_sock(sock):
    """Read one varint-framed message from a stream socket."""
    shift = 0
    length = 0
    while True:
        b = _recv_exact(sock, 1)[0]
        length |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return _recv_exact(sock, length)


def _read_framed_fd(fd, deadline_select_timeout):
    """Read one varint-framed message from a pipe fd, or None at EOF.

    `deadline_select_timeout` is a callable returning the remaining seconds
    to wait; raises TimeoutError if it returns <= 0 while blocked.
    """

    def _read_n(n):
        out = bytearray()
        while len(out) < n:
            to = deadline_select_timeout()
            if to is not None and to <= 0:
                raise TimeoutError("timed out reading tpd result stream")
            r, _, _ = select.select([fd], [], [], to)
            if not r:
                raise TimeoutError("timed out reading tpd result stream")
            chunk = os.read(fd, n - len(out))
            if not chunk:
                if not out:
                    return None  # clean EOF on a message boundary
                raise EOFError(f"pipe closed mid-frame: got {len(out)} of {n}")
            out += chunk
        return bytes(out)

    shift = 0
    length = 0
    while True:
        b = _read_n(1)
        if b is None:
            return None
        v = b[0]
        length |= (v & 0x7F) << shift
        if not (v & 0x80):
            break
        shift += 7
    payload = _read_n(length)
    if payload is None:
        raise EOFError("pipe closed after length prefix")
    return payload


def _sendmsg_with_fd(sock, payload, fd):
    """sendmsg the varint-framed payload, attaching `fd` via SCM_RIGHTS."""
    framed = _encode_varint(len(payload)) + payload
    fds = array.array("i", [fd])
    sent = sock.sendmsg(
        [framed], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)]
    )
    # The request is tiny, but loop on the off chance of a short send. The
    # ancillary fd rides only the first segment.
    while sent < len(framed):
        sent += sock.send(framed[sent:])


# ---------------------------------------------------------------------------
# QueryResult decoding — matches perfetto/common/query_result_iterator.py.
# ---------------------------------------------------------------------------

_CELL_NULL = 1
_CELL_VARINT = 2
_CELL_FLOAT64 = 3
_CELL_STRING = 4
_CELL_BLOB = 5


def _extract_strings(raw):
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", "ignore")
    else:
        text = raw
    parts = text.split("\0")
    if parts:
        parts.pop()  # trailing NUL terminator yields an empty tail element
    return parts


def decode_query_results(chunks):
    """Decode a list of TpdQueryResult protos into (columns, rows, error).

    `chunks` are the per-trace QueryResult messages tpd streamed (a trace
    can produce >1, exactly like Bigtrace's `repeated QueryResult`).
    column_names come from the first message that carries them; batches are
    concatenated across all messages — the same merge the Bigtrace python
    client performs.
    """
    columns = []
    error = None
    batches = []
    for qr in chunks:
        if qr.column_names and not columns:
            columns = list(qr.column_names)
        if qr.error and not error:
            error = qr.error
        batches.extend(qr.batch)
    if error:
        return columns, [], error
    if batches and not batches[-1].is_last_batch:
        return columns, [], "result stream truncated (no is_last_batch)"

    typed = [
        None,
        None,
        list(itertools.chain.from_iterable(b.varint_cells for b in batches)),
        list(itertools.chain.from_iterable(b.float64_cells for b in batches)),
        list(
            itertools.chain.from_iterable(
                _extract_strings(b.string_cells) for b in batches
            )
        ),
        list(itertools.chain.from_iterable(b.blob_cells for b in batches)),
    ]
    offsets = [0] * 6
    flat = []
    for ct in itertools.chain.from_iterable(b.cells for b in batches):
        if ct == _CELL_NULL:
            flat.append(None)
        else:
            val = typed[ct][offsets[ct]]
            if ct == _CELL_BLOB:
                val = base64.b64encode(val).decode("ascii")
            flat.append(val)
        offsets[ct] += 1

    ncol = len(columns)
    if ncol == 0:
        return columns, [], None
    if len(flat) % ncol != 0:
        return columns, [], (
            f"result has {len(flat)} cells, not divisible by {ncol} columns"
        )
    rows = [flat[i : i + ncol] for i in range(0, len(flat), ncol)]
    return columns, rows, None


# ---------------------------------------------------------------------------
# tpd client.
# ---------------------------------------------------------------------------

# TraceFooter.Status enum (envelope.proto).
_FOOTER_STATUS = {0: "unknown", 1: "ok", 2: "error", 3: "cancelled", 4: "truncated"}


class TpdError(Exception):
    pass


class TpdClient:
    def __init__(self, sock_path, timeout=120.0):
        self.sock_path = sock_path
        self.timeout = timeout

    def _connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        try:
            s.connect(self.sock_path)
        except OSError as e:
            s.close()
            raise TpdError(f"cannot connect to tpd at {self.sock_path}: {e}")
        return s

    def _round_trip(self, client_msg):
        """Send a fd-less ClientMsg, return the ServerMsg."""
        s = self._connect()
        try:
            payload = client_msg.SerializeToString()
            framed = _encode_varint(len(payload)) + payload
            s.sendall(framed)
            raw = _read_framed_sock(s)
        finally:
            s.close()
        resp = control_pb2.ServerMsg()
        resp.ParseFromString(raw)
        if resp.HasField("error"):
            raise TpdError(f"tpd error {resp.error.code}: {resp.error.message}")
        return resp

    def ping(self):
        msg = control_pb2.ClientMsg()
        msg.ping.SetInParent()
        return self._round_trip(msg).ping

    def list_traces(self, glob="", state_filter=""):
        msg = control_pb2.ClientMsg()
        lt = msg.list_traces
        lt.SetInParent()  # select the oneof even when no filters are set
        if glob:
            lt.glob = glob
        if state_filter:
            lt.state_filter = state_filter
        return list(self._round_trip(msg).list_traces.traces)

    def run_query(self, sql, trace_glob, max_rows=0):
        """Run `sql` on traces matching `trace_glob`.

        Returns a dict keyed by trace_uuid -> {path, status, error, chunks}.
        """
        s = self._connect()
        r_fd, w_fd = os.pipe()
        try:
            msg = control_pb2.ClientMsg()
            rq = msg.run_query
            rq.sql = sql
            if trace_glob:
                rq.trace_filter_globs.append(trace_glob)
            if max_rows:
                rq.max_rows = max_rows
            _sendmsg_with_fd(s, msg.SerializeToString(), w_fd)
            os.close(w_fd)  # tpd holds its own copy now
            w_fd = -1

            server_msg = control_pb2.ServerMsg()
            server_msg.ParseFromString(_read_framed_sock(s))
            if server_msg.HasField("error"):
                raise TpdError(
                    f"tpd error {server_msg.error.code}: "
                    f"{server_msg.error.message}"
                )
            rr = server_msg.run_query
            dispatched = rr.num_traces_dispatched

            deadline = [None]
            if self.timeout:
                import time

                end = time.monotonic() + self.timeout

                def remaining():
                    return end - time.monotonic()

                deadline[0] = remaining

            def to_fn():
                return deadline[0]() if deadline[0] else None

            results = {}
            while True:
                frame = _read_framed_fd(r_fd, to_fn)
                if frame is None:
                    break
                env = envelope_pb2.ResultEnvelope()
                env.ParseFromString(frame)
                payload = env.WhichOneof("payload")
                # The terminal QueryComplete envelope is query-global: it
                # carries no trace_uuid, so handle it before keying by trace
                # (otherwise it would mint a phantom empty-uuid entry).
                if payload == "complete":
                    break
                key = env.trace_uuid
                entry = results.setdefault(
                    key,
                    {"path": "", "status": "unknown", "error": "", "chunks": []},
                )
                if env.trace_path:
                    entry["path"] = env.trace_path
                if payload == "chunk":
                    qr = tpd_query_pb2.TpdQueryResult()
                    qr.ParseFromString(env.chunk.raw_query_result)
                    entry["chunks"].append(qr)
                elif payload == "footer":
                    entry["status"] = _FOOTER_STATUS.get(
                        env.footer.status, "unknown"
                    )
                    entry["error"] = env.footer.error_message
                    entry["elapsed_ms"] = env.footer.elapsed_ns / 1e6
            return dispatched, results
        finally:
            if w_fd >= 0:
                os.close(w_fd)
            os.close(r_fd)
            s.close()


# ---------------------------------------------------------------------------
# Bigtrace-shaped HTTP front-end.
# ---------------------------------------------------------------------------


def _trace_to_glob(addr):
    """Map a Bigtrace trace address to a tpd basename glob.

    tpd matches trace_filter_globs against the trace path basename, so an
    address like "/bench/cf30.pftrace" maps to "cf30.pftrace" and a bare
    "cf30*" passes through as a glob.
    """
    base = addr.rsplit("/", 1)[-1] if "/" in addr else addr
    return base


def query_one_trace(client, addr, sql, max_rows):
    """Run `sql` on a single Bigtrace address, return its response dict."""
    glob = _trace_to_glob(addr)
    try:
        dispatched, results = client.run_query(sql, glob, max_rows=max_rows)
    except TpdError as e:
        # tpd rejects an unmatched filter ("no traces matched filter") and
        # surfaces connection failures as errors. Keep the failure scoped to
        # this address so the other requested traces still return.
        return [{
            "trace": addr,
            "columns": [],
            "rows": [],
            "error": f"no tpd trace matched address '{addr}' (glob '{glob}'): {e}",
        }]
    if dispatched == 0 or not results:
        return [{
            "trace": addr,
            "columns": [],
            "rows": [],
            "error": f"no tpd trace matched address '{addr}' (glob '{glob}')",
        }]
    out = []
    for uuid, entry in results.items():
        columns, rows, decode_err = decode_query_results(entry["chunks"])
        err = entry.get("error") or decode_err or None
        if entry.get("status") == "error" and not err:
            err = "query failed"
        # When the glob resolved to exactly the requested address, echo the
        # caller's address; otherwise surface the concrete trace path so an
        # ambiguous glob stays traceable.
        trace_name = addr if dispatched == 1 else (entry["path"] or addr)
        out.append({
            "trace": trace_name,
            "trace_uuid": uuid,
            "trace_path": entry["path"],
            "columns": columns,
            "rows": rows,
            "error": err,
            "elapsed_ms": entry.get("elapsed_ms"),
        })
    return out


def execute_bigtrace_query(client, sql, limit):
    """Run `sql` across every trace tpd knows and merge into one flat table.

    This is what the Bigtrace UI's /execute_bigtrace_query wants: a single
    {columnNames, rows} table, not a per-trace stream. Each trace's rows are
    prefixed with a `_trace` provenance column (the basename), mirroring the
    `_trace_address` column the Bigtrace python client injects. `limit` caps
    the total merged row count.

    Returns (column_names, rows, errors): rows is a list of value-lists,
    errors is a list of (trace, message) for traces that failed.
    """
    # Empty glob => fan out across all registered traces.
    _dispatched, results = client.run_query(sql, "", max_rows=0)
    column_names = None
    merged = []
    errors = []
    for uuid, entry in results.items():
        cols, rows, decode_err = decode_query_results(entry["chunks"])
        name = os.path.basename(entry["path"]) or uuid
        err = entry.get("error") or decode_err
        if entry.get("status") == "error" and not err:
            err = "query failed"
        if err:
            errors.append((name, err))
            continue
        if column_names is None and cols:
            column_names = ["_trace"] + list(cols)
        for r in rows:
            merged.append([name] + r)
            if limit and len(merged) >= limit:
                break
        if limit and len(merged) >= limit:
            break
    if column_names is None:
        column_names = ["_trace"]
    return column_names, merged, errors


def _now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class ExecStore:
    """In-memory record of query executions.

    The upstream/main Bigtrace UI runs even a "sync" query as a tracked
    execution: it learns the query reached a terminal status (SUCCESS) by
    listing GET /query_executions (history merge) and/or polling
    GET /query_executions/{uuid}:status, and pages materialized results via
    :fetch_results. We run each query synchronously against tpd, then keep
    its result here so those endpoints can answer.
    """

    def __init__(self, cap=200):
        self._d = collections.OrderedDict()
        self._lock = threading.Lock()
        self._cap = cap

    def put(self, ex):
        with self._lock:
            self._d[ex["queryUuid"]] = ex
            while len(self._d) > self._cap:
                self._d.popitem(last=False)

    def get(self, uuid):
        with self._lock:
            return self._d.get(uuid)

    def list_newest_first(self):
        with self._lock:
            return list(reversed(self._d.values()))

    def delete(self, uuid):
        with self._lock:
            return self._d.pop(uuid, None) is not None


def _raw_exec(ex):
    """RawQueryExecution view (drops the internal _columns/_rows)."""
    return {k: v for k, v in ex.items() if not k.startswith("_")}


def make_execution(sql, columns, rows, errors, materialized):
    now = _now_iso()
    err = "; ".join(f"[{t}] {m}" for t, m in errors) if errors else None
    return {
        "queryUuid": _uuid.uuid4().hex,
        "status": "SUCCESS",
        "startTime": now,
        "endTime": now,
        "processedRows": len(rows),
        "processedTraces": 0,
        "totalTraces": 0,
        "perfettoSql": sql,
        "limit": 0,
        "materialized": materialized,
        "error": err,
        "errorMessage": err,
        "_columns": columns,
        "_rows": rows,
    }


# MIME types for the static UI assets.
_MIME = {
    ".html": "text/html", ".js": "application/javascript", ".css": "text/css",
    ".wasm": "application/wasm", ".json": "application/json", ".map": "application/json",
    ".png": "image/png", ".svg": "image/svg+xml", ".ico": "image/x-icon",
    ".woff": "font/woff", ".woff2": "font/woff2", ".ttf": "font/ttf",
}


class Handler(BaseHTTPRequestHandler):
    server_version = "tpd-bigtrace-adapter/1.0"

    # Injected by make_server.
    client = None
    pool = None
    max_rows = 0
    ui_dist = None  # directory of the built Bigtrace UI, or None
    execs = None    # ExecStore

    def log_message(self, fmt, *args):
        sys.stderr.write(
            "[adapter] %s - %s\n" % (self.address_string(), fmt % args)
        )

    def _cors_headers(self):
        # The Bigtrace UI fetches with credentials:'include', so a "*" origin
        # is rejected by the browser — echo the caller's Origin and allow
        # credentials. Same-origin requests (no Origin header) get "*".
        origin = self.headers.get("Origin")
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Vary", "Origin")
        else:
            self.send_header("Access-Control-Allow-Origin", "*")

    def _send_json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors_headers()
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    # ---- request bodies ----------------------------------------------------

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        return json.loads(raw or b"{}")

    # ---- GET ---------------------------------------------------------------

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            try:
                p = self.client.ping()
                self._send_json(200, {"ok": True, "tpd_server_unix_nanos": p.server_unix_nanos})
            except TpdError as e:
                self._send_json(503, {"ok": False, "error": str(e)})
            return
        if path == "/traces":
            try:
                traces = self.client.list_traces()
                self._send_json(200, {"traces": [
                    {"trace_uuid": t.trace_uuid, "trace_path": t.trace_path,
                     "state": t.state, "pid": t.pid}
                    for t in traces
                ]})
            except TpdError as e:
                self._send_json(503, {"error": str(e)})
            return
        # Bigtrace UI query-executions API (GET): history list, status,
        # detail, fetch_results.
        if path == "/query_executions":
            return self._send_json(200, {
                "queryExecutions": [_raw_exec(e) for e in self.execs.list_newest_first()]
            })
        if path.startswith("/query_executions/"):
            uuid, action = self._parse_exec_path(path)
            ex = self.execs.get(uuid)
            if ex is None:
                return self._send_json(404, {"detail": f"Query {uuid} not found"})
            if action == "fetch_results":
                q = urllib.parse.parse_qs(self.path.split("?", 1)[1]
                                          if "?" in self.path else "")
                limit = int((q.get("limit") or ["0"])[0] or 0)
                offset = int((q.get("offset") or ["0"])[0] or 0)
                rows = ex["_rows"][offset:(offset + limit) if limit else None]
                return self._send_json(200, {
                    "queryUuid": uuid,
                    "columnNames": ex["_columns"],
                    "rows": [{"values": r} for r in rows],
                    "totalFilteredRows": len(ex["_rows"]),
                })
            # action in {"", "status"} -> the RawQueryExecution.
            return self._send_json(200, _raw_exec(ex))
        # Everything else is a static UI asset (when --ui-dist is configured).
        if self.ui_dist:
            self._serve_static(path)
            return
        if path == "/":
            self._send_json(200, {"ok": True, "hint": "POST /execute_bigtrace_query or /query"})
            return
        self._send_json(404, {"error": f"unknown path {path}"})

    def _serve_static(self, path):
        rel = path.lstrip("/")
        if rel == "":
            rel = "bigtrace.html"
        root = os.path.abspath(self.ui_dist)
        full = os.path.normpath(os.path.join(root, rel))
        if not (full == root or full.startswith(root + os.sep)):
            self._send_json(403, {"error": "forbidden"})
            return
        if not os.path.isfile(full):
            self._send_json(404, {"error": f"not found: {rel}"})
            return
        with open(full, "rb") as f:
            data = f.read()
        ctype = _MIME.get(os.path.splitext(full)[1].lower(), "application/octet-stream")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    # ---- POST --------------------------------------------------------------

    @staticmethod
    def _parse_exec_path(path):
        """/query_executions/{uuid}[:action] -> (uuid, action)."""
        rest = path[len("/query_executions/"):]
        if ":" in rest:
            uuid, action = rest.split(":", 1)
        else:
            uuid, action = rest, ""
        return uuid, action

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        try:
            if path == "/execute_bigtrace_query":
                return self._handle_execute(materialized=False)
            if path == "/execute_bigtrace_query_async":
                return self._handle_execute(materialized=True)
            if path == "/bigtrace_execution_config":
                # No server-side execution filters to advertise.
                return self._send_json(200, {"setting": []})
            if path == "/trace_metadata_settings":
                # No metadata-derived filters either.
                return self._send_json(200, {"setting": []})
            if path == "/query":
                return self._handle_query()
            if path.startswith("/query_executions/") and path.endswith(":cancel"):
                uuid, _ = self._parse_exec_path(path)
                ex = self.execs.get(uuid)
                if ex is not None and ex["status"] == "IN_PROGRESS":
                    ex["status"] = "CANCELLED"
                return self._send_json(200, {})
        except (ValueError, json.JSONDecodeError) as e:
            return self._send_json(400, {"error": f"bad JSON body: {e}"})
        self._send_json(404, {"error": f"unknown path {path}"})

    def do_DELETE(self):
        path = self.path.split("?", 1)[0]
        if path.startswith("/query_executions/"):
            uuid, _ = self._parse_exec_path(path)
            self.execs.delete(uuid)
            return self._send_json(200, {})
        self._send_json(404, {"error": f"unknown path {path}"})

    def _handle_execute(self, materialized):
        """Bigtrace UI endpoint: {limit, perfetto_sql, settings} -> table.

        Runs the query against tpd now, records the execution (so the
        query-executions API can report it terminal + page results), and
        returns the result page. `/execute_bigtrace_query_async` lands here
        too — we execute synchronously, so the UI's poll sees SUCCESS at once.
        """
        req = self._read_json_body()
        sql = req.get("perfetto_sql") or req.get("sql_query") or req.get("sql")
        if not sql or not isinstance(sql, str):
            return self._send_json(400, {"error": "field 'perfetto_sql' required"})
        limit = int(req.get("limit", 0) or 0)
        try:
            columns, rows, errors = execute_bigtrace_query(self.client, sql, limit)
        except TpdError as e:
            return self._send_json(400, {"detail": str(e)})
        if not rows and errors:
            # Nothing succeeded — surface the failure so the UI shows it.
            msg = "; ".join(f"[{t}] {m}" for t, m in errors)
            return self._send_json(400, {"detail": msg})
        ex = make_execution(sql, columns, rows, errors, materialized)
        self.execs.put(ex)
        # queryUuid is required by the upstream/main Bigtrace UI's runSync
        # (it throws "Backend did not return a queryUuid" otherwise) and keys
        # the query-executions API. Harmless extra field for the older
        # flame-branch UI.
        out = {"queryUuid": ex["queryUuid"],
               "columnNames": columns,
               "rows": [{"values": r} for r in rows]}
        if errors:
            out["partialErrors"] = [{"trace": t, "error": m} for t, m in errors]
        self._send_json(200, out)

    def _handle_query(self):
        """Adapter-native endpoint mirroring Bigtrace's per-trace shape."""
        req = self._read_json_body()
        traces = req.get("traces")
        sql = req.get("sql_query") or req.get("sql")
        if not isinstance(traces, list) or not traces:
            return self._send_json(400, {"error": "field 'traces' must be a non-empty list"})
        if not sql or not isinstance(sql, str):
            return self._send_json(400, {"error": "field 'sql_query' must be a non-empty string"})
        max_rows = int(req.get("max_rows", self.max_rows) or 0)
        responses = []
        futures = [
            self.pool.submit(query_one_trace, self.client, addr, sql, max_rows)
            for addr in traces
        ]
        for fut in futures:
            responses.extend(fut.result())
        any_trace_err = any(r.get("error") for r in responses)
        self._send_json(207 if any_trace_err else 200, {"responses": responses})


def make_server(listen_host, listen_port, sock_path, timeout, max_workers,
                max_rows, ui_dist):
    client = TpdClient(sock_path, timeout=timeout)
    pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="tpd-q")

    class BoundHandler(Handler):
        pass

    BoundHandler.client = client
    BoundHandler.pool = pool
    BoundHandler.max_rows = max_rows
    BoundHandler.ui_dist = ui_dist
    BoundHandler.execs = ExecStore()

    httpd = ThreadingHTTPServer((listen_host, listen_port), BoundHandler)
    httpd.daemon_threads = True
    return httpd, client, pool


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tpd-socket", required=True,
                    help="path to the tpd_server unix socket "
                         "(e.g. $HOME/.tpd/session/sock)")
    ap.add_argument("--listen-host", default="127.0.0.1")
    ap.add_argument("--listen-port", type=int, default=5051,
                    help="HTTP port to serve on (default 5051, Bigtrace's port)")
    ap.add_argument("--timeout", type=float, default=120.0,
                    help="per-query tpd timeout in seconds")
    ap.add_argument("--max-workers", type=int, default=8,
                    help="concurrent per-trace tpd queries")
    ap.add_argument("--max-rows", type=int, default=0,
                    help="default cumulative row cap per trace (0 = unlimited)")
    ap.add_argument("--ui-dist", default=None,
                    help="serve the built Bigtrace UI from this dist dir on the "
                         "same origin (so the UI's connect-src 'self' CSP lets "
                         "it reach the API). Open /bigtrace.html and set the UI "
                         "backend endpoint to '' (empty).")
    args = ap.parse_args(argv)

    sock_path = os.path.expanduser(args.tpd_socket)
    if not os.path.exists(sock_path):
        sys.stderr.write(
            f"[adapter] warning: tpd socket {sock_path} not present yet; "
            "will connect lazily per request\n")

    ui_dist = os.path.abspath(os.path.expanduser(args.ui_dist)) if args.ui_dist else None
    if ui_dist and not os.path.isdir(ui_dist):
        sys.stderr.write(f"[adapter] warning: --ui-dist {ui_dist} is not a directory\n")

    httpd, client, pool = make_server(
        args.listen_host, args.listen_port, sock_path,
        args.timeout, args.max_workers, args.max_rows, ui_dist)

    # Best-effort startup probe so operators see tpd reachability immediately.
    try:
        client.ping()
        sys.stderr.write(f"[adapter] connected to tpd at {sock_path}\n")
    except TpdError as e:
        sys.stderr.write(f"[adapter] tpd not reachable yet: {e}\n")

    sys.stderr.write(
        f"[adapter] serving Bigtrace HTTP on "
        f"http://{args.listen_host}:{args.listen_port} -> tpd {sock_path}\n")
    if ui_dist:
        sys.stderr.write(
            f"[adapter] serving Bigtrace UI from {ui_dist} "
            f"(open http://{args.listen_host}:{args.listen_port}/bigtrace.html)\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
