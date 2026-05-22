#!/usr/bin/env python3
"""Tiny assertion helper for e2e_test.sh. Usage: _check.py <case> <resp.json>."""
import json
import sys


def main():
    case, path = sys.argv[1], sys.argv[2]
    resp = json.load(open(path))
    rs = resp["responses"]

    if case == "cross":
        assert len(rs) == 2, f"expected 2 per-trace responses, got {len(rs)}: {rs}"
        seen = {}
        for r in rs:
            assert r["error"] is None, f"{r['trace']} errored: {r['error']}"
            assert r["columns"] == ["n"], f"bad columns: {r['columns']}"
            assert len(r["rows"]) == 1, f"bad row count: {r['rows']}"
            n = r["rows"][0][0]
            assert isinstance(n, int) and n > 0, f"bad thread count {n} for {r['trace']}"
            seen[r["trace"]] = n
        assert set(seen) == {"cf30.pftrace", "cf60.pftrace"}, seen
        print("PASS: thread counts =", seen)

    elif case == "multicol":
        assert len(rs) == 1, rs
        r = rs[0]
        assert r["error"] is None, r["error"]
        assert r["columns"] == ["tid", "name"], r["columns"]
        assert len(r["rows"]) >= 1, r["rows"]
        for tid, name in r["rows"]:
            assert isinstance(tid, int), f"tid not int: {tid!r}"
            assert name is None or isinstance(name, str), f"name type: {name!r}"
        print("PASS: rows =", r["rows"])

    elif case == "error":
        assert len(rs) == 1, rs
        assert rs[0]["error"], "expected an error for bad SQL"
        print("PASS: error surfaced:", rs[0]["error"][:80])

    elif case == "unknown":
        assert len(rs) == 1, rs
        assert rs[0]["error"] and "no tpd trace matched" in rs[0]["error"], rs[0]
        print("PASS: unknown-trace error:", rs[0]["error"][:80])

    else:
        print("unknown case", case, file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
