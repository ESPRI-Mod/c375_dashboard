#!/usr/bin/env python3
"""Synchronize a generated C375 dashboard CSV with an existing Grist table."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

INTEGER_FIELDS = {
    "query_count", "file_total", "file_done", "file_error", "planning_rows",
    "complete_rows", "in_progress_rows", "error_rows", "no_match_rows",
    "not_configured_rows", "row_count",
}
FLOAT_FIELDS = {"completion", "downloaded_tib", "total_tib"}


def human_size(value: str | int | float) -> str:
    size = float(value or 0)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    unit = units[0]
    for candidate in units:
        unit = candidate
        if abs(size) < 1024 or candidate == units[-1]:
            break
        size /= 1024
    precision = 0 if unit == "B" else 1
    return f"{size:.{precision}f} {unit}"


def typed_row(row: dict[str, str]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in row.items():
        if key in {"bytes_total", "bytes_done"}:
            remote_key = "Total_Size" if key == "bytes_total" else "Downloaded_Size"
            result[remote_key] = human_size(value)
        elif key in INTEGER_FIELDS:
            result[key] = int(value or 0)
        elif key in FLOAT_FIELDS:
            result[key] = float(value or 0)
        else:
            result[key] = value
    return result


def payload(rows: list[dict[str, object]], key: str = "requirement_id") -> dict:
    return {
        "records": [
            {
                "require": {key: row[key]},
                "fields": {name: value for name, value in row.items() if name != key},
            }
            for row in rows
        ]
    }


def put_batch(host: str, doc: str, table: str, token: str, body: dict) -> None:
    host = host.rstrip("/")
    path = "/api/docs/{}/tables/{}/records?onmany=all".format(
        urllib.parse.quote(doc, safe=""), urllib.parse.quote(table, safe="")
    )
    request = urllib.request.Request(
        host + path,
        data=json.dumps(body).encode("utf-8"),
        method="PUT",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(f"Grist returned HTTP {response.status}")


def fetch_records(host: str, doc: str, table: str, token: str) -> list[dict]:
    host = host.rstrip("/")
    path = "/api/docs/{}/tables/{}/records".format(
        urllib.parse.quote(doc, safe=""), urllib.parse.quote(table, safe="")
    )
    request = urllib.request.Request(
        host + path,
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(f"Grist returned HTTP {response.status}")
        result = json.load(response)
    return result.get("records", [])


def delete_batch(
    host: str, doc: str, table: str, token: str, record_ids: list[int]
) -> None:
    host = host.rstrip("/")
    path = "/api/docs/{}/tables/{}/records/delete".format(
        urllib.parse.quote(doc, safe=""), urllib.parse.quote(table, safe="")
    )
    request = urllib.request.Request(
        host + path,
        data=json.dumps(record_ids).encode("utf-8"),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(f"Grist returned HTTP {response.status}")


def prune_records(
    host: str,
    doc: str,
    table: str,
    token: str,
    key: str,
    incoming_keys: set[object],
    batch_size: int,
) -> int:
    """Remove stale rows and duplicate keys after successful upserts."""
    seen: set[object] = set()
    record_ids: list[int] = []
    for record in fetch_records(host, doc, table, token):
        value = record.get("fields", {}).get(key)
        if value not in incoming_keys or value in seen:
            record_ids.append(int(record["id"]))
        else:
            seen.add(value)
    for start in range(0, len(record_ids), batch_size):
        delete_batch(
            host, doc, table, token, record_ids[start:start + batch_size]
        )
    return len(record_ids)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("outputs/c375_dashboard.csv"))
    parser.add_argument("--host", default=os.getenv("GRIST_HOST", "https://docs.getgrist.com"))
    parser.add_argument("--doc", default=os.getenv("GRIST_DOC_ID"))
    parser.add_argument("--table", default=os.getenv("GRIST_TABLE_ID", "C375_Replication"))
    parser.add_argument("--key", default="requirement_id", help="Stable key column used for upserts")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--apply", action="store_true", help="Actually send the upserts")
    parser.add_argument(
        "--prune",
        action="store_true",
        help="After successful upserts, delete stale rows and duplicate keys",
    )
    args = parser.parse_args(argv)
    with args.input.open(newline="", encoding="utf-8") as handle:
        rows = [typed_row(row) for row in csv.DictReader(handle)]
    if args.prune and not rows:
        parser.error("refusing to prune a Grist table from an empty input file")
    keys = [row.get(args.key) for row in rows]
    if any(value in (None, "") for value in keys):
        parser.error(f"input contains an empty {args.key!r} value")
    if len(set(keys)) != len(keys):
        parser.error(f"input contains duplicate {args.key!r} values")
    if not args.apply:
        preview = payload(rows[: min(2, len(rows))], args.key)
        print(json.dumps(preview, indent=2))
        print(f"Dry run: {len(rows)} rows would be upserted into {args.table}.")
        if args.prune:
            print("Dry run: stale rows and duplicate keys would then be removed.")
        return 0
    token = os.getenv("GRIST_API_KEY")
    if not args.doc or not token:
        parser.error("--apply requires GRIST_DOC_ID/--doc and GRIST_API_KEY")
    try:
        for start in range(0, len(rows), args.batch_size):
            put_batch(
                args.host, args.doc, args.table, token,
                payload(rows[start:start + args.batch_size], args.key),
            )
        pruned = 0
        if args.prune:
            pruned = prune_records(
                args.host, args.doc, args.table, token, args.key,
                set(keys), args.batch_size,
            )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"Grist HTTP {exc.code}: {detail}", file=sys.stderr)
        return 1
    print(f"Upserted {len(rows)} rows into {args.table}.")
    if args.prune:
        print(f"Removed {pruned} stale or duplicate rows from {args.table}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
