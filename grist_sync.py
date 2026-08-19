#!/usr/bin/env python3
"""Upsert a generated C375 dashboard CSV into an existing Grist table."""

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
    path = "/api/docs/{}/tables/{}/records?onmany=none".format(
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("outputs/c375_dashboard.csv"))
    parser.add_argument("--host", default=os.getenv("GRIST_HOST", "https://docs.getgrist.com"))
    parser.add_argument("--doc", default=os.getenv("GRIST_DOC_ID"))
    parser.add_argument("--table", default=os.getenv("GRIST_TABLE_ID", "C375_Replication"))
    parser.add_argument("--key", default="requirement_id", help="Stable key column used for upserts")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--apply", action="store_true", help="Actually send the upserts")
    args = parser.parse_args(argv)
    with args.input.open(newline="", encoding="utf-8") as handle:
        rows = [typed_row(row) for row in csv.DictReader(handle)]
    if not args.apply:
        preview = payload(rows[: min(2, len(rows))], args.key)
        print(json.dumps(preview, indent=2))
        print(f"Dry run: {len(rows)} rows would be upserted into {args.table}.")
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
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"Grist HTTP {exc.code}: {detail}", file=sys.stderr)
        return 1
    print(f"Upserted {len(rows)} rows into {args.table}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
