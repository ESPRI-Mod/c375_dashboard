#!/usr/bin/env python3
"""Build compact dashboard tables from the authoritative manifest checkpoint."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

STATUSES = ("Downloaded", "To download", "Failed")


def _readonly(path: Path, immutable: bool = False) -> sqlite3.Connection:
    encoded = urllib.parse.quote(str(path.resolve()), safe="/")
    suffix = "&immutable=1" if immutable else ""
    conn = sqlite3.connect(f"file:{encoded}?mode=ro{suffix}", uri=True, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def _normal_status(value: object) -> str:
    return "" if value is None else str(value).lower().split(".")[-1]


def _file_index(db_path: Path) -> tuple[dict[str, dict], dict[str, dict]]:
    """Index ESGpull files by authoritative URL and file identity."""
    conn = _readonly(db_path, immutable=True)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(file)")}
    required = {"url", "file_id", "status", "size"}
    missing = required - columns
    if missing:
        raise RuntimeError(f"ESGpull file table is missing: {', '.join(sorted(missing))}")
    by_url: dict[str, dict] = {}
    by_id: dict[str, dict] = {}
    for row in conn.execute("SELECT url, file_id, status, size FROM file"):
        item = dict(row)
        by_url[str(row["url"])] = item
        by_id[str(row["file_id"])] = item
    conn.close()
    return by_url, by_id


def _metadata_file_id(value: object) -> str:
    if not value:
        return ""
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    return str(parsed.get("file_id") or "")


def classify(row: sqlite3.Row, file: dict | None) -> tuple[str, str]:
    """Return dashboard state and a compact reason.

    ``deferred`` is deliberately pending work even though its checkpoint error
    explains why it was deferred. A completed ESGpull file always wins over a
    stale checkpoint error from an earlier attempt.
    """
    checkpoint = _normal_status(row["status"])
    file_status = _normal_status(file.get("status")) if file else ""
    error = str(row["error"] or "").strip()
    if file_status == "done":
        return "Downloaded", ""
    if checkpoint == "deferred":
        return "To download", "Deferred"
    if checkpoint == "invalid":
        return "Failed", error or "Invalid manifest URL"
    if file_status in {"error", "cancelled"}:
        return "Failed", error or f"ESGpull status: {file_status}"
    # Metadata lookup is only an optimization. A pending URL remains valid
    # manifest work and can still be handled by the direct-download fallback.
    if checkpoint == "pending" and error.startswith("ESGF lookup failed"):
        return "To download", "Awaiting direct fallback"
    if error:
        return "Failed", error
    return "To download", "Unresolved" if checkpoint == "pending" else "Queued"


def build_rows(state_path: Path, db_path: Path) -> tuple[list[dict], list[dict], list[dict]]:
    by_url, by_id = _file_index(db_path)
    state = _readonly(state_path)
    columns = {row[1] for row in state.execute("PRAGMA table_info(manifest_url)")}
    required = {"url", "hostname", "status", "metadata_json", "error"}
    missing = required - columns
    if missing:
        raise RuntimeError(f"Manifest checkpoint is missing: {', '.join(sorted(missing))}")

    totals = Counter()
    host_totals: dict[str, Counter] = defaultdict(Counter)
    failures: Counter[tuple[str, str]] = Counter()
    failure_examples: dict[tuple[str, str], str] = {}
    bytes_by_status = Counter()
    for row in state.execute(
        "SELECT url, hostname, status, metadata_json, error FROM manifest_url"
    ):
        file = by_url.get(str(row["url"]))
        if file is None:
            file = by_id.get(_metadata_file_id(row["metadata_json"]))
        dashboard_status, reason = classify(row, file)
        hostname = str(row["hostname"] or "<missing>")
        size = int((file or {}).get("size") or 0)
        totals[dashboard_status] += 1
        host_totals[hostname][dashboard_status] += 1
        bytes_by_status[dashboard_status] += size
        if _normal_status(row["status"]) == "deferred":
            host_totals[hostname]["Deferred"] += 1
        if dashboard_status == "Failed":
            category = reason.split(":", 1)[0].strip() or "Unknown error"
            key = (hostname, category)
            failures[key] += 1
            failure_examples.setdefault(key, reason[:500])
    state.close()

    generated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    total_urls = sum(totals.values())
    status_rows = []
    for status in STATUSES:
        count = totals[status]
        status_rows.append({
            "status": status,
            "url_count": count,
            "percentage": count / total_urls if total_urls else 0.0,
            "known_tib": bytes_by_status[status] / 2**40,
            "snapshot_time": generated,
        })

    host_rows = []
    for hostname, counts in sorted(host_totals.items()):
        host_total = sum(counts[status] for status in STATUSES)
        host_rows.append({
            "hostname": hostname,
            "total_urls": host_total,
            "downloaded_urls": counts["Downloaded"],
            "to_download_urls": counts["To download"],
            "failed_urls": counts["Failed"],
            "deferred_urls": counts["Deferred"],
            "completion": counts["Downloaded"] / host_total if host_total else 0.0,
            "snapshot_time": generated,
        })

    failure_rows = []
    for (hostname, reason), count in sorted(
        failures.items(), key=lambda item: (-item[1], item[0])
    ):
        failure_rows.append({
            "failure_id": hashlib.sha1(f"{hostname}\0{reason}".encode()).hexdigest()[:16],
            "hostname": hostname,
            "failure_reason": reason,
            "example_error": failure_examples[(hostname, reason)],
            "url_count": count,
            "snapshot_time": generated,
        })
    return status_rows, host_rows, failure_rows


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path("outputs/manifest_state.sqlite"))
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    args = parser.parse_args(argv)
    status, hosts, failures = build_rows(args.state, args.db)
    write_csv(
        args.output_dir / "manifest_status_summary.csv", status,
        ["status", "url_count", "percentage", "known_tib", "snapshot_time"],
    )
    write_csv(
        args.output_dir / "manifest_host_summary.csv", hosts,
        ["hostname", "total_urls", "downloaded_urls", "to_download_urls",
         "failed_urls", "deferred_urls", "completion", "snapshot_time"],
    )
    write_csv(
        args.output_dir / "manifest_failure_summary.csv", failures,
        ["failure_id", "hostname", "failure_reason", "example_error",
         "url_count", "snapshot_time"],
    )
    print(
        "Manifest dashboard: "
        + ", ".join(f"{row['status']}={row['url_count']:,}" for row in status)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
