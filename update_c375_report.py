#!/usr/bin/env python3
"""Safely snapshot a live esgpull SQLite database and refresh the C375 Grist report."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import c375_dashboard
import grist_sync


def sqlite_backup(source: Path, backup_dir: Path, keep: int) -> Path:
    """Create and validate a transactionally consistent SQLite backup."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    final = backup_dir / f"c375-{stamp}.db"
    temporary = backup_dir / f".{final.name}.partial"
    if temporary.exists():
        temporary.unlink()

    encoded_source = urllib.parse.quote(str(source.resolve()), safe="/")
    source_uri = f"file:{encoded_source}?mode=ro"
    src = sqlite3.connect(source_uri, uri=True, timeout=60)
    dst = sqlite3.connect(temporary)
    try:
        src.execute("PRAGMA busy_timeout=60000")
        src.backup(dst, pages=1000, sleep=0.1)
    finally:
        dst.close()
        src.close()

    check = sqlite3.connect(f"file:{temporary.resolve()}?mode=ro", uri=True)
    try:
        result = check.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        check.close()
    if result != "ok":
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"SQLite backup validation failed: {result}")
    temporary.replace(final)

    if keep > 0:
        backups = sorted(backup_dir.glob("c375-*.db"), reverse=True)
        for old in backups[keep:]:
            old.unlink()
    return final


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("/home/abennasser/c3s/db/c375.db"))
    parser.add_argument("--backup-dir", type=Path, default=Path("/home/abennasser/c3s/backups"))
    parser.add_argument("--keep-backups", type=int, default=14)
    parser.add_argument("--requirements", type=Path, default=Path("work/c375_requirements.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--refresh-sheet", action="store_true")
    parser.add_argument("--no-publish", action="store_true", help="Build files but do not update Grist")
    args = parser.parse_args(argv)

    backup = sqlite_backup(args.db, args.backup_dir, args.keep_backups)
    print(f"Validated SQLite backup: {backup}")
    output = args.output_dir / "c375_dashboard.csv"
    dashboard_args = ["--db", str(backup), "--requirements", str(args.requirements), "--output", str(output)]
    if args.refresh_sheet:
        dashboard_args.append("--refresh-sheet")
    if c375_dashboard.main(dashboard_args) != 0:
        return 1
    if args.no_publish:
        print("Report built; Grist publication skipped.")
        return 0

    required = ("GRIST_API_KEY", "GRIST_DOC_ID")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        parser.error("publishing requires " + ", ".join(missing))
    host = os.getenv("GRIST_HOST", "https://grist.numerique.gouv.fr")
    doc = os.environ["GRIST_DOC_ID"]
    jobs = (
        (output, "C375_Replication", "requirement_id"),
        (args.output_dir / "c375_institution_summary.csv", "Institution_Summary", "institution"),
        (args.output_dir / "c375_status_summary.csv", "Status_Summary", "status"),
    )
    for csv_path, table, key in jobs:
        rc = grist_sync.main([
            "--input", str(csv_path), "--host", host, "--doc", doc,
            "--table", table, "--key", key, "--apply",
        ])
        if rc:
            return rc
    print("C375 detail and dashboard summaries refreshed in Grist.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
