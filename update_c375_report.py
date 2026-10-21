#!/usr/bin/env python3
"""Safely snapshot ESGpull and publish a manifest-authoritative Grist report."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import grist_sync
import manifest_dashboard


def sqlite_backup(source: Path, backup_dir: Path, keep: int) -> Path:
    """Create and validate a transactionally consistent SQLite backup."""
    backup_dir.mkdir(parents=True, exist_ok=True)

    # A killed/failed run can leave the temporary database or SQLite journal
    # sidecars behind.  Never touch recent files because another invocation may
    # still be using them; artifacts older than one day are abandoned.
    cutoff = time.time() - 86400
    for artifact in backup_dir.glob(".c375-*.db.partial*"):
        if artifact.stat().st_mtime < cutoff:
            artifact.unlink()

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

    # sqlite_backup may copy WAL persistence from the source.  Convert the
    # completed snapshot to DELETE journal mode so it is a single portable
    # file and no .partial-wal/.partial-shm files survive the rename.
    normalize = sqlite3.connect(temporary)
    try:
        normalize.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        normalize.execute("PRAGMA journal_mode=DELETE")
    finally:
        normalize.close()

    check = sqlite3.connect(
        f"file:{temporary.resolve()}?mode=ro&immutable=1", uri=True
    )
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
    parser.add_argument(
        "--manifest-state",
        type=Path,
        default=Path("outputs/manifest_state.sqlite"),
        help="Checkpoint maintained by manifest_esgpull.py",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--no-publish", action="store_true", help="Build files but do not update Grist")
    args = parser.parse_args(argv)

    backup = sqlite_backup(args.db, args.backup_dir, args.keep_backups)
    print(f"Validated SQLite backup: {backup}")
    if manifest_dashboard.main([
        "--db", str(backup),
        "--state", str(args.manifest_state),
        "--output-dir", str(args.output_dir),
    ]) != 0:
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
        (args.output_dir / "manifest_status_summary.csv", "Manifest_Status_Summary", "status"),
        (args.output_dir / "manifest_host_summary.csv", "Manifest_Host_Summary", "hostname"),
        (args.output_dir / "manifest_failure_summary.csv", "Manifest_Failure_Summary", "failure_id"),
    )
    for csv_path, table, key in jobs:
        rc = grist_sync.main([
            "--input", str(csv_path), "--host", host, "--doc", doc,
            "--table", table, "--key", key, "--apply", "--prune",
        ])
        if rc:
            return rc
    print("Manifest URL dashboard summaries refreshed in Grist.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
