#!/usr/bin/env python3
"""Build a C3S2-375 replication dashboard snapshot from an esgpull database.

The Google Sheet is the reporting model; esgpull queries are execution units.
This collector maps every sheet row to any compatible query and derives metrics
from the linked files, deduplicated by file SHA.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

SHEET_ID = "1PtIBS5rI85ebiylIgNQVXhJyZbVmFdnM0EZfP0RsJaI"
SHEETS = {
    "CMCC": "CMCC",
    "BSC": "BSC",
    "UiB/NERSC": "UiB/NERSC",
    "DWD": "DWD",
    "MOHC": "MOHC",
}
CENTRES = tuple(SHEETS)
TAG_TO_CENTRE = {
    "test": "CMCC",  # Name used by the current C375 database.
    "bsc": "BSC",
    "uib": "UiB/NERSC",
    "dwd": "DWD",
    "mohc": "MOHC",
}
EXPERIMENTS = {
    "dcpp-a": "dcppA-hindcast",
    "dcpp-b": "dcppB-forecast",
}

HISTORY_FIELDS = (
    "history_id",
    "snapshot_time",
    "institution",
    "downloaded_tib",
    "total_tib",
    "remaining_tib",
    "downloaded_delta_tib",
    "elapsed_hours",
    "daily_rate_tib_day",
    "daily_rate_mib_s",
    "weekly_rate_tib_day",
    "weekly_rate_mib_s",
    "eta_days",
    "estimated_completion",
    "rate_basis",
)

TIB_PER_DAY_TO_MIB_PER_SECOND = 2**20 / 86400


@dataclass(frozen=True)
class Requirement:
    requirement_id: str
    centre: str
    start_date: str
    experiment_group: str
    project_label: str
    esgf_node: str
    cat1_state: str
    cat1_variables: str
    cat2_state: str
    cat2_variables: str
    notes: str


def _clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def _stable_id(*parts: str) -> str:
    return hashlib.sha1("\x1f".join(parts).encode()).hexdigest()[:12]


def _sheet_url(sheet: str) -> str:
    query = urllib.parse.urlencode({"tqx": "out:csv", "sheet": sheet})
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?{query}"


def fetch_requirements(output: Path) -> list[Requirement]:
    rows: list[Requirement] = []
    for centre, sheet_name in SHEETS.items():
        with urllib.request.urlopen(_sheet_url(sheet_name), timeout=30) as response:
            text = response.read().decode("utf-8-sig")
        parsed = list(csv.reader(text.splitlines()))
        header_index = next(
            (i for i, row in enumerate(parsed) if row and _clean(row[0]) == "Start date"),
            None,
        )
        if header_index is None:
            continue
        for source_row in parsed[header_index + 1 :]:
            values = (source_row + [""] * 9)[:9]
            if not _clean(values[0]):
                continue
            start = _clean(values[0])
            req_id = _stable_id(centre, start, _clean(values[1]), _clean(values[2]))
            rows.append(
                Requirement(
                    req_id,
                    centre,
                    start,
                    _clean(values[1]),
                    _clean(values[2]),
                    _clean(values[3]),
                    _clean(values[4]),
                    _clean(values[5]),
                    _clean(values[6]),
                    _clean(values[7]),
                    _clean(values[8]),
                )
            )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(rows[0]).keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(asdict(row) for row in rows)
    return rows


def load_requirements(path: Path) -> list[Requirement]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [Requirement(**row) for row in csv.DictReader(handle)]


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("select name from sqlite_master where type='table'")}


def read_esgpull(db_path: Path) -> tuple[dict[str, dict[str, set[str]]], dict[str, set[str]], dict[str, dict]]:
    # Dashboard inputs are offline backup snapshots. immutable=1 prevents
    # SQLite from attempting journal/WAL sidecar writes beside the source DB.
    uri_path = urllib.parse.quote(str(db_path.resolve()), safe="/")
    conn = sqlite3.connect(f"file:{uri_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    required = {"query", "selection_facet", "facet", "query_file", "file"}
    missing = required - _tables(conn)
    if missing:
        raise RuntimeError(f"Not an esgpull database; missing tables: {', '.join(sorted(missing))}")

    selections: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    sql = """
      select q.sha query_sha, f.name, f.value
      from query q
      join selection_facet sf on sf.selection_sha = q.selection_sha
      join facet f on f.sha = sf.facet_sha
    """
    for row in conn.execute(sql):
        selections[row["query_sha"]][row["name"]].add(row["value"])

    tags: dict[str, set[str]] = defaultdict(set)
    if {"query_tag", "tag"} <= _tables(conn):
        for row in conn.execute("select qt.query_sha, t.name from query_tag qt join tag t on t.sha=qt.tag_sha"):
            tags[row["query_sha"]].add(row["name"])

    # Only query-linked files are dashboard inputs.  esgpull intentionally
    # retains orphan file records after a query is removed; those records may
    # still describe files on disk, but they are outside the current
    # replication plan and must not affect report counts or sizes.
    files: dict[str, dict] = {}
    linked_files_sql = """
      select f.*, qf.query_sha
      from file f
      join query_file qf on qf.file_sha = f.sha
    """
    for row in conn.execute(linked_files_sql):
        item = files.get(row["sha"])
        if item is None:
            item = {name: row[name] for name in row.keys() if name != "query_sha"}
            item["query_shas"] = set()
            files[item["sha"]] = item
        item["query_shas"].add(row["query_sha"])
    conn.close()
    return selections, tags, files


def _normal(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _query_matches(req: Requirement, facets: dict[str, set[str]], tags: set[str]) -> bool:
    starts = facets.get("sub_experiment_id", set())
    if starts and req.start_date not in starts:
        return False
    tagged_centres = {TAG_TO_CENTRE[_normal(tag)] for tag in tags if _normal(tag) in TAG_TO_CENTRE}
    if tagged_centres and req.centre not in tagged_centres:
        return False
    projects = {_normal(v) for v in facets.get("project", set())}
    if projects and _normal(req.project_label) not in projects:
        # Sheet says "CMIP6 Plus" while the facet is commonly "CMIP6Plus".
        return False
    expected_experiment = EXPERIMENTS.get(req.experiment_group.lower())
    experiments = facets.get("experiment_id", set())
    if expected_experiment and experiments and expected_experiment not in experiments:
        return False
    return True


def _status(value: object) -> str:
    return _clean(value).lower().split(".")[-1]


def _file_matches_requirement(file: dict, req: Requirement) -> bool:
    identity = " ".join(
        _clean(file.get(name)) for name in ("dataset_id", "file_id", "filename", "master_id")
    )
    if not identity:
        return True  # Supports old/minimal schemas; query matching remains useful.
    start = req.start_date
    markers = (f".{start}-", f"_{start}-", f".{start}.", f"_{start}_")
    return any(marker in identity for marker in markers)


def build_snapshot(requirements: list[Requirement], db_path: Path) -> list[dict]:
    selections, tags, files = read_esgpull(db_path)
    files_by_start: dict[str, list[dict]] = defaultdict(list)
    for file in files.values():
        identity = " ".join(
            _clean(file.get(name)) for name in ("dataset_id", "file_id", "filename", "master_id")
        )
        starts = set(re.findall(r"(?:^|[._])(s\d{4})(?=[-._])", identity))
        if not starts:
            files_by_start[""].append(file)
        for start in starts:
            files_by_start[start].append(file)
    result: list[dict] = []
    for req in requirements:
        matched_queries = {
            sha for sha, facets in selections.items() if _query_matches(req, facets, tags.get(sha, set()))
        }
        candidates = files_by_start.get(req.start_date, []) + files_by_start.get("", [])
        matched_files = [
            f
            for f in candidates
            if f["query_shas"] & matched_queries and _file_matches_requirement(f, req)
        ]
        done = [f for f in matched_files if _status(f.get("status")) == "done"]
        errors = [f for f in matched_files if _status(f.get("status")) in {"error", "cancelled"}]
        total_bytes = sum(int(f.get("size") or 0) for f in matched_files)
        done_bytes = sum(int(f.get("size") or 0) for f in done)
        if not matched_queries:
            state = "Not configured"
        elif not matched_files:
            state = "No ESGF match"
        elif errors:
            state = "Error"
        elif len(done) == len(matched_files):
            state = "Complete"
        elif done:
            state = "In progress"
        else:
            state = "Available"
        row = asdict(req)
        row.update(
            query_count=len(matched_queries),
            query_shas=", ".join(sorted(matched_queries)),
            file_total=len(matched_files),
            file_done=len(done),
            file_error=len(errors),
            bytes_total=total_bytes,
            bytes_done=done_bytes,
            completion=(done_bytes / total_bytes if total_bytes else 0.0),
            replication_state=state,
        )
        result.append(row)
    return result


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _rate(delta_tib: float, elapsed: timedelta) -> float | None:
    days = elapsed.total_seconds() / 86400
    if days <= 0 or delta_tib < 0:
        return None
    return delta_tib / days


def update_history(
    summary_rows: list[dict],
    history_path: Path,
    snapshot_time: datetime,
    retention_days: int = 90,
) -> tuple[dict[str, dict], list[dict]]:
    """Append rate snapshots and return the latest metrics by institution."""
    snapshot_time = snapshot_time.astimezone(timezone.utc)
    existing: list[dict] = []
    if history_path.exists():
        with history_path.open(newline="", encoding="utf-8-sig") as handle:
            existing = list(csv.DictReader(handle))

    cutoff = snapshot_time - timedelta(days=retention_days)
    valid_existing = []
    for row in existing:
        try:
            when = _parse_time(row["snapshot_time"])
        except (KeyError, TypeError, ValueError):
            continue
        if cutoff <= when < snapshot_time:
            valid_existing.append(row)

    totals = list(summary_rows)
    totals.append({
        "institution": "ALL",
        "downloaded_tib": sum(float(row["downloaded_tib"]) for row in summary_rows),
        "total_tib": sum(float(row["total_tib"]) for row in summary_rows),
    })

    latest_metrics: dict[str, dict] = {}
    new_rows: list[dict] = []
    timestamp = snapshot_time.isoformat().replace("+00:00", "Z")
    weekly_target = snapshot_time - timedelta(days=7)
    for summary in totals:
        institution = str(summary["institution"])
        downloaded = float(summary["downloaded_tib"])
        total = float(summary["total_tib"])
        remaining = max(total - downloaded, 0.0)
        prior = [
            row for row in valid_existing
            if row.get("institution") == institution
        ]
        prior.sort(key=lambda row: _parse_time(row["snapshot_time"]))
        previous = prior[-1] if prior else None
        last_reset = max(
            (
                index for index, row in enumerate(prior)
                if row.get("rate_basis") == "Baseline reset"
            ),
            default=0,
        )
        weekly_prior = prior[last_reset:]
        weekly_candidates = [
            row for row in weekly_prior
            if _parse_time(row["snapshot_time"]) <= weekly_target
        ]
        weekly_previous = weekly_candidates[-1] if weekly_candidates else None

        daily_rate = None
        downloaded_delta = None
        elapsed_hours = None
        if previous is not None:
            previous_time = _parse_time(previous["snapshot_time"])
            elapsed = snapshot_time - previous_time
            downloaded_delta = downloaded - float(previous["downloaded_tib"])
            elapsed_hours = elapsed.total_seconds() / 3600
            daily_rate = _rate(downloaded_delta, elapsed)

        weekly_rate = None
        if weekly_previous is not None and (downloaded_delta is None or downloaded_delta >= 0):
            weekly_elapsed = snapshot_time - _parse_time(weekly_previous["snapshot_time"])
            weekly_delta = downloaded - float(weekly_previous["downloaded_tib"])
            weekly_rate = _rate(weekly_delta, weekly_elapsed)

        preferred_rate = weekly_rate if weekly_rate is not None and weekly_rate > 0 else daily_rate
        if remaining <= 1e-9:
            eta_days: float | None = 0.0
            estimated_completion = timestamp
            rate_basis = "Complete"
        elif preferred_rate is not None and preferred_rate > 0:
            eta_days = remaining / preferred_rate
            estimated_completion = (
                snapshot_time + timedelta(days=eta_days)
            ).isoformat().replace("+00:00", "Z")
            rate_basis = "Weekly" if weekly_rate is not None and weekly_rate > 0 else "Daily"
        else:
            eta_days = None
            estimated_completion = ""
            if previous is None:
                rate_basis = "Establishing baseline"
            elif downloaded_delta is not None and downloaded_delta < 0:
                rate_basis = "Baseline reset"
            else:
                rate_basis = "Stalled"

        metrics = {
            "snapshot_time": timestamp,
            "downloaded_delta_tib": downloaded_delta,
            "elapsed_hours": elapsed_hours,
            "daily_rate_tib_day": daily_rate,
            "daily_rate_mib_s": (
                daily_rate * TIB_PER_DAY_TO_MIB_PER_SECOND
                if daily_rate is not None else None
            ),
            "weekly_rate_tib_day": weekly_rate,
            "weekly_rate_mib_s": (
                weekly_rate * TIB_PER_DAY_TO_MIB_PER_SECOND
                if weekly_rate is not None else None
            ),
            "eta_days": eta_days,
            "estimated_completion": estimated_completion,
            "rate_basis": rate_basis,
        }
        latest_metrics[institution] = metrics
        new_rows.append({
            "history_id": f"{timestamp}|{institution}",
            "snapshot_time": timestamp,
            "institution": institution,
            "downloaded_tib": downloaded,
            "total_tib": total,
            "remaining_tib": remaining,
            **metrics,
        })

    history_rows = valid_existing + new_rows
    history_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = history_path.with_suffix(history_path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDS)
        writer.writeheader()
        writer.writerows(history_rows)
    temporary.replace(history_path)
    return latest_metrics, history_rows


def write_snapshot(
    rows: list[dict],
    output: Path,
    snapshot_time: datetime | None = None,
    history_days: int = 90,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        if not rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    output.with_suffix(".json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    summary_path = output.with_name("c375_institution_summary.csv")
    states = (
        "Complete",
        "In progress",
        "Available",
        "Error",
        "No ESGF match",
        "Not configured",
    )
    summary_rows = []
    for centre in CENTRES:
        subset = [row for row in rows if row["centre"] == centre]
        total_bytes = sum(int(row["bytes_total"]) for row in subset)
        done_bytes = sum(int(row["bytes_done"]) for row in subset)
        summary = {
            "institution": centre,
            "planning_rows": len(subset),
            "complete_rows": sum(row["replication_state"] == "Complete" for row in subset),
            "in_progress_rows": sum(row["replication_state"] == "In progress" for row in subset),
            "error_rows": sum(row["replication_state"] == "Error" for row in subset),
            "no_match_rows": sum(row["replication_state"] == "No ESGF match" for row in subset),
            "not_configured_rows": sum(row["replication_state"] == "Not configured" for row in subset),
            "file_total": sum(int(row["file_total"]) for row in subset),
            "file_done": sum(int(row["file_done"]) for row in subset),
            "completion": done_bytes / total_bytes if total_bytes else 0.0,
            "downloaded_tib": done_bytes / 2**40,
            "total_tib": total_bytes / 2**40,
        }
        summary_rows.append(summary)

    history_path = output.with_name("c375_institution_history.csv")
    latest_metrics, _ = update_history(
        summary_rows,
        history_path,
        snapshot_time or datetime.now(timezone.utc),
        history_days,
    )
    for summary in summary_rows:
        summary.update(latest_metrics[summary["institution"]])
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    status_path = output.with_name("c375_status_summary.csv")
    status_rows = []
    for state in states:
        subset = [row for row in rows if row["replication_state"] == state]
        total_bytes = sum(int(row["bytes_total"]) for row in subset)
        downloaded_bytes = sum(int(row["bytes_done"]) for row in subset)
        status_rows.append({
            "status": state,
            "row_count": len(subset),
            "downloaded_tib": downloaded_bytes / 2**40,
            "remaining_tib": max(total_bytes - downloaded_bytes, 0) / 2**40,
            "total_tib": total_bytes / 2**40,
        })
    with status_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(status_rows[0]))
        writer.writeheader()
        writer.writerows(status_rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, help="Path to esgpull.db")
    parser.add_argument("--requirements", type=Path, default=Path("work/c375_requirements.csv"))
    parser.add_argument("--refresh-sheet", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("outputs/c375_dashboard.csv"))
    parser.add_argument("--history-days", type=int, default=90)
    args = parser.parse_args(argv)
    if args.refresh_sheet or not args.requirements.exists():
        requirements = fetch_requirements(args.requirements)
    else:
        requirements = load_requirements(args.requirements)
    if args.db is None:
        print(f"Captured {len(requirements)} planning rows in {args.requirements}; pass --db next.")
        return 0
    rows = build_snapshot(requirements, args.db)
    write_snapshot(rows, args.output, history_days=args.history_days)
    print(f"Wrote {len(rows)} dashboard rows to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
