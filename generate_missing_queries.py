#!/usr/bin/env python3
"""Generate a review-only esgpull command plan for C375 rows not yet configured."""

from __future__ import annotations

import argparse
import csv
import shlex
import sqlite3
import urllib.parse
from collections import defaultdict
from pathlib import Path

from c375_dashboard import EXPERIMENTS, TAG_TO_CENTRE, build_snapshot, load_requirements, read_esgpull, _normal


def query_options(db_path: Path) -> dict[str, dict[str, str]]:
    uri = urllib.parse.quote(str(db_path.resolve()), safe="/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        select q.sha, o.distrib, o.latest, o.replica, o.retracted
        from query q join options o on o.sha = q.options_sha
    """)
    result = {row["sha"]: dict(row) for row in rows}
    conn.close()
    return result


def command(tags: set[str], facets: dict[str, set[str]], options: dict[str, str]) -> str:
    parts = ["esgpull", "add", "--track"]
    for tag in sorted(tags):
        parts += ["--tag", tag]
    for name in ("distrib", "latest", "replica", "retracted"):
        value = options.get(name)
        if value:
            parts += [f"--{name}", value]
    for name, values in sorted(facets.items()):
        parts.append(f"{name}:{','.join(sorted(values))}")
    return " ".join(shlex.quote(part) for part in parts)


def generate(requirements_path: Path, db_path: Path) -> list[dict[str, str]]:
    requirements = load_requirements(requirements_path)
    rows = build_snapshot(requirements, db_path)
    missing = [row for row in rows if row["replication_state"] == "Not configured"]
    selections, tags_by_sha, _ = read_esgpull(db_path)
    options_by_sha = query_options(db_path)

    templates: dict[tuple, tuple[set[str], dict[str, set[str]], dict[str, str]]] = {}
    for sha, facets in selections.items():
        centres = {
            TAG_TO_CENTRE[_normal(tag)] for tag in tags_by_sha.get(sha, set())
            if _normal(tag) in TAG_TO_CENTRE
        }
        experiments = facets.get("experiment_id", set())
        if len(centres) != 1 or len(experiments) != 1:
            continue
        centre = next(iter(centres))
        experiment = next(iter(experiments))
        base = {name: set(values) for name, values in facets.items() if name != "sub_experiment_id"}
        signature = (centre, experiment, tuple((name, tuple(sorted(vals))) for name, vals in sorted(base.items())))
        templates[signature] = (set(tags_by_sha.get(sha, set())), base, options_by_sha.get(sha, {}))

    years: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in missing:
        experiment = EXPERIMENTS.get(row["experiment_group"].lower())
        if experiment:
            years[(row["centre"], experiment)].add(row["start_date"])

    plans: list[dict[str, str]] = []
    for signature, (tags, base, options) in sorted(templates.items(), key=lambda item: str(item[0])):
        centre, experiment = signature[:2]
        missing_years = years.get((centre, experiment))
        if not missing_years:
            continue
        facets = {name: set(values) for name, values in base.items()}
        facets["sub_experiment_id"] = set(missing_years)
        plans.append({
            "centre": centre,
            "experiment_id": experiment,
            "start_dates": ",".join(sorted(missing_years)),
            "command": command(tags, facets, options),
        })
    return plans


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="Use a backup snapshot, not the live database")
    parser.add_argument("--requirements", type=Path, default=Path("work/c375_requirements.csv"))
    parser.add_argument("--output", type=Path, default=Path("outputs/c375_missing_query_plan.csv"))
    args = parser.parse_args()
    plans = generate(args.requirements, args.db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=("centre", "experiment_id", "start_dates", "command"))
        writer.writeheader()
        writer.writerows(plans)
    print(f"Wrote {len(plans)} review-only query commands to {args.output}")
    print("No esgpull queries were created or tracked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
