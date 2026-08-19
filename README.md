# C3S2-375 replication dashboard collector

This first-stage collector preserves the reporting rows from the original
planning workbook while treating `esgpull` queries as many-to-many execution
units. It reads `esgpull.db` in read-only mode and writes CSV and JSON snapshots
that are ready to import or upsert into Grist.

## Run

```bash
python3 c375_dashboard.py --refresh-sheet
python3 c375_dashboard.py --db /path/to/esgpull.db
```

The first command captures the five centre sheets into
`work/c375_requirements.csv`. The second produces
`outputs/c375_dashboard.csv` and `outputs/c375_dashboard.json`.

No database records are changed and no data is sent to Grist yet. The next
stage will add authenticated Grist upserts after the snapshot has been checked
against `esgpull show` and `esgpull status`.

## Grist dry run

The Grist table must contain columns matching the CSV headers, including a
unique text column named `requirement_id`. Preview the exact upsert payload:

```bash
python3 grist_sync.py
```

Publishing is explicit and uses `requirement_id` as the stable match key:

```bash
GRIST_API_KEY=... GRIST_DOC_ID=... python3 grist_sync.py --apply
```

The default table ID is `C375_Replication`; override it with
`GRIST_TABLE_ID` or `--table`. Rows are added or updated, never deleted.

## Current matching rules

- A requirement matches a query when its start date occurs in the query's
  `sub_experiment_id` selection.
- Recognized centre tags (for example `dwd`) must match the workbook sheet.
- The current database's `test` tag is explicitly mapped to CMCC and `uib` to
  the workbook's `UiB/NERSC` label.
- `CMIP6 Plus` and `CMIP6Plus` are normalized to the same project value.
- `dcpp-a` maps to `dcppA-hindcast`; `dcpp-b` maps to `dcppB-forecast`.
- Files linked through multiple queries are deduplicated by file SHA and then
  assigned to the sheet row using the start-date token in their DRS identity.

The collector intentionally keeps sheet notes and Cat-1/Cat-2 variable lists
verbatim. The current reporting grain is centre plus start date, matching the
source workbook. Variable-level drill-down can be added after validating the
real database's dataset IDs and query conventions.

## Periodic VM refresh

`update_c375_report.py` performs the complete refresh safely:

1. Uses SQLite's online backup API to take a transactionally consistent copy
   of the live database (including committed WAL content).
2. Runs `PRAGMA quick_check` on the copy before using it.
3. Builds the report only from that validated copy.
4. Upserts the detail, institution summary, and status summary tables in Grist.
5. Keeps the latest 14 backups by default.

Keep the Grist secret outside the scripts. For example, create
`/home/abennasser/c3s/c375-report.env`, readable only by its owner:

```ini
GRIST_HOST=https://grist.numerique.gouv.fr
GRIST_DOC_ID=rhuWpdLQZ4i83edYxfQJQq
GRIST_API_KEY=replace-with-a-new-key
```

Then test without publishing:

```bash
cd /home/abennasser/c3s/dashboard
python3 update_c375_report.py --no-publish
```

Run the real refresh after loading the environment:

```bash
set -a
. /home/abennasser/c3s/c375-report.env
set +a
python3 update_c375_report.py
```

The included `c375-report.service` and `c375-report.timer` are example systemd
user units. Copy them to `~/.config/systemd/user/`, then enable the daily timer:

```bash
systemctl --user daemon-reload
systemctl --user enable --now c375-report.timer
systemctl --user list-timers c375-report.timer
```

The timer runs every day at 06:15 and catches up after VM downtime. Adjust
`OnCalendar` in the timer if a different schedule is preferable.

## Review plan for missing queries

Generate candidates from a validated backup:

```bash
python3 generate_missing_queries.py \
  --db /home/abennasser/c3s/backups/c375-YYYYMMDDTHHMMSSZ.db
```

This writes `outputs/c375_missing_query_plan.csv`. It clones the established
facet patterns for the same centre and experiment and substitutes only the
missing `sub_experiment_id` values. It never changes the esgpull database. The
commands should first be checked with `esgpull search`; adding, updating, and
downloading remain separate, deliberate steps.
