import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from manifest_dashboard import build_rows, main


class ManifestDashboardTest(unittest.TestCase):
    def make_databases(self, root: Path) -> tuple[Path, Path]:
        state = root / "state.sqlite"
        conn = sqlite3.connect(state)
        conn.executescript("""
            CREATE TABLE manifest_url (
                url TEXT PRIMARY KEY, hostname TEXT, status TEXT,
                metadata_json TEXT, error TEXT
            );
        """)
        rows = [
            ("https://a.example/done.nc", "a.example", "resolved",
             json.dumps({"file_id": "done-id"}), None),
            ("https://a.example/queued.nc", "a.example", "resolved",
             json.dumps({"file_id": "queued-id"}), None),
            ("https://b.example/deferred.nc", "b.example", "deferred",
             None, "network unavailable"),
            ("https://b.example/failed.nc", "b.example", "pending",
             None, "ConnectTimeout"),
            ("https://b.example/lookup.nc", "b.example", "pending",
             None, "ESGF lookup failed after 4 attempts: unavailable"),
            ("bad", "", "invalid", None, "malformed hostname"),
        ]
        conn.executemany("INSERT INTO manifest_url VALUES (?,?,?,?,?)", rows)
        conn.commit(); conn.close()

        db = root / "esgpull.sqlite"
        conn = sqlite3.connect(db)
        conn.execute(
            "CREATE TABLE file (url TEXT, file_id TEXT, status TEXT, size INTEGER)"
        )
        conn.executemany(
            "INSERT INTO file VALUES (?,?,?,?)",
            [
                ("https://old.example/done.nc", "done-id", "Done", 100),
                ("https://a.example/queued.nc", "queued-id", "Queued", 200),
            ],
        )
        conn.commit(); conn.close()
        return state, db

    def test_manifest_classification_and_aggregation(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, db = self.make_databases(Path(tmp))
            statuses, hosts, failures = build_rows(state, db)
            status = {row["status"]: row for row in statuses}
            self.assertEqual(status["Downloaded"]["url_count"], 1)
            self.assertEqual(status["To download"]["url_count"], 3)
            self.assertEqual(status["Failed"]["url_count"], 2)
            self.assertAlmostEqual(status["Downloaded"]["known_tib"], 100 / 2**40)
            host = {row["hostname"]: row for row in hosts}
            self.assertEqual(host["b.example"]["deferred_urls"], 1)
            self.assertEqual(host["b.example"]["to_download_urls"], 2)
            self.assertEqual(sum(row["url_count"] for row in failures), 2)

    def test_writes_three_compact_csv_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state, db = self.make_databases(root)
            output = root / "output"
            self.assertEqual(main([
                "--state", str(state), "--db", str(db),
                "--output-dir", str(output),
            ]), 0)
            with (output / "manifest_status_summary.csv").open(newline="") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 3)
            self.assertTrue((output / "manifest_host_summary.csv").is_file())
            self.assertTrue((output / "manifest_failure_summary.csv").is_file())


if __name__ == "__main__":
    unittest.main()
