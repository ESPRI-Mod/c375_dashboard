import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from manifest_esgpull import (
    ParsedURL,
    STATE_SCHEMA,
    bookkeeping_key,
    dataset_facets,
    parse_manifest_url,
    resolve_from_database,
    seed_state,
)


SAMPLE = (
    "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/DCPP/MOHC/"
    "HadGEM3-GC31-MM/dcppA-hindcast/s1960-r10i1p1f2/AERday/zg500/gn/"
    "v20200417/zg500_AERday_HadGEM3-GC31-MM_dcppA-hindcast_"
    "s1960-r10i1p1f2_gn_19601101-19601230.nc"
)


class ManifestURLTests(unittest.TestCase):
    def test_extracts_dataset_id(self):
        parsed = parse_manifest_url(SAMPLE)
        self.assertIsNone(parsed.error)
        self.assertEqual(
            parsed.dataset_id,
            "CMIP6.DCPP.MOHC.HadGEM3-GC31-MM.dcppA-hindcast."
            "s1960-r10i1p1f2.AERday.zg500.gn.v20200417",
        )

    def test_rejects_malformed_hostname(self):
        parsed = parse_manifest_url(SAMPLE.replace("esgf.ceda", "esgf..ceda"))
        self.assertEqual(parsed.error, "malformed hostname")

    def test_dashboard_key(self):
        metadata = {"dataset_id": parse_manifest_url(SAMPLE).dataset_id}
        self.assertEqual(
            bookkeeping_key(metadata),
            ("mohc", "CMIP6", "dcppA-hindcast", "s1960"),
        )

    def test_dataset_facets(self):
        facets = dataset_facets(parse_manifest_url(SAMPLE).dataset_id or "")
        self.assertIsNotNone(facets)
        assert facets is not None
        self.assertEqual(facets["variable_id"], "zg500")
        self.assertEqual(facets["version"], "v20200417")

    def test_bsc_institution_maps_to_dashboard_tag(self):
        dataset_id = (
            "CMIP6.DCPP.EC-Earth-Consortium.EC-Earth3.dcppA-hindcast."
            "s1960-r1i1p1f1.Amon.tas.gr.v20200101"
        )
        self.assertEqual(
            bookkeeping_key({"dataset_id": dataset_id}),
            ("bsc", "CMIP6", "dcppA-hindcast", "s1960"),
        )

    def test_database_identity_keeps_manifest_url(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """CREATE TABLE manifest_url (
                url TEXT PRIMARY KEY, hostname TEXT, dataset_id_hint TEXT,
                status TEXT, source TEXT, metadata_json TEXT, error TEXT,
                updated_at TEXT
            );"""
        )
        manifest_url = SAMPLE.replace("esgf.ceda.ac.uk", "preferred.example")
        dataset_id = parse_manifest_url(SAMPLE).dataset_id
        conn.execute(
            """INSERT INTO manifest_url
               (url, hostname, dataset_id_hint, status)
               VALUES (?, ?, ?, 'pending')""",
            (manifest_url, "preferred.example", dataset_id),
        )
        fake_file = SimpleNamespace(
            url=SAMPLE,
            dataset_id=dataset_id,
            filename=manifest_url.rsplit("/", 1)[-1],
            asdict=lambda: {
                "file_id": "id", "dataset_id": dataset_id,
                "master_id": "master", "url": SAMPLE, "version": "v20200417",
                "filename": manifest_url.rsplit("/", 1)[-1], "local_path": "path",
                "data_node": "esgf.ceda.ac.uk", "checksum": "abc",
                "checksum_type": "SHA256", "size": 1, "status": "Error",
            },
        )
        fake_session = SimpleNamespace(scalars=lambda statement: [fake_file])
        fake_esg = SimpleNamespace(db=SimpleNamespace(session=fake_session))
        fake_sqlalchemy = SimpleNamespace(
            select=lambda model: SimpleNamespace(
                execution_options=lambda **kwargs: object()
            )
        )
        with patch.dict(
            "sys.modules",
            {
                "esgpull.models": SimpleNamespace(File=object),
                "sqlalchemy": fake_sqlalchemy,
            },
        ):
            count = resolve_from_database(conn, fake_esg)
        self.assertEqual(count, 1)
        row = conn.execute(
            "SELECT status, source, metadata_json FROM manifest_url"
        ).fetchone()
        self.assertEqual(row["status"], "resolved")
        self.assertEqual(row["source"], "database-file-identity")
        self.assertEqual(__import__("json").loads(row["metadata_json"])["url"], manifest_url)

    def test_revised_manifest_removes_stale_checkpoint_urls(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(STATE_SCHEMA)
        old = ParsedURL("https://old.example/fileServer/old", "old.example", None)
        new = ParsedURL("https://new.example/fileServer/new", "new.example", None)
        seed_state(conn, [old])
        seed_state(conn, [new])
        self.assertEqual(
            [row[0] for row in conn.execute("SELECT url FROM manifest_url")],
            [new.url],
        )


if __name__ == "__main__":
    unittest.main()
