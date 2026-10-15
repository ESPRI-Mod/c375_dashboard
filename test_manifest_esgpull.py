import unittest

from manifest_esgpull import bookkeeping_key, dataset_facets, parse_manifest_url


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


if __name__ == "__main__":
    unittest.main()

