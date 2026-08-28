import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from c375_dashboard import Requirement, _query_matches, build_snapshot


class SnapshotTest(unittest.TestCase):
    def test_explicit_centre_and_experiment_mapping(self):
        req = Requirement('x', 'UiB/NERSC', 's2025', 'dcpp-b', 'CMIP6 Plus', '', '', '', '', '', '')
        facets = {
            'project': {'CMIP6Plus'},
            'sub_experiment_id': {'s2025'},
            'experiment_id': {'dcppB-forecast'},
        }
        self.assertTrue(_query_matches(req, facets, {'uib'}))
        self.assertFalse(_query_matches(req, facets, {'dwd'}))
        self.assertFalse(_query_matches(req, {**facets, 'experiment_id': {'dcppA-hindcast'}}, {'uib'}))

    def test_condensed_query_maps_to_multiple_rows_without_double_counting(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "esgpull.db"
            conn = sqlite3.connect(db)
            conn.executescript("""
              create table query(sha text primary key, selection_sha text);
              create table selection_facet(selection_sha text, facet_sha text);
              create table facet(sha text primary key, name text, value text);
              create table tag(sha text primary key, name text);
              create table query_tag(query_sha text, tag_sha text);
              create table query_file(query_sha text, file_sha text);
              create table file(sha text primary key, file_id text, dataset_id text,
                                filename text, master_id text, size integer, status text);
              insert into query values('ebe2ad','sel');
              insert into facet values('f1','project','CMIP6Plus');
              insert into facet values('f2','sub_experiment_id','s1960');
              insert into facet values('f3','sub_experiment_id','s1961');
              insert into selection_facet values('sel','f1');
              insert into selection_facet values('sel','f2');
              insert into selection_facet values('sel','f3');
              insert into tag values('t1','dwd');
              insert into query_tag values('ebe2ad','t1');
              insert into file values('a','x.s1960-r1.pr.nc','x.s1960-r1.v1',
                                      'pr_x_s1960-r1.nc','x.s1960-r1.pr.nc',100,'Done');
              insert into file values('b','x.s1961-r1.pr.nc','x.s1961-r1.v1',
                                      'pr_x_s1961-r1.nc','x.s1961-r1.pr.nc',300,'Error');
              insert into file values('orphan','x.s1960-r1.extra.nc','x.s1960-r1.v1',
                                      'extra_x_s1960-r1.nc','x.s1960-r1.extra.nc',9999,'Done');
              insert into query_file values('ebe2ad','a');
              insert into query_file values('ebe2ad','b');
            """)
            conn.commit(); conn.close()
            def req(start):
                return Requirement(start, 'DWD', start, 'dcpp-a', 'CMIP6 Plus', '', '', '', '', '', '')
            rows = build_snapshot([req('s1960'), req('s1961')], db)
            self.assertEqual([r['query_count'] for r in rows], [1, 1])
            self.assertEqual(rows[0]['file_total'], 1)
            self.assertEqual(rows[0]['file_done'], 1)
            self.assertEqual(rows[0]['replication_state'], 'Complete')
            self.assertEqual(rows[1]['file_total'], 1)
            self.assertEqual(rows[1]['replication_state'], 'Error')
            self.assertEqual(rows[1]['completion'], 0.0)


if __name__ == '__main__':
    unittest.main()
