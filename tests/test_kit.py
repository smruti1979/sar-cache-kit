import json
import os
import sqlite3
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import bundle_builder as bb  # noqa: E402
import linksim as ls  # noqa: E402

POLICY = os.path.join(ROOT, "configs", "cache-policy.toml")


class TileMath(unittest.TestCase):
    def test_known_tiles(self):
        self.assertEqual(bb.lonlat_to_tile(0, 0, 1), (1, 1))
        self.assertEqual(bb.lonlat_to_tile(-179.9, 85, 3), (0, 0))
        self.assertEqual(bb.tms_row(3, 0), 7)

    def test_counts_grow_about_4x_per_zoom(self):
        c = bb.count_tiles(bb.bbox_from_center(0, 0, 25), 14, 17)
        self.assertTrue(3.5 < c[17] / c[16] < 4.5)


class Bundle(unittest.TestCase):
    def build(self, d, version, synth="0"):
        key = os.path.join(d, "key")
        with open(key, "w") as f:
            f.write("k")
        bb.main(["build", "--lat", "10", "--lon", "10", "--radius-km", "8", "--minzoom", "9",
                 "--maxzoom", "12", "--name", "t", "--version", version, "--out", d + "/" + version,
                 "--synthetic-version", synth, "--key-file", key])
        return key, d + "/" + version

    def test_roundtrip_tamper_diff_apply(self):
        with tempfile.TemporaryDirectory() as d:
            key, v1 = self.build(d, "v1")
            self.assertEqual(bb.main(["verify", v1 + "/t-v1.manifest.json", "--key-file", key]), 0)
            _, v2 = self.build(d, "v2", synth="1")
            delta = d + "/delta.json"
            bb.main(["diff", v1 + "/t-v1.tiles.json.gz", v2 + "/t-v2.tiles.json.gz", "--out", delta,
                     "--new-mbtiles", v2 + "/t-v2.mbtiles", "--delta-mbtiles", d + "/d.mbtiles"])
            bb.main(["apply", v1 + "/t-v1.mbtiles", delta, d + "/d.mbtiles", "--out", d + "/out.mbtiles"])
            q = "select zoom_level,tile_column,tile_row,tile_data from tiles order by 1,2,3"
            self.assertEqual(sqlite3.connect(d + "/out.mbtiles").execute(q).fetchall(),
                             sqlite3.connect(v2 + "/t-v2.mbtiles").execute(q).fetchall())
            man = v1 + "/t-v1.manifest.json"
            m = json.load(open(man))
            m["tile_count"] += 1                      # tamper with signed field
            json.dump(m, open(man, "w"))
            self.assertEqual(bb.main(["verify", man, "--key-file", key]), 1)


class Sim(unittest.TestCase):
    def args(self, **kw):
        class A:
            duration = 3000
            rate = 20.0
            keys = 100
            zipf = 1.1
            latency = 2
            cap = 8
            seed = 3
        a = A()
        for k, v in kw.items():
            setattr(a, k, v)
        return a

    def test_invariants(self):
        cls = ls.load_policy(POLICY, "weather_forecast")
        link = ls.Link([(1000, 1600)])
        a = self.args()
        res = {p: ls.run_cache(cls, a, link, p) for p in ls.POLICIES}
        self.assertEqual(res["naive"]["stale_flagged_%"], 0)          # naive never serves stale
        self.assertEqual(res["coalesced"]["duplicate_fetches"], 0)    # one fetch per key in flight
        self.assertGreaterEqual(res["stale_if_error"]["availability_%"], res["naive"]["availability_%"])
        for r in res.values():
            self.assertLessEqual(r["age_max_s"], cls["max_stale_s"])  # never beyond the hard limit
        self.assertEqual(res["naive"], ls.run_cache(cls, a, link, "naive"))  # repeatable

    def test_priority_beats_fifo(self):
        class D:
            outage_start, outage_end, horizon = 300, 600, 2400
            bandwidth_kbps, assets, drones = 2000, 10, 5
            pos_bytes, thumb_bytes, thumb_every = 200, 30000, 5
            clip_bytes, clip_every, log_bytes, fresh_s = 500000, 60, 10000, 15
        f, p, l = (ls.run_drain(D, m) for m in ("fifo", "priority", "latest"))
        self.assertLess(p["time_to_fresh_positions_s"], f["time_to_fresh_positions_s"])
        self.assertLessEqual(l["time_to_fresh_positions_s"], p["time_to_fresh_positions_s"])


if __name__ == "__main__":
    unittest.main()
