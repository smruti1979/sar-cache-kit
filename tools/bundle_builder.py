#!/usr/bin/env python3
"""bundle_builder: build signed, versioned offline map bundles for SAR deployments.

Subcommands
  estimate  tile counts and size for an area and zoom range (no network)
  build     fetch tiles into an MBTiles file and write a signed manifest
  verify    check signature, file hashes and every tile hash
  diff      compare two bundles and write a delta description
  apply     apply a delta to a base bundle to produce the new bundle

Sources for `build`
  synthetic  deterministic placeholder tiles (tests, dry runs, no network)
  dir        tiles from a local directory laid out as <z>/<x>/<y>.<ext>
  url        HTTP(S) tile server you are allowed to download from.
             Do NOT point this at public community tile servers: their usage
             policies forbid bulk downloads. Use your own server or a provider
             whose terms permit offline use.

Standard library only. Signing here is HMAC-SHA256 with a shared key to keep
the kit dependency-free; for production use asymmetric signatures (for
example minisign or cosign) so field devices hold only a public key.
"""
import argparse
import gzip
import hashlib
import hmac
import json
import math
import os
import shutil
import sqlite3
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

MAX_LAT = 85.0511287798
KM_PER_DEG_LAT = 111.32


# ---------------------------------------------------------------- tile math --
def lonlat_to_tile(lon, lat, z):
    lat = max(-MAX_LAT, min(MAX_LAT, lat))
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def bbox_from_center(lat, lon, radius_km):
    """Approximate bounding box (west, south, east, north) around a point."""
    dlat = radius_km / KM_PER_DEG_LAT
    dlon = radius_km / (KM_PER_DEG_LAT * max(0.01, math.cos(math.radians(lat))))
    return lon - dlon, lat - dlat, lon + dlon, lat + dlat


def tile_ranges(bbox, minzoom, maxzoom):
    w, s, e, n = bbox
    if w > e:
        raise SystemExit("bbox crosses the antimeridian: build two bundles instead")
    for z in range(minzoom, maxzoom + 1):
        x0, y0 = lonlat_to_tile(w, n, z)
        x1, y1 = lonlat_to_tile(e, s, z)
        yield z, x0, x1, y0, y1


def count_tiles(bbox, minzoom, maxzoom):
    per_zoom = {}
    for z, x0, x1, y0, y1 in tile_ranges(bbox, minzoom, maxzoom):
        per_zoom[z] = (x1 - x0 + 1) * (y1 - y0 + 1)
    return per_zoom


def iter_tiles(bbox, minzoom, maxzoom):
    for z, x0, x1, y0, y1 in tile_ranges(bbox, minzoom, maxzoom):
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                yield z, x, y


def tms_row(z, y):
    """MBTiles stores rows in TMS order (origin at the bottom)."""
    return (2 ** z - 1) - y


# ------------------------------------------------------------------ sources --
def synthetic_tile(z, x, y, version="0"):
    seed = f"{version}/{z}/{x}/{y}".encode()
    return hashlib.sha256(seed).digest() * 8  # 256 bytes, deterministic


def make_fetcher(args):
    if args.source == "synthetic":
        return lambda z, x, y: synthetic_tile(z, x, y, args.synthetic_version)
    if args.source == "dir":
        def read_dir(z, x, y):
            p = os.path.join(args.source_dir, str(z), str(x), f"{y}.{args.format}")
            if not os.path.exists(p):
                return None
            with open(p, "rb") as f:
                return f.read()
        return read_dir
    if args.source == "url":
        if not args.url_template:
            raise SystemExit("--url-template is required for --source url")
        delay = 1.0 / args.rate if args.rate > 0 else 0.0

        def fetch(z, x, y):
            url = args.url_template.format(z=z, x=x, y=y)
            req = urllib.request.Request(url, headers={"User-Agent": args.user_agent})
            for attempt in range(4):
                try:
                    with urllib.request.urlopen(req, timeout=20) as r:
                        data = r.read()
                    time.sleep(delay)
                    return data
                except Exception:
                    time.sleep(min(30, 2 ** attempt))
            return None
        return fetch
    raise SystemExit(f"unknown source {args.source}")


# ------------------------------------------------------------------ helpers --
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def tile_hash(data):
    return hashlib.sha256(data).hexdigest()[:32]  # 128 bits is plenty for change detection


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def sign(manifest, key):
    body = {k: v for k, v in manifest.items() if k != "signature"}
    return hmac.new(key, canonical(body), hashlib.sha256).hexdigest()


def load_key(args):
    if args.key_file:
        with open(args.key_file, "rb") as f:
            return f.read().strip()
    env = os.environ.get("SAR_BUNDLE_KEY")
    if env:
        return env.encode()
    raise SystemExit("signing key required: --key-file or SAR_BUNDLE_KEY")


def open_mbtiles(path, create=False):
    db = sqlite3.connect(path)
    if create:
        db.executescript(
            "CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT);"
            "CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER,"
            " tile_row INTEGER, tile_data BLOB);"
            "CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles"
            " (zoom_level, tile_column, tile_row);"
        )
    return db


def write_index(path, index):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(index, f, sort_keys=True, separators=(",", ":"))


def read_index(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


# -------------------------------------------------------------- subcommands --
def resolve_bbox(args):
    if args.bbox:
        w, s, e, n = (float(v) for v in args.bbox.split(","))
        return w, s, e, n
    if args.lat is None or args.lon is None:
        raise SystemExit("give --bbox or --lat/--lon with a radius")
    radius = args.radius_km
    if args.rings_file and args.category:
        with open(args.rings_file) as f:
            rings = json.load(f)
        radius = float(rings[args.category]["p95_km"])
    if radius is None:
        raise SystemExit("give --radius-km or --rings-file with --category")
    return bbox_from_center(args.lat, args.lon, radius + args.buffer_km)


def cmd_estimate(args):
    bbox = resolve_bbox(args)
    per_zoom = count_tiles(bbox, args.minzoom, args.maxzoom)
    total = sum(per_zoom.values())
    print(f"bbox (w,s,e,n): {', '.join(f'{v:.5f}' for v in bbox)}")
    for z, c in per_zoom.items():
        print(f"  z{z:>2}: {c:>9,} tiles")
    mb = total * args.avg_kb / 1024
    print(f"total: {total:,} tiles, about {mb:,.0f} MB at {args.avg_kb} KB per tile")


def cmd_build(args):
    bbox = resolve_bbox(args)
    per_zoom = count_tiles(bbox, args.minzoom, args.maxzoom)
    total = sum(per_zoom.values())
    if total > args.max_tiles:
        raise SystemExit(f"{total:,} tiles exceeds --max-tiles {args.max_tiles:,}")
    key = load_key(args)
    os.makedirs(args.out, exist_ok=True)
    mb_path = os.path.join(args.out, f"{args.name}-{args.version}.mbtiles")
    idx_path = os.path.join(args.out, f"{args.name}-{args.version}.tiles.json.gz")
    db = open_mbtiles(mb_path, create=True)
    have = {(z, c, r) for z, c, r in db.execute(
        "SELECT zoom_level, tile_column, tile_row FROM tiles")}
    todo = [(z, x, y) for z, x, y in iter_tiles(bbox, args.minzoom, args.maxzoom)
            if (z, x, tms_row(z, y)) not in have]
    fetch = make_fetcher(args)
    missing = 0
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for (z, x, y), data in zip(todo, pool.map(lambda t: fetch(*t), todo)):
            if data is None:
                missing += 1
                continue
            db.execute("INSERT OR REPLACE INTO tiles VALUES (?,?,?,?)",
                       (z, x, tms_row(z, y), data))
            done += 1
            if done % 2000 == 0:
                db.commit()
                print(f"  {done:,}/{len(todo):,} tiles", file=sys.stderr)
    meta = {
        "name": args.name, "format": args.format, "type": "baselayer",
        "version": args.version, "description": args.description,
        "bounds": ",".join(f"{v:.6f}" for v in bbox),
        "minzoom": str(args.minzoom), "maxzoom": str(args.maxzoom),
    }
    db.execute("DELETE FROM metadata")
    db.executemany("INSERT INTO metadata VALUES (?,?)", meta.items())
    db.commit()
    index = {}
    for z, c, r, data in db.execute(
            "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles"):
        index[f"{z}/{c}/{(2 ** z - 1) - r}"] = tile_hash(data)
    db.close()
    write_index(idx_path, index)
    manifest = {
        "schema": 1, "name": args.name, "version": args.version,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bbox": list(bbox), "minzoom": args.minzoom, "maxzoom": args.maxzoom,
        "format": args.format, "tile_count": len(index), "missing_tiles": missing,
        "tiles_per_zoom": {str(k): v for k, v in per_zoom.items()},
        "files": {
            os.path.basename(mb_path): sha256_file(mb_path),
            os.path.basename(idx_path): sha256_file(idx_path),
        },
        "index_file": os.path.basename(idx_path),
        "mbtiles_file": os.path.basename(mb_path),
    }
    manifest["signature"] = {"alg": "HMAC-SHA256", "value": sign(manifest, key)}
    man_path = os.path.join(args.out, f"{args.name}-{args.version}.manifest.json")
    with open(man_path, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    size_mb = os.path.getsize(mb_path) / 1e6
    print(f"built {mb_path} ({size_mb:,.1f} MB, {len(index):,} tiles, {missing} missing)")
    print(f"manifest {man_path}")


def cmd_verify(args):
    key = load_key(args)
    base = os.path.dirname(os.path.abspath(args.manifest))
    with open(args.manifest) as f:
        manifest = json.load(f)
    ok = True
    expected = sign(manifest, key)
    got = manifest.get("signature", {}).get("value", "")
    if not hmac.compare_digest(expected, got):
        print("FAIL signature")
        ok = False
    for name, digest in manifest["files"].items():
        p = os.path.join(base, name)
        if not os.path.exists(p) or sha256_file(p) != digest:
            print(f"FAIL file hash {name}")
            ok = False
    if ok:
        index = read_index(os.path.join(base, manifest["index_file"]))
        db = open_mbtiles(os.path.join(base, manifest["mbtiles_file"]))
        bad = 0
        seen = 0
        for z, c, r, data in db.execute(
                "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles"):
            seen += 1
            if index.get(f"{z}/{c}/{(2 ** z - 1) - r}") != tile_hash(data):
                bad += 1
        db.close()
        if bad or seen != len(index):
            print(f"FAIL tiles: {bad} mismatched, {seen} in file vs {len(index)} indexed")
            ok = False
    print("OK" if ok else "VERIFY FAILED")
    return 0 if ok else 1


def cmd_diff(args):
    old = read_index(args.old_index)
    new = read_index(args.new_index)
    changed = sorted(k for k in new if k in old and old[k] != new[k])
    added = sorted(k for k in new if k not in old)
    removed = sorted(k for k in old if k not in new)
    delta = {"changed": changed, "added": added, "removed": removed}
    with open(args.out, "w") as f:
        json.dump(delta, f)
    print(f"changed {len(changed):,}, added {len(added):,}, removed {len(removed):,} "
          f"of {len(new):,} tiles")
    if args.new_mbtiles and args.delta_mbtiles:
        src = open_mbtiles(args.new_mbtiles)
        dst = open_mbtiles(args.delta_mbtiles, create=True)
        for k in changed + added:
            z, x, y = (int(v) for v in k.split("/"))
            row = src.execute(
                "SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                (z, x, tms_row(z, y))).fetchone()
            dst.execute("INSERT OR REPLACE INTO tiles VALUES (?,?,?,?)",
                        (z, x, tms_row(z, y), row[0]))
        dst.commit()
        dst.close()
        src.close()
        print(f"delta tiles written to {args.delta_mbtiles}")


def cmd_apply(args):
    shutil.copyfile(args.base_mbtiles, args.out)
    with open(args.delta) as f:
        delta = json.load(f)
    db = open_mbtiles(args.out)
    db.execute("ATTACH DATABASE ? AS d", (args.delta_mbtiles,))
    db.execute("INSERT OR REPLACE INTO tiles SELECT * FROM d.tiles")
    db.commit()
    db.execute("DETACH DATABASE d")
    for k in delta["removed"]:
        z, x, y = (int(v) for v in k.split("/"))
        db.execute("DELETE FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                   (z, x, tms_row(z, y)))
    db.commit()
    db.close()
    print(f"applied delta to produce {args.out}")


# --------------------------------------------------------------------- main --
def add_area(p):
    p.add_argument("--lat", type=float)
    p.add_argument("--lon", type=float)
    p.add_argument("--radius-km", type=float)
    p.add_argument("--bbox", help="west,south,east,north")
    p.add_argument("--buffer-km", type=float, default=0.0)
    p.add_argument("--rings-file", help="JSON like {\"category\": {\"p95_km\": 12}} "
                   "that you supply from your own data")
    p.add_argument("--category")
    p.add_argument("--minzoom", type=int, default=8)
    p.add_argument("--maxzoom", type=int, default=15)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("estimate")
    add_area(p)
    p.add_argument("--avg-kb", type=float, default=30.0)
    p.set_defaults(fn=cmd_estimate)

    p = sub.add_parser("build")
    add_area(p)
    p.add_argument("--name", required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--description", default="SAR offline bundle")
    p.add_argument("--out", required=True)
    p.add_argument("--format", default="png", choices=["png", "jpg", "webp", "pbf"])
    p.add_argument("--source", choices=["synthetic", "dir", "url"], default="synthetic")
    p.add_argument("--source-dir")
    p.add_argument("--url-template", help="e.g. https://tiles.example.org/{z}/{x}/{y}.png")
    p.add_argument("--user-agent", default="sar-bundle-builder/1.0 (contact: ops@example.org)")
    p.add_argument("--rate", type=float, default=5.0, help="max requests per second per worker")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--max-tiles", type=int, default=500000)
    p.add_argument("--synthetic-version", default="0")
    p.add_argument("--key-file")
    p.set_defaults(fn=cmd_build)

    p = sub.add_parser("verify")
    p.add_argument("manifest")
    p.add_argument("--key-file")
    p.set_defaults(fn=cmd_verify)

    p = sub.add_parser("diff")
    p.add_argument("old_index")
    p.add_argument("new_index")
    p.add_argument("--out", required=True)
    p.add_argument("--new-mbtiles")
    p.add_argument("--delta-mbtiles")
    p.set_defaults(fn=cmd_diff)

    p = sub.add_parser("apply")
    p.add_argument("base_mbtiles")
    p.add_argument("delta")
    p.add_argument("delta_mbtiles")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_apply)

    args = ap.parse_args(argv)
    rc = args.fn(args)
    return rc or 0


if __name__ == "__main__":
    sys.exit(main())
