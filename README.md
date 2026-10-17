# SAR Cache Kit

Reference implementation pieces for the Agentic Search and Rescue (SAR) platform design:

| Path | What it is |
| --- | --- |
| `configs/cache-policy.toml` | Single source of truth: freshness classes A-D, per-data TTLs, staleness limits, never-cache list |
| `configs/redis-cache.conf` | Evictable cache Redis (`allkeys-lfu`) |
| `configs/redis-state.conf` | Authoritative state Redis (`noeviction`, AOF) |
| `configs/nginx-tiles.conf` | Tile and overlay proxy cache (lock, stale-while-updating, versioned overlay paths) |
| `configs/mosquitto.conf` | MQTT broker with persistence and retained last-value messages |
| `configs/postgresql-cache.conf` | PostGIS memory tuning plus pg_prewarm |
| `configs/vllm-serve.sh` | Local LLM serving with prefix caching and fp8 KV cache |
| `tools/bundle_builder.py` | Offline map bundles: estimate, build (MBTiles + signed manifest), verify, diff, apply |
| `tools/linksim.py` | Simulator for link loss, cache staleness and outbox drain order |
| `tools/warm_prefix.py` | Primes the shared prompt prefix after a server restart |
| `tests/test_kit.py` | Unit tests (`python3 -m unittest discover -s tests`) |

Requires Python 3.11+ (standard library only).

## Quick start

```bash
# 1. How big is the bundle? (50 km square, zooms 8-17)
python3 tools/bundle_builder.py estimate --lat 37.7 --lon -119.5 --radius-km 25 --minzoom 8 --maxzoom 17

# 2. Build a signed bundle (synthetic tiles for a dry run; use --source dir or url for real data)
echo "change-me" > key.txt
python3 tools/bundle_builder.py build --lat 37.7 --lon -119.5 --radius-km 10 --minzoom 8 --maxzoom 13 \
  --name demo --version v1 --out out/v1 --key-file key.txt
python3 tools/bundle_builder.py verify out/v1/demo-v1.manifest.json --key-file key.txt

# 3. Simulate link loss
python3 tools/linksim.py cache                       # weather forecast, 4 policies
python3 tools/linksim.py cache --data hazard_feed    # hazard feed (30 min staleness limit)
python3 tools/linksim.py drain                       # outbox drain order after an outage
```

## Important notes

* **Tile sources.** Do not bulk-download from public community tile servers; their policies forbid it. Use your own tile server, or a provider whose terms allow offline use. Check imagery licences too.
* **Signing.** The kit uses HMAC-SHA256 with a shared key to stay dependency-free. In production use asymmetric signatures (minisign, cosign) so devices only hold a public key.
* **Simulator numbers are assumptions.** Request rates, link capacity, outage lengths and message sizes are defaults you should replace with measurements. The simulator shows trade-offs, not predictions.
* **Rings file.** `--rings-file` expects numbers you supply from your own lost-person behavior data; none are included.
* **Safety.** This is reference code for a decision-support system. Humans approve any action affecting people. Validate everything with SAR professionals before field use.
