# FastPlay Resolver (Python microservice)

Parallel multi-source stream resolver for the Android app. The phone sends one
request; this service queries **VidLink, YTS, Torrentio, 1337x, TorrentGalaxy,
EZTV, 1TamilMV and TamilBlasters** concurrently, parses quality / audio
languages / seeders / size, scores every stream and returns a sorted list.

```
Android --GET /resolve--> FastAPI --asyncio.gather--> 8 providers
                              |
                              +- VidLink direct MP4/HLS (TMDB-keyed, no debrid)
                              +- normalise: 4K/1080p/720p, Hindi/Dual/English,
                              |             seeders, health, size
                              +- sort: direct playability -> language -> score
```

## Run locally

```powershell
cd server
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn fastplay_service:app --host 0.0.0.0 --port 8000 --reload
```

or simply:

```powershell
python server\fastplay_service.py
```

Open <http://127.0.0.1:8000/docs> for the interactive API.

### Quick checks

```powershell
curl "http://127.0.0.1:8000/health"
curl "http://127.0.0.1:8000/resolve?imdb=tt1375666&title=Inception&year=2010"
curl "http://127.0.0.1:8000/fast-play?imdb=tt1375666&title=Inception&year=2010"
```

Example (trimmed) response:

```json
{
  "success": true,
  "count": 42,
  "elapsedMs": 1840,
  "providers": [{"name": "Torrentio", "ok": true, "count": 27, "ms": 810}],
  "streams": [
    {
      "provider": "1337x",
      "title": "Inception 2010 1080p BluRay Hindi Dual Audio",
      "quality": "1080p",
      "languages": ["Hindi", "Dual Audio", "English"],
      "seeders": 2401,
      "size": "2.1 GB",
      "magnet": "magnet:?xt=urn:btih:...",
      "streamUrl": "magnet:?xt=urn:btih:...",
      "isDirect": false,
      "playable": false,
      "health": 88,
      "score": 91.4
    }
  ]
}
```

`/fast-play` returns `{ "stream": {...}, "reason": "720p - in-app direct stream", "alternatives": [...] }`
and picks **direct HTTP/MP4/HLS first** so ExoPlayer can start immediately,
then Hindi / Dual Audio, quality and seeders. A direct stream looks like:

```json
{
  "provider": "VidLink",
  "title": "Fight Club [720p] [VidLink] [hevc]",
  "quality": "720p",
  "streamUrl": "https://bcdn.hakunaymatata.com/.../index.mp4?sign=...",
  "isDirect": true,
  "isMagnet": false,
  "playable": true,
  "sourceType": "mp4"
}
```

## Automatic IMDb resolution

Torrentio is IMDb-keyed, so a title-only request first resolves a `tt` id:

1. **TMDB proxy** (default): `search/movie` -> first exact title/year match ->
   `movie/{id}/external_ids` -> `imdb_id`.
2. **Cinemeta fallback** when the TMDB key is missing or the call fails:
   `catalog/movie/top/search={title}.json` (the path form; Cinemeta ignores
   `?search=`), with local title/year matching.

Resolved ids are cached for 24 h (10 min for misses), so repeat requests skip
the lookup. The response exposes `query.imdb` and `query.imdbSource`
(`request`, `tmdb`, `cinemeta` or `cache`).

```powershell
curl "http://127.0.0.1:8000/resolve?title=Interstellar&year=2014"
```

## Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `FASTPLAY_PROVIDER_TIMEOUT` | `9` | Per-provider timeout (seconds) |
| `FASTPLAY_TOTAL_TIMEOUT` | `6` | Overall soft deadline; slow providers are cancelled so the phone gets partial results fast |
| `FASTPLAY_CACHE_TTL` | `300` | In-memory response cache TTL |
| `FASTPLAY_MAX_RESULTS` | `60` | Max streams in a response |
| `FASTPLAY_DEBRID_KEY` | empty | Real-Debrid key; Torrentio then returns direct HTTP URLs that ExoPlayer can play |
| `TMDB_API_KEY` | bundled public key | TMDB key for title -> IMDb resolution; set empty to use Cinemeta only |
| `TMDB_PROXY_URL` | `https://late-bar-1e40tmdb-proxy.rdevi162277.workers.dev/3` | TMDB API proxy base |
| `CINEMETA_BASE` | `https://v3-cinemeta.strem.io` | Cinemeta fallback base |
| `VIDLINK_BASE` | `https://vidlink.pro` | VidLink player/API host |
| `VIDLINK_SECRETBOX_KEY` | bundled key | Hex key for VidLink's token; override if its bundle rotates it |
| `VIDLINK_ATTEMPTS` | `8` | API retries; VidLink edge nodes intermittently answer `null` |
| `FASTPLAY_IMDB_CACHE_TTL` | `86400` | Positive IMDb lookup cache TTL (seconds) |
| `FASTPLAY_IMDB_NEGATIVE_TTL` | `600` | Failed IMDb lookup cache TTL (seconds) |
| `YTS_MIRRORS` | `yts.mx,yts.lt,yts.am,yts.rs` | Comma separated |
| `TGX_MIRRORS` | `torrentgalaxy.to,.mx,tgx.rs` | Comma separated |
| `X1337_MIRRORS` | `1337x.to,.st,x1337x.ws,1337x.gd` | Comma separated |
| `EZTV_MIRRORS` | `eztv.re,.wf,.tf,.yt` | Comma separated |
| `TAMILMV_DOMAINS` | `1tamilmv.win,.lol,.dog,.com` | Comma separated |
| `TAMILBLASTERS_DOMAINS` | `tamilblasters.buzz,.rodeo,.art,.xyz` | Comma separated |
| `TORRENTIO_BASE` | `https://torrentio.strem.fun` | Alternate Torrentio mirror |
| `HTTP_PROXY` / `HTTPS_PROXY` | unset | Honoured via `trust_env` (useful on ISP-blocked networks) |

## Playback reality check

* **VidLink** resolves TMDB id -> encrypted token -> private API, returning
  per-quality direct MP4 URLs (and HLS/DASH when offered). No debrid key is
  required. The provider retries its flaky edge nodes and fails softly.
* **Direct HTTP(S)** streams (`.mp4`, `.m3u8`, `.mpd`, `.mkv`, or any
  Real-Debrid link) go straight into the app's ExoPlayer. `/fast-play` ranks
  these first, before language/seeders, so a direct 720p stream beats a
  magnet-only 1080p one. `MAX_RESULTS` truncation also keeps direct streams.
* `/fast-play` exposes `directAvailable`, and every stream carries
  `isDirect`/`is_direct`, `isMagnet` and `playable`. Magnet streams are always
  normalised to `isDirect: false` and `playable: false`.
* **Magnet links** are played entirely in-app. The Android client starts an
  embedded `libtorrent4j` session, prioritises the first 3% and last 1% of the
  selected video file, and exposes it through a localhost HTTP proxy. The
  existing ExoPlayer screen then plays `http://127.0.0.1:<port>/stream.<ext>`
  with no external torrent/VLC client involved.
  Configure `FASTPLAY_DEBRID_KEY` if you prefer direct HTTP streams and want to
  skip the peer-to-peer buffering step.
* The scraped sites change domains often. Every mirror list above is
  environment-overridable, and each provider fails softly: one dead source
  never blocks the response.

## Tests

```powershell
python -m unittest discover -s server -p "test_*.py" -v
```

The suite covers quality/language/size parsing, Torrentio payloads (including
the current `infoHash`-only format), YTS parsing with a mocked payload,
VidLink token/quality parsing, TMDB/Cinemeta IMDb resolution, deduplication
and FastPlay ranking. No network required.

## Deploy on Koyeb

The included `server/Dockerfile` is Koyeb-ready and reads Koyeb's dynamic
`PORT` environment variable (falling back to `8000` for local Docker runs).

Koyeb Service settings:

| Setting | Value |
| --- | --- |
| Builder | `Dockerfile` |
| Work directory | `server` (required for this monorepo) |
| Dockerfile location | `Dockerfile` (relative to the work directory) |
| Service port | `8000` HTTP (match Koyeb's injected `PORT`; default is `8000`) |
| Health check path | `/health` |

Koyeb injects `PORT`; the container starts with:

```sh
uvicorn fastplay_service:app --host 0.0.0.0 --port ${PORT:-8000}
```

Local Docker check:

```powershell
docker build -t fastplay-server server
docker run --rm -p 8000:8000 -e PORT=8000 fastplay-server
curl "http://127.0.0.1:8000/health"
```

Then point the app at the public URL - no APK update needed:

`Firebase -> server_config -> fastplay_base_url = https://your-service.koyeb.app`

### Android base URL

`FastPlayService` resolves the base URL in this order:

1. `FastPlayService.baseUrlOverride` (runtime QA override)
2. Firebase `server_config/fastplay_base_url`
3. `http://10.119.156.74:8000` (built-in default)

For a physical phone on the same Wi-Fi, make sure the laptop IP is reachable
and Windows Firewall allows port 8000. Update `fastplay_base_url` in Firebase
(or `baseUrlOverride`) when the IP changes.
