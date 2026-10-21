# FastPlay Resolver (Python microservice)

Parallel multi-source stream resolver for the Android app. The phone sends one
request; this service queries **YTS, Torrentio, 1337x, TorrentGalaxy, EZTV,
1TamilMV and TamilBlasters** concurrently, parses quality / audio languages /
seeders / size, scores every stream and returns a sorted list.

```
Android --GET /resolve--> FastAPI --asyncio.gather--> 7 providers
                              |
                              +- normalise: 4K/1080p/720p, Hindi/Dual/English,
                              |             seeders, health, size
                              +- sort: language priority -> score -> seeders
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

`/fast-play` returns `{ "stream": {...}, "reason": "Hindi / Dual Audio - 1080p - 2,401 seeders", "alternatives": [...] }`
and picks **Hindi / Dual Audio first**, then direct playability, then seeders.

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

* **Direct HTTP(S)** streams (`.mp4`, `.m3u8`, `.mkv`, or any Real-Debrid link)
  go straight into the app's ExoPlayer. `/fast-play` ranks these first, before
  language/seeders, so a direct 720p stream beats a magnet-only 1080p one.
* `/fast-play` exposes `directAvailable`, and every stream carries
  `isDirect`/`is_direct`, `isMagnet` and `playable`. Magnet streams are always
  normalised to `isDirect: false` and `playable: false`.
* **Magnet links** cannot be opened by ExoPlayer. The Android app shows a clean
  chooser: open the torrent/VLC client, or switch to Server 1 (AutoEmbed) /
  Server 2 (VidSrc) inside the in-app WebView player.
  Configure `FASTPLAY_DEBRID_KEY` if you want one-tap in-app playback without an
  external torrent client.
* The scraped sites change domains often. Every mirror list above is
  environment-overridable, and each provider fails softly: one dead source
  never blocks the response.

## Tests

```powershell
python -m unittest discover -s server -p "test_*.py" -v
```

The suite covers quality/language/size parsing, Torrentio payloads (including
the current `infoHash`-only format), YTS parsing with a mocked payload,
TMDB/Cinemeta IMDb resolution, deduplication and FastPlay ranking. No network
required.

## Deploy free

The included `Dockerfile` works on Render / Railway / Koyeb:

```
Build:  docker
Port:   8000   (or set PORT=7860 on Hugging Face Spaces)
Health: /health
```

Then point the app at the public URL - no APK update needed:

`Firebase -> server_config -> fastplay_base_url = https://your-service.onrender.com`

### Android base URL

`FastPlayService` resolves the base URL in this order:

1. `FastPlayService.baseUrlOverride` (runtime QA override)
2. Firebase `server_config/fastplay_base_url`
3. `http://10.119.156.74:8000` (built-in default)

For a physical phone on the same Wi-Fi, make sure the laptop IP is reachable
and Windows Firewall allows port 8000. Update `fastplay_base_url` in Firebase
(or `baseUrlOverride`) when the IP changes.
