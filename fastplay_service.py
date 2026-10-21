"""FastPlay - multi-source movie / show stream resolver.

A tiny FastAPI microservice that fans out to several public sources in
parallel, normalises the result (quality, audio languages, seeders, size),
sorts by a health/quality score, and exposes:

    GET /resolve    -> every stream, sorted
    GET /fast-play  -> the single best stream (Hindi/Dual Audio first)
    GET /health     -> liveness + provider list

Run locally:
    pip install -r server/requirements.txt
    uvicorn fastplay_service:app --host 0.0.0.0 --port 8000 --reload

Environment (all optional):
    FASTPLAY_PROVIDER_TIMEOUT  per-provider timeout seconds (default 9)
    FASTPLAY_TOTAL_TIMEOUT     overall soft deadline seconds (default 6)
    FASTPLAY_CACHE_TTL         response cache seconds (default 300)
    FASTPLAY_MAX_RESULTS       max streams returned (default 60)
    FASTPLAY_DEBRID_KEY        Real-Debrid API key for Torrentio direct URLs
    TMDB_API_KEY               TMDB key used to auto-resolve IMDb ids
    TMDB_PROXY_URL             TMDB API proxy base (default /3 worker)
    CINEMETA_BASE              Cinemeta fallback base URL
    FASTPLAY_IMDB_CACHE_TTL    positive IMDb lookup cache seconds (default 86400)
    YTS_MIRRORS                comma separated YTS mirrors
    TGX_MIRRORS                comma separated TorrentGalaxy mirrors
    X1337_MIRRORS              comma separated 1337x mirrors
    TAMILMV_DOMAINS            comma separated 1TamilMV domains
    TAMILBLASTERS_DOMAINS      comma separated TamilBlasters domains
    HTTP_PROXY / HTTPS_PROXY   honoured automatically (trust_env)

NOTE: without a debrid key most sources return *magnet* links. ExoPlayer
cannot open a magnet URI; the Android app therefore hands magnets to an
installed torrent/VLC client and plays direct HTTP streams in-app.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import math
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, quote_plus, urljoin

import aiohttp
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

APP_VERSION = "1.0.0"
LOG = logging.getLogger("fastplay")

PROVIDER_TIMEOUT = float(os.getenv("FASTPLAY_PROVIDER_TIMEOUT", "9"))
TOTAL_TIMEOUT = float(os.getenv("FASTPLAY_TOTAL_TIMEOUT", "6"))
CACHE_TTL = int(os.getenv("FASTPLAY_CACHE_TTL", "300"))
MAX_RESULTS = int(os.getenv("FASTPLAY_MAX_RESULTS", "60"))

# IMDb auto-resolution. The key is the same public TMDB key the Android app
# already ships; override it with TMDB_API_KEY (or set it empty to disable TMDB
# and use the Cinemeta fallback only).
TMDB_PROXY_URL = os.getenv(
    "TMDB_PROXY_URL",
    "https://late-bar-1e40tmdb-proxy.rdevi162277.workers.dev/3",
).rstrip("/")
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "59fa8f04083deff6e81e42d34daf9524").strip()
CINEMETA_BASE = os.getenv("CINEMETA_BASE", "https://v3-cinemeta.strem.io").rstrip("/")
IMDB_CACHE_TTL = int(os.getenv("FASTPLAY_IMDB_CACHE_TTL", "86400"))
IMDB_NEGATIVE_TTL = int(os.getenv("FASTPLAY_IMDB_NEGATIVE_TTL", "600"))

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
}

QUALITY_ORDER = {
    "4K": 5,
    "1080p": 4,
    "720p": 3,
    "480p": 2,
    "360p": 1,
    "CAM": 0,
    "Unknown": 0,
}

QUALITY_SCORE = {
    "4K": 20,
    "1080p": 17,
    "720p": 12,
    "480p": 6,
    "360p": 3,
    "CAM": 1,
    "Unknown": 2,
}

QUALITY_RE = re.compile(
    r"(?<![a-z0-9])(2160p|4k|1080p|1080i|720p|576p|480p|360p|hdrip|camrip|cam|hdts|hdcam|dvdrip|webrip|web-dl|bluray|blu-ray)(?![a-z0-9])",
    re.IGNORECASE,
)
SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(TB|TIB|GB|GIB|MB|MIB|KB|KIB)\b", re.IGNORECASE)
SEEDERS_RE = re.compile(r"(?:👤|👥|seeders?\s*[:=]?)\s*(\d[\d,]*)", re.IGNORECASE)
PEERS_RE = re.compile(r"(?:🌱|peers?\s*[:=]?)\s*(\d[\d,]*)", re.IGNORECASE)
TORRENTIO_SIZE_RE = re.compile(r"(?:💾|size\s*[:=]?)\s*(\d+(?:[.,]\d+)?\s*(?:TB|GB|MB|KB))", re.IGNORECASE)
TORRENTIO_PROVIDER_RE = re.compile(r"(?:⚙️|⚙)\s*([^\n]+)", re.IGNORECASE)
# Slash separated ISO codes, e.g. "EN/ITA/FR/ES/DE" in Torrentio titles.
ISO_CODE_RUN_RE = re.compile(r"\b([A-Za-z]{2,3}(?:\s*[/.]\s*[A-Za-z]{2,3})+)\b")

LANG_ORDER = [
    "hindi", "english", "tamil", "telugu", "malayalam", "kannada", "marathi",
    "punjabi", "bengali", "gujarati", "urdu", "nepali", "sinhala",
    "japanese", "korean", "chinese", "french", "spanish", "german",
    "russian", "italian", "dutch", "portuguese", "turkish", "arabic",
]

LANG_ALIASES = {
    "en": "English", "hi": "Hindi", "ta": "Tamil", "te": "Telugu",
    "ml": "Malayalam", "kn": "Kannada", "mr": "Marathi", "pa": "Punjabi",
    "bn": "Bengali", "gu": "Gujarati", "ur": "Urdu",
    "fr": "French", "es": "Spanish", "de": "German", "it": "Italian",
    "ru": "Russian", "ja": "Japanese", "ko": "Korean", "zh": "Chinese",
    "pt": "Portuguese", "nl": "Dutch", "tr": "Turkish", "ar": "Arabic",
    "latino": "Spanish", "castellano": "Spanish", "esp": "Spanish",
    "hin": "Hindi", "eng": "English", "tam": "Tamil", "tel": "Telugu",
    "mal": "Malayalam", "kan": "Kannada", "mar": "Marathi", "pan": "Punjabi",
    "ben": "Bengali", "guj": "Gujarati", "urd": "Urdu", "jpn": "Japanese",
    "kor": "Korean", "chi": "Chinese", "zho": "Chinese", "fre": "French",
    "fra": "French", "spa": "Spanish", "ger": "German", "deu": "German",
    "rus": "Russian", "ita": "Italian", "dut": "Dutch", "nld": "Dutch",
    "por": "Portuguese", "tur": "Turkish", "ara": "Arabic",
}

LANG_RE = re.compile(
    r"(?<![a-z0-9])(" + "|".join(LANG_ORDER) + r"|dual[\s._-]?audio|multi[\s._-]?audio)(?![a-z0-9])",
    re.IGNORECASE,
)

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _size_to_bytes(amount: float, unit: str) -> int:
    unit = unit.upper()
    factor = {
        "TB": 1024 ** 4, "TIB": 1024 ** 4,
        "GB": 1024 ** 3, "GIB": 1024 ** 3,
        "MB": 1024 ** 2, "MIB": 1024 ** 2,
        "KB": 1024, "KIB": 1024,
    }.get(unit, 1)
    return int(amount * factor)


def _human_size(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "Unknown"
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit in ("B", "KB"):
                return f"{value:.0f} {unit}"
            pretty = f"{value:.2f}".rstrip("0").rstrip(".")
            return f"{pretty} {unit}"
        value /= 1024
    return "Unknown"


def parse_size(text: str) -> Tuple[str, int]:
    """Return (human size, bytes) from a title / row string."""
    match = SIZE_RE.search(text or "")
    if not match:
        return "Unknown", 0
    amount = float(match.group(1).replace(",", "."))
    unit = match.group(2)
    size_bytes = _size_to_bytes(amount, unit)
    return _human_size(size_bytes), size_bytes


def parse_quality(text: str) -> str:
    match = QUALITY_RE.search(text or "")
    if not match:
        return "Unknown"
    token = match.group(1).lower()
    if token in ("2160p", "4k"):
        return "4K"
    if token in ("1080p", "1080i"):
        return "1080p"
    if token == "720p":
        return "720p"
    if token in ("576p", "480p"):
        return "480p"
    if token in ("360p",):
        return "360p"
    if token in ("cam", "camrip", "hdcam", "hdts"):
        return "CAM"
    # WEB-DL / BluRay without a resolution -> let the caller's fallback win.
    return "Unknown"


def _clean_language(token: str) -> str:
    token = token.lower()
    if "dual" in token:
        return "Dual Audio"
    if "multi" in token:
        return "Multi Audio"
    return token.title()


def parse_languages(*texts: Any) -> List[str]:
    """Extract audio languages / dual-audio pills from one or more strings."""
    blob = " ".join(str(t) for t in texts if t)
    found: List[str] = []
    for match in LANG_RE.finditer(blob):
        cleaned = _clean_language(match.group(1))
        if cleaned not in found:
            found.append(cleaned)

    # Slash / dot separated language codes from release names: EN/ITA/FR/ES/DE
    for run in ISO_CODE_RUN_RE.finditer(blob):
        tokens = [token.strip() for token in re.split(r"\s*[/.]\s*", run.group(1))]
        mapped = [LANG_ALIASES.get(token.lower()) for token in tokens]
        known = [name for name in mapped if name]
        # Require a majority of the run to be real language codes so release
        # groups like "BEN.THE.MEN" do not turn into a Bengali pill.
        if tokens and len(known) / len(tokens) >= 0.5:
            for name in known:
                if name not in found:
                    found.append(name)

    # explicit ISO codes are common in API payloads ("en", "hi", "ta")
    for raw in texts:
        if not raw:
            continue
        for code in re.split(r"[,/|+]", str(raw)):
            code = code.strip().lower()
            if code in LANG_ALIASES and LANG_ALIASES[code] not in found:
                found.append(LANG_ALIASES[code])

    def sort_key(name: str) -> int:
        lowered = name.lower()
        if lowered == "hindi":
            return -3
        if lowered == "dual audio":
            return -2
        if lowered == "multi audio":
            return -1
        try:
            return LANG_ORDER.index(lowered)
        except ValueError:
            return 99

    return sorted(found, key=sort_key)


def language_priority(languages: Sequence[str]) -> int:
    """3 = Hindi / Dual, 2 = regional / multi, 1 = other, 0 = unknown."""
    lowered = {lang.lower() for lang in languages}
    if "hindi" in lowered or "dual audio" in lowered:
        return 3
    if "multi audio" in lowered or lowered & {"tamil", "telugu", "malayalam", "kannada", "punjabi", "bengali", "marathi"}:
        return 2
    if lowered:
        return 1
    return 0


def magnet_from_hash(info_hash: str, display_name: str = "FastPlay") -> str:
    trackers = "".join(f"&tr={quote(t, safe='')}" for t in TRACKERS)
    return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(display_name)}{trackers}"


def _health(seeders: int, peers: int) -> int:
    if seeders <= 0:
        return 0
    ratio = seeders / max(1, seeders + peers)
    seed_factor = min(1.0, seeders / 500.0)
    return int(round(min(1.0, ratio * 0.6 + seed_factor * 0.4) * 100))


def _score(stream: "Stream") -> float:
    score = 0.0
    score += {3: 32, 2: 22, 1: 10, 0: 0}[language_priority(stream.languages)]
    score += min(25.0, math.log10(stream.seeders + 1) * 8.0)
    score += QUALITY_SCORE.get(stream.quality, 2)
    score += 8.0 if stream.is_direct else 0.0
    score += stream.health * 0.15
    if stream.size_bytes:
        score += 2.0
    return round(score, 2)


def _clean_title_query(title: str, year: Optional[int]) -> str:
    cleaned = re.sub(r"\s+", " ", title or "").strip()
    # drop a trailing "(2019)" / "2019" because the year is passed separately
    cleaned = re.sub(r"\s*[\(\[]?(?:19|20)\d{2}[\)\]]?\s*$", "", cleaned).strip()
    if year:
        return f"{cleaned} {year}"
    return cleaned


# ---------------------------------------------------------------------------
# model
# ---------------------------------------------------------------------------

@dataclass
class Context:
    title: str = ""
    year: Optional[int] = None
    imdb: Optional[str] = None
    media_type: str = "movie"
    season: int = 1
    episode: int = 1
    imdb_source: str = ""

    @property
    def search_query(self) -> str:
        return _clean_title_query(self.title, self.year)

    @property
    def is_tv(self) -> bool:
        return self.media_type in ("tv", "show", "series", "episode")


@dataclass
class Stream:
    provider: str
    title: str
    quality: str = "Unknown"
    languages: List[str] = field(default_factory=list)
    seeders: int = 0
    peers: int = 0
    size_bytes: int = 0
    size: str = "Unknown"
    magnet: str = ""
    url: str = ""
    is_direct: bool = False
    info_hash: str = ""
    source_type: str = "torrent"
    headers: Dict[str, str] = field(default_factory=dict)
    health: int = 0
    score: float = 0.0

    def __post_init__(self) -> None:
        if not self.languages:
            self.languages = parse_languages(self.title)
        if self.quality == "Unknown":
            self.quality = parse_quality(self.title)
        if not self.size_bytes and self.size == "Unknown":
            self.size, self.size_bytes = parse_size(self.title)
        self.health = _health(self.seeders, self.peers)
        self.score = _score(self)

    @property
    def playback_url(self) -> str:
        if self.is_direct and self.url:
            return self.url
        return self.magnet or self.url

    @property
    def playable(self) -> bool:
        return self.is_direct and self.url.lower().startswith(("http://", "https://"))

    def identity(self) -> str:
        if self.info_hash:
            return self.info_hash.lower()
        return f"{self.provider}:{(self.title or '').lower().strip()}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": f"{self.provider.lower()}:{self.identity()}",
            "provider": self.provider,
            "title": self.title,
            "quality": self.quality,
            "resolution": self.quality,
            "languages": list(self.languages),
            "seeders": self.seeders,
            "peers": self.peers,
            "size": self.size,
            "sizeBytes": self.size_bytes,
            "magnet": self.magnet,
            "streamUrl": self.playback_url,
            "url": self.playback_url,
            "isDirect": self.is_direct,
            "is_direct": self.is_direct,
            "isMagnet": bool(self.magnet and not self.is_direct),
            "playable": self.playable,
            "sourceType": self.source_type,
            "infoHash": self.info_hash,
            "headers": dict(self.headers),
            "health": self.health,
            "score": self.score,
        }


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _get_text(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    try:
        merged = dict(DEFAULT_HEADERS)
        if headers:
            merged.update(headers)
        async with session.get(
            url,
            params=params,
            headers=merged,
            timeout=aiohttp.ClientTimeout(total=PROVIDER_TIMEOUT),
            allow_redirects=True,
        ) as response:
            if response.status >= 400:
                LOG.debug("GET %s -> HTTP %s", url, response.status)
                return None
            return await response.text(errors="ignore")
    except Exception as exc:  # network / TLS / timeout - provider is best effort
        LOG.debug("GET %s failed: %s", url, exc)
        return None


async def _get_json(session: aiohttp.ClientSession, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    text = await _get_text(session, url, params=params, headers={"Accept": "application/json"})
    if not text:
        return None
    import json

    try:
        return json.loads(text)
    except ValueError:
        LOG.debug("Non-JSON response from %s", url)
        return None


def _mirrors(env_key: str, defaults: Sequence[str]) -> List[str]:
    raw = os.getenv(env_key, "").strip()
    values = [v.strip().rstrip("/") for v in raw.split(",") if v.strip()] if raw else []
    return values or list(defaults)


# ---------------------------------------------------------------------------
# IMDb auto-resolution (TMDB proxy -> Cinemeta fallback)
# ---------------------------------------------------------------------------

_IMDB_CACHE: Dict[str, Tuple[float, str]] = {}


def _norm_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _year_of(value: Any) -> Optional[int]:
    match = re.search(r"\b(18|19|20|21)\d{2}\b", str(value or ""))
    return int(match.group(0)) if match else None


def _valid_imdb(value: Any) -> Optional[str]:
    match = re.search(r"(tt\d{5,})", str(value or ""), re.IGNORECASE)
    return match.group(1).lower() if match else None


def _imdb_cache_key(ctx: Context) -> str:
    clean = _clean_title_query(ctx.title, None) or ctx.title
    return f"{ctx.media_type}|{_norm_title(clean)}|{ctx.year or ''}"


def _imdb_cache_get(key: str) -> Tuple[bool, Optional[str]]:
    entry = _IMDB_CACHE.get(key)
    if not entry:
        return False, None
    expires_at, value = entry
    if time.time() > expires_at:
        _IMDB_CACHE.pop(key, None)
        return False, None
    return True, value or None


def _imdb_cache_set(key: str, value: str, ttl: int) -> None:
    if len(_IMDB_CACHE) > 1000:
        for old_key, _ in sorted(_IMDB_CACHE.items(), key=lambda kv: kv[1][0])[:200]:
            _IMDB_CACHE.pop(old_key, None)
    _IMDB_CACHE[key] = (time.time() + max(0, ttl), value)


def _pick_tmdb_result(results: Sequence[Dict[str, Any]], ctx: Context) -> Optional[Dict[str, Any]]:
    """Prefer exact title + year, then same year, then exact title, then first."""
    target = _norm_title(_clean_title_query(ctx.title, None) or ctx.title)
    exact: List[Dict[str, Any]] = []
    same_year: List[Dict[str, Any]] = []
    usable: List[Dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        usable.append(item)
        name = item.get("title") or item.get("name") or ""
        item_year = _year_of(item.get("release_date") or item.get("first_air_date"))
        if ctx.year is not None and item_year == ctx.year:
            same_year.append(item)
        if _norm_title(name) == target:
            if ctx.year is None or item_year == ctx.year:
                return item
            exact.append(item)
    if same_year:
        return same_year[0]
    if exact:
        return exact[0]
    return usable[0] if usable else None


async def _tmdb_imdb(session: aiohttp.ClientSession, ctx: Context) -> Optional[str]:
    if not TMDB_API_KEY:
        return None
    kind = "tv" if ctx.is_tv else "movie"
    query = _clean_title_query(ctx.title, None) or ctx.title
    params: Dict[str, Any] = {
        "api_key": TMDB_API_KEY,
        "query": query,
        "include_adult": "false",
        "language": "en-US",
    }
    if ctx.year:
        params["first_air_date_year" if ctx.is_tv else "year"] = ctx.year
    payload = await _get_json(session, f"{TMDB_PROXY_URL}/search/{kind}", params=params)
    results = payload.get("results") if isinstance(payload, dict) else None
    if not results and ctx.year:
        # TMDB's year filter is strict (release vs festival year); retry once
        # without it and let the title matcher decide.
        params.pop("year", None)
        params.pop("first_air_date_year", None)
        payload = await _get_json(session, f"{TMDB_PROXY_URL}/search/{kind}", params=params)
        results = payload.get("results") if isinstance(payload, dict) else None
    if not results:
        return None
    chosen = _pick_tmdb_result(results, ctx)
    tmdb_id = chosen.get("id") if chosen else None
    if not tmdb_id:
        return None
    external = await _get_json(
        session,
        f"{TMDB_PROXY_URL}/{kind}/{tmdb_id}/external_ids",
        params={"api_key": TMDB_API_KEY},
    )
    imdb = _valid_imdb(external.get("imdb_id") if isinstance(external, dict) else None)
    if imdb:
        LOG.info("TMDB resolved %r (%s) -> %s", ctx.title, ctx.year or "?", imdb)
    return imdb


def _pick_cinemeta_result(metas: Sequence[Dict[str, Any]], ctx: Context) -> Optional[str]:
    target = _norm_title(_clean_title_query(ctx.title, None) or ctx.title)
    fallback: Optional[str] = None
    for meta in metas:
        if not isinstance(meta, dict):
            continue
        imdb = _valid_imdb(meta.get("imdb_id"))
        if not imdb:
            continue
        fallback = fallback or imdb
        name = meta.get("name") or meta.get("title") or ""
        item_year = _year_of(meta.get("releaseInfo") or meta.get("year"))
        if _norm_title(name) == target and (ctx.year is None or item_year == ctx.year):
            return imdb
    return fallback


async def _cinemeta_imdb(session: aiohttp.ClientSession, ctx: Context) -> Optional[str]:
    kind = "series" if ctx.is_tv else "movie"
    query = _clean_title_query(ctx.title, None) or ctx.title
    search = quote(query.strip(), safe="")
    # Cinemeta expects the search term in the path; ?search= is ignored.
    payload = await _get_json(session, f"{CINEMETA_BASE}/catalog/{kind}/top/search={search}.json")
    metas = payload.get("metas") if isinstance(payload, dict) else None
    if not metas:
        # Spec-style query-parameter form as a last resort (filtered locally).
        payload = await _get_json(
            session,
            f"{CINEMETA_BASE}/catalog/{kind}/top.json",
            params={"search": query},
        )
        metas = payload.get("metas") if isinstance(payload, dict) else None
    imdb = _pick_cinemeta_result(metas or [], ctx)
    if imdb:
        LOG.info("Cinemeta resolved %r (%s) -> %s", ctx.title, ctx.year or "?", imdb)
    return imdb


async def resolve_imdb_id(
    session: aiohttp.ClientSession, ctx: Context
) -> Tuple[Optional[str], str]:
    """Return (imdb_id, source) for a title-only context."""
    if ctx.imdb:
        return ctx.imdb, "request"
    if not ctx.title:
        return None, ""
    imdb = await _tmdb_imdb(session, ctx)
    if imdb:
        return imdb, "tmdb"
    imdb = await _cinemeta_imdb(session, ctx)
    if imdb:
        return imdb, "cinemeta"
    return None, ""


async def ensure_imdb(ctx: Context) -> None:
    """Fill ctx.imdb in place, using a small positive/negative lookup cache."""
    if ctx.imdb:
        ctx.imdb_source = ctx.imdb_source or "request"
        return
    if not ctx.title:
        return
    key = _imdb_cache_key(ctx)
    hit, cached = _imdb_cache_get(key)
    if hit:
        if cached:
            ctx.imdb = cached
            ctx.imdb_source = "cache"
        return
    try:
        async with aiohttp.ClientSession(trust_env=True) as session:
            imdb, source = await resolve_imdb_id(session, ctx)
    except Exception as exc:  # a lookup failure must never break the resolver
        LOG.info("IMDb lookup for %r failed: %s", ctx.title, exc)
        imdb, source = None, ""
    if imdb:
        ctx.imdb = imdb
        ctx.imdb_source = source
        _imdb_cache_set(key, imdb, IMDB_CACHE_TTL)
    else:
        _imdb_cache_set(key, "", IMDB_NEGATIVE_TTL)
        LOG.info("No IMDb id found for %r (%s)", ctx.title, ctx.year or "?")


# ---------------------------------------------------------------------------
# providers
# ---------------------------------------------------------------------------

async def fetch_yts(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    """YTS public JSON API - high quality movie torrents with real seeders."""
    if ctx.is_tv:
        return []
    query = ctx.imdb or _clean_title_query(ctx.title, None) or ctx.title
    if not query:
        return []
    bases = _mirrors("YTS_MIRRORS", ("https://yts.mx", "https://yts.lt", "https://yts.am", "https://yts.rs"))
    payload = None
    for base in bases:
        payload = await _get_json(
            session,
            f"{base}/api/v2/list_movies.json",
            params={"query_term": query, "limit": 20, "sort_by": "seeds"},
        )
        if payload:
            break
    if not isinstance(payload, dict):
        return []

    movies = ((payload.get("data") or {}).get("movies") or []) if isinstance(payload.get("data"), dict) else []
    streams: List[Stream] = []
    for movie in movies:
        if not isinstance(movie, dict):
            continue
        movie_imdb = str(movie.get("imdb_code") or "").lower()
        if ctx.imdb and movie_imdb and movie_imdb != ctx.imdb:
            continue
        try:
            movie_year = int(movie.get("year")) if movie.get("year") else None
        except (TypeError, ValueError):
            movie_year = None
        if ctx.year and movie_year and abs(movie_year - ctx.year) > 1:
            continue

        base_title = movie.get("title_long") or f"{movie.get('title', ctx.title)} ({movie_year or ''})"
        languages = parse_languages(movie.get("language"), base_title)
        for torrent in movie.get("torrents") or []:
            if not isinstance(torrent, dict):
                continue
            info_hash = str(torrent.get("hash") or "").strip()
            if not info_hash:
                continue
            quality = str(torrent.get("quality") or "") or parse_quality(base_title)
            if quality.lower() in ("3d",):
                continue
            quality = parse_quality(quality) or quality
            codec = torrent.get("video_codec") or ""
            kind = torrent.get("type") or "WEB"
            title = f"{base_title} [{quality}] [{kind}]" + (f" [{codec}]" if codec else "")
            size_bytes = _to_int(torrent.get("size_bytes"))
            if size_bytes <= 0:
                _, size_bytes = parse_size(str(torrent.get("size") or ""))
            size = str(torrent.get("size") or "") or _human_size(size_bytes)
            streams.append(
                Stream(
                    provider="YTS",
                    title=title,
                    quality=quality,
                    languages=languages,
                    seeders=_to_int(torrent.get("seeds")),
                    peers=_to_int(torrent.get("peers")),
                    size_bytes=size_bytes,
                    size=size,
                    magnet=magnet_from_hash(info_hash, title),
                    url=str(torrent.get("url") or ""),
                    info_hash=info_hash,
                    source_type="torrent",
                )
            )
    return streams


def _parse_torrentio_stream(item: Dict[str, Any]) -> Optional[Stream]:
    raw_name = str(item.get("name") or "")
    raw_title = str(item.get("title") or "")
    lines = [line.strip() for line in raw_title.splitlines() if line.strip()]
    display = lines[0] if lines else (str((item.get("behaviorHints") or {}).get("filename") or raw_name))
    meta = " ".join(lines[1:]) if len(lines) > 1 else raw_title

    url = str(item.get("url") or "")
    info_hash = str(item.get("infoHash") or "").strip()
    if info_hash and not re.fullmatch(r"[a-fA-F0-9]{32,40}", info_hash):
        info_hash = ""

    magnet = ""
    is_direct = False
    if url.lower().startswith("magnet:"):
        magnet = url
        hash_match = re.search(r"btih:([a-zA-Z0-9]{32,40})", url, re.IGNORECASE)
        info_hash = info_hash or (hash_match.group(1) if hash_match else "")
    elif re.fullmatch(r"[a-fA-F0-9]{40}", url):
        info_hash = info_hash or url
        magnet = magnet_from_hash(info_hash, display)
    elif url.lower().startswith(("http://", "https://")):
        is_direct = True

    if not is_direct and not magnet and info_hash:
        # Current Torrentio payloads carry only `infoHash` + `fileIdx`.
        magnet = magnet_from_hash(info_hash, display)
    if not is_direct and not magnet:
        return None

    provider_match = TORRENTIO_PROVIDER_RE.search(meta) or TORRENTIO_PROVIDER_RE.search(raw_title)
    provider = "Torrentio"
    if provider_match:
        raw_provider = re.split(r"\s*/\s*", provider_match.group(1))[0]
        ascii_provider = re.sub(r"[^\x20-\x7E]", " ", raw_provider)
        ascii_provider = re.sub(r"\s+", " ", ascii_provider).strip(" -|")
        if ascii_provider:
            provider = ascii_provider[:40]

    seeders_match = SEEDERS_RE.search(meta) or SEEDERS_RE.search(raw_title)
    seeders = _to_int(seeders_match.group(1)) if seeders_match else 0
    peers_match = PEERS_RE.search(meta)
    peers = _to_int(peers_match.group(1)) if peers_match else 0
    size_match = TORRENTIO_SIZE_RE.search(meta) or TORRENTIO_SIZE_RE.search(raw_title)
    size_text = size_match.group(1) if size_match else ""
    size, size_bytes = parse_size(size_text)
    if not size_bytes:
        size, size_bytes = parse_size(display)

    quality = parse_quality(display) if parse_quality(display) != "Unknown" else parse_quality(raw_name)
    if quality == "Unknown":
        quality = parse_quality(raw_title)

    return Stream(
        provider=provider,
        title=display,
        quality=quality,
        languages=parse_languages(display, meta),
        seeders=seeders,
        peers=peers,
        size_bytes=size_bytes,
        size=size,
        magnet=magnet,
        url=url,
        is_direct=is_direct,
        info_hash=info_hash,
        source_type="http" if is_direct else "torrent",
    )


async def fetch_torrentio(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    """Torrentio aggregator (YTS, EZTV, 1337x, TorrentGalaxy, RARBG, ...)."""
    if not ctx.imdb:
        return []
    base = os.getenv("TORRENTIO_BASE", "https://torrentio.strem.fun").rstrip("/")
    debrid = os.getenv("FASTPLAY_DEBRID_KEY", "").strip()
    prefix = f"realdebrid={quote(debrid, safe='')}/" if debrid else ""
    kind = "series" if ctx.is_tv else "movie"
    payload = await _get_json(session, f"{base}/{prefix}stream/{kind}/{ctx.imdb}.json")
    if not isinstance(payload, dict):
        return []

    streams: List[Stream] = []
    for item in payload.get("streams") or []:
        if not isinstance(item, dict):
            continue
        stream = _parse_torrentio_stream(item)
        if stream:
            streams.append(stream)
    return streams


async def fetch_eztv(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    """EZTV - TV episodes. Uses the numeric IMDb id the API expects."""
    if not ctx.is_tv:
        return []
    if not ctx.imdb:
        return []
    numeric = ctx.imdb[2:] if ctx.imdb.startswith("tt") else ctx.imdb
    if not numeric.isdigit():
        return []

    bases = _mirrors("EZTV_MIRRORS", ("https://eztv.re", "https://eztv.wf", "https://eztv.tf", "https://eztv.yt"))
    payload = None
    for base in bases:
        payload = await _get_json(session, f"{base}/api/get-torrents", params={"imdb_id": numeric, "limit": 100})
        if isinstance(payload, dict):
            break
    if not isinstance(payload, dict):
        return []

    streams: List[Stream] = []
    for torrent in payload.get("torrents") or []:
        if not isinstance(torrent, dict):
            continue
        title = str(torrent.get("title") or torrent.get("filename") or "").strip()
        if not title:
            continue
        if ctx.season and ctx.episode:
            if f"S{ctx.season:02d}E{ctx.episode:02d}".lower() not in title.lower():
                # keep pack / complete-season releases, drop other episodes
                if not re.search(rf"s{ctx.season:02d}(?!e\d)", title, re.IGNORECASE):
                    continue
        magnet = str(torrent.get("magnet_url") or "")
        if not magnet:
            continue
        hash_match = re.search(r"btih:([a-zA-Z0-9]{32,40})", magnet, re.IGNORECASE)
        streams.append(
            Stream(
                provider="EZTV",
                title=title,
                quality=parse_quality(title),
                languages=parse_languages(title),
                seeders=_to_int(torrent.get("seeds")),
                peers=_to_int(torrent.get("peers")),
                size_bytes=_to_int(torrent.get("size_bytes")),
                size=_human_size(_to_int(torrent.get("size_bytes"))),
                magnet=magnet,
                info_hash=hash_match.group(1) if hash_match else "",
                source_type="torrent",
            )
        )
    return streams


def _row_seeders(row) -> int:
    for selector in ("td.seeds", "font.green", ".seeders", "span.seeders"):
        node = row.select_one(selector)
        if node and node.get_text(strip=True):
            value = _to_int(re.sub(r"[^\d]", "", node.get_text(strip=True)))
            if value:
                return value
    return 0


async def fetch_1337x(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    """1337x search + detail page (Cloudflare usually blocks plain HTTP)."""
    query = ctx.search_query
    if not query:
        return []
    bases = _mirrors("X1337_MIRRORS", ("https://1337x.to", "https://1337x.st", "https://x1337x.ws", "https://1337x.gd"))
    soup: Optional[BeautifulSoup] = None
    active_base = ""
    for base in bases:
        text = await _get_text(session, f"{base}/search/{quote(query)}/1/")
        if text:
            candidate_soup = BeautifulSoup(text, "html.parser")
            if candidate_soup.select("table.table-list tbody tr"):
                soup = candidate_soup
                active_base = base.rstrip("/")
                break
    if soup is None or not active_base:
        return []

    candidates: List[Dict[str, Any]] = []
    for row in soup.select("table.table-list tbody tr")[:8]:
        anchors = row.select("td.name a")
        if len(anchors) < 2:
            continue
        href = str(anchors[-1].get("href") or "")
        if not href:
            continue
        title = anchors[-1].get_text(" ", strip=True)
        seeds_node = row.select_one("td.seeds")
        leech_node = row.select_one("td.leeches")
        size_node = row.select_one("td.size")
        candidates.append(
            {
                "url": urljoin(active_base, href),
                "title": title,
                "seeders": _to_int(seeds_node.get_text(strip=True)) if seeds_node else 0,
                "peers": _to_int(leech_node.get_text(strip=True)) if leech_node else 0,
                "size_text": size_node.get_text(strip=True) if size_node else "",
            }
        )

    streams: List[Stream] = []

    async def load_detail(candidate: Dict[str, Any]) -> None:
        page = await _get_text(session, str(candidate["url"]))
        if not page:
            return
        detail = BeautifulSoup(page, "html.parser")
        magnet_anchor = detail.select_one('a[href^="magnet:"]')
        if not magnet_anchor:
            return
        magnet = str(magnet_anchor.get("href") or "")
        heading = detail.select_one("#info h1") or detail.select_one("div.box-info-heading h1") or detail.select_one("h1")
        title = heading.get_text(" ", strip=True) if heading else candidate["title"]
        size, size_bytes = parse_size(candidate.get("size_text") or "")
        hash_match = re.search(r"btih:([a-zA-Z0-9]{32,40})", magnet, re.IGNORECASE)
        streams.append(
            Stream(
                provider="1337x",
                title=title,
                quality=parse_quality(title),
                languages=parse_languages(title),
                seeders=int(candidate["seeders"]),
                peers=int(candidate["peers"]),
                size_bytes=size_bytes,
                size=size,
                magnet=magnet,
                info_hash=hash_match.group(1) if hash_match else "",
                source_type="torrent",
            )
        )

    semaphore = asyncio.Semaphore(4)

    async def guarded(candidate: Dict[str, Any]) -> None:
        async with semaphore:
            await load_detail(candidate)

    await asyncio.gather(*(guarded(c) for c in candidates), return_exceptions=True)
    return streams


async def fetch_torrentgalaxy(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    """TorrentGalaxy search rows carry magnets directly (no detail fetch)."""
    query = ctx.search_query
    if not query:
        return []
    bases = _mirrors("TGX_MIRRORS", ("https://torrentgalaxy.to", "https://torrentgalaxy.mx", "https://tgx.rs"))
    soup: Optional[BeautifulSoup] = None
    for base in bases:
        text = await _get_text(
            session,
            f"{base}/torrents.php",
            params={"search": query, "sort": "seeders", "order": "desc"},
        )
        if text:
            soup = BeautifulSoup(text, "html.parser")
            if soup.select("div.tgxtablerow"):
                break
    if soup is None:
        return []

    streams: List[Stream] = []
    for row in soup.select("div.tgxtablerow")[:20]:
        magnet_anchor = row.select_one('a[href^="magnet:"]')
        if not magnet_anchor:
            continue
        magnet = str(magnet_anchor.get("href") or "")
        title_anchor = row.select_one('a[href*="/torrent/"]') or row.select_one("a.txlight")
        title = title_anchor.get_text(" ", strip=True) if title_anchor else ""
        if not title:
            title = row.get_text(" ", strip=True)
        row_text = row.get_text(" ", strip=True)
        size, size_bytes = parse_size(row_text)
        hash_match = re.search(r"btih:([a-zA-Z0-9]{32,40})", magnet, re.IGNORECASE)
        streams.append(
            Stream(
                provider="TorrentGalaxy",
                title=title[:300],
                quality=parse_quality(title),
                languages=parse_languages(title),
                seeders=_row_seeders(row),
                peers=0,
                size_bytes=size_bytes,
                size=size,
                magnet=magnet,
                info_hash=hash_match.group(1) if hash_match else "",
                source_type="torrent",
            )
        )
    return streams


async def _fetch_invision_source(
    session: aiohttp.ClientSession,
    ctx: Context,
    provider: str,
    bases: Sequence[str],
) -> List[Stream]:
    """Shared scraper for 1TamilMV / TamilBlasters (Invision Community)."""
    query = ctx.search_query
    if not query:
        return []

    search_soup: Optional[BeautifulSoup] = None
    active_base = ""
    for base in bases:
        search_url = (
            f"{base.rstrip('/')}/index.php?/search/&q={quote_plus(query)}"
            "&quick=1&type=forums_topic"
        )
        text = await _get_text(session, search_url)
        if text:
            candidate_soup = BeautifulSoup(text, "html.parser")
            if candidate_soup.select('a[href*="/topic/"]'):
                search_soup = candidate_soup
                active_base = base.rstrip("/")
                break
    if search_soup is None or not active_base:
        return []

    topic_links: List[Tuple[str, str]] = []
    seen = set()
    for anchor in search_soup.select('a[href*="/topic/"], a[href*="/forums/topic/"]'):
        href = str(anchor.get("href") or "")
        title = anchor.get_text(" ", strip=True)
        if not href or len(title) < 4 or href in seen:
            continue
        seen.add(href)
        topic_links.append((urljoin(active_base, href), title))
        if len(topic_links) >= 6:
            break
    if not topic_links:
        return []

    streams: List[Stream] = []
    semaphore = asyncio.Semaphore(3)

    async def load_topic(href: str, search_title: str) -> None:
        async with semaphore:
            page = await _get_text(session, href)
            if not page:
                return
            detail = BeautifulSoup(page, "html.parser")
            heading = detail.select_one("h1") or detail.select_one(".ipsType_pageTitle")
            title = heading.get_text(" ", strip=True) if heading else search_title
            for anchor in detail.select('a[href^="magnet:"]')[:3]:
                magnet = str(anchor.get("href") or "")
                if not magnet:
                    continue
                hash_match = re.search(r"btih:([a-zA-Z0-9]{32,40})", magnet, re.IGNORECASE)
                # seeders are not exposed before opening a magnet client
                streams.append(
                    Stream(
                        provider=provider,
                        title=title,
                        quality=parse_quality(title),
                        languages=parse_languages(title),
                        seeders=0,
                        peers=0,
                        magnet=magnet,
                        info_hash=hash_match.group(1) if hash_match else "",
                        source_type="torrent",
                    )
                )

    await asyncio.gather(*(load_topic(href, title) for href, title in topic_links), return_exceptions=True)
    return streams


async def fetch_tamilmv(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    bases = _mirrors(
        "TAMILMV_DOMAINS",
        ("https://www.1tamilmv.win", "https://www.1tamilmv.lol", "https://www.1tamilmv.dog", "https://www.1tamilmv.com"),
    )
    return await _fetch_invision_source(session, ctx, "1TamilMV", bases)


async def fetch_tamilblasters(session: aiohttp.ClientSession, ctx: Context) -> List[Stream]:
    bases = _mirrors(
        "TAMILBLASTERS_DOMAINS",
        ("https://tamilblasters.buzz", "https://tamilblasters.rodeo", "https://tamilblasters.art", "https://tamilblasters.xyz"),
    )
    return await _fetch_invision_source(session, ctx, "TamilBlasters", bases)


PROVIDERS: List[Tuple[str, Any]] = [
    ("YTS", fetch_yts),
    ("Torrentio", fetch_torrentio),
    ("1337x", fetch_1337x),
    ("TorrentGalaxy", fetch_torrentgalaxy),
    ("EZTV", fetch_eztv),
    ("1TamilMV", fetch_tamilmv),
    ("TamilBlasters", fetch_tamilblasters),
]


# ---------------------------------------------------------------------------
# aggregator + cache
# ---------------------------------------------------------------------------

_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _cache_key(ctx: Context) -> str:
    return "|".join(
        [
            (ctx.imdb or "").lower(),
            re.sub(r"\s+", " ", ctx.title.lower()).strip(),
            str(ctx.year or ""),
            ctx.media_type,
            str(ctx.season),
            str(ctx.episode),
        ]
    )


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    entry = _CACHE.get(key)
    if not entry:
        return None
    expires_at, payload = entry
    if time.time() > expires_at:
        _CACHE.pop(key, None)
        return None
    cached = copy.deepcopy(payload)
    cached["cached"] = True
    return cached


def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    if CACHE_TTL <= 0:
        return
    if len(_CACHE) > 200:
        # drop the 50 oldest entries
        for old_key, _ in sorted(_CACHE.items(), key=lambda kv: kv[1][0])[:50]:
            _CACHE.pop(old_key, None)
    _CACHE[key] = (time.time() + CACHE_TTL, copy.deepcopy(payload))


def _dedupe(streams: Iterable[Stream]) -> List[Stream]:
    best: Dict[str, Stream] = {}
    for stream in streams:
        key = stream.identity()
        existing = best.get(key)
        if existing is None:
            best[key] = stream
            continue
        # prefer the same torrent with real seed counts / direct playability
        if (stream.playable and not existing.playable) or stream.seeders > existing.seeders:
            best[key] = stream
        elif stream.seeders == existing.seeders and stream.score > existing.score:
            best[key] = stream
    return list(best.values())


def _sort_streams(streams: List[Stream]) -> List[Stream]:
    return sorted(
        streams,
        key=lambda s: (
            -language_priority(s.languages),
            -s.score,
            -s.seeders,
            -QUALITY_ORDER.get(s.quality, 0),
            s.title.lower(),
        ),
    )


async def _run_provider(name: str, fn: Any, session: aiohttp.ClientSession, ctx: Context) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        streams = await asyncio.wait_for(fn(session, ctx), timeout=PROVIDER_TIMEOUT + 4)
        if not isinstance(streams, list):
            streams = []
        return {
            "name": name,
            "ok": True,
            "count": len(streams),
            "ms": int((time.perf_counter() - started) * 1000),
            "streams": streams,
        }
    except Exception as exc:
        LOG.info("provider %s failed: %s", name, exc)
        return {
            "name": name,
            "ok": False,
            "count": 0,
            "ms": int((time.perf_counter() - started) * 1000),
            "error": str(exc)[:200],
            "streams": [],
        }


def build_context(
    imdb: Optional[str],
    title: str,
    year: Optional[int],
    media_type: str = "movie",
    season: int = 1,
    episode: int = 1,
) -> Optional[Context]:
    imdb = (imdb or "").strip().lower()
    if imdb:
        match = re.search(r"(tt\d{5,})", imdb)
        imdb = match.group(1) if match else ""
    clean_title = re.sub(r"\s+", " ", (title or "")).strip()[:200]
    if not imdb and not clean_title:
        return None
    if year is not None and not (1888 <= int(year) <= 2100):
        year = None
    media_type = (media_type or "movie").strip().lower()
    if media_type not in ("movie", "tv", "show", "series", "episode"):
        media_type = "movie"
    return Context(
        title=clean_title,
        year=year,
        imdb=imdb or None,
        media_type=media_type,
        season=max(1, int(season or 1)),
        episode=max(1, int(episode or 1)),
    )


async def aggregate(ctx: Context) -> Dict[str, Any]:
    # Torrentio (and other IMDb-keyed providers) need a real tt id. Resolve it
    # from the title before the cache key so title-only and IMDb requests share
    # the same cached payload.
    await ensure_imdb(ctx)
    key = _cache_key(ctx)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    started = time.perf_counter()
    connector = aiohttp.TCPConnector(limit=40, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        tasks = {
            asyncio.create_task(_run_provider(name, fn, session, ctx)): name
            for name, fn in PROVIDERS
        }
        done, pending = await asyncio.wait(tasks.keys(), timeout=TOTAL_TIMEOUT)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        reports: List[Dict[str, Any]] = []
        for task, name in tasks.items():
            if task in done and not task.cancelled():
                try:
                    reports.append(task.result())
                except Exception as exc:  # defensive: provider must never break the response
                    reports.append(
                        {"name": name, "ok": False, "count": 0, "ms": 0,
                         "error": str(exc)[:200], "streams": []}
                    )
            else:
                reports.append(
                    {"name": name, "ok": False, "count": 0,
                     "ms": int(TOTAL_TIMEOUT * 1000), "error": "timed out", "streams": []}
                )

    collected: List[Stream] = []
    providers_meta: List[Dict[str, Any]] = []
    for report in reports:
        collected.extend(report.pop("streams", []))
        providers_meta.append(report)

    deduped = _dedupe(collected)
    # Drop obvious samples / mislabelled 60 MB "movies" before ranking.
    min_bytes = (30 if ctx.is_tv else 80) * 1024 * 1024
    deduped = [
        stream for stream in deduped
        if not (0 < stream.size_bytes < min_bytes)
        and not (re.search(r"\bsample\b", stream.title, re.IGNORECASE) and 0 < stream.size_bytes < 500 * 1024 * 1024)
    ]
    streams = _sort_streams(deduped)[:MAX_RESULTS]
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    payload = {
        "success": True,
        "version": APP_VERSION,
        "query": {
            "imdb": ctx.imdb,
            "imdbSource": ctx.imdb_source or ("request" if ctx.imdb else ""),
            "title": ctx.title,
            "year": ctx.year,
            "type": ctx.media_type,
            "season": ctx.season,
            "episode": ctx.episode,
        },
        "count": len(streams),
        "elapsedMs": elapsed_ms,
        "cached": False,
        "providers": providers_meta,
        "streams": [s.to_dict() for s in streams],
    }
    _cache_set(key, payload)
    return payload


def _is_http_playable(stream: Dict[str, Any]) -> bool:
    """True only for direct HTTP(S) media ExoPlayer can open without a torrent client."""
    if not stream.get("playable") and not stream.get("isDirect") and not stream.get("is_direct"):
        return False
    url = str(stream.get("streamUrl") or stream.get("url") or "").lower()
    return url.startswith(("http://", "https://"))


def _normalise_stream_flags(stream: Dict[str, Any]) -> Dict[str, Any]:
    """Keep isDirect/playable honest, especially for magnet-only streams."""
    url = str(stream.get("streamUrl") or stream.get("url") or "")
    lower = url.lower()
    if lower.startswith("magnet:"):
        stream["isDirect"] = False
        stream["is_direct"] = False
        stream["isMagnet"] = True
        stream["playable"] = False
        stream["sourceType"] = "torrent"
        if not stream.get("magnet"):
            stream["magnet"] = url
    elif lower.startswith(("http://", "https://")):
        direct = bool(stream.get("isDirect") or stream.get("is_direct") or stream.get("playable"))
        stream["isDirect"] = direct
        stream["is_direct"] = direct
        stream["isMagnet"] = False
        stream["playable"] = direct
        if direct:
            stream["sourceType"] = "http"
    else:
        stream["isDirect"] = False
        stream["is_direct"] = False
        stream["isMagnet"] = False
        stream["playable"] = False
    return stream


def pick_best(streams: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not streams:
        return None

    def rank(stream: Dict[str, Any]) -> Tuple[int, int, int, int, float]:
        # Direct HTTP/HLS first: ExoPlayer can start it in-app without a torrent
        # client. Language, seeders, quality and score only break ties within
        # the same playability class.
        direct = 1 if _is_http_playable(stream) else 0
        priority = language_priority(stream.get("languages") or [])
        return (
            direct,
            priority,
            int(stream.get("seeders") or 0),
            QUALITY_ORDER.get(str(stream.get("quality") or "Unknown"), 0),
            float(stream.get("score") or 0),
        )

    return max(streams, key=rank)


def _reason(stream: Dict[str, Any]) -> str:
    parts: List[str] = []
    priority = language_priority(stream.get("languages") or [])
    if priority >= 3:
        parts.append("Hindi / Dual Audio")
    elif priority == 2:
        parts.append("Regional / Multi Audio")
    quality = stream.get("quality") or "Unknown"
    if quality != "Unknown":
        parts.append(str(quality))
    seeders = int(stream.get("seeders") or 0)
    if seeders:
        parts.append(f"{seeders:,} seeders")
    if stream.get("playable"):
        parts.append("in-app direct stream")
    elif stream.get("isMagnet") or str(stream.get("streamUrl") or "").startswith("magnet:"):
        parts.append("magnet only")
    return " • ".join(parts) or "Best available stream"


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------

app = FastAPI(
    title="FastPlay Resolver",
    version=APP_VERSION,
    description="Parallel multi-source movie/TV stream resolver for the Android app.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


def _error(status: int, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"success": False, "error": message})


class JsonResponseWithCache(JSONResponse):
    """JSONResponse with the resolver cache TTL advertised to clients."""

    def __init__(self, content: Any) -> None:
        headers = {"Cache-Control": f"public, max-age={max(0, CACHE_TTL)}"}
        super().__init__(content=content, headers=headers)


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "fastplay",
        "version": APP_VERSION,
        "providers": [name for name, _ in PROVIDERS],
        "cacheEntries": len(_CACHE),
        "cacheTtlSeconds": CACHE_TTL,
        "totalTimeoutSeconds": TOTAL_TIMEOUT,
        "debridConfigured": bool(os.getenv("FASTPLAY_DEBRID_KEY", "").strip()),
        "tmdbConfigured": bool(TMDB_API_KEY),
        "tmdbProxy": TMDB_PROXY_URL,
        "cinemetaBase": CINEMETA_BASE,
        "imdbCacheEntries": len(_IMDB_CACHE),
    }


@app.get("/resolve")
async def resolve(
    imdb: Optional[str] = Query(None, description="IMDb id, e.g. tt1375666"),
    imdb_id: Optional[str] = Query(None, description="Alias for imdb"),
    title: str = Query("", description="Movie / show title"),
    year: Optional[int] = Query(None, ge=1888, le=2100),
    type: str = Query("movie", description="movie | tv"),
    season: int = Query(1, ge=1, le=100),
    episode: int = Query(1, ge=1, le=1000),
) -> Any:
    ctx = build_context(imdb or imdb_id, title, year, type, season, episode)
    if ctx is None:
        return _error(400, "Provide at least an IMDb id (imdb=tt...) or a title.")
    payload = await aggregate(ctx)
    return JsonResponseWithCache(payload)


@app.get("/fast-play")
async def fast_play(
    imdb: Optional[str] = Query(None),
    imdb_id: Optional[str] = Query(None),
    title: str = Query(""),
    year: Optional[int] = Query(None, ge=1888, le=2100),
    type: str = Query("movie"),
    season: int = Query(1, ge=1, le=100),
    episode: int = Query(1, ge=1, le=1000),
) -> Any:
    ctx = build_context(imdb or imdb_id, title, year, type, season, episode)
    if ctx is None:
        return _error(400, "Provide at least an IMDb id (imdb=tt...) or a title.")

    payload = await aggregate(ctx)
    streams = [_normalise_stream_flags(dict(stream)) for stream in payload.get("streams") or []]
    picked = pick_best(streams)
    response = {
        "success": bool(picked),
        "version": APP_VERSION,
        "query": payload.get("query"),
        "count": len(streams),
        "elapsedMs": payload.get("elapsedMs"),
        "cached": payload.get("cached", False),
        "directAvailable": any(_is_http_playable(stream) for stream in streams),
        "providers": payload.get("providers", []),
        "stream": picked,
        "picked": picked,
        "reason": _reason(picked) if picked else "",
        "alternatives": streams[:6] if picked else [],
    }
    if picked is None:
        response["error"] = "No streams found from any provider."
        return JSONResponse(status_code=200, content=response)
    return JsonResponseWithCache(response)


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(
        "fastplay_service:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("RELOAD", "0") == "1",
    )
