"""Unit tests for the FastPlay resolver parsing / ranking logic.

Run from the repository root:
    python -m unittest discover -s server -p "test_*.py" -v

These tests do not touch the network: provider payloads are mocked.
"""

import base64
import os
import struct
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))

import fastplay_service as fp  # noqa: E402


class ParseTests(unittest.TestCase):
    def test_quality_normalisation(self):
        self.assertEqual(fp.parse_quality("Movie.2019.2160p.UHD.BluRay"), "4K")
        self.assertEqual(fp.parse_quality("Movie 2019 1080p WEB-DL"), "1080p")
        self.assertEqual(fp.parse_quality("Show S01E02 720p HDTV"), "720p")
        self.assertEqual(fp.parse_quality("Movie 2019 CAMRip"), "CAM")
        self.assertEqual(fp.parse_quality("Movie with no tags"), "Unknown")

    def test_size_parsing(self):
        size, size_bytes = fp.parse_size("Inception 2010 1080p [1.85 GB]")
        self.assertEqual(size, "1.85 GB")
        self.assertEqual(size_bytes, int(1.85 * 1024 ** 3))

        size, size_bytes = fp.parse_size("Tiny 720p 700 MB")
        self.assertEqual(size, "700 MB")
        self.assertEqual(size_bytes, 700 * 1024 ** 2)

        self.assertEqual(fp.parse_size("nothing here"), ("Unknown", 0))

    def test_language_pills(self):
        languages = fp.parse_languages("Movie 2019 1080p Hindi Dual Audio x264")
        self.assertIn("Hindi", languages)
        self.assertIn("Dual Audio", languages)
        self.assertEqual(fp.language_priority(languages), 3)

    def test_language_iso_codes(self):
        self.assertEqual(fp.parse_languages("en"), ["English"])
        self.assertEqual(fp.parse_languages("Movie 2020", "hi"), ["Hindi"])

    def test_multi_audio(self):
        languages = fp.parse_languages("Movie 2023 Multi Audio 1080p")
        self.assertIn("Multi Audio", languages)
        self.assertEqual(fp.language_priority(languages), 2)

    def test_context(self):
        ctx = fp.build_context("tt1375666", "Inception (2010)", 2010)
        self.assertEqual(ctx.imdb, "tt1375666")
        self.assertEqual(ctx.search_query, "Inception 2010")
        self.assertIsNone(fp.build_context(None, "   ", None))
        self.assertIsNone(fp.build_context("not-an-id", "", None))

    def test_health_and_score_prefers_seeded(self):
        strong = fp.Stream(provider="YTS", title="Movie 1080p Hindi", quality="1080p",
                           languages=["Hindi"], seeders=900, peers=20, size_bytes=2 * 1024 ** 3)
        weak = fp.Stream(provider="YTS", title="Movie 1080p Hindi", quality="1080p",
                         languages=["Hindi"], seeders=2, peers=30, size_bytes=2 * 1024 ** 3)
        self.assertGreater(strong.health, weak.health)
        self.assertGreater(strong.score, weak.score)


class TorrentioParseTests(unittest.TestCase):
    def test_torrentio_stream(self):
        item = {
            "name": "Torrentio\n4k",
            "title": "Inception.2010.2160p.BluRay.x265\n👤 1234 💾 15.2 GB ⚙️ TorrentGalaxy",
            "url": "0123456789abcdef0123456789abcdef01234567",
        }
        stream = fp._parse_torrentio_stream(item)
        self.assertIsNotNone(stream)
        self.assertEqual(stream.provider, "TorrentGalaxy")
        self.assertEqual(stream.quality, "4K")
        self.assertEqual(stream.seeders, 1234)
        self.assertEqual(stream.size, "15.2 GB")
        self.assertTrue(stream.magnet.startswith("magnet:?xt=urn:btih:"))
        self.assertFalse(stream.playable)

    def test_torrentio_infohash_only(self):
        # current Torrentio payload: no `url`, only `infoHash` + `fileIdx`
        item = {
            "name": "Torrentio\n4k",
            "title": "Inception 2010 Bluray 2160p EN/ITA/FR/ES/DE\n👤 304 💾 8.91 GB ⚙️ 1337x\n🇬🇧 / 🇮🇹 / 🇪🇸 / 🇫🇷 / 🇩🇪",
            "infoHash": "2849211f150e966ef7924f43aec22727a644e10d",
            "fileIdx": 0,
            "behaviorHints": {"filename": "Inception.2010.2160p.mkv"},
        }
        stream = fp._parse_torrentio_stream(item)
        self.assertIsNotNone(stream)
        self.assertEqual(stream.provider, "1337x")
        self.assertIn("English", stream.languages)
        self.assertIn("Italian", stream.languages)
        self.assertIn("2849211f150e966ef7924f43aec22727a644e10d", stream.magnet)
        self.assertEqual(stream.seeders, 304)
        self.assertEqual(stream.size, "8.91 GB")

    def test_torrentio_direct_url(self):
        item = {
            "name": "Torrentio\n1080p",
            "title": "Movie.2019.1080p\n👤 50 💾 2 GB ⚙️ RealDebrid",
            "url": "https://cdn.example.com/movie.mkv",
        }
        stream = fp._parse_torrentio_stream(item)
        self.assertIsNotNone(stream)
        self.assertTrue(stream.is_direct)
        self.assertTrue(stream.playable)
        self.assertEqual(stream.playback_url, "https://cdn.example.com/movie.mkv")


class YtsProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_yts_payload(self):
        payload = {
            "data": {
                "movies": [
                    {
                        "title": "Inception",
                        "title_long": "Inception (2010)",
                        "year": 2010,
                        "imdb_code": "tt1375666",
                        "language": "en",
                        "torrents": [
                            {
                                "hash": "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                                "quality": "2160p",
                                "type": "BluRay",
                                "size": "15.2 GB",
                                "size_bytes": 16320875724,
                                "seeds": 421,
                                "peers": 19,
                                "video_codec": "x265",
                            }
                        ],
                    }
                ]
            }
        }

        async def fake_json(session, url, params=None):
            return payload

        with patch.object(fp, "_get_json", fake_json):
            ctx = fp.build_context("tt1375666", "Inception", 2010)
            streams = await fp.fetch_yts(None, ctx)

        self.assertEqual(len(streams), 1)
        stream = streams[0]
        self.assertEqual(stream.provider, "YTS")
        self.assertEqual(stream.quality, "4K")
        self.assertEqual(stream.seeders, 421)
        self.assertEqual(stream.languages, ["English"])
        self.assertTrue(stream.magnet.startswith("magnet:?xt=urn:btih:ABCDEF"))


class ImdbResolutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        fp._IMDB_CACHE.clear()

    async def test_tmdb_prefers_exact_title_then_external_id(self):
        calls = []

        async def fake_json(session, url, params=None):
            calls.append(url)
            if "/search/movie" in url:
                return {
                    "results": [
                        {"id": 336592, "title": "The Science of Interstellar",
                         "release_date": "2014-11-13"},
                        {"id": 157336, "title": "Interstellar",
                         "release_date": "2014-11-04"},
                    ]
                }
            if url.endswith("/movie/157336/external_ids"):
                return {"imdb_id": "tt0816692"}
            return None

        ctx = fp.build_context(None, "Interstellar", 2014)
        with patch.object(fp, "_get_json", fake_json):
            imdb, source = await fp.resolve_imdb_id(None, ctx)

        self.assertEqual((imdb, source), ("tt0816692", "tmdb"))
        self.assertTrue(any("/search/movie" in url for url in calls))
        self.assertTrue(any(url.endswith("/movie/157336/external_ids") for url in calls))

    async def test_cinemeta_fallback_when_tmdb_unavailable(self):
        async def fake_json(session, url, params=None):
            if "/search/movie" in url:
                return None
            if "/catalog/movie/top/search=Interstellar.json" in url:
                return {
                    "metas": [
                        {"imdb_id": "tt34384900", "name": "Love of Your Life",
                         "releaseInfo": "2026"},
                        {"imdb_id": "tt0816692", "name": "Interstellar",
                         "releaseInfo": "2014"},
                    ]
                }
            return None

        ctx = fp.build_context(None, "Interstellar", 2014)
        with patch.object(fp, "TMDB_API_KEY", ""), patch.object(fp, "_get_json", fake_json):
            imdb, source = await fp.resolve_imdb_id(None, ctx)

        self.assertEqual((imdb, source), ("tt0816692", "cinemeta"))

    async def test_existing_imdb_is_never_overwritten(self):
        async def fail_json(session, url, params=None):
            self.fail(f"unexpected lookup: {url}")

        ctx = fp.build_context("tt1375666", "Inception", 2010)
        with patch.object(fp, "_get_json", fail_json):
            imdb, source = await fp.resolve_imdb_id(None, ctx)

        self.assertEqual((imdb, source), ("tt1375666", "request"))

    async def test_ensure_imdb_uses_lookup_cache(self):
        calls = 0

        async def fake_resolve(session, ctx):
            nonlocal calls
            calls += 1
            return "tt0816692", "tmdb"

        with patch.object(fp, "resolve_imdb_id", fake_resolve):
            first = fp.build_context(None, "Interstellar", 2014)
            await fp.ensure_imdb(first)
            second = fp.build_context(None, "Interstellar", 2014)
            await fp.ensure_imdb(second)

        self.assertEqual(first.imdb, "tt0816692")
        self.assertEqual(first.imdb_source, "tmdb")
        self.assertEqual(second.imdb, "tt0816692")
        self.assertEqual(second.imdb_source, "cache")
        self.assertEqual(calls, 1)


class VidLinkTests(unittest.TestCase):
    @unittest.skipIf(fp.SecretBox is None, "PyNaCl is not installed")
    def test_token_round_trip(self):
        token = fp._vidlink_token(550)
        self.assertIsNotNone(token)
        raw = base64.urlsafe_b64decode(token + "=" * ((4 - len(token) % 4) % 4))
        self.assertEqual(raw[:24], b"\x00" * 24)
        box = fp.SecretBox(bytes.fromhex(fp.VIDLINK_SECRETBOX_KEY))
        plain = box.decrypt(raw[24:], raw[:24])
        self.assertEqual(plain[:7], b"550\x00\x00\x00\x00")
        stamp = struct.unpack(">I", plain[-4:])[0]
        self.assertLessEqual(abs(stamp - int(time.time())), 5)

    def test_parse_qualities_marks_direct_mp4(self):
        payload = {
            "sourceId": "mwVault",
            "stream": {
                "qualities": {
                    "1080": {"url": "https://cdn.example.com/1080.mp4?sig=1",
                             "codecName": "hevc", "size": "500000000"},
                    "720": {"url": "https://cdn.example.com/720.m3u8?sig=2",
                            "codecName": "h264", "size": "250000000"},
                },
                "playlistHeaders": {},
            },
        }
        ctx = fp.build_context("tt0137523", "Fight Club", 1999)
        streams = fp._parse_vidlink_payload(payload, ctx)
        self.assertEqual(len(streams), 2)
        best = next(s for s in streams if s.quality == "1080p")
        self.assertTrue(best.is_direct)
        self.assertTrue(best.playable)
        self.assertEqual(best.source_type, "mp4")
        self.assertEqual(best.playback_url, "https://cdn.example.com/1080.mp4?sig=1")
        hls = next(s for s in streams if s.quality == "720p")
        self.assertEqual(hls.source_type, "hls")

    def test_normalise_preserves_mp4_source_type(self):
        stream = fp.Stream(
            provider="VidLink",
            title="Fight Club [1080p] [VidLink]",
            quality="1080p",
            url="https://cdn.example.com/1080.mp4",
            is_direct=True,
            source_type="mp4",
        ).to_dict()
        normalised = fp._normalise_stream_flags(stream)
        self.assertTrue(normalised["isDirect"])
        self.assertTrue(normalised["playable"])
        self.assertEqual(normalised["sourceType"], "mp4")


class TmdbIdTests(unittest.IsolatedAsyncioTestCase):
    async def test_imdb_find_returns_tmdb_id(self):
        async def fake_json(session, url, params=None):
            if "/find/tt0816692" in url:
                return {"movie_results": [{"id": 157336, "title": "Interstellar"}]}
            return None

        ctx = fp.build_context("tt0816692", "Interstellar", 2014)
        with patch.object(fp, "_get_json", fake_json):
            tmdb_id = await fp.resolve_tmdb_id(None, ctx)
        self.assertEqual(tmdb_id, 157336)
        self.assertEqual(ctx.tmdb_id, 157336)


class RankingTests(unittest.TestCase):
    def _stream(self, quality, languages, seeders, direct=False, title=""):
        return fp.Stream(
            provider="Test",
            title=title or f"Movie {quality} {' '.join(languages)}",
            quality=quality,
            languages=languages,
            seeders=seeders,
            is_direct=direct,
            url="https://example.com/stream.m3u8" if direct else "",
            magnet="" if direct else "magnet:?xt=urn:btih:abc123",
        ).to_dict()

    def test_fast_play_prefers_hindi_over_more_seeders(self):
        hindi = self._stream("720p", ["Hindi", "Dual Audio"], 80)
        english = self._stream("1080p", ["English"], 5000)
        picked = fp.pick_best([english, hindi])
        self.assertEqual(picked["languages"][0], "Hindi")

    def test_fast_play_prefers_direct_when_language_equal(self):
        magnet = self._stream("1080p", ["English"], 900)
        direct = self._stream("720p", ["English"], 100, direct=True)
        picked = fp.pick_best([magnet, direct])
        self.assertEqual(picked["streamUrl"], "https://example.com/stream.m3u8")
        self.assertTrue(picked["playable"])

    def test_fast_play_prefers_direct_over_hindi_magnet(self):
        hindi_magnet = self._stream("1080p", ["Hindi", "Dual Audio"], 5000)
        direct_english = self._stream("720p", ["English"], 25, direct=True)
        picked = fp.pick_best([hindi_magnet, direct_english])
        self.assertEqual(picked["streamUrl"], "https://example.com/stream.m3u8")
        self.assertTrue(picked["isDirect"])
        self.assertFalse(picked["isMagnet"])

    def test_direct_stream_sorts_before_hindi_magnets(self):
        direct = fp.Stream(
            provider="VidLink",
            title="Movie [720p] [VidLink]",
            quality="720p",
            languages=[],
            url="https://cdn.example.com/stream.mp4",
            is_direct=True,
            source_type="mp4",
        )
        hindi_magnets = [
            fp.Stream(
                provider="Torrentio",
                title=f"Movie {quality} Hindi Dual Audio",
                quality=quality,
                languages=["Hindi", "Dual Audio"],
                seeders=5000,
                magnet="magnet:?xt=urn:btih:abc",
                info_hash=f"hash-{index}",
            )
            for index, quality in enumerate(("1080p", "4K", "1080p"))
        ]
        ordered = fp._sort_streams([*hindi_magnets, direct])
        self.assertIs(ordered[0], direct)
        self.assertTrue(ordered[0].playable)

    def test_magnet_flags_are_forced_to_not_direct(self):
        stream = self._stream("1080p", ["Hindi"], 500)
        stream["streamUrl"] = "magnet:?xt=urn:btih:abc123"
        stream["isDirect"] = True
        stream["is_direct"] = True
        stream["playable"] = True
        normalised = fp._normalise_stream_flags(stream)
        self.assertFalse(normalised["isDirect"])
        self.assertFalse(normalised["is_direct"])
        self.assertFalse(normalised["playable"])
        self.assertTrue(normalised["isMagnet"])
        self.assertEqual(normalised["sourceType"], "torrent")

    def test_sort_and_dedupe(self):
        a = fp.Stream(provider="A", title="Same Movie 1080p", quality="1080p",
                      languages=["English"], seeders=10, info_hash="abc")
        b = fp.Stream(provider="B", title="Same Movie 1080p", quality="1080p",
                      languages=["English"], seeders=99, info_hash="abc")
        c = fp.Stream(provider="C", title="Other Movie 1080p", quality="1080p",
                      languages=["English"], seeders=5, info_hash="def")
        deduped = fp._dedupe([a, b, c])
        self.assertEqual(len(deduped), 2)
        self.assertEqual(max(s.seeders for s in deduped), 99)
        self.assertEqual(fp._sort_streams([a, c])[0].seeders, 10)


if __name__ == "__main__":
    unittest.main(verbosity=2)
