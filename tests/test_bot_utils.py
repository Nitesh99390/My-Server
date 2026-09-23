"""Offline unit tests for bot.py helpers (no Telegram / network needed)."""
import os
import sys
import importlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("SESSION_NAME", "/tmp/test_session")

bot = importlib.import_module("bot")


def test_normalize_server_url():
    assert bot.normalize_server_url("https://example.onrender.com/") == "https://example.onrender.com"
    assert bot.normalize_server_url("http://localhost:10000/tts") == "http://localhost:10000"
    # scheme-less, credentials or query strings are rejected
    assert bot.normalize_server_url("example.onrender.com") == ""
    assert bot.normalize_server_url("https://user:pw@host.com") == ""
    assert bot.normalize_server_url("https://host.com/?x=1") == ""


def test_split_text_respects_limit_and_keeps_content():
    text = ("यह एक वाक्य है। " * 400).strip()
    chunks = bot.split_text(text, 500)
    assert chunks and all(len(c) <= 500 for c in chunks)
    assert "".join(chunks).replace(" ", "") == text.replace(" ", "")


def test_split_chapters_detects_headings():
    text = "Chapter 1\nHello world.\n\nअध्याय 2\nनमस्ते दुनिया।\n"
    chapters = bot.split_chapters(text)
    assert len(chapters) == 2
    assert chapters[0][0].startswith("Chapter 1")


def test_clean_text_and_helpers():
    assert bot.clean_text("a\r\n\r\n\r\n\r\nb   c") == "a\n\nb c"
    assert bot.fmt_duration(3725) == "1h 02m 05s"
    assert bot.fmt_size(2048).endswith("KB")
    assert bot.progress_bar(50, 10).count("█") == 5
    assert bot.safe_filename('my book: "x"/y') == "my_book_xy"
    assert bot.safe_filename("///") == "audiobook"


def test_estimate_seconds_rate_scaling():
    base = bot.estimate_seconds(10_000, "+0%")
    assert bot.estimate_seconds(10_000, "+50%") < base
    assert bot.estimate_seconds(10_000, "-25%") > base


def test_voice_regex():
    assert bot.VOICE_RE.match("hi-IN-MadhurNeural")
    assert bot.VOICE_RE.match("en-US-AvaMultilingualNeural")
    assert not bot.VOICE_RE.match("not a voice")


def test_database_servers_and_users():
    db = bot.Database(":memory:")
    db.add_server("https://s1.example.com")
    db.add_server("https://s2.example.com")
    assert sorted(db.servers()) == ["https://s1.example.com", "https://s2.example.com"]
    db.remove_server("https://s1.example.com")
    assert db.servers() == ["https://s2.example.com"]


# ---------------------------------------------------------------------------
# Built-in free Edge-TTS engine helpers
# ---------------------------------------------------------------------------
def test_backend_urls_puts_local_engine_last():
    urls = bot.backend_urls(["https://a.example", "", "https://b.example"])
    assert urls[:2] == ["https://a.example", "https://b.example"]
    if bot.LOCAL_TTS_ENABLED:
        assert urls[-1] == bot.LOCAL_TTS_URL
        assert bot.ServerState(bot.LOCAL_TTS_URL).is_local
    assert not bot.ServerState("https://a.example").is_local


def test_is_throttle_error_classification():
    assert bot._is_throttle_error(bot.ThrottleError("x"))
    assert bot._is_throttle_error(RuntimeError("HTTP 403 forbidden"))
    assert bot._is_throttle_error(RuntimeError("too many requests"))
    assert not bot._is_throttle_error(ValueError("bad text"))


def test_adaptive_limiter_shrinks_and_grows():
    lim = bot.AdaptiveLimiter(start=4, maximum=6, min_gap=0)
    assert lim.current == 4
    pause = lim.report_throttle()
    assert lim.current == 2 and pause > 0
    lim.report_throttle()
    assert lim.current == 1
    for _ in range(lim.grow_after):
        lim.report_success()
    assert lim.current == 2
    for _ in range(lim.grow_after * 10):
        lim.report_success()
    assert lim.current == 6  # never above maximum


def test_adaptive_limiter_enforces_concurrency():
    import asyncio

    async def run():
        lim = bot.AdaptiveLimiter(start=2, maximum=2, min_gap=0)
        peak = 0
        running = 0

        async def worker():
            nonlocal peak, running
            await lim.acquire()
            try:
                running += 1
                peak = max(peak, running)
                await asyncio.sleep(0.02)
                running -= 1
            finally:
                await lim.release()

        await asyncio.gather(*(worker() for _ in range(8)))
        return peak

    assert asyncio.run(run()) == 2


def test_pool_records_failure_reasons():
    pool = bot.ServerPool(["https://a.example", bot.LOCAL_TTS_URL])
    remote, local = pool.states
    pool.report(remote, False, error="HTTP 401: API key rejected")
    pool.report(local, False, error="Edge-TTS returned no audio")
    summary = pool.failure_summary()
    assert "https://a.example -> HTTP 401: API key rejected" in summary
    assert "built-in Edge-TTS -> Edge-TTS returned no audio" in summary
    # success clears the stored reason
    pool.report(remote, True, latency=0.5)
    assert remote.last_error == ""
    assert "no response" in bot.ServerPool(["https://b.example"]).failure_summary()


def test_describe_exc_classifies_edge_failures():
    import asyncio
    assert bot._describe_exc(asyncio.TimeoutError()) == "timed out"
    assert "403" in bot._describe_exc(RuntimeError("Invalid response status: 403"))
    assert bot._describe_exc(ValueError("bad")).startswith("ValueError: bad")
    assert bot._describe_exc(None) == "unknown error"


def test_describe_http_error_gives_actionable_401_hint():
    import asyncio

    class FakeResp:
        def __init__(self, status, body):
            self.status = status
            self._body = body

        async def text(self):
            return self._body

    msg = asyncio.run(bot._describe_http_error(FakeResp(401, '{"error":"Unauthorized: invalid or missing API key"}')))
    assert msg.startswith("HTTP 401") and "TTS_API_KEY" in msg
    msg = asyncio.run(bot._describe_http_error(FakeResp(400, '{"error":"Text too long (7000 chars)"}')))
    assert msg == "HTTP 400: Text too long (7000 chars)"
    assert "not found" in asyncio.run(bot._describe_http_error(FakeResp(404, "")))


def test_check_server_flags_missing_api_key(monkeypatch):
    """A server that reports auth_required must not be treated as usable without a key."""
    import asyncio
    from aiohttp import web

    async def health(_):
        return web.json_response({"status": "ok", "version": "3.2.0", "auth_required": True,
                                  "max_text_length": 6000, "active_jobs": 0})

    async def tts(_):
        return web.json_response({"error": "Unauthorized: invalid or missing API key", "status": 401}, status=401)

    async def run():
        app = web.Application()
        app.router.add_get("/health", health)
        app.router.add_post("/tts", tts)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        url = f"http://127.0.0.1:{port}"
        try:
            async with bot.aiohttp.ClientSession() as session:
                monkeypatch.setattr(bot, "TTS_API_KEY", "")
                no_key = await bot.check_server(session, url)
                monkeypatch.setattr(bot, "TTS_API_KEY", "wrong")
                wrong_key = await bot.check_server(session, url, deep=True)
                shallow = await bot.check_server(session, url)
        finally:
            await runner.cleanup()
        return no_key, wrong_key, shallow

    no_key, wrong_key, shallow = asyncio.run(run())
    assert not no_key["ok"] and "TTS_API_KEY" in no_key["error"]
    assert not wrong_key["ok"] and wrong_key["error"].startswith("HTTP 401")
    assert shallow["ok"] and shallow["version"] == "3.2.0"


def test_pool_concurrency_accounts_for_local_engine():
    urls = bot.backend_urls(["https://a.example"])
    pool = bot.ServerPool(urls)
    expected = bot.PER_SERVER_CONCURRENCY + (bot.local_limiter.current if bot.LOCAL_TTS_ENABLED else 0)
    assert pool.concurrency() == min(bot.MAX_TOTAL_CONCURRENCY, expected)


def test_split_text_for_tts_respects_byte_limit_for_hindi():
    # Devanagari is 3 bytes/char: 3000 chars would be ~9 KB, i.e. several
    # sequential Edge connections. The byte-aware splitter must keep every chunk
    # inside one websocket message while preserving all text.
    text = ("यह एक बहुत लंबा वाक्य है जिसमें कई शब्द हैं। " * 300).strip()
    chunks = bot.split_text_for_tts(text, 3000, 3900)
    assert chunks
    assert all(len(c) <= 3000 for c in chunks)
    assert all(bot.edge_payload_bytes(c) <= 3900 for c in chunks)
    assert "".join(chunks).replace(" ", "").replace("\n", "") == text.replace(" ", "")
    # ASCII text is not split more than the character limit requires.
    ascii_text = ("Hello world, this is a test sentence. " * 200).strip()
    assert bot.split_text_for_tts(ascii_text, 3000, 3900) == bot.split_text(ascii_text, 3000)


def test_edge_payload_bytes_counts_xml_escaping():
    assert bot.edge_payload_bytes("a&b") == len("a&amp;b")
    assert bot.edge_payload_bytes("नमस्ते") == len("नमस्ते".encode("utf-8"))


def test_adaptive_limiter_grows_and_shrinks():
    lim = bot.AdaptiveLimiter(start=2, maximum=6, min_gap=0.0)
    for _ in range(lim.grow_after):
        lim.report_success()
    assert lim.current == 3
    pause = lim.report_throttle()
    assert lim.current == 1 and pause > 0


def test_extract_text_to_file_txt_upload_like_document_job(tmp_path):
    """Regression: a .txt upload used to be truncated because the extraction
    output path was also '<tmpdir>/source.txt' (the download path)."""
    tmpdir = tmp_path / "abp_source_x"
    tmpdir.mkdir()
    ext = ".txt"
    src = tmpdir / ("source" + ext)
    src.write_text("अध्याय 1\n\nयह एक कहानी है। राम वन में गया।\n", encoding="utf-8")
    out = tmpdir / "extracted.utf8"
    chars = bot.extract_text_to_file(str(src), ext, str(out))
    assert chars > 0
    assert "कहानी" in out.read_text(encoding="utf-8")
    # Same path in and out must be refused instead of silently wiping the source
    import pytest
    with pytest.raises(ValueError):
        bot.extract_text_to_file(str(src), ext, str(src))
    assert src.stat().st_size > 0


def test_read_text_file_encodings(tmp_path):
    import codecs
    cases = {
        "utf8": "Hello दुनिया।".encode("utf-8"),
        "utf8_bom": codecs.BOM_UTF8 + b"Hello world",
        "utf16": "नमस्ते दुनिया".encode("utf-16"),
        "cp1252": b"Hello \x93quoted\x94 text",
        "crlf": b"Line one.\r\nLine two.\r\n",
    }
    for name, raw in cases.items():
        p = tmp_path / f"{name}.txt"
        p.write_bytes(raw)
        assert bot.clean_text(bot._read_text_file(str(p)))


# --------------------------------------------------------------------------
# v4.2 speed work: engine-aware chunk plan, least-loaded scheduling,
# server-advertised concurrency, background uploads
# --------------------------------------------------------------------------
def test_plan_limits_uses_big_chunks_for_render_servers():
    # With render servers the plan must NOT be squeezed to the built-in
    # engine's ~3900-byte websocket cap (=> ~1300 Hindi chars per request).
    probes = [{"ok": True, "max_text_length": 6000}, {"ok": True, "max_text_length": 6000}]
    limit, max_bytes = bot.plan_limits(probes)
    assert limit == min(6000, bot.REMOTE_CHUNK_SIZE) and max_bytes is None
    # smallest advertised limit wins
    limit, _ = bot.plan_limits([{"ok": True, "max_text_length": 6000}, {"ok": True, "max_text_length": 2500}])
    assert limit == 2500
    # old servers without max_text_length -> CHUNK_SIZE, still no byte cap
    assert bot.plan_limits([{"ok": True}]) == (bot.CHUNK_SIZE, None)
    # built-in engine only -> byte-aware plan (1 chunk = 1 websocket)
    limit, max_bytes = bot.plan_limits([])
    assert max_bytes == bot.LOCAL_TTS_MAX_BYTES and limit <= bot.LOCAL_TTS_CHUNK_SIZE


def test_render_server_plan_needs_far_fewer_requests_for_hindi():
    text = ("यह एक बहुत लंबा वाक्य है जिसमें कई शब्द हैं। " * 1500).strip()  # ~60k chars
    old_plan = bot.build_plan(text, 3000, bot.LOCAL_TTS_MAX_BYTES, False)
    limit, max_bytes = bot.plan_limits([{"ok": True, "max_text_length": 6000}])
    new_plan = bot.build_plan(text, limit, max_bytes, False)
    assert len(new_plan) * 2 <= len(old_plan)
    assert all(len(c) <= 6000 for _, c in new_plan)
    assert "".join(c for _, c in new_plan).replace(" ", "").replace("\n", "") == text.replace(" ", "")


def test_pool_pick_prefers_engine_with_most_free_slots():
    a, b = "https://fast.example", "https://slow.example"
    pool = bot.ServerPool([a, b])
    la, lb = bot.remote_limiter(a), bot.remote_limiter(b)
    la.current = lb.current = 4
    la._active, lb._active = 1, 4  # b is saturated
    for _ in range(6):
        assert pool.pick().url == a
    la._active, lb._active = 3, 3
    sa, sb = pool.states
    sa.latency, sb.latency = 2.0, 0.5  # equal load -> lower latency wins
    assert pool.pick().url == b
    la._active = lb._active = 0
    # engines in cooldown are skipped while another one is live
    sb.cooldown_until = bot.time.time() + 60
    assert pool.pick().url == a


def test_pool_pick_shares_load_between_equal_engines():
    urls = [f"https://s{i}.example" for i in range(3)]
    pool = bot.ServerPool(urls)
    for u in urls:
        lim = bot.remote_limiter(u)
        lim.current, lim._active = 4, 0
    picked = {pool.pick().url for _ in range(3)}
    assert picked == set(urls)  # round-robin tie-break, not always the first


def test_apply_probe_clamps_limiter_to_server_capacity():
    url = "https://small.example"
    pool = bot.ServerPool([url])
    lim = bot.remote_limiter(url)
    assert lim.maximum == bot.PER_SERVER_MAX_CONCURRENCY
    pool.apply_probe(url, {"ok": True, "max_concurrency": 4})
    assert lim.maximum == 4 and lim.current <= 4
    assert pool.states[0].advertised_limit == 4
    pool.apply_probe(url, {"ok": True, "max_concurrency": 6})
    assert lim.maximum == 6
    # never above the hard per-IP ceiling of 8
    pool.apply_probe(url, {"ok": True, "max_concurrency": 64})
    assert lim.maximum == 8
    # garbage is ignored
    pool.apply_probe(url, {"ok": True, "max_concurrency": "many"})
    assert lim.maximum == 8


def test_remote_limiter_ramps_faster_than_local():
    lim = bot.remote_limiter("https://ramp.example")
    assert lim.grow_after == bot.REMOTE_GROW_AFTER < bot.LOCAL_TTS_GROW_AFTER
    assert lim.min_gap == bot.REMOTE_MIN_GAP < bot.LOCAL_TTS_MIN_GAP
    start = lim.current
    for _ in range(lim.grow_after):
        lim.report_success()
    assert lim.current == min(lim.maximum, start + 1)


def test_pool_reports_chars_for_throughput():
    pool = bot.ServerPool(["https://t.example"])
    s = pool.states[0]
    pool.report(s, True, latency=1.0, chars=1200)
    pool.report(s, True, latency=1.0, chars=800)
    assert s.chars_done == 2000
    pool.report(s, False, error="boom", chars=500)
    assert s.chars_done == 2000


def test_local_synthesize_splits_big_chunks_and_keeps_order(monkeypatch):
    import asyncio
    if not bot.LOCAL_TTS_ENABLED:
        return
    calls = []

    async def fake_once(text, settings):
        calls.append(text)
        await asyncio.sleep(0.01 if len(calls) % 2 else 0.0)
        return text.encode("utf-8"), 10

    monkeypatch.setattr(bot, "_local_synth_once", fake_once)
    monkeypatch.setattr(bot.local_limiter, "min_gap", 0.0)
    text = ("यह एक बहुत लंबा वाक्य है जिसमें कई शब्द हैं। " * 120).strip()  # ~5.4k chars, ~15 KB
    assert bot.edge_payload_bytes(text) > bot.LOCAL_TTS_MAX_BYTES

    async def run():
        return await bot.local_synthesize(text, {"voice": "hi-IN-MadhurNeural", "rate": "+0%",
                                                 "pitch": "+0Hz", "volume": "+0%"}, asyncio.Event())

    audio, dur = asyncio.run(run())
    assert len(calls) >= 3  # split into several websocket-sized pieces
    assert all(bot.edge_payload_bytes(c) <= bot.LOCAL_TTS_MAX_BYTES for c in calls)
    assert dur == 10 * len(calls)
    # concatenated in plan order regardless of completion order
    assert audio.decode("utf-8").replace(" ", "").replace("\n", "") == text.replace(" ", "")
