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
