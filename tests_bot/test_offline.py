"""Offline tests for bot.py - no Telegram, no Cloudflare, no credentials.

    python tests_bot/test_offline.py

1. Upgrading a database written by bot.py <= 4.3 (users.expiry_date / render_servers /
   user_settings) - this used to crash every handler with
   ``IndexError: No item with that key`` (row["voice"]).
2. Every UI builder renders; the reply keyboard is at most two rows.
3. A whole job against a fake Worker: prepare -> voice -> send (with upload %) -> done,
   plus the "no Worker" and "Worker wants an API key" error paths.
"""
import asyncio
import os
import shutil
import sqlite3
import sys
import time

TMP = "/tmp/abp_offline"
shutil.rmtree(TMP, ignore_errors=True)
os.makedirs(TMP)
DB = os.path.join(TMP, "legacy.db")

c = sqlite3.connect(DB)
c.executescript("""
CREATE TABLE users (user_id INTEGER PRIMARY KEY, expiry_date TEXT, usage_count INTEGER DEFAULT 0,
  is_admin INTEGER DEFAULT 0, first_name TEXT, username TEXT, is_banned INTEGER DEFAULT 0,
  chars_total INTEGER DEFAULT 0, audio_seconds INTEGER DEFAULT 0, joined_at TEXT, last_active TEXT);
CREATE TABLE render_servers (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, added_at TEXT,
  fail_count INTEGER DEFAULT 0, last_ok TEXT);
CREATE TABLE user_settings (user_id INTEGER PRIMARY KEY, voice TEXT, max_hours INTEGER, rate TEXT, pitch TEXT,
  volume TEXT, split_mode TEXT);
CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, source TEXT, chars INTEGER DEFAULT 0,
  parts INTEGER DEFAULT 0, audio_seconds INTEGER DEFAULT 0, status TEXT, created_at TEXT, finished_at TEXT,
  title TEXT, chat_id INTEGER);
INSERT INTO users VALUES (1,'2099-12-31 23:59:59',7,1,'Owner','own',0,120000,8000,'2025-01-01 00:00:00',NULL);
INSERT INTO users VALUES (111,'2030-01-01 00:00:00',2,0,'Approved','ap',0,100,10,'2025-01-01 00:00:00',NULL);
INSERT INTO users VALUES (222,'2020-01-01 00:00:00',0,0,'Expired','ex',0,0,0,'2025-01-01 00:00:00',NULL);
INSERT INTO users VALUES (333,'2030-01-01 00:00:00',0,0,'Banned','bn',1,0,0,'2025-01-01 00:00:00',NULL);
INSERT INTO render_servers(url) VALUES ('https://edge-tts-worker.acct.workers.dev/tts');
INSERT INTO user_settings VALUES (111,'en-US-AriaNeural',4,'+10%','+0Hz','+0%','chapter');
INSERT INTO jobs(user_id,source,chars,audio_seconds,status,created_at) VALUES (111,'Book.txt',5000,300,'done','2025-05-05 10:00:00');
INSERT INTO jobs(user_id,source,chars,audio_seconds,status,created_at) VALUES (111,'Old.txt',5000,0,'running','2025-05-06 10:00:00');
""")
c.commit()
c.close()

os.environ.update(API_ID="12345", API_HASH="x" * 32, BOT_TOKEN="123:abc", OWNER_ID="1", DB_PATH=DB,
                  JOBS_DIR=os.path.join(TMP, "jobs"), SESSION_NAME=os.path.join(TMP, "sess"),
                  LOG_FILE=os.path.join(TMP, "bot.log"), PROGRESS_EDIT_INTERVAL="2", REMOTE_CHUNK_SIZE="600")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import aiohttp  # noqa: E402
from aiohttp import web  # noqa: E402

import bot as B  # noqa: E402


def test_migration() -> None:
    db = B.db
    s = db.settings(1)                       # <- the call that crashed on v4 databases
    assert s["voice"] == B.DEFAULT_VOICE
    s = db.settings(111)
    assert s["voice"] == "en-US-AriaNeural" and s["hours"] == 4 and s["split_mode"] == "chapter", s
    assert db.is_approved(1) and db.is_approved(111)
    assert not db.is_approved(222), "expired user must not be approved"
    assert not db.is_approved(333), "banned user must not be approved"
    assert db.servers() == ["https://edge-tts-worker.acct.workers.dev"], db.servers()
    hist = db.user_history(111)
    assert hist[0]["status"] == "failed" and hist[1]["title"] == "Book.txt" and hist[1]["duration"] == 300
    db.ensure_user(999, "new", "New")
    assert db.settings(999)["voice"] == B.DEFAULT_VOICE
    B.Database(DB)                           # idempotent re-open
    print("migration OK")


def test_ui() -> None:
    B.pool.sync_from_db()
    for uid in (1, 111, 222):
        kb = B.main_keyboard(uid)
        assert len(kb.keyboard) <= 2, "reply keyboard must stay small"
    assert B.status_text(1) and B.status_markup(1)
    assert B.history_text(111) and B.queue_text(111)
    assert B.settings_text(111) and B.settings_markup(111)
    B.voice_groups_markup()
    B.voice_list_markup(0, "hi-IN-MadhurNeural")
    B.options_markup("rate", B.RATE_OPTIONS, "+0%")
    B.help_markup(111)
    B.help_markup(222)
    for name, fn in B.ADMIN_PAGES.items():
        t, m = fn()
        assert "<b>" in t and m.inline_keyboard, name
    B.admin_user_list(0)
    B.admin_user_list(99)
    for st in ("queued", "preparing", "synth", "paused", "uploading", "done"):
        assert B.stage_line(st)
    j = B.Job(user_id=5, title="T")
    B.active_jobs[5] = j
    assert B.current_job(5) is j
    j.queued_at -= 1000                      # never started -> stale -> auto-released
    assert B.current_job(5) is None and 5 not in B.active_jobs
    print("ui OK")


MP3 = b"\xff\xfb\x90\x00" + b"\x00" * 6000 * 2      # 2 s of CBR "audio"
REQUIRE_KEY = {"v": False}


async def _health(_r):
    return web.json_response({"status": "ok", "version": "5.0.0-cf", "active_jobs": 0,
                              "auth_required": REQUIRE_KEY["v"], "max_text_length": 6000, "max_concurrency": 6})


async def _tts(r):
    if REQUIRE_KEY["v"] and r.headers.get("X-API-Key") != "secret":
        return web.json_response({"error": "unauthorized"}, status=401)
    await asyncio.sleep(0.1)
    return web.Response(body=MP3, content_type="audio/mpeg", headers={"X-Duration-Ms": "2000"})


class FakeMsg:
    edits: list = []

    async def edit_text(self, text, reply_markup=None, disable_web_page_preview=True):
        FakeMsg.edits.append(text)

    async def delete(self):
        pass


class FakeClient:
    sent: list = []

    async def send_audio(self, chat_id, path, progress=None, **kw):
        size = os.path.getsize(path)
        for i in range(1, 4):
            progress(int(size * i / 3), size)
            await asyncio.sleep(0.9)
        FakeClient.sent.append((kw.get("file_name"), kw.get("duration")))

    async def send_message(self, chat_id, text, **kw):
        return FakeMsg()


async def test_pipeline() -> None:
    app = web.Application()
    app.add_routes([web.get("/health", _health), web.post("/tts", _tts)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 8799)
    await site.start()
    B.http_session = aiohttp.ClientSession()
    url = "http://127.0.0.1:8799"
    try:
        B.db.remove_server("https://edge-tts-worker.acct.workers.dev")
        B.pool.sync_from_db()
        text = ("यह एक परीक्षण वाक्य है। " * 40 + "\n\n") * 6

        # 1) no Worker registered -> clear failure, job slot released
        tp = os.path.join(TMP, "src0.txt")
        open(tp, "w").write(text)
        job = B.reserve_job(1, "NoWorker", len(text))
        await B.run_audiobook_job(FakeClient(), FakeMsg(), 1, job, tp, "NoWorker", B.db.settings(1), chat_id=1)
        assert "No TTS Worker registered" in FakeMsg.edits[-1] and 1 not in B.active_jobs
        print("no-worker path OK")

        # 2) Worker wants a key, bot has none -> explained; without key requirement -> fine
        REQUIRE_KEY["v"] = True
        info = await B.check_server(B.http_session, url, deep=True)
        assert not info["ok"] and "TTS_API_KEY" in info["error"], info
        REQUIRE_KEY["v"] = False
        info = await B.check_server(B.http_session, url, deep=True)
        assert info["ok"], info
        print("api-key-optional probes OK")

        # 3) full job: prepare -> voice -> send (upload %) -> done
        B.db.add_server(url)
        B.pool.sync_from_db()
        FakeMsg.edits.clear()
        tp = os.path.join(TMP, "src.txt")
        open(tp, "w").write(text * 2)
        job = B.reserve_job(1, "Test Book", len(text) * 2)
        t0 = time.time()
        await B.run_audiobook_job(FakeClient(), FakeMsg(), 1, job, tp, "Test Book", B.db.settings(1), chat_id=1)
        e = FakeMsg.edits
        assert job.stage == "done" and FakeClient.sent, "job must complete and deliver"
        assert any("Checking TTS Workers" in x for x in e) and any("Splitting" in x for x in e)
        assert any("chars/s" in x for x in e), "synthesis progress must be shown"
        assert any("Sending part 1 ·" in x and "%" in x for x in e), "upload % must be shown"
        assert "Done" in e[-1] and 1 not in B.active_jobs
        row = B.db.job(job.job_id)
        assert row["status"] == "done" and row["parts"] == 1
        print(f"pipeline OK ({time.time() - t0:.1f}s, {len(e)} status edits)")
        print("---- last status card ----")
        print(e[-1])
    finally:
        await B.http_session.close()
        await runner.cleanup()


if __name__ == "__main__":
    test_migration()
    test_ui()
    B.LOOP.run_until_complete(test_pipeline())
    print("\nALL OFFLINE TESTS PASSED")
