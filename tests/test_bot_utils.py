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
