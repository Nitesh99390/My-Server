"""
=============================================================================
 AudioBook Pro - Telegram TTS Bot  (bot.py)  -  v3.0 Professional Edition
=============================================================================
 Converts novels / stories / documents into MP3 audiobooks using a pool of
 Edge-TTS render servers (app.py).  Built on Pyrogram (MTProto) so it can
 upload large audio files (up to 2 GB).

 Highlights
 ----------
   * English UI, clean reply/inline keyboards, /commands for everything
   * Supports  .txt .md .docx .html .htm .epub .pdf  + plain text messages
   * Smart sentence-aware chunking, duration-aware part splitting
   * Optional chapter-aware splitting (Chapter 1 / अध्याय 1 / Part 1 ...)
   * Built-in FREE Edge-TTS engine (no API key) with adaptive, throttle-safe
     parallelism - fast, but automatically slows down before Microsoft blocks the IP
   * Optional multi-server load balancing with health scoring and automatic failover
   * Real-time progress bar with ETA, /cancel support, per-user job queue
   * Voice preview, 20+ built-in voices (Hindi, English, Indian languages),
     custom voice input validated against the render server
   * Admin panel: servers, approvals, revoke, ban, user list, broadcast, stats
   * Job history, per-user statistics, rotating file logs, keep-alive pinger
   * Optional ffmpeg re-mux for perfect MP3 headers (auto-detected)

 Environment variables
 ---------------------
   API_ID, API_HASH, BOT_TOKEN, OWNER_ID          (required - or edit below)
   CONTACT_USERNAME                               (optional)
   TTS_API_KEY        API key sent to render servers as X-API-Key (optional)
   CHUNK_SIZE         characters per TTS request (default 3000)
   PER_SERVER_CONCURRENCY  parallel requests per server (default 4)
   MAX_PARALLEL_JOBS  simultaneous audiobook jobs (default 1)
   DB_PATH            default bot_database.db
   LOG_LEVEL          default INFO

   Built-in FREE Edge-TTS engine (no API key, no render server needed)
   LOCAL_TTS_ENABLED          default true  - synthesise directly inside the bot
   LOCAL_TTS_CONCURRENCY      default 4     - parallel Edge-TTS streams to start with
   LOCAL_TTS_MAX_CONCURRENCY  default 6     - hard ceiling (auto-scales up while healthy)
   LOCAL_TTS_CHUNK_SIZE       default 3000  - characters per Edge-TTS request
   LOCAL_TTS_MAX_BYTES        default 3900  - wire bytes per request (1 chunk = 1 connection)
   LOCAL_TTS_RETRIES          default 4     - attempts per chunk before failing over
   LOCAL_TTS_MIN_GAP_MS       default 150   - spacing between new connections (anti-throttle)
   LOCAL_TTS_GROW_AFTER       default 8     - clean chunks before adding one more stream
   PROGRESS_EDIT_INTERVAL     default 4     - seconds between progress-message edits

 Requirements
 ------------
   pip install pyrogram tgcrypto aiohttp edge-tts python-docx ebooklib beautifulsoup4 pypdf
=============================================================================
"""

from __future__ import annotations

import asyncio
import contextlib
import codecs
import math
import uuid
import zipfile
from urllib.parse import urlsplit, urlunsplit
import html
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

try:  # Built-in free Microsoft Edge neural TTS (no API key required)
    import edge_tts
except ImportError:  # pragma: no cover
    edge_tts = None  # type: ignore

# Pyrogram 2.x expects an event loop at import time on Python 3.13+.
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified, RPCError
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

# Optional parsers -----------------------------------------------------------
try:
    import docx  # python-docx
except ImportError:  # pragma: no cover
    docx = None
try:
    import ebooklib
    from ebooklib import epub
except ImportError:  # pragma: no cover
    ebooklib = None
    epub = None
try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    BeautifulSoup = None
try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover
    try:
        from PyPDF2 import PdfReader  # type: ignore
    except ImportError:
        PdfReader = None

# =============================================================================
# Configuration
# =============================================================================
VERSION = "4.1.1"


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# Required credentials have no embedded defaults; set environment variables.
API_ID = _env_int("API_ID", 0)
API_HASH = _env("API_HASH")
BOT_TOKEN = _env("BOT_TOKEN")
OWNER_ID = _env_int("OWNER_ID", 0)
CONTACT_USERNAME = _env("CONTACT_USERNAME", "Niteshbhumihar")
# Shared secret for render servers.  Fall back to API_KEY so a single .env shared
# between bot.py and app.py "just works" instead of producing silent HTTP 401s.
TTS_API_KEY = _env("TTS_API_KEY", "") or _env("API_KEY", "")

CHUNK_SIZE = max(500, min(_env_int("CHUNK_SIZE", 3000), 6000))
# Render free instances share egress IP ranges that Microsoft throttles quickly
# ("NoAudioReceived" on every chunk).  Start gently and let the per-server
# limiter grow to PER_SERVER_MAX_CONCURRENCY while the server stays healthy.
PER_SERVER_CONCURRENCY = max(1, min(_env_int("PER_SERVER_CONCURRENCY", 3), 8))
PER_SERVER_MAX_CONCURRENCY = max(PER_SERVER_CONCURRENCY, min(_env_int("PER_SERVER_MAX_CONCURRENCY", 6), 8))
# Hard ceiling on in-flight requests across ALL engines; with many render servers
# this is the only cap (each server also has its own adaptive limiter).
MAX_TOTAL_CONCURRENCY = max(4, min(_env_int("MAX_TOTAL_CONCURRENCY", 64), 256))
MAX_PARALLEL_JOBS = max(1, _env_int("MAX_PARALLEL_JOBS", 1))
# Attempts per *round* on the engines before the job pauses (it never gives up on
# its own - see JOB_MAX_STALL_MINUTES).
MAX_CHUNK_ATTEMPTS = 4
CHUNK_TIMEOUT = 240  # seconds per TTS request
KEEP_ALIVE_INTERVAL = max(60, _env_int("KEEP_ALIVE_INTERVAL", 300))  # seconds
# "always": ping every render server around the clock (keeps Render free
# instances awake but burns their free hours); "jobs": only while a job runs.
KEEP_ALIVE_MODE = _env("KEEP_ALIVE_MODE", "always").lower()
# Optional HTTP health endpoint so the bot itself can run as a Render/Koyeb
# *web* service (they require a bound port).  Render sets PORT automatically.
BOT_PORT = _env_int("BOT_PORT", _env_int("PORT", 0))
# Public URL of the bot service; it pings itself so a free web service is never
# put to sleep in the middle of a long audiobook.  Render sets RENDER_EXTERNAL_URL.
SELF_URL = _env("SELF_URL") or _env("RENDER_EXTERNAL_URL")
# Refuse to start a job when the disk has less than this much free space
# (one 12 h MP3 part at 48 kbit/s is ~260 MB; a 20 MB Hindi EPUB spans several).
MIN_FREE_DISK_MB = max(0, _env_int("MIN_FREE_DISK_MB", 400))
DB_PATH = _env("DB_PATH", "bot_database.db")
SESSION_NAME = _env("SESSION_NAME", "audiobook_pro_bot")
MAX_FILE_MB = max(1, _env_int("MAX_FILE_MB", 200))
# 20 MB Hindi EPUBs can contain 5-10 million characters; the extractor streams
# the text into the chunk plan so this is only a sanity cap, not a memory limit.
MAX_EXTRACTED_CHARS = max(1000, _env_int("MAX_EXTRACTED_CHARS", 50_000_000))
MAX_ARCHIVE_BYTES = 800 * 1024 * 1024
MAX_AUDIO_BYTES = 1900 * 1024 * 1024  # below Telegram's 2 GB limit
MAX_QUEUED_JOBS = max(MAX_PARALLEL_JOBS, _env_int("MAX_QUEUED_JOBS", 20))

# --- Never-give-up job engine ------------------------------------------------
# Every chunk's audio is checkpointed to disk (JOBS_DIR) and the job row in the
# database remembers how far it got, so a bot restart / Render sleep resumes the
# job instead of losing hours of synthesis.  When *every* engine fails at the
# same time (Microsoft throttling all IPs at once) the job PAUSES with an
# escalating back-off and keeps retrying instead of dying.
JOBS_DIR = _env("JOBS_DIR", os.path.join(os.path.dirname(os.path.abspath(DB_PATH)) or ".", "jobs"))
# 0 = wait forever for the engines to come back (only /cancel stops the job).
JOB_MAX_STALL_MINUTES = max(0, _env_int("JOB_MAX_STALL_MINUTES", 0))
# Escalating pause (seconds) between rounds when all engines are failing.
STALL_BACKOFF_STEPS = [5, 10, 20, 30, 60, 90, 120, 180, 300]
# Resume unfinished jobs automatically after the bot restarts.
RESUME_ON_START = os.getenv("RESUME_ON_START", "true").strip().lower() in ("1", "true", "yes", "on")
# Text is streamed through the planner in slices of this many characters so a
# 10-million-character book never needs to be held twice in memory.
PLAN_SLICE_CHARS = 200_000

# --- Built-in free Edge-TTS engine -------------------------------------------
# Microsoft's free endpoint silently throttles (HTTP 403 / closed websockets)
# when one IP opens too many streams at once.  These defaults are tuned to be
# noticeably faster than sequential generation while staying under that limit;
# the engine scales itself down automatically when it sees throttling and
# creeps back up once things are healthy again.
#
# Speed notes (why the defaults look the way they do):
#   * Edge accepts at most ~4096 bytes of (XML-escaped, UTF-8) text per websocket
#     message.  Hindi/Devanagari is 3 bytes per character, so a 2500-character
#     chunk used to be sent as TWO sequential connections inside one slot.
#     Chunks are therefore sized by *bytes* (LOCAL_TTS_MAX_BYTES) so every chunk
#     is exactly one connection and all of them can run in parallel.
#   * Chunks are fed through a sliding window instead of fixed batches, so a slow
#     chunk never leaves the other slots idle.
#   * 6 concurrent streams per IP is the highest level that stays reliably free
#     of HTTP 403 in practice; the limiter still halves itself on any throttle.
LOCAL_TTS_URL = "local://edge-tts"
LOCAL_TTS_ENABLED = (os.getenv("LOCAL_TTS_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
                     and edge_tts is not None)
LOCAL_TTS_MAX_CONCURRENCY = max(1, min(_env_int("LOCAL_TTS_MAX_CONCURRENCY", 6), 8))
LOCAL_TTS_CONCURRENCY = max(1, min(_env_int("LOCAL_TTS_CONCURRENCY", 4), LOCAL_TTS_MAX_CONCURRENCY))
LOCAL_TTS_CHUNK_SIZE = max(500, min(_env_int("LOCAL_TTS_CHUNK_SIZE", 3000), 5000))
# Edge's hard limit is 4096 bytes per message (after XML escaping); keep a margin.
LOCAL_TTS_MAX_BYTES = max(1000, min(_env_int("LOCAL_TTS_MAX_BYTES", 3900), 4000))
LOCAL_TTS_RETRIES = max(1, min(_env_int("LOCAL_TTS_RETRIES", 4), 8))
LOCAL_TTS_MIN_GAP = max(0, _env_int("LOCAL_TTS_MIN_GAP_MS", 150)) / 1000.0
LOCAL_TTS_TIMEOUT = max(20, _env_int("LOCAL_TTS_TIMEOUT", 90))  # seconds per chunk attempt
# How many consecutive clean chunks before the limiter adds one more stream.
LOCAL_TTS_GROW_AFTER = max(3, min(_env_int("LOCAL_TTS_GROW_AFTER", 8), 50))
# Chunks kept in flight / buffered ahead of the writer (bounds memory).
PIPELINE_WINDOW_EXTRA = 4
# Minimum seconds between progress-message edits (Telegram flood protection;
# every edit used to block the pipeline for a network round-trip).
PROGRESS_EDIT_INTERVAL = max(2.0, _env_int("PROGRESS_EDIT_INTERVAL", 4))
SUPPORTED_EXT = {".txt", ".md", ".docx", ".html", ".htm", ".epub", ".pdf"}
FFMPEG = shutil.which("ffmpeg")

DEFAULT_VOICE = "hi-IN-MadhurNeural"
DEFAULT_HOURS = 12
DEFAULT_RATE = "+0%"
DEFAULT_PITCH = "+0Hz"
DEFAULT_VOLUME = "+0%"
DEFAULT_SPLIT = "duration"  # or "chapter"

VOICE_RE = re.compile(r"^[a-z]{2,3}-[A-Za-z]{2,4}(-[A-Za-z]+)?-[A-Za-z0-9]+Neural$")
CHAPTER_RE = re.compile(
    r"^\s*(?:chapter|part|book|prologue|epilogue|अध्याय|भाग|प्रकरण|खंड)\b[^\n]{0,80}$",
    re.IGNORECASE | re.MULTILINE,
)

# Built-in voice catalogue: label -> voice id  (grouped for the picker)
VOICE_GROUPS: Dict[str, List[Tuple[str, str]]] = {
    "Hindi": [
        ("Madhur (Male)", "hi-IN-MadhurNeural"),
        ("Swara (Female)", "hi-IN-SwaraNeural"),
    ],
    "English (India)": [
        ("Prabhat (Male)", "en-IN-PrabhatNeural"),
        ("Neerja (Female)", "en-IN-NeerjaNeural"),
    ],
    "English (US)": [
        ("Christopher (Male)", "en-US-ChristopherNeural"),
        ("Guy (Male)", "en-US-GuyNeural"),
        ("Aria (Female)", "en-US-AriaNeural"),
        ("Jenny (Female)", "en-US-JennyNeural"),
    ],
    "English (UK)": [
        ("Ryan (Male)", "en-GB-RyanNeural"),
        ("Sonia (Female)", "en-GB-SoniaNeural"),
    ],
    "Indian Languages": [
        ("Bengali - Bashkar (M)", "bn-IN-BashkarNeural"),
        ("Bengali - Tanishaa (F)", "bn-IN-TanishaaNeural"),
        ("Tamil - Valluvar (M)", "ta-IN-ValluvarNeural"),
        ("Tamil - Pallavi (F)", "ta-IN-PallaviNeural"),
        ("Telugu - Mohan (M)", "te-IN-MohanNeural"),
        ("Telugu - Shruti (F)", "te-IN-ShrutiNeural"),
        ("Marathi - Manohar (M)", "mr-IN-ManoharNeural"),
        ("Marathi - Aarohi (F)", "mr-IN-AarohiNeural"),
        ("Gujarati - Niranjan (M)", "gu-IN-NiranjanNeural"),
        ("Gujarati - Dhwani (F)", "gu-IN-DhwaniNeural"),
        ("Urdu - Salman (M)", "ur-IN-SalmanNeural"),
        ("Urdu - Gul (F)", "ur-IN-GulNeural"),
    ],
}
VOICE_LABELS: Dict[str, str] = {vid: f"{lbl} [{grp}]" for grp, items in VOICE_GROUPS.items() for lbl, vid in items}

RATE_OPTIONS = [("Slow -25%", "-25%"), ("Slightly slow -10%", "-10%"), ("Normal +0%", "+0%"),
                ("Slightly fast +10%", "+10%"), ("Fast +25%", "+25%"), ("Very fast +50%", "+50%")]
PITCH_OPTIONS = [("Deep -20Hz", "-20Hz"), ("Low -10Hz", "-10Hz"), ("Normal +0Hz", "+0Hz"),
                 ("High +10Hz", "+10Hz"), ("Very high +20Hz", "+20Hz")]
VOLUME_OPTIONS = [("Quiet -20%", "-20%"), ("Normal +0%", "+0%"), ("Loud +20%", "+20%"), ("Very loud +50%", "+50%")]
HOURS_OPTIONS = [1, 2, 3, 6, 12, 24]

# Reply-keyboard button labels
BTN_REQUEST = "🔑 Request Access"
BTN_ACCOUNT = "👤 My Account"
BTN_SETTINGS = "⚙️ Settings"
BTN_CREATE = "🎧 Create Audio"
BTN_HISTORY = "📜 My History"
BTN_HELP = "❓ Help"
BTN_ADMIN = "👑 Admin Panel"
BTN_ADD_SERVER = "➕ Add Server"
BTN_DEL_SERVER = "🗑 Remove Server"
BTN_SERVER_STATUS = "🌐 Server Status"
BTN_APPROVE = "✅ Approve User"
BTN_REVOKE = "🚫 Revoke User"
BTN_USERS = "👥 Users List"
BTN_BROADCAST = "📢 Broadcast"
BTN_STATS = "📊 Bot Stats"
BTN_MAIN = "🔙 Main Menu"

# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=getattr(logging, _env("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("audiobook-bot")
try:
    _fh = RotatingFileHandler("bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
    log.addHandler(_fh)
except Exception:  # pragma: no cover
    pass
logging.getLogger("pyrogram").setLevel(logging.WARNING)


# =============================================================================
# Database layer
# =============================================================================
class Database:
    """Thin thread-safe wrapper around sqlite3 with schema migrations."""

    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._init_schema()

    # -- low level --------------------------------------------------------
    def execute(self, sql: str, params: Tuple = ()) -> sqlite3.Cursor:
        with self.lock:
            try:
                cur = self.conn.execute(sql, params)
                self.conn.commit()
                return cur
            except Exception:
                self.conn.rollback()
                raise

    def fetchone(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, params).fetchall()

    def _ensure_column(self, table: str, column: str, decl: str) -> None:
        cols = {r["name"] for r in self.fetchall(f"PRAGMA table_info({table})")}
        if column not in cols:
            self.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")

    # -- schema -----------------------------------------------------------
    def _init_schema(self) -> None:
        self.execute("PRAGMA journal_mode=WAL")
        self.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                expiry_date TEXT,
                usage_count INTEGER DEFAULT 0,
                is_admin INTEGER DEFAULT 0
            )""")
        self.execute("""
            CREATE TABLE IF NOT EXISTS render_servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE
            )""")
        self.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                voice TEXT DEFAULT 'hi-IN-MadhurNeural',
                max_hours INTEGER DEFAULT 12
            )""")
        self.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                source TEXT,
                chars INTEGER DEFAULT 0,
                parts INTEGER DEFAULT 0,
                audio_seconds INTEGER DEFAULT 0,
                status TEXT,
                created_at TEXT,
                finished_at TEXT
            )""")
        # migrations (each column checked individually - fixes the old all-or-nothing ALTER)
        for col, decl in [("first_name", "TEXT"), ("username", "TEXT"), ("is_banned", "INTEGER DEFAULT 0"),
                          ("chars_total", "INTEGER DEFAULT 0"), ("audio_seconds", "INTEGER DEFAULT 0"),
                          ("joined_at", "TEXT"), ("last_active", "TEXT")]:
            self._ensure_column("users", col, decl)
        for col, decl in [("rate", "TEXT DEFAULT '+0%'"), ("pitch", "TEXT DEFAULT '+0Hz'"),
                          ("volume", "TEXT DEFAULT '+0%'"), ("split_mode", "TEXT DEFAULT 'duration'")]:
            self._ensure_column("user_settings", col, decl)
        for col, decl in [("added_at", "TEXT"), ("fail_count", "INTEGER DEFAULT 0"), ("last_ok", "TEXT")]:
            self._ensure_column("render_servers", col, decl)
        # v4: resumable jobs.  The source text lives in JOBS_DIR/<job_id>/source.txt,
        # each finished chunk in JOBS_DIR/<job_id>/chunks/NNNNNN.mp3.
        for col, decl in [("title", "TEXT"), ("chat_id", "INTEGER"), ("status_msg_id", "INTEGER"),
                          ("settings_json", "TEXT"), ("total_chunks", "INTEGER DEFAULT 0"),
                          ("done_chunks", "INTEGER DEFAULT 0"), ("next_write", "INTEGER DEFAULT 0"),
                          ("delivered_chars", "INTEGER DEFAULT 0"), ("last_progress_at", "TEXT"),
                          ("stall_note", "TEXT"), ("resumes", "INTEGER DEFAULT 0")]:
            self._ensure_column("jobs", col, decl)
        self.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")

        # normalise legacy server URLs that were stored with a trailing /tts
        for row in self.fetchall("SELECT id, url FROM render_servers"):
            clean = normalize_server_url(row["url"])
            if clean != row["url"]:
                try:
                    self.execute("UPDATE render_servers SET url = ? WHERE id = ?", (clean, row["id"]))
                except sqlite3.IntegrityError:
                    self.execute("DELETE FROM render_servers WHERE id = ?", (row["id"],))

        # owner is always an approved admin
        self.execute(
            "INSERT OR IGNORE INTO users (user_id, expiry_date, is_admin, joined_at) VALUES (?, ?, 1, ?)",
            (OWNER_ID, "2099-12-31 23:59:59", now_str()),
        )
        self.execute("UPDATE users SET is_admin = 1, is_banned = 0, expiry_date = '2099-12-31 23:59:59' "
                     "WHERE user_id = ?", (OWNER_ID,))

    # -- users ------------------------------------------------------------
    def get_user(self, user_id: int) -> Optional[sqlite3.Row]:
        return self.fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))

    def touch_user(self, user) -> None:
        """Create/refresh the profile row (name, username, last_active)."""
        if user is None:
            return
        self.execute(
            "INSERT INTO users (user_id, first_name, username, joined_at, last_active) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET first_name = excluded.first_name, "
            "username = excluded.username, last_active = excluded.last_active",
            (user.id, (user.first_name or "")[:64], user.username or "", now_str(), now_str()),
        )

    def is_admin(self, user_id: int) -> bool:
        row = self.get_user(user_id)
        return bool(row and row["is_admin"] == 1 and not row["is_banned"])

    def is_approved(self, user_id: int) -> bool:
        row = self.get_user(user_id)
        if not row or row["is_banned"]:
            return False
        if row["is_admin"] == 1:
            return True
        return parse_dt(row["expiry_date"]) > datetime.now()

    def approve(self, user_id: int, days: int) -> str:
        if user_id <= 0 or not 1 <= days <= 36500:
            raise ValueError("User ID must be positive and days must be between 1 and 36500")
        expiry = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        self.execute(
            "INSERT INTO users (user_id, expiry_date, joined_at) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET expiry_date = excluded.expiry_date, is_banned = 0",
            (user_id, expiry, now_str()),
        )
        return expiry

    def revoke(self, user_id: int) -> None:
        self.execute("UPDATE users SET expiry_date = ? WHERE user_id = ? AND is_admin = 0",
                     ("2000-01-01 00:00:00", user_id))

    def set_banned(self, user_id: int, banned: bool) -> None:
        self.execute("UPDATE users SET is_banned = ? WHERE user_id = ? AND is_admin = 0",
                     (1 if banned else 0, user_id))

    def set_admin(self, user_id: int, admin: bool) -> None:
        self.execute("UPDATE users SET is_admin = ? WHERE user_id = ?", (1 if admin else 0, user_id))

    def list_users(self, only_active: bool = False) -> List[sqlite3.Row]:
        rows = self.fetchall("SELECT * FROM users ORDER BY last_active DESC, user_id")
        if only_active:
            rows = [r for r in rows if not r["is_banned"] and
                    (r["is_admin"] == 1 or parse_dt(r["expiry_date"]) > datetime.now())]
        return rows

    def admin_ids(self) -> List[int]:
        return [r["user_id"] for r in self.fetchall("SELECT user_id FROM users WHERE is_admin = 1")]

    def record_usage(self, user_id: int, chars: int, seconds: int) -> None:
        self.execute("UPDATE users SET usage_count = COALESCE(usage_count,0) + 1, chars_total = COALESCE(chars_total,0) + ?, "
                     "audio_seconds = COALESCE(audio_seconds,0) + ? WHERE user_id = ?", (chars, seconds, user_id))

    # -- settings ---------------------------------------------------------
    def get_settings(self, user_id: int) -> Dict[str, Any]:
        row = self.fetchone("SELECT * FROM user_settings WHERE user_id = ?", (user_id,))
        if not row:
            self.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,))
            return {"voice": DEFAULT_VOICE, "max_hours": DEFAULT_HOURS, "rate": DEFAULT_RATE,
                    "pitch": DEFAULT_PITCH, "volume": DEFAULT_VOLUME, "split_mode": DEFAULT_SPLIT}
        def option(key, choices, default):
            return row[key] if row[key] in choices else default
        try:
            hours = max(1, min(24, int(row["max_hours"] or DEFAULT_HOURS)))
        except (ValueError, TypeError):
            hours = DEFAULT_HOURS
        voice = row["voice"] or DEFAULT_VOICE
        return {
            "voice": voice if isinstance(voice, str) and VOICE_RE.fullmatch(voice) else DEFAULT_VOICE,
            "max_hours": hours,
            "rate": option("rate", {v for _, v in RATE_OPTIONS}, DEFAULT_RATE),
            "pitch": option("pitch", {v for _, v in PITCH_OPTIONS}, DEFAULT_PITCH),
            "volume": option("volume", {v for _, v in VOLUME_OPTIONS}, DEFAULT_VOLUME),
            "split_mode": option("split_mode", {"duration", "chapter"}, DEFAULT_SPLIT),
        }

    def set_setting(self, user_id: int, key: str, value: Any) -> None:
        if key not in {"voice", "max_hours", "rate", "pitch", "volume", "split_mode"}:
            raise ValueError("Unsupported settings key")
        self.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,))
        self.execute(f"UPDATE user_settings SET {key} = ? WHERE user_id = ?", (value, user_id))

    def reset_settings(self, user_id: int) -> None:
        self.execute("DELETE FROM user_settings WHERE user_id = ?", (user_id,))

    # -- servers ----------------------------------------------------------
    def servers(self) -> List[str]:
        return [r["url"] for r in self.fetchall("SELECT url FROM render_servers ORDER BY id")]

    def add_server(self, url: str) -> bool:
        try:
            self.execute("INSERT INTO render_servers (url, added_at) VALUES (?, ?)", (url, now_str()))
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_server(self, url: str) -> int:
        return self.execute("DELETE FROM render_servers WHERE url = ?", (url,)).rowcount

    def server_health(self, url: str, ok: bool) -> None:
        if ok:
            self.execute("UPDATE render_servers SET fail_count = 0, last_ok = ? WHERE url = ?", (now_str(), url))
        else:
            self.execute("UPDATE render_servers SET fail_count = fail_count + 1 WHERE url = ?", (url,))

    # -- jobs -------------------------------------------------------------
    def job_start(self, user_id: int, source: str, chars: int) -> int:
        cur = self.execute("INSERT INTO jobs (user_id, source, chars, status, created_at) VALUES (?, ?, ?, ?, ?)",
                           (user_id, source[:120], chars, "running", now_str()))
        return int(cur.lastrowid)

    def job_finish(self, job_id: int, status: str, parts: int, seconds: int) -> None:
        self.execute("UPDATE jobs SET status = ?, parts = ?, audio_seconds = ?, finished_at = ? WHERE id = ?",
                     (status, parts, seconds, now_str(), job_id))

    # -- resumable job checkpoints (v4) --------------------------------------
    def job_init_resume(self, job_id: int, title: str, chat_id: int, status_msg_id: Optional[int],
                        settings: Dict[str, Any], total_chunks: int) -> None:
        self.execute("UPDATE jobs SET title = ?, chat_id = ?, status_msg_id = ?, settings_json = ?, "
                     "total_chunks = ?, last_progress_at = ? WHERE id = ?",
                     (title[:120], chat_id, status_msg_id, json.dumps(settings), total_chunks, now_str(), job_id))

    def job_checkpoint(self, job_id: int, next_write: int, done_chunks: int, parts: int,
                       seconds: int, delivered_chars: int, note: str = "") -> None:
        self.execute("UPDATE jobs SET next_write = ?, done_chunks = ?, parts = ?, audio_seconds = ?, "
                     "delivered_chars = ?, last_progress_at = ?, stall_note = ? WHERE id = ?",
                     (next_write, done_chunks, parts, seconds, delivered_chars, now_str(), note[:200], job_id))

    def job_set_status(self, job_id: int, status: str, note: str = "") -> None:
        self.execute("UPDATE jobs SET status = ?, stall_note = ? WHERE id = ?", (status, note[:200], job_id))

    def job_get(self, job_id: int) -> Optional[sqlite3.Row]:
        return self.fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))

    def resumable_jobs(self) -> List[sqlite3.Row]:
        """Jobs interrupted by a restart that still have their source text on disk."""
        return self.fetchall("SELECT * FROM jobs WHERE status IN ('running', 'paused', 'interrupted') "
                             "AND chat_id IS NOT NULL AND settings_json IS NOT NULL ORDER BY id")

    def user_resumable_job(self, user_id: int) -> Optional[sqlite3.Row]:
        return self.fetchone("SELECT * FROM jobs WHERE user_id = ? AND status IN ('paused', 'interrupted') "
                             "AND chat_id IS NOT NULL AND settings_json IS NOT NULL ORDER BY id DESC LIMIT 1",
                             (user_id,))

    def running_jobs(self) -> List[sqlite3.Row]:
        return self.fetchall("SELECT * FROM jobs WHERE status IN ('running', 'paused') ORDER BY id")

    def job_history(self, user_id: int, limit: int = 10) -> List[sqlite3.Row]:
        return self.fetchall("SELECT * FROM jobs WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit))

    def global_stats(self) -> Dict[str, Any]:
        u = self.fetchone("SELECT COUNT(*) c FROM users")["c"]
        a = len(self.list_users(only_active=True))
        j = self.fetchone("SELECT COUNT(*) c, COALESCE(SUM(chars),0) ch, COALESCE(SUM(audio_seconds),0) s, "
                          "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) ok FROM jobs")
        return {"users": u, "active_users": a, "jobs": j["c"], "jobs_ok": j["ok"] or 0,
                "chars": j["ch"], "audio_seconds": j["s"], "servers": len(self.servers())}


# =============================================================================
# Small helpers
# =============================================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: Optional[str]) -> datetime:
    if not value:
        return datetime.min
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return datetime.min


def normalize_server_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    try:
        parsed = urlsplit(url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            return ""
        if parsed.query or parsed.fragment or any(c.isspace() for c in parsed.netloc):
            return ""
        _ = parsed.port
        path = parsed.path.rstrip("/")
        for suffix in ("/tts", "/health"):
            if path.endswith(suffix):
                path = path[:-len(suffix)].rstrip("/")
        return urlunsplit((parsed.scheme.lower(), parsed.netloc, path, "", ""))
    except ValueError:
        return ""


def fmt_duration(seconds: float) -> str:
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def fmt_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024  # type: ignore
    return f"{nbytes:.1f} TB"


def progress_bar(percent: int, width: int = 12) -> str:
    filled = int(round(width * percent / 100))
    return "█" * filled + "░" * (width - filled)


def voice_label(voice: str) -> str:
    return VOICE_LABELS.get(voice, voice)


def safe_filename(name: str) -> str:
    name = re.sub(r"[^\w\s.-]", "", name, flags=re.UNICODE).strip()
    return re.sub(r"\s+", "_", name)[:60] or "audiobook"


async def safe_edit(msg: Message, text: str, reply_markup=None) -> None:
    """edit_text that tolerates MessageNotModified / FloodWait."""
    try:
        await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(float(getattr(e, "value", getattr(e, "x", 5))) + 1)
        try:
            await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
        except Exception:
            pass
    except RPCError as e:
        log.debug("safe_edit failed: %s", e)


async def safe_send(client: Client, chat_id: int, text: str, **kwargs) -> Optional[Message]:
    try:
        return await client.send_message(chat_id, text, disable_web_page_preview=True, **kwargs)
    except FloodWait as e:
        await asyncio.sleep(float(getattr(e, "value", getattr(e, "x", 5))) + 1)
        try:
            return await client.send_message(chat_id, text, disable_web_page_preview=True, **kwargs)
        except Exception:
            return None
    except Exception as e:  # user blocked bot, etc.
        log.debug("safe_send to %s failed: %s", chat_id, e)
        return None


# =============================================================================
# Text extraction & chunking
# =============================================================================
def _read_text_file(path: str) -> str:
    with open(path, "rb") as f:
        raw = f.read()
    # Decode UTF-16/32 only when a BOM or NUL pattern identifies it.
    if raw.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
        return raw.decode("utf-32")
    if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return raw.decode("utf-16")
    if b"\x00" in raw[:512]:
        even = raw[:512:2].count(0)
        odd = raw[1:512:2].count(0)
        if max(even, odd) > max(1, len(raw[:512]) // 8):
            return raw.decode("utf-16-le" if odd > even else "utf-16-be")
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not decode text file")


def _html_to_text(markup: Any) -> str:
    if isinstance(markup, bytes):
        markup = markup.decode("utf-8", errors="replace")
    if BeautifulSoup is None:
        return html.unescape(re.sub(r"<[^>]+>", "\n", str(markup)))
    soup = BeautifulSoup(markup, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "noscript"]):
        tag.decompose()
    # keep paragraph breaks
    for br in soup.find_all("br"):
        br.replace_with("\n")
    return soup.get_text(separator="\n")


def extract_text_to_file(path: str, ext: str, out_path: str) -> int:
    """Stream-extract a document into ``out_path`` (UTF-8) and return the char count.

    Runs in a worker thread.  EPUB / PDF are processed item-by-item and written
    as we go, so a 20 MB book with millions of characters never has to be held
    in memory as one giant string (the old ``"\\n".join(texts)`` doubled RAM).
    Each written piece is already passed through :func:`clean_text`.
    """
    if os.path.abspath(path) == os.path.abspath(out_path):
        raise ValueError("Extraction output path must differ from the source path")
    total = 0
    with open(out_path, "w", encoding="utf-8") as out:
        def emit(piece: str) -> None:
            nonlocal total
            piece = clean_text(piece)
            if not piece:
                return
            if total:
                out.write("\n\n")
                total += 2
            out.write(piece)
            total += len(piece)
            if total > MAX_EXTRACTED_CHARS:
                raise ValueError(f"Document exceeds the {MAX_EXTRACTED_CHARS:,} character limit")

        if ext in {".epub", ".docx"}:
            with zipfile.ZipFile(path) as archive:
                members = archive.infolist()
                if len(members) > 20000 or sum(m.file_size for m in members) > MAX_ARCHIVE_BYTES:
                    raise ValueError("Expanded document is too large")
        if ext == ".epub":
            if epub is None:
                raise RuntimeError("ebooklib is not installed on the bot server")
            book = epub.read_epub(path, options={"ignore_ncx": True}) \
                if "options" in epub.read_epub.__code__.co_varnames else epub.read_epub(path)
            spine_ids = [item[0] for item in getattr(book, "spine", [])]
            items = {it.get_id(): it for it in book.get_items() if it.get_type() == ebooklib.ITEM_DOCUMENT}
            ordered = [items[i] for i in spine_ids if i in items] + [it for k, it in items.items() if k not in spine_ids]
            for it in ordered:
                emit(_html_to_text(it.get_body_content()))
            return total
        if ext == ".pdf":
            if PdfReader is None:
                raise RuntimeError("pypdf is not installed on the bot server")
            reader = PdfReader(path)
            for page in reader.pages:
                emit(page.extract_text() or "")
            return total
        emit(extract_text(path, ext))
    return total


def extract_text(path: str, ext: str) -> str:
    """Extract plain text from a supported document (runs in a worker thread)."""
    if ext in {".epub", ".docx"}:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) > 20000 or sum(m.file_size for m in members) > MAX_ARCHIVE_BYTES:
                raise ValueError("Expanded document is too large")
    if ext in (".txt", ".md"):
        return _read_text_file(path)
    if ext == ".docx":
        if docx is None:
            raise RuntimeError("python-docx is not installed on the bot server")
        d = docx.Document(path)
        parts = [p.text for p in d.paragraphs]
        for table in d.tables:  # include table text too
            for row in table.rows:
                parts.append(" | ".join(c.text for c in row.cells))
        return "\n".join(parts)
    if ext in (".html", ".htm"):
        return _html_to_text(_read_text_file(path))
    if ext == ".epub":
        if epub is None:
            raise RuntimeError("ebooklib is not installed on the bot server")
        book = epub.read_epub(path, options={"ignore_ncx": True}) if "options" in epub.read_epub.__code__.co_varnames \
            else epub.read_epub(path)
        texts = []
        # respect reading order (spine) when available
        spine_ids = [item[0] for item in getattr(book, "spine", [])]
        items = {it.get_id(): it for it in book.get_items() if it.get_type() == ebooklib.ITEM_DOCUMENT}
        ordered = [items[i] for i in spine_ids if i in items] + [it for k, it in items.items() if k not in spine_ids]
        for it in ordered:
            texts.append(_html_to_text(it.get_body_content()))
        return "\n".join(texts)
    if ext == ".pdf":
        if PdfReader is None:
            raise RuntimeError("pypdf is not installed on the bot server")
        reader = PdfReader(path)
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    raise RuntimeError(f"Unsupported file type: {ext}")


def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


_SENTENCE_END = re.compile(r"(?<=[.!?।؟。])\s+")


def split_text(text: str, max_len: int) -> List[str]:
    """Split at word boundaries when possible; hard-split oversized tokens."""
    if not isinstance(max_len, int) or max_len <= 0:
        raise ValueError("max_len must be a positive integer")
    chunks: List[str] = []
    for para in text.split("\n\n"):
        para = para.strip()
        if not para:
            continue
        if len(para) <= max_len:
            _append_chunk(chunks, para, max_len)
            continue
        buf = ""
        for sentence in _SENTENCE_END.split(para):
            sentence = sentence.strip()
            if not sentence:
                continue
            while len(sentence) > max_len:  # extremely long sentence -> split on spaces
                cut = sentence.rfind(" ", 0, max_len)
                cut = cut if cut > max_len // 3 else max_len
                if buf:
                    chunks.append(buf)
                    buf = ""
                chunks.append(sentence[:cut].strip())
                sentence = sentence[cut:].strip()
            if len(buf) + len(sentence) + 1 <= max_len:
                buf = f"{buf} {sentence}".strip()
            else:
                if buf:
                    chunks.append(buf)
                buf = sentence
        if buf:
            _append_chunk(chunks, buf, max_len)
    return [c for c in chunks if c.strip()]


def _append_chunk(chunks: List[str], piece: str, max_len: int) -> None:
    """Merge small paragraphs together to reduce request count."""
    if chunks and len(chunks[-1]) + len(piece) + 1 <= max_len * 0.9:
        chunks[-1] = f"{chunks[-1]}\n{piece}"
    else:
        chunks.append(piece)


def edge_payload_bytes(text: str) -> int:
    """Bytes Edge-TTS will put on the wire for ``text`` (XML-escaped UTF-8).

    Microsoft's websocket accepts ~4096 bytes per message; edge-tts silently
    splits anything bigger into several *sequential* connections, which is the
    main reason Hindi (3 bytes/char) audiobooks felt slow.
    """
    return len(html.escape(text, quote=False).encode("utf-8"))


def split_text_for_tts(text: str, max_chars: int, max_bytes: Optional[int] = None) -> List[str]:
    """Sentence-aware chunking bounded by characters *and* (optionally) wire bytes.

    Every returned chunk satisfies ``len(chunk) <= max_chars`` and, when
    ``max_bytes`` is given, ``edge_payload_bytes(chunk) <= max_bytes`` so that each
    chunk maps to exactly one Edge-TTS connection and all chunks can run in parallel.
    """
    chunks = split_text(text, max_chars)
    if not max_bytes or max_bytes <= 0:
        return chunks
    out: List[str] = []
    for chunk in chunks:
        if edge_payload_bytes(chunk) <= max_bytes:
            out.append(chunk)
            continue
        # Derive a character budget from this chunk's own byte density, then
        # tighten it until every piece fits (density can vary inside a chunk).
        density = edge_payload_bytes(chunk) / max(1, len(chunk))
        budget = max(50, min(max_chars, int(max_bytes / density * 0.97)))
        pieces = split_text(chunk, budget)
        while any(edge_payload_bytes(p) > max_bytes for p in pieces) and budget > 50:
            budget = max(50, int(budget * 0.85))
            pieces = split_text(chunk, budget)
        out.extend(pieces)
    return [c for c in out if c.strip()]


def iter_text_slices(text: str, slice_chars: int = PLAN_SLICE_CHARS):
    """Yield ``text`` in slices that end on a paragraph boundary when possible."""
    n = len(text)
    pos = 0
    while pos < n:
        end = min(n, pos + slice_chars)
        if end < n:
            cut = text.rfind("\n\n", pos + slice_chars // 2, end)
            if cut == -1:
                cut = text.rfind("\n", pos + slice_chars // 2, end)
            if cut == -1:
                cut = text.rfind(" ", pos + slice_chars // 2, end)
            if cut > pos:
                end = cut
        yield text[pos:end]
        pos = end
        while pos < n and text[pos] in " \n":
            pos += 1


def build_plan(text: str, chunk_limit: int, max_bytes: Optional[int], chapter_mode: bool) -> List[Tuple[str, str]]:
    """Return ``[(label, chunk_text), ...]`` for the whole book.

    Chapters (optional) are detected on the full text; each chapter body is then
    fed through the sentence/byte-aware splitter in slices so planning a
    multi-million-character book stays memory-flat.
    """
    plan: List[Tuple[str, str]] = []
    sections = split_chapters(text) if chapter_mode else [("Audiobook", text)]
    for label, body in sections:
        for piece in iter_text_slices(body):
            for chunk in split_text_for_tts(piece, chunk_limit, max_bytes):
                plan.append((label, chunk))
    return plan


def write_plan(plan_path: str, plan: List[Tuple[str, str]]) -> None:
    with open(plan_path, "w", encoding="utf-8") as f:
        for label, chunk in plan:
            f.write(json.dumps({"l": label, "t": chunk}, ensure_ascii=False) + "\n")


def read_plan(plan_path: str) -> List[Tuple[str, str]]:
    plan: List[Tuple[str, str]] = []
    with open(plan_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rec = json.loads(line)
                plan.append((rec["l"], rec["t"]))
    return plan


def split_chapters(text: str) -> List[Tuple[str, str]]:
    """Return [(title, body)] using chapter headings; single entry if none found."""
    matches = list(CHAPTER_RE.finditer(text))
    if len(matches) < 2:
        return [("Audiobook", text)]
    chapters: List[Tuple[str, str]] = []
    preface = text[: matches[0].start()].strip()
    if preface:
        chapters.append(("Introduction", preface))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[m.start():end].strip()  # Preserve headings as spoken text.
        title = m.group(0).strip()[:60]
        if body:
            chapters.append((title, body))
    return chapters or [("Audiobook", text)]


def estimate_seconds(chars: int, rate: str) -> float:
    """~15 chars/sec for neural voices at +0%, adjusted for rate."""
    try:
        pct = int(rate.replace("%", ""))
    except ValueError:
        pct = 0
    return max(0, chars) / 15.0 / max(0.01, 1 + pct / 100.0)


# =============================================================================
# Render-server pool (health scoring + failover)
# =============================================================================
@dataclass
class ServerState:
    url: str
    failures: int = 0
    successes: int = 0
    latency: float = 0.0
    cooldown_until: float = 0.0
    last_error: str = ""
    # consecutive failures (reset on success) - drives the per-server cooldown
    streak: int = 0

    @property
    def label(self) -> str:
        return "built-in Edge-TTS" if self.is_local else self.url

    @property
    def tts_url(self) -> str:
        return f"{self.url}/tts"

    @property
    def is_local(self) -> bool:
        return self.url == LOCAL_TTS_URL

    @property
    def score(self) -> float:
        return self.latency + self.failures * 2.0


class ServerPool:
    def __init__(self, urls: List[str]) -> None:
        self.states = [ServerState(u) for u in dict.fromkeys(urls) if u]
        if not self.states:
            raise ValueError("No render servers are configured")
        self._rr = 0

    def available(self) -> List[ServerState]:
        now = time.time()
        live = [s for s in self.states if s.cooldown_until <= now]
        return live

    def pick(self) -> ServerState:
        live = self.available()
        if not live:
            return min(self.states, key=lambda s: s.cooldown_until)
        self._rr += 1
        # mostly round-robin, but skew toward the best scoring server
        if self._rr % 3 == 0:
            return min(live, key=lambda s: s.score)
        return live[self._rr % len(live)]

    def report(self, s: ServerState, ok: bool, latency: float = 0.0, error: str = "") -> None:
        if ok:
            s.cooldown_until = 0
            s.successes += 1
            s.streak = 0
            s.failures = max(0, s.failures - 1)
            s.latency = latency if not s.latency else (s.latency * 0.7 + latency * 0.3)
            s.last_error = ""
            if not s.is_local:
                remote_limiter(s.url).report_success()
        else:
            s.failures += 1
            s.streak += 1
            if error:
                s.last_error = error[:200]
            if s.streak >= 2:
                # Escalating per-server cooldown; never permanent - the server may recover.
                s.cooldown_until = time.time() + min(180, 10 * s.streak)

    def all_cooling(self) -> bool:
        """True when every engine is in cooldown (global throttle / outage)."""
        now = time.time()
        return all(s.cooldown_until > now for s in self.states)

    def soonest_available(self) -> float:
        """Seconds until at least one engine leaves cooldown (0 if one is free)."""
        now = time.time()
        return max(0.0, min(s.cooldown_until for s in self.states) - now)

    def reset_cooldowns(self) -> None:
        for s in self.states:
            s.cooldown_until = 0
            s.streak = 0

    def failure_summary(self) -> str:
        """Human readable 'why did every engine fail' text for error messages."""
        parts = [f"{s.label} -> {s.last_error}" for s in self.states if s.last_error]
        if not parts:
            parts = [f"{s.label} -> no response" for s in self.states]
        return "; ".join(parts)

    def concurrency(self) -> int:
        remote = sum(remote_limiter(s.url).current for s in self.states if not s.is_local)
        local = local_limiter.current if any(s.is_local for s in self.states) else 0
        return max(1, min(MAX_TOTAL_CONCURRENCY, remote + local))

    def max_concurrency(self) -> int:
        """Upper bound used to size the request pipeline.

        The real per-engine throttles (``local_limiter`` / ``remote_limiter``) are
        enforced inside ``fetch_chunk``; this only has to be large enough that the
        adaptive limiters can actually grow up to their ceilings mid-job.  It scales
        linearly with the number of servers: more IPs = more total throughput.
        """
        remote = sum(remote_limiter(s.url).maximum for s in self.states if not s.is_local)
        local = local_limiter.maximum if any(s.is_local for s in self.states) else 0
        return max(1, min(MAX_TOTAL_CONCURRENCY, remote + local))


def _headers() -> Dict[str, str]:
    h = {"User-Agent": f"AudioBookPro/{VERSION}"}
    if TTS_API_KEY:
        h["X-API-Key"] = TTS_API_KEY
    return h


async def _describe_http_error(resp: aiohttp.ClientResponse) -> str:
    """Short, user-facing reason for a non-200 render-server reply."""
    detail = ""
    try:
        body = await resp.text()
        try:
            j = json.loads(body)
            detail = str(j.get("error") or j.get("message") or "") if isinstance(j, dict) else ""
        except ValueError:
            detail = body.strip()
    except Exception:  # noqa: BLE001
        pass
    detail = re.sub(r"\s+", " ", detail)[:120]
    if resp.status == 401:
        hint = ("API key rejected - set TTS_API_KEY on the bot to the same value as API_KEY on the server"
                if TTS_API_KEY else "server requires an API key - set TTS_API_KEY on the bot (same as API_KEY on the server)")
        return f"HTTP 401: {hint}"
    if resp.status == 404:
        return "HTTP 404: /tts endpoint not found (is this really an app.py render server?)"
    if resp.status == 429:
        return f"HTTP 429: rate limited{(' - ' + detail) if detail else ''}"
    if resp.status in (502, 503, 504):
        if "noaudio" in detail.lower().replace(" ", "") or "no audio" in detail.lower():
            return f"HTTP {resp.status}: Microsoft Edge endpoint is throttling this server's IP (no audio) - will retry"
        return f"HTTP {resp.status}: server busy / upstream error{(' - ' + detail) if detail else ''} - will retry"
    return f"HTTP {resp.status}{(': ' + detail) if detail else ''}"


# A realistic probe sentence (Hindi, ~120 chars).  A 2-letter "OK" probe used to
# pass while every real chunk failed, so /servers showed 4/4 green during outages.
PROBE_TEXT = "यह एक परीक्षण वाक्य है। इसका उपयोग यह जाँचने के लिए किया जाता है कि सर्वर सही ढंग से ऑडियो बना रहा है या नहीं।"


async def probe_tts(session: aiohttp.ClientSession, url: str) -> Tuple[bool, str]:
    """Authenticated end-to-end /tts probe: proves the bot can actually render on this server."""
    payload = {"text": PROBE_TEXT, "voice": DEFAULT_VOICE, "rate": "+0%", "pitch": "+0Hz", "volume": "+0%"}
    try:
        async with session.post(f"{url}/tts", json=payload, headers=_headers(), allow_redirects=False,
                                timeout=aiohttp.ClientTimeout(total=90)) as r:
            if r.status != 200:
                return False, await _describe_http_error(r)
            data = await r.content.read(4096)
            mp3 = data.startswith(b"ID3") or (len(data) >= 2 and data[0] == 255 and data[1] & 224 == 224)
            if not mp3:
                return False, "HTTP 200 but response is not MP3 audio (proxy / login page?)"
            return True, ""
    except asyncio.TimeoutError:
        return False, "timed out waiting for /tts (server asleep or overloaded)"
    except aiohttp.ClientError as exc:
        return False, f"connection error: {type(exc).__name__}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {str(exc)[:80]}"


async def check_server(session: aiohttp.ClientSession, url: str, deep: bool = False) -> Dict[str, Any]:
    """Probe /health (preferred) or / for a render server.

    With ``deep=True`` an authenticated ``/tts`` request is made as well, so an
    API-key mismatch or broken synthesis shows up as ``ok=False`` with a reason
    instead of every chunk failing later during a real job.
    """
    info: Dict[str, Any] = {"url": url, "ok": False, "latency": None, "version": None, "active": None,
                            "error": "", "auth_required": None}
    if url == LOCAL_TTS_URL:
        return await local_engine_health()
    if not normalize_server_url(url):
        info["error"] = "invalid URL"
        return info
    t0 = time.time()
    try:
        async with session.get(f"{url}/health", headers=_headers(),
                               timeout=aiohttp.ClientTimeout(total=25)) as r:
            info["latency"] = round(time.time() - t0, 2)
            if r.status == 200:
                try:
                    j = await r.json(content_type=None)
                    if not isinstance(j, dict) or j.get("status") != "ok":
                        info["error"] = "/health did not return status=ok"
                        return info
                    info["version"] = j.get("version")
                    info["active"] = j.get("active_jobs")
                    info["auth_required"] = j.get("auth_required")
                    info["throttled"] = bool(j.get("throttled"))
                    info["queued"] = j.get("queued")
                    limit = j.get("max_text_length")
                    if isinstance(limit, int) and limit > 0:
                        info["max_text_length"] = limit
                except (ValueError, TypeError):
                    info["error"] = "/health returned invalid JSON"
                    return info
                info["ok"] = True
            else:
                info["error"] = await _describe_http_error(r)
    except Exception as exc:  # noqa: BLE001
        info["error"] = f"unreachable ({type(exc).__name__})"
    if not info["ok"] and not info["version"]:
        try:
            t0 = time.time()
            async with session.get(url, headers=_headers(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                info["latency"] = round(time.time() - t0, 2)
                if r.status == 200 and (await r.text()).strip() == "OK":
                    info["ok"] = True
                    info["error"] = ""
        except Exception:  # noqa: BLE001
            pass
    if info["ok"] and info.get("auth_required") and not TTS_API_KEY:
        info["ok"] = False
        info["error"] = "server requires an API key but TTS_API_KEY is not set on the bot"
        return info
    if info["ok"] and deep:
        ok, reason = await probe_tts(session, url)
        if not ok:
            info["ok"] = False
            info["error"] = reason
    return info


_network_slots = asyncio.Semaphore(MAX_TOTAL_CONCURRENCY)


# =============================================================================
# Built-in FREE Edge-TTS engine (runs inside the bot, no API key)
# =============================================================================
class ThrottleError(RuntimeError):
    """Raised when Microsoft's free endpoint rejects / throttles a request."""


def _is_throttle_error(exc: BaseException) -> bool:
    """Classify edge-tts / aiohttp failures that indicate rate limiting."""
    name = type(exc).__name__
    text = str(exc).lower()
    if isinstance(exc, ThrottleError):
        return True
    if "403" in text or "429" in text or "too many" in text or "throttl" in text:
        return True
    if name in {"WSServerHandshakeError", "ClientResponseError", "ClientConnectorError",
                "ServerDisconnectedError", "WebSocketError", "NoAudioReceived", "ClientOSError"}:
        return True
    if isinstance(exc, asyncio.TimeoutError):
        return True
    return False


class AdaptiveLimiter:
    """Semaphore-like limiter whose capacity shrinks on throttling and grows when healthy.

    * Only `current` coroutines may synthesise at the same time.
    * New connections are spaced at least `min_gap` seconds apart (burst protection).
    * After a throttle signal the capacity halves and a cool-down starts.
    * After `grow_after` consecutive successes the capacity grows by one, up to `maximum`.
    """

    def __init__(self, start: int, maximum: int, min_gap: float) -> None:
        self.maximum = max(1, maximum)
        self.current = max(1, min(start, self.maximum))
        self.min_gap = max(0.0, min_gap)
        self.grow_after = LOCAL_TTS_GROW_AFTER
        self._active = 0
        self._streak = 0
        self._last_start = 0.0
        self._cooldown_until = 0.0
        self._cond: Optional[asyncio.Condition] = None
        self._gap_lock: Optional[asyncio.Lock] = None
        self.throttle_events = 0

    # Lazily create primitives on the running loop (safe for import-time construction).
    def _ensure(self) -> None:
        if self._cond is None:
            self._cond = asyncio.Condition()
            self._gap_lock = asyncio.Lock()

    @property
    def active(self) -> int:
        return self._active

    async def acquire(self, cancel: Optional[asyncio.Event] = None) -> None:
        self._ensure()
        assert self._cond is not None and self._gap_lock is not None
        async with self._cond:
            while self._active >= self.current:
                if cancel is not None and cancel.is_set():
                    raise asyncio.CancelledError
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
            self._active += 1
        # Serialise connection starts so we never fire a burst at the endpoint.
        async with self._gap_lock:
            now = time.monotonic()
            wait = max(self._cooldown_until - now, self._last_start + self.min_gap - now)
            if wait > 0:
                if cancel is not None:
                    await cancellable_sleep(wait, cancel)
                else:
                    await asyncio.sleep(wait)
            self._last_start = time.monotonic()

    async def release(self) -> None:
        self._ensure()
        assert self._cond is not None
        async with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()

    def report_success(self) -> None:
        self._streak += 1
        if self._streak >= self.grow_after and self.current < self.maximum:
            self.current += 1
            self._streak = 0
            log.info("Edge-TTS healthy - concurrency raised to %d", self.current)

    def report_throttle(self) -> float:
        """Shrink capacity; return the back-off delay the caller should sleep."""
        self._streak = 0
        self.throttle_events += 1
        new = max(1, self.current // 2)
        if new != self.current:
            log.warning("Edge-TTS throttling detected - concurrency lowered %d -> %d", self.current, new)
        self.current = new
        pause = min(30.0, 2.0 * self.throttle_events) + random.uniform(0.0, 1.0)
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + pause)
        return pause

    def report_ok_window(self) -> None:
        """Forget old throttle events once traffic has been clean for a while."""
        if self._streak >= self.grow_after * 2:
            self.throttle_events = 0

    def snapshot(self) -> Dict[str, Any]:
        return {"active": self._active, "limit": self.current, "max": self.maximum,
                "throttle_events": self.throttle_events}


local_limiter = AdaptiveLimiter(LOCAL_TTS_CONCURRENCY, LOCAL_TTS_MAX_CONCURRENCY, LOCAL_TTS_MIN_GAP)

# One adaptive limiter per render server.  Each server sits on its own IP and
# has its own Edge-TTS quota, so throttling on one must not slow the others.
_remote_limiters: Dict[str, AdaptiveLimiter] = {}


def remote_limiter(url: str) -> AdaptiveLimiter:
    lim = _remote_limiters.get(url)
    if lim is None:
        # Start at PER_SERVER_CONCURRENCY, allow growth up to 2x (max 8) while healthy.
        lim = AdaptiveLimiter(PER_SERVER_CONCURRENCY, min(8, max(PER_SERVER_CONCURRENCY, PER_SERVER_CONCURRENCY * 2)),
                              0.1)
        _remote_limiters[url] = lim
    return lim


def _make_local_communicate(text: str, voice: str, rate: str, pitch: str, volume: str):
    """Build edge_tts.Communicate across edge-tts versions (boundary/pitch kwargs are newer)."""
    assert edge_tts is not None
    try:
        # SentenceBoundary = far fewer metadata frames than WordBoundary (less
        # websocket chatter per chunk) while still giving us the audio duration.
        return edge_tts.Communicate(text, voice, rate=rate, pitch=pitch, volume=volume,
                                    boundary="SentenceBoundary", connect_timeout=15, receive_timeout=60)
    except TypeError:
        pass
    try:
        return edge_tts.Communicate(text, voice, rate=rate, pitch=pitch, volume=volume)
    except TypeError:
        return edge_tts.Communicate(text, voice, rate=rate, volume=volume)


async def _local_synth_once(text: str, settings: Dict[str, Any]) -> Tuple[bytes, int]:
    communicate = _make_local_communicate(text, settings["voice"], settings["rate"],
                                          settings["pitch"], settings["volume"])
    audio = bytearray()
    duration_ms = 0
    async for chunk in communicate.stream():
        ctype = chunk.get("type")
        if ctype == "audio":
            audio.extend(chunk["data"])
        elif ctype in ("WordBoundary", "SentenceBoundary"):
            start = chunk.get("offset", 0) / 10_000
            dur = chunk.get("duration", 0) / 10_000
            duration_ms = max(duration_ms, int(start + dur))
    if len(audio) < 100:
        raise ThrottleError("Edge-TTS returned no audio (endpoint may be throttling)")
    if duration_ms <= 0:
        # Edge streams 48 kbit/s mono MP3 - derive the length from the byte count.
        duration_ms = int(len(audio) * 8 / 48)
    return bytes(audio), duration_ms


def _describe_exc(exc: Optional[BaseException]) -> str:
    if exc is None:
        return "unknown error"
    text = re.sub(r"\s+", " ", str(exc)).strip()
    name = type(exc).__name__
    if isinstance(exc, asyncio.TimeoutError):
        return "timed out"
    if "403" in text or name == "WSServerHandshakeError":
        return f"{name}: Microsoft Edge endpoint refused the connection (HTTP 403 - IP throttled / blocked)"
    return f"{name}: {text[:120]}" if text else name


local_last_error: str = ""


async def local_synthesize(text: str, settings: Dict[str, Any], cancel: asyncio.Event,
                           attempts: int = LOCAL_TTS_RETRIES) -> Tuple[Optional[bytes], int]:
    """Synthesise one chunk with the free Edge-TTS endpoint.

    Uses the adaptive limiter so we stay fast but under Microsoft's per-IP limits,
    and retries with exponential back-off + jitter on throttle-like errors.
    The reason for the last failure is kept in ``local_last_error``.
    """
    global local_last_error
    if not LOCAL_TTS_ENABLED:
        local_last_error = "built-in engine disabled (edge-tts not installed or LOCAL_TTS_ENABLED=false)"
        return None, 0
    last_exc: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        if cancel.is_set():
            raise asyncio.CancelledError
        await local_limiter.acquire(cancel)
        pause = 0.0
        try:
            audio, dur = await asyncio.wait_for(_local_synth_once(text, settings), timeout=LOCAL_TTS_TIMEOUT)
            local_limiter.report_success()
            local_limiter.report_ok_window()
            if dur <= 0:
                dur = max(1, round(estimate_seconds(len(text), settings["rate"]) * 1000))
            return audio, dur
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if _is_throttle_error(exc):
                pause = local_limiter.report_throttle()
            log.warning("Edge-TTS attempt %d/%d failed (%s): %s", attempt, attempts, type(exc).__name__,
                        str(exc)[:160])
        finally:
            await local_limiter.release()
        if attempt < attempts:
            backoff = min(45.0, (1.5 ** attempt) + pause + random.uniform(0.2, 1.2))
            await cancellable_sleep(backoff, cancel)
    local_last_error = _describe_exc(last_exc)
    log.error("Edge-TTS chunk failed after %d attempts: %s", attempts, local_last_error)
    return None, 0


async def local_voice_exists(voice: str) -> Optional[bool]:
    """Validate a voice id against Microsoft's catalogue (None if unavailable)."""
    if not LOCAL_TTS_ENABLED:
        return None
    try:
        voices = await asyncio.wait_for(edge_tts.list_voices(), timeout=30)
        return any(v.get("ShortName") == voice for v in voices)
    except Exception:  # noqa: BLE001
        return None


async def local_engine_health() -> Dict[str, Any]:
    """Tiny synthesis probe used by /servers and the admin panel."""
    info: Dict[str, Any] = {"url": LOCAL_TTS_URL, "ok": False, "latency": None,
                            "version": getattr(edge_tts, "__version__", None) if edge_tts else None,
                            "active": local_limiter.active, "max_text_length": LOCAL_TTS_CHUNK_SIZE,
                            "error": ""}
    if not LOCAL_TTS_ENABLED:
        info["error"] = "disabled"
        return info
    t0 = time.time()
    try:
        await local_limiter.acquire()
        try:
            audio, _ = await asyncio.wait_for(
                _local_synth_once(PROBE_TEXT, {"voice": DEFAULT_VOICE, "rate": "+0%", "pitch": "+0Hz", "volume": "+0%"}),
                timeout=45)
        finally:
            await local_limiter.release()
        info["ok"] = bool(audio)
        info["latency"] = round(time.time() - t0, 2)
        local_limiter.report_success()
    except Exception as exc:  # noqa: BLE001
        info["latency"] = round(time.time() - t0, 2)
        info["error"] = _describe_exc(exc)
        if _is_throttle_error(exc):
            local_limiter.report_throttle()
    return info


def backend_urls(remote: List[str]) -> List[str]:
    """Remote render servers first (they spread the load over many IPs), local engine last."""
    urls = [u for u in remote if u]
    if LOCAL_TTS_ENABLED:
        urls.append(LOCAL_TTS_URL)
    return urls


async def cancellable_sleep(seconds: float, cancel: asyncio.Event):
    try:
        await asyncio.wait_for(cancel.wait(), timeout=max(0.001, seconds))
    except asyncio.TimeoutError:
        return
    raise asyncio.CancelledError


async def fetch_chunk(session: aiohttp.ClientSession, pool: ServerPool, chunk: str, index: int,
                      settings: Dict[str, Any], sem: asyncio.Semaphore,
                      cancel: asyncio.Event) -> Tuple[int, Optional[bytes], int]:
    """Synthesise one chunk on the best available engine, with failover.

    Returns ``(index, None, 0)`` only after one *round* of attempts on every
    engine failed; the caller (``ChunkPipeline``) then pauses the job with an
    escalating back-off and re-queues the chunk - text is never dropped and the
    job never gives up by itself.
    """
    payload = {"text": chunk, "voice": settings["voice"], "rate": settings["rate"],
               "pitch": settings["pitch"], "volume": settings["volume"]}
    rejected = set()
    async with sem:
        for attempt in range(1, MAX_CHUNK_ATTEMPTS + 1):
            if cancel.is_set():
                raise asyncio.CancelledError
            candidates = [s for s in pool.states if s.url not in rejected]
            if not candidates:
                break
            server = pool.pick()
            if server.url in rejected:
                server = min(candidates, key=lambda s: (s.cooldown_until, s.score))
            delay = server.cooldown_until - time.time()
            if delay > 0:
                # Never sleep long inside a slot: bounded wait, then let the pipeline decide.
                await cancellable_sleep(min(delay, 10.0), cancel)
                if server.cooldown_until > time.time() and len(candidates) > 1:
                    server = min(candidates, key=lambda s: (s.cooldown_until, s.score))
            if server.is_local:
                # Built-in free Edge-TTS engine: no HTTP hop, adaptive throttle-safe limiter.
                if len(chunk) > LOCAL_TTS_CHUNK_SIZE:
                    pool.report(server, False, error=f"chunk longer than LOCAL_TTS_CHUNK_SIZE ({LOCAL_TTS_CHUNK_SIZE})")
                    rejected.add(server.url)
                    continue
                t0 = time.monotonic()
                data, dur = await local_synthesize(chunk, settings, cancel)
                if data:
                    pool.report(server, True, time.monotonic() - t0)
                    return index, data, dur
                pool.report(server, False, error=local_last_error or "Edge-TTS returned no audio")
                if len(pool.states) == 1:
                    # Nothing else to fail over to; the engine already retried internally.
                    break
                rejected.add(server.url)
                continue
            limiter = remote_limiter(server.url)
            t0 = time.monotonic()
            retry_after = min(10, 1.5 * attempt)
            throttled = False
            try:
                await limiter.acquire(cancel)
                try:
                    async with _network_slots:
                        if cancel.is_set():
                            raise asyncio.CancelledError
                        async with session.post(server.tts_url, json=payload, headers=_headers(),
                                                allow_redirects=False,
                                                timeout=aiohttp.ClientTimeout(total=CHUNK_TIMEOUT)) as resp:
                            if resp.status == 200:
                                content_type = resp.headers.get("Content-Type", "").split(";")[0].lower()
                                data = bytearray()
                                async for piece in resp.content.iter_chunked(65536):
                                    data.extend(piece)
                                    if len(data) > 32 * 1024 * 1024:
                                        raise ValueError("TTS chunk response is unexpectedly large")
                                # Edge MP3s start with ID3 or MPEG frame sync; reject HTML/JSON 200 responses.
                                mp3 = data.startswith(b"ID3") or (len(data) >= 2 and data[0] == 255 and data[1] & 224 == 224)
                                if len(data) > 100 and mp3 and content_type in {"", "audio/mpeg", "audio/mp3",
                                                                                "application/octet-stream"}:
                                    try:
                                        dur = int(resp.headers.get("X-Duration-Ms", "0"))
                                    except (ValueError, TypeError):
                                        dur = 0
                                    if dur <= 0:
                                        dur = max(1, round(estimate_seconds(len(chunk), settings["rate"]) * 1000))
                                    pool.report(server, True, time.monotonic() - t0)
                                    return index, bytes(data), dur
                                pool.report(server, False,
                                            error=f"HTTP 200 but body is not MP3 ({content_type or 'no content-type'}, {len(data)} bytes)")
                            elif resp.status in (400, 401, 403, 413, 404, 405):
                                reason = await _describe_http_error(resp)
                                rejected.add(server.url)
                                if resp.status in (401, 404):
                                    # Configuration problem - this server will reject every chunk, stop hammering it.
                                    server.cooldown_until = time.time() + 300
                                pool.report(server, False, error=reason)
                                log.warning("Chunk %d rejected by %s (%s); trying another server without truncation",
                                            index, server.url, reason)
                            elif resp.status in (429, 502, 503, 504):
                                # 429 = our own rate limit, 502/503/504 = the server's Edge-TTS
                                # upstream is throttled / busy.  Both mean "back off this IP".
                                throttled = True
                                try:
                                    retry_after = min(120, max(1, float(resp.headers.get("Retry-After", "5"))))
                                except ValueError:
                                    retry_after = 5
                                reason = await _describe_http_error(resp)
                                server.cooldown_until = max(server.cooldown_until, time.time() + retry_after)
                                pool.report(server, False, error=reason)
                            else:
                                pool.report(server, False, error=await _describe_http_error(resp))
                finally:
                    await limiter.release()
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                throttled = True
                pool.report(server, False, error=f"timed out after {CHUNK_TIMEOUT}s")
                log.debug("Chunk %d attempt %d timed out on %s", index, attempt, server.url)
            except Exception as exc:
                throttled = _is_throttle_error(exc)
                pool.report(server, False, error=_describe_exc(exc))
                log.debug("Chunk %d attempt %d failed on %s: %s", index, attempt, server.url, exc)
            if throttled:
                limiter.report_throttle()
            if attempt < MAX_CHUNK_ATTEMPTS and len(rejected) < len(pool.states):
                await cancellable_sleep(min(retry_after, 10.0), cancel)
    log.warning("Chunk %d failed on every engine this round: %s", index, pool.failure_summary())
    return index, None, 0


async def remux_mp3(path: str) -> str:
    """Optionally re-mux concatenated MP3 with ffmpeg so players show correct duration."""
    if not FFMPEG:
        return path
    out = path[:-4] + "_fixed.mp3"
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            FFMPEG, "-y", "-loglevel", "error", "-i", path, "-c:a", "copy", "-map_metadata", "-1", out,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.wait_for(proc.wait(), timeout=300)
        if proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
            os.remove(path)
            return out
    except asyncio.CancelledError:
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
        if os.path.exists(out):
            os.remove(out)
        raise
    except Exception as e:
        log.debug("ffmpeg remux skipped: %s", e)
    finally:
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
    if os.path.exists(out):
        os.remove(out)
    return path


# =============================================================================
# Bot state
# =============================================================================
db = Database(DB_PATH)
bot = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

admin_states: Dict[int, str] = {}          # admin_id -> pending input state
pending_text: Dict[int, Tuple[str, str, float]] = {}  # token, text, expiry
preview_tasks: Dict[int, asyncio.Task] = {}
job_slots = asyncio.Semaphore(MAX_PARALLEL_JOBS)


@dataclass
class Job:
    user_id: int
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    started: float = field(default_factory=time.time)
    task: Optional[asyncio.Task] = None

    def stop(self):
        self.cancel.set()
        if self.task is not None and not self.task.done():
            self.task.cancel()


active_jobs: Dict[int, Job] = {}           # user_id -> running job
BOT_START = time.time()


def reserve_job(uid: int) -> Optional[Job]:
    if uid in active_jobs or len(active_jobs) >= MAX_QUEUED_JOBS:
        return None
    job = Job(user_id=uid)
    active_jobs[uid] = job
    return job


def track_job(job: Job):
    def finished(task):
        if active_jobs.get(job.user_id) is job:
            active_jobs.pop(job.user_id, None)
        if not task.cancelled():
            error = task.exception()
            if error:
                log.error("Background job failed", exc_info=(type(error), error, error.__traceback__))
    job.task.add_done_callback(finished)


@bot.on_callback_query(group=-10)
async def guard_callbacks(client: Client, cq: CallbackQuery):
    # Old inline buttons must not bypass revocation or run in forwarded/group messages.
    if not cq.message or cq.message.chat.id != cq.from_user.id:
        await cq.answer("Open this bot in a private chat.", show_alert=True)
        cq.stop_propagation()
    data = cq.data or ""
    protected = ("set_", "back_settings", "vgrp_", "voice_", "rate_", "pitch_", "vol_",
                 "hrs_", "split_", "reset_settings", "preview_voice", "text_")
    if data.startswith(protected) and not db.is_approved(cq.from_user.id):
        await cq.answer("Access expired or revoked. Please request access again.", show_alert=True)
        cq.stop_propagation()


# =============================================================================
# Keyboards
# =============================================================================
def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    if not db.is_approved(user_id):
        return ReplyKeyboardMarkup([[KeyboardButton(BTN_REQUEST)], [KeyboardButton(BTN_HELP)]], resize_keyboard=True)
    rows = [
        [KeyboardButton(BTN_CREATE), KeyboardButton(BTN_SETTINGS)],
        [KeyboardButton(BTN_ACCOUNT), KeyboardButton(BTN_HISTORY)],
        [KeyboardButton(BTN_HELP)],
    ]
    if db.is_admin(user_id):
        rows.append([KeyboardButton(BTN_ADMIN)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([
        [KeyboardButton(BTN_ADD_SERVER), KeyboardButton(BTN_DEL_SERVER)],
        [KeyboardButton(BTN_SERVER_STATUS), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_APPROVE), KeyboardButton(BTN_REVOKE)],
        [KeyboardButton(BTN_USERS), KeyboardButton(BTN_BROADCAST)],
        [KeyboardButton(BTN_MAIN)],
    ], resize_keyboard=True)


def settings_markup(user_id: int) -> InlineKeyboardMarkup:
    s = db.get_settings(user_id)
    split = "Chapters" if s["split_mode"] == "chapter" else f"{s['max_hours']}h parts"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎙 Voice: {voice_label(s['voice'])}", callback_data="set_voice")],
        [InlineKeyboardButton(f"⏱ Speed: {s['rate']}", callback_data="set_rate"),
         InlineKeyboardButton(f"🎵 Pitch: {s['pitch']}", callback_data="set_pitch")],
        [InlineKeyboardButton(f"🔊 Volume: {s['volume']}", callback_data="set_volume"),
         InlineKeyboardButton(f"✂️ Split: {split}", callback_data="set_split")],
        [InlineKeyboardButton("🔉 Preview voice", callback_data="preview_voice"),
         InlineKeyboardButton("♻️ Reset", callback_data="reset_settings")],
        [InlineKeyboardButton("✖ Close", callback_data="close")],
    ])


def settings_text(user_id: int) -> str:
    s = db.get_settings(user_id)
    return (
        "⚙️ **Audio Settings**\n\n"
        f"🎙 Voice: `{s['voice']}`\n"
        f"⏱ Speed: `{s['rate']}`   🎵 Pitch: `{s['pitch']}`   🔊 Volume: `{s['volume']}`\n"
        f"✂️ Split mode: `{s['split_mode']}`"
        + (f" (max {s['max_hours']}h per part)" if s["split_mode"] == "duration" else "")
        + "\n\nTap a button to change it."
    )


def voice_groups_markup() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"🌐 {g}", callback_data=f"vgrp_{i}")] for i, g in enumerate(VOICE_GROUPS)]
    rows.append([InlineKeyboardButton("✍️ Custom voice ID", callback_data="voice_custom")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="back_settings")])
    return InlineKeyboardMarkup(rows)


def voice_list_markup(group_index: int, current: str) -> InlineKeyboardMarkup:
    group = list(VOICE_GROUPS.keys())[group_index]
    rows, row = [], []
    for label, vid in VOICE_GROUPS[group]:
        mark = "✅ " if vid == current else ""
        row.append(InlineKeyboardButton(f"{mark}{label}", callback_data=f"voice_{vid}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="set_voice")])
    return InlineKeyboardMarkup(rows)


def options_markup(prefix: str, options: List[Tuple[str, str]], current: str, cols: int = 2) -> InlineKeyboardMarkup:
    rows, row = [], []
    for label, val in options:
        mark = "✅ " if val == current else ""
        row.append(InlineKeyboardButton(f"{mark}{label}", callback_data=f"{prefix}_{val}"))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="back_settings")])
    return InlineKeyboardMarkup(rows)


def approval_markup(target: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1 day", callback_data=f"apprv_{target}_1"),
         InlineKeyboardButton("7 days", callback_data=f"apprv_{target}_7"),
         InlineKeyboardButton("30 days", callback_data=f"apprv_{target}_30")],
        [InlineKeyboardButton("90 days", callback_data=f"apprv_{target}_90"),
         InlineKeyboardButton("365 days", callback_data=f"apprv_{target}_365"),
         InlineKeyboardButton("Custom", callback_data=f"apprv_{target}_c")],
        [InlineKeyboardButton("❌ Reject", callback_data=f"apprv_{target}_no"),
         InlineKeyboardButton("⛔ Ban", callback_data=f"apprv_{target}_ban")],
    ])


HELP_TEXT = (
    "❓ **How to use AudioBook Pro**\n\n"
    f"1. Send a document: `.txt` `.md` `.docx` `.html` `.epub` `.pdf` (max {MAX_FILE_MB} MB, any length)\n"
    "   — or simply paste text as a message.\n"
    "2. The bot extracts the text, splits it smartly and synthesises it with Microsoft Edge neural voices.\n"
    "3. You receive MP3 parts as they are finished.\n"
    "4. Big books are safe: progress is saved continuously. If the engines are throttled the job "
    "pauses and retries by itself; if the bot restarts it resumes where it stopped.\n\n"
    "**Commands**\n"
    "/start – main menu\n"
    "/settings – voice, speed, pitch, volume, split mode\n"
    "/preview – hear a sample of your current voice\n"
    "/account – subscription & usage\n"
    "/history – your last jobs\n"
    "/resume – continue an interrupted audiobook\n"
    "/cancel – cancel the running job\n"
    "/status – bot status\n"
    "/help – this message\n\n"
    + (f"Support: @{CONTACT_USERNAME}" if CONTACT_USERNAME else "")
)


def _is_admin_msg(message: Message) -> bool:
    return bool(message.from_user and db.is_admin(message.from_user.id))


# =============================================================================
# Basic commands
# =============================================================================
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    uid = message.from_user.id
    db.touch_user(message.from_user)
    admin_states.pop(uid, None)
    pending_text.pop(uid, None)
    if db.is_approved(uid):
        await message.reply(
            f"👋 Welcome to **AudioBook Pro** v{VERSION}!\n\n"
            "Send me a novel, story or document (`.txt` `.docx` `.epub` `.pdf` `.html` `.md`) "
            "and I will turn it into a high-quality MP3 audiobook.\n\n"
            "Use ⚙️ Settings to pick a voice and adjust speed, pitch and volume.",
            reply_markup=main_keyboard(uid))
    else:
        await message.reply(
            "🔒 You do not have access to this bot yet.\n\n"
            "Tap **🔑 Request Access** and the administrator will review your request.",
            reply_markup=main_keyboard(uid))


@bot.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, message: Message):
    await message.reply(HELP_TEXT, reply_markup=main_keyboard(message.from_user.id))


@bot.on_message(filters.regex(f"^{re.escape(BTN_HELP)}$") & filters.private)
async def btn_help(client: Client, message: Message):
    await cmd_help(client, message)


@bot.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, message: Message):
    uid = message.from_user.id
    if not db.is_approved(uid):
        return
    servers = db.servers()
    engine = (f"built-in Edge-TTS (free, {local_limiter.current}x parallel)" if LOCAL_TTS_ENABLED
              else "render servers only")
    text = (f"🤖 **Bot Status**\n\n"
            f"Version: `{VERSION}`\n"
            f"Uptime: `{fmt_duration(time.time() - BOT_START)}`\n"
            f"TTS engine: `{engine}`\n"
            f"Render servers: `{len(servers)}`\n"
            f"Running jobs: `{len(active_jobs)}/{MAX_PARALLEL_JOBS}`\n"
            f"ffmpeg: `{'available' if FFMPEG else 'not installed'}`")
    if uid in active_jobs:
        text += f"\n\n⏳ Your job has been running for `{fmt_duration(time.time() - active_jobs[uid].started)}`"
    await message.reply(text)


# =============================================================================
# Access requests & approvals
# =============================================================================
@bot.on_message(filters.regex(f"^{re.escape(BTN_REQUEST)}$") & filters.private)
async def request_access(client: Client, message: Message):
    uid = message.from_user.id
    db.touch_user(message.from_user)
    row = db.get_user(uid)
    if row and row["is_banned"]:
        await message.reply("⛔ Your account has been banned. Contact the administrator if you think this is a mistake.")
        return
    if db.is_approved(uid):
        await message.reply("✅ You already have access.", reply_markup=main_keyboard(uid))
        return
    await message.reply("📨 Your request has been sent to the administrator. You will be notified once it is reviewed.")
    name = html.escape(message.from_user.first_name or "User")
    uname = f"@{message.from_user.username}" if message.from_user.username else "—"
    for admin_id in db.admin_ids():
        await safe_send(client, admin_id,
                        f"🔔 **New access request**\n\nName: {name}\nUsername: {uname}\nID: `{uid}`\n\n"
                        "How many days of access should be granted?",
                        reply_markup=approval_markup(uid))


async def grant_access(client: Client, admin_id: int, target: int, days: int) -> str:
    expiry = db.approve(target, days)
    await safe_send(client, target,
                    f"🎉 Access granted for **{days} day(s)**!\nValid until `{expiry}`.\n\n"
                    "Send a document or text to start creating audiobooks.",
                    reply_markup=main_keyboard(target))
    log.info("Admin %s approved %s for %d days", admin_id, target, days)
    return expiry


@bot.on_callback_query(filters.regex(r"^apprv_(\d+)_(\w+)$"))
async def cb_approval(client: Client, cq: CallbackQuery):
    if not db.is_admin(cq.from_user.id):
        await cq.answer("Admins only.", show_alert=True)
        return
    target, action = int(cq.matches[0].group(1)), cq.matches[0].group(2)
    if target == OWNER_ID or db.is_admin(target):
        await cq.answer("Manage administrators through the owner account.", show_alert=True)
        return
    if action == "no":
        await safe_edit(cq.message, f"❌ Request from `{target}` rejected.")
        await safe_send(client, target, "❌ Your access request was rejected by the administrator.")
    elif action == "ban":
        db.touch_user(type("U", (), {"id": target, "first_name": "", "username": ""})())
        db.set_banned(target, True)
        if target in active_jobs:
            active_jobs[target].stop()
        await safe_edit(cq.message, f"⛔ User `{target}` has been banned.")
        await safe_send(client, target, "⛔ You have been banned from using this bot.")
    elif action == "c":
        admin_states[cq.from_user.id] = f"custom_apprv_{target}"
        await safe_edit(cq.message, f"✍️ Send the number of days to grant to user `{target}` (e.g. `45`).")
    else:
        if not action.isdigit() or not 1 <= int(action) <= 36500:
            await cq.answer("Invalid approval period", show_alert=True)
            return
        days = int(action)
        expiry = await grant_access(client, cq.from_user.id, target, days)
        await safe_edit(cq.message, f"✅ User `{target}` approved for {days} day(s) — until `{expiry}`.")
    await cq.answer()


# =============================================================================
# Account / history
# =============================================================================
@bot.on_message((filters.command("account") | filters.regex(f"^{re.escape(BTN_ACCOUNT)}$")) & filters.private)
async def my_account(client: Client, message: Message):
    uid = message.from_user.id
    db.touch_user(message.from_user)
    row = db.get_user(uid)
    if not row:
        await message.reply("Your account is not registered yet. Use /start.")
        return
    role = "Administrator" if row["is_admin"] == 1 else "User"
    expiry = parse_dt(row["expiry_date"])
    if row["is_admin"] == 1:
        status = "♾ Unlimited"
    elif expiry > datetime.now():
        left = expiry - datetime.now()
        status = f"✅ Active — {left.days}d {left.seconds // 3600}h left (until `{row['expiry_date']}`)"
    else:
        status = "❌ Expired"
    if row["is_banned"]:
        status = "⛔ Banned"
    await message.reply(
        "👤 **Your Account**\n\n"
        f"🆔 ID: `{uid}`\n"
        f"🎭 Role: {role}\n"
        f"📅 Subscription: {status}\n\n"
        f"🎧 Audiobooks created: `{row['usage_count'] or 0}`\n"
        f"🔤 Characters converted: `{row['chars_total'] or 0:,}`\n"
        f"⏱ Total audio: `{fmt_duration(row['audio_seconds'] or 0)}`\n"
        f"📆 Member since: `{row['joined_at'] or '—'}`")


@bot.on_message((filters.command("history") | filters.regex(f"^{re.escape(BTN_HISTORY)}$")) & filters.private)
async def my_history(client: Client, message: Message):
    uid = message.from_user.id
    if not db.is_approved(uid):
        return
    rows = db.job_history(uid, 10)
    if not rows:
        await message.reply("📜 No jobs yet. Send a document to create your first audiobook!")
        return
    icons = {"done": "✅", "partial": "⚠️", "failed": "❌", "cancelled": "🚫", "running": "⏳"}
    lines = ["📜 **Your last jobs**\n"]
    for r in rows:
        lines.append(f"{icons.get(r['status'], '•')} `{r['created_at']}` — {html.escape(r['source'] or 'text')}\n"
                     f"     {r['chars']:,} chars · {r['parts']} part(s) · {fmt_duration(r['audio_seconds'] or 0)}")
    await message.reply("\n".join(lines))


# =============================================================================
# Settings
# =============================================================================
@bot.on_message((filters.command("settings") | filters.regex(f"^{re.escape(BTN_SETTINGS)}$")) & filters.private)
async def open_settings(client: Client, message: Message):
    uid = message.from_user.id
    if not db.is_approved(uid):
        await message.reply("🔒 You need access to change settings.")
        return
    await message.reply(settings_text(uid), reply_markup=settings_markup(uid))


@bot.on_callback_query(filters.regex("^back_settings$"))
async def cb_back_settings(client: Client, cq: CallbackQuery):
    # BUG FIX: the old code passed cq.message to open_settings -> from_user was the BOT, not the user
    await safe_edit(cq.message, settings_text(cq.from_user.id), settings_markup(cq.from_user.id))
    await cq.answer()


@bot.on_callback_query(filters.regex("^close$"))
async def cb_close(client: Client, cq: CallbackQuery):
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.answer()


@bot.on_callback_query(filters.regex("^set_voice$"))
async def cb_set_voice(client: Client, cq: CallbackQuery):
    await safe_edit(cq.message, "🎙 **Choose a voice group**", voice_groups_markup())
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^vgrp_(\d+)$"))
async def cb_voice_group(client: Client, cq: CallbackQuery):
    idx = int(cq.matches[0].group(1))
    if idx >= len(VOICE_GROUPS):
        await cq.answer("Unknown group", show_alert=True)
        return
    current = db.get_settings(cq.from_user.id)["voice"]
    await safe_edit(cq.message, f"🎙 **{list(VOICE_GROUPS)[idx]}** — pick a voice:", voice_list_markup(idx, current))
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^voice_(.+)$"))
async def cb_voice(client: Client, cq: CallbackQuery):
    value = cq.matches[0].group(1)
    uid = cq.from_user.id
    if value == "custom":
        admin_states[uid] = "custom_voice"
        await safe_edit(cq.message,
                        "✍️ Send a voice ID, e.g. `en-AU-NatashaNeural` or `ta-IN-PallaviNeural`.\n"
                        "Full list: https://speech.microsoft.com/portal/voicegallery\n\nSend /cancel to abort.")
        await cq.answer()
        return
    if not VOICE_RE.match(value):
        await cq.answer("Invalid voice.", show_alert=True)
        return
    db.set_setting(uid, "voice", value)
    await cq.answer(f"Voice set to {voice_label(value)}")
    await safe_edit(cq.message, settings_text(uid), settings_markup(uid))


@bot.on_callback_query(filters.regex("^set_rate$"))
async def cb_set_rate(client: Client, cq: CallbackQuery):
    cur = db.get_settings(cq.from_user.id)["rate"]
    await safe_edit(cq.message, "⏱ **Speaking speed**", options_markup("rate", RATE_OPTIONS, cur))
    await cq.answer()


@bot.on_callback_query(filters.regex("^set_pitch$"))
async def cb_set_pitch(client: Client, cq: CallbackQuery):
    cur = db.get_settings(cq.from_user.id)["pitch"]
    await safe_edit(cq.message, "🎵 **Voice pitch**", options_markup("pitch", PITCH_OPTIONS, cur))
    await cq.answer()


@bot.on_callback_query(filters.regex("^set_volume$"))
async def cb_set_volume(client: Client, cq: CallbackQuery):
    cur = db.get_settings(cq.from_user.id)["volume"]
    await safe_edit(cq.message, "🔊 **Volume**", options_markup("vol", VOLUME_OPTIONS, cur))
    await cq.answer()


@bot.on_callback_query(filters.regex("^set_split$"))
async def cb_set_split(client: Client, cq: CallbackQuery):
    s = db.get_settings(cq.from_user.id)
    rows = []
    row = []
    for h in HOURS_OPTIONS:
        mark = "✅ " if s["split_mode"] == "duration" and s["max_hours"] == h else ""
        row.append(InlineKeyboardButton(f"{mark}{h}h", callback_data=f"hrs_{h}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    mark = "✅ " if s["split_mode"] == "chapter" else ""
    rows.append([InlineKeyboardButton(f"{mark}📖 Split by chapters", callback_data="split_chapter")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="back_settings")])
    await safe_edit(cq.message,
                    "✂️ **Split mode**\n\nChoose the maximum audio length per part, or split by detected "
                    "chapter headings (Chapter 1 / अध्याय 1 / Part 1 ...).", InlineKeyboardMarkup(rows))
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^(rate|pitch|vol|hrs|split)_(.+)$"))
async def cb_apply_option(client: Client, cq: CallbackQuery):
    kind, value = cq.matches[0].group(1), cq.matches[0].group(2)
    uid = cq.from_user.id
    valid = {
        "rate": {v for _, v in RATE_OPTIONS},
        "pitch": {v for _, v in PITCH_OPTIONS},
        "vol": {v for _, v in VOLUME_OPTIONS},
        "hrs": {str(h) for h in HOURS_OPTIONS},
        "split": {"chapter", "duration"},
    }[kind]
    if value not in valid:
        await cq.answer("Invalid option.", show_alert=True)
        return
    if kind == "rate":
        db.set_setting(uid, "rate", value)
    elif kind == "pitch":
        db.set_setting(uid, "pitch", value)
    elif kind == "vol":
        db.set_setting(uid, "volume", value)
    elif kind == "hrs":
        db.set_setting(uid, "max_hours", int(value))
        db.set_setting(uid, "split_mode", "duration")
    elif kind == "split":
        db.set_setting(uid, "split_mode", value)
    await cq.answer("Saved ✅")
    await safe_edit(cq.message, settings_text(uid), settings_markup(uid))


@bot.on_callback_query(filters.regex("^reset_settings$"))
async def cb_reset_settings(client: Client, cq: CallbackQuery):
    db.reset_settings(cq.from_user.id)
    await cq.answer("Settings reset to defaults")
    await safe_edit(cq.message, settings_text(cq.from_user.id), settings_markup(cq.from_user.id))


async def send_voice_preview(client: Client, chat_id: int, uid: int, reply_to: Optional[Message] = None):
    settings = db.get_settings(uid)
    servers = backend_urls(db.servers())
    if not servers:
        await safe_send(client, chat_id, "❌ No TTS engine available. Ask the administrator.")
        return
    sample = ("Hello! This is a preview of your selected voice. "
              "नमस्ते! यह आपकी चुनी हुई आवाज़ का एक नमूना है।")
    if settings["voice"].startswith("en-"):
        sample = "Hello! This is a preview of your selected voice. Your audiobook will sound like this."
    elif settings["voice"].startswith("hi-"):
        sample = "नमस्ते! यह आपकी चुनी हुई आवाज़ का एक नमूना है। आपकी ऑडियोबुक इसी आवाज़ में बनेगी।"
    pool = ServerPool(servers)
    async with aiohttp.ClientSession() as session:
        _, data, dur = await fetch_chunk(session, pool, sample, 0, settings, asyncio.Semaphore(1), asyncio.Event())
    if not data:
        await safe_send(client, chat_id, "❌ Preview failed — " + html.escape(pool.failure_summary())[:500]
                        + "\n\nTry again in a minute or ask the administrator to check /servers.")
        return
    fd, path = tempfile.mkstemp(prefix=f"preview_{uid}_", suffix=".mp3")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    try:
        await client.send_audio(chat_id, path, caption=f"Preview: `{settings['voice']}`",
                                title="Voice preview", duration=max(1, dur // 1000))
    finally:
        if os.path.exists(path):
            os.remove(path)


async def start_preview(client, chat_id, uid):
    if uid in preview_tasks:
        await safe_send(client, chat_id, "A preview is already being generated.")
        return
    if len(preview_tasks) >= MAX_TOTAL_CONCURRENCY:
        await safe_send(client, chat_id, "Preview service is busy. Please try again later.")
        return
    async def work():
        try:
            await send_voice_preview(client, chat_id, uid)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Voice preview failed")
            await safe_send(client, chat_id, "Voice preview failed. Please try again.")
        finally:
            preview_tasks.pop(uid, None)
    task = asyncio.create_task(work())
    preview_tasks[uid] = task
    task.add_done_callback(lambda completed: preview_tasks.pop(uid, None)
                           if preview_tasks.get(uid) is completed else None)


@bot.on_callback_query(filters.regex("^preview_voice$"))
async def cb_preview(client: Client, cq: CallbackQuery):
    await cq.answer("Generating preview…")
    await start_preview(client, cq.message.chat.id, cq.from_user.id)


@bot.on_message(filters.command("preview") & filters.private)
async def cmd_preview(client: Client, message: Message):
    if not db.is_approved(message.from_user.id):
        return
    await start_preview(client, message.chat.id, message.from_user.id)


@bot.on_message(filters.regex(f"^{re.escape(BTN_CREATE)}$") & filters.private)
async def btn_create(client: Client, message: Message):
    if db.is_approved(message.from_user.id):
        s = db.get_settings(message.from_user.id)
        await message.reply(
            "🎧 **Create an audiobook**\n\n"
            "Send a document (`.txt` `.md` `.docx` `.html` `.epub` `.pdf`, up to 50 MB) "
            "or paste the text directly as a message.\n\n"
            f"Current voice: `{s['voice']}` · speed `{s['rate']}`\n"
            "Change it anytime in ⚙️ Settings.")


# =============================================================================
# Cancel
# =============================================================================
@bot.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(client: Client, message: Message):
    uid = message.from_user.id
    cancelled = False
    preview = preview_tasks.get(uid)
    if preview is not None:
        preview.cancel()
        cancelled = True
    if admin_states.pop(uid, None):
        cancelled = True
    if pending_text.pop(uid, None):
        cancelled = True
    job = active_jobs.get(uid)
    if job:
        job.stop()
        await message.reply("🛑 Cancelling your job… parts already finished have been delivered.")
        return
    # Discard an interrupted (resumable) job so the user can start over.
    row = db.user_resumable_job(uid)
    if row is not None:
        db.job_finish(int(row["id"]), "cancelled", int(row["parts"] or 0), int(row["audio_seconds"] or 0))
        shutil.rmtree(job_dir(int(row["id"])), ignore_errors=True)
        cancelled = True
    await message.reply("✅ Cancelled." if cancelled else "Nothing to cancel.", reply_markup=main_keyboard(uid))


@bot.on_callback_query(filters.regex(r"^cancel_job_(\d+)$"))
async def cb_cancel_job(client: Client, cq: CallbackQuery):
    uid = int(cq.matches[0].group(1))
    if cq.from_user.id != uid and not db.is_admin(cq.from_user.id):
        await cq.answer("This is not your job.", show_alert=True)
        return
    job = active_jobs.get(uid)
    if job:
        job.stop()
        await cq.answer("Cancelling...")
    else:
        await cq.answer("Job already finished.")


# =============================================================================
# Admin panel
# =============================================================================
@bot.on_message((filters.command("admin") | filters.regex(f"^{re.escape(BTN_ADMIN)}$")) & filters.private)
async def admin_panel(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    admin_states.pop(message.from_user.id, None)
    await message.reply("👑 **Admin Panel**\n\nChoose an action:", reply_markup=admin_keyboard())


@bot.on_message(filters.regex(f"^{re.escape(BTN_MAIN)}$") & filters.private)
async def back_main(client: Client, message: Message):
    uid = message.from_user.id
    admin_states.pop(uid, None)
    await message.reply("🏠 Main menu", reply_markup=main_keyboard(uid))


@bot.on_message(filters.regex(f"^{re.escape(BTN_ADD_SERVER)}$") & filters.private)
async def btn_add_server(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    admin_states[message.from_user.id] = "add_server"
    await message.reply("🔗 **Add render server**\n\nSend the base URL, e.g. `https://my-tts.onrender.com`\n"
                        "(one per line to add several). The bot will verify it before saving.\n\n/cancel to abort.")


@bot.on_message(filters.regex(f"^{re.escape(BTN_DEL_SERVER)}$") & filters.private)
async def btn_remove_server(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    servers = db.servers()
    if not servers:
        await message.reply("No servers configured.", reply_markup=admin_keyboard())
        return
    rows = [[InlineKeyboardButton(r["url"].replace("https://", "")[:40], callback_data=f"delsrv_{r['id']}")]
            for r in db.fetchall("SELECT id, url FROM render_servers ORDER BY id")]
    rows.append([InlineKeyboardButton("✖ Close", callback_data="close")])
    await message.reply("🗑 **Remove server** — tap to delete:", reply_markup=InlineKeyboardMarkup(rows))


@bot.on_callback_query(filters.regex(r"^delsrv_(\d+)$"))
async def cb_del_server(client: Client, cq: CallbackQuery):
    if not db.is_admin(cq.from_user.id):
        await cq.answer("Admins only.", show_alert=True)
        return
    servers = db.servers()
    server_id = int(cq.matches[0].group(1))
    record = db.fetchone("SELECT url FROM render_servers WHERE id = ?", (server_id,))
    if record is None:
        await cq.answer("Server no longer exists.", show_alert=True)
        return
    url = record["url"]
    db.remove_server(url)
    await cq.answer("Removed")
    await safe_edit(cq.message, f"🗑 Removed server:\n`{url}`\n\nRemaining: {len(servers) - 1}")


@bot.on_message((filters.command("servers") | filters.regex(f"^{re.escape(BTN_SERVER_STATUS)}$")) & filters.private)
async def btn_server_status(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    servers = backend_urls(db.servers())
    if not servers:
        await message.reply("No TTS engine available. Use ➕ Add Server or install `edge-tts` on the bot host.")
        return
    msg = await message.reply(f"🔄 Checking {len(servers)} engine(s) (including a real test synthesis)…")
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*(check_server(session, s, deep=True) for s in servers))
    lines = [f"🌐 **TTS Engine Status** ({len(servers)} total)\n"]
    online = 0
    for i, r in enumerate(results, 1):
        is_local = r["url"] == LOCAL_TTS_URL
        if not is_local:
            db.server_health(r["url"], r["ok"])
        label = "built-in Edge-TTS (free)" if is_local else r["url"]
        if r["ok"]:
            online += 1
            extra = f" · {r['latency']}s" if r["latency"] is not None else ""
            extra += f" · v{r['version']}" if r["version"] else ""
            extra += f" · {r['active']} active" if r["active"] is not None else ""
            if r.get("throttled"):
                extra += " · ⚠️ MS throttling"
            if is_local:
                snap = local_limiter.snapshot()
                extra += f" · limit {snap['limit']}/{snap['max']} · throttles {snap['throttle_events']}"
            if not is_local:
                lim = remote_limiter(r["url"]).snapshot()
                extra += f" · limit {lim['limit']}/{lim['max']}"
                if lim["throttle_events"]:
                    extra += f" · throttles {lim['throttle_events']}"
                extra += " · 🔐 auth OK" if r.get("auth_required") else " · 🔓 no key"
            lines.append(f"{i}. 🟢 `{label}`{extra}")
        else:
            reason = html.escape(r.get("error") or "offline")[:200]
            lines.append(f"{i}. 🔴 `{label}` — {reason}")
    lines.append(f"\n✅ Online: **{online}/{len(servers)}**")
    running = db.running_jobs()
    if running:
        lines.append(f"🎧 Jobs in progress: {len(running)} (see /jobs)")
    lines.append(f"\n_Probe = real {len(PROBE_TEXT)}-char Hindi synthesis on every engine. "
                 f"Jobs never fail on a throttled engine: they pause and retry automatically._")
    if not TTS_API_KEY and any(r.get("auth_required") for r in results):
        lines.append("⚠️ A server requires an API key but `TTS_API_KEY` is not set on the bot.")
    await safe_edit(msg, "\n".join(lines))


@bot.on_message(filters.regex(f"^{re.escape(BTN_APPROVE)}$") & filters.private)
async def btn_approve(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    admin_states[message.from_user.id] = "manual_approve"
    await message.reply("✅ **Approve user**\n\nSend `user_id days`, e.g. `123456789 30`\n"
                        "Add `admin` to promote: `123456789 3650 admin`\n\n/cancel to abort.")


@bot.on_message(filters.regex(f"^{re.escape(BTN_REVOKE)}$") & filters.private)
async def btn_revoke(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    admin_states[message.from_user.id] = "revoke_user"
    await message.reply("🚫 **Revoke / ban user**\n\nSend `user_id` to revoke access,\n"
                        "`user_id ban` to ban, or `user_id unban` to lift a ban.\n\n/cancel to abort.")


@bot.on_message((filters.command("users") | filters.regex(f"^{re.escape(BTN_USERS)}$")) & filters.private)
async def btn_users(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    rows = db.list_users()
    if not rows:
        await message.reply("No users yet.")
        return
    lines = [f"👥 **Users** ({len(rows)} total)\n"]
    for r in rows[:60]:
        if r["is_banned"]:
            icon = "⛔"
        elif r["is_admin"]:
            icon = "👑"
        elif parse_dt(r["expiry_date"]) > datetime.now():
            icon = "🟢"
        else:
            icon = "⚪"
        name = html.escape((r["first_name"] or "")[:20]) or "—"
        uname = f"@{r['username']}" if r["username"] else ""
        exp = "∞" if r["is_admin"] else (r["expiry_date"] or "—")[:10]
        lines.append(f"{icon} `{r['user_id']}` {name} {uname} · exp {exp} · {r['usage_count'] or 0} jobs")
    if len(rows) > 60:
        lines.append(f"\n…and {len(rows) - 60} more")
    await message.reply("\n".join(lines))


@bot.on_message(filters.regex(f"^{re.escape(BTN_BROADCAST)}$") & filters.private)
async def btn_broadcast(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    admin_states[message.from_user.id] = "broadcast"
    await message.reply("📢 **Broadcast**\n\nSend the message to deliver to all active users.\n\n/cancel to abort.")


@bot.on_message((filters.command("stats") | filters.regex(f"^{re.escape(BTN_STATS)}$")) & filters.private)
async def btn_stats(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    s = db.global_stats()
    await message.reply(
        "📊 **Bot Statistics**\n\n"
        f"👥 Users: `{s['users']}` (active: `{s['active_users']}`)\n"
        f"🌐 Servers: `{s['servers']}`\n"
        f"🎧 Jobs: `{s['jobs']}` (successful: `{s['jobs_ok']}`)\n"
        f"🔤 Characters synthesised: `{s['chars']:,}`\n"
        f"⏱ Audio produced: `{fmt_duration(s['audio_seconds'])}`\n"
        f"⏳ Uptime: `{fmt_duration(time.time() - BOT_START)}`\n"
        f"🏃 Running now: `{len(active_jobs)}`")


# =============================================================================
# Text message handler (admin states, custom voice, plain-text TTS)
# =============================================================================
async def _handle_admin_state(client: Client, message: Message, state: str, text: str) -> bool:
    uid = message.from_user.id

    if state == "add_server":
        urls = list(dict.fromkeys(filter(None, (normalize_server_url(u) for u in text.split()))))[:20]
        if not urls:
            await message.reply("❌ Invalid URL. It must start with http:// or https://")
            return True
        status = await message.reply(f"🔎 Verifying {len(urls)} server(s)…")
        lines = []
        async with aiohttp.ClientSession() as session:
            for url in urls:
                info = await check_server(session, url, deep=True)
                if not info["ok"]:
                    reason = html.escape(info.get("error") or "unreachable")[:200]
                    lines.append(f"🔴 `{url}` — not added: {reason}")
                    continue
                if db.add_server(url):
                    auth = " · 🔐 API key OK" if info.get("auth_required") else " · 🔓 no API key"
                    lines.append(f"🟢 `{url}` — added" + (f" (v{info['version']})" if info["version"] else "")
                                 + auth + " · test synthesis OK")
                else:
                    lines.append(f"🟡 `{url}` — already exists")
        admin_states.pop(uid, None)
        await safe_edit(status, "\n".join(lines))
        await message.reply(f"Total servers: {len(db.servers())}", reply_markup=admin_keyboard())
        return True

    if state == "manual_approve":
        parts = text.split()
        try:
            target, days = int(parts[0]), int(parts[1])
            if days <= 0 or days > 36500:
                raise ValueError
        except (IndexError, ValueError):
            await message.reply("❌ Wrong format. Send: `123456789 30`")
            return True
        expiry = await grant_access(client, uid, target, days)
        make_admin = len(parts) > 2 and parts[2].lower() == "admin" and uid == OWNER_ID
        if make_admin:
            db.set_admin(target, True)
        admin_states.pop(uid, None)
        await message.reply(f"✅ User `{target}` approved for {days} day(s) — until `{expiry}`"
                            + (" and promoted to admin 👑" if make_admin else ""), reply_markup=admin_keyboard())
        return True

    if state == "revoke_user":
        parts = text.split()
        try:
            target = int(parts[0])
        except (IndexError, ValueError):
            await message.reply("❌ Send a numeric user ID.")
            return True
        if target == OWNER_ID or (db.is_admin(target) and uid != OWNER_ID):
            await message.reply("❌ You cannot revoke this user.")
            return True
        action = parts[1].lower() if len(parts) > 1 else "revoke"
        if action not in {"ban", "unban", "revoke"}:
            await message.reply("Valid actions: ban, unban, revoke")
            return True
        if not db.get_user(target):
            await message.reply("User is not registered.")
            return True
        if db.is_admin(target) and uid == OWNER_ID and action != "unban":
            db.set_admin(target, False)
        if action in {"ban", "revoke"} and target in active_jobs:
            active_jobs[target].stop()
        if action == "ban":
            db.set_banned(target, True)
            db.revoke(target)
            await safe_send(client, target, "⛔ You have been banned from using this bot.")
            result = f"⛔ User `{target}` banned."
        elif action == "unban":
            db.set_banned(target, False)
            result = f"✅ User `{target}` unbanned (access still needs approval)."
        else:
            db.revoke(target)
            await safe_send(client, target, "🚫 Your access to this bot has been revoked.",
                            reply_markup=main_keyboard(target))
            result = f"🚫 Access revoked for `{target}`."
        admin_states.pop(uid, None)
        await message.reply(result, reply_markup=admin_keyboard())
        return True

    if state == "broadcast":
        admin_states.pop(uid, None)
        targets = [r["user_id"] for r in db.list_users(only_active=True) if r["user_id"] != uid]
        status = await message.reply(f"📢 Sending to {len(targets)} user(s)…")
        sent = 0
        for t in targets:
            if await safe_send(client, t, f"📢 **Announcement**\n\n{text}"):
                sent += 1
            await asyncio.sleep(0.05)
        await safe_edit(status, f"📢 Broadcast delivered to {sent}/{len(targets)} user(s).")
        return True

    if state.startswith("custom_apprv_"):
        try:
            target = int(state.split("_")[2])
            days = int(text)
            if not 1 <= days <= 36500:
                raise ValueError
        except ValueError:
            await message.reply("❌ Please send a positive number of days (e.g. `15`).")
            return True
        expiry = await grant_access(client, uid, target, days)
        admin_states.pop(uid, None)
        await message.reply(f"✅ User `{target}` approved for {days} day(s) — until `{expiry}`.")
        return True

    return False


@bot.on_message(filters.text & filters.private & ~filters.command(
    ["start", "help", "settings", "preview", "account", "history", "cancel", "status",
     "admin", "servers", "users", "stats"]))
async def text_handler(client: Client, message: Message):
    uid = message.from_user.id
    db.touch_user(message.from_user)
    text = (message.text or "").strip()
    state = admin_states.get(uid)
    if state == "custom_voice" and not db.is_approved(uid):
        admin_states.pop(uid, None)
        await message.reply("Your access has expired. Please request access again.")
        return
    if text.startswith("/"):
        await message.reply("Unknown command. Use /help.")
        return

    # -- user custom voice state ----------------------------------------------
    if state == "custom_voice":
        voice = text.strip()
        if not VOICE_RE.match(voice):
            await message.reply("❌ That does not look like a valid voice ID (example: `en-AU-NatashaNeural`).")
            return
        # Validate against Microsoft's catalogue (built-in engine) or a render server
        servers = db.servers()
        exists: Optional[bool] = await local_voice_exists(voice)
        if exists is None and servers:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{servers[0]}/voices", params={"q": voice}, headers=_headers(),
                                           timeout=aiohttp.ClientTimeout(total=30)) as r:
                        if r.status == 200:
                            j = await r.json(content_type=None)
                            exists = any(v.get("name") == voice for v in j.get("voices", []))
            except Exception:
                exists = None
        if exists is False:
            await message.reply("❌ This voice is not available in Edge-TTS. Try another one.")
            return
        db.set_setting(uid, "voice", voice)
        admin_states.pop(uid, None)
        await message.reply(f"✅ Voice set to `{voice}`", reply_markup=main_keyboard(uid))
        return

    # -- admin states -------------------------------------------------------------
    if state and db.is_admin(uid):
        if await _handle_admin_state(client, message, state, text):
            return
    elif state:
        admin_states.pop(uid, None)

    # -- ignore keyboard button labels that have their own handlers -----------
    if text in {BTN_REQUEST, BTN_ACCOUNT, BTN_SETTINGS, BTN_CREATE, BTN_HISTORY, BTN_HELP, BTN_ADMIN,
                BTN_ADD_SERVER, BTN_DEL_SERVER, BTN_SERVER_STATUS, BTN_APPROVE, BTN_REVOKE, BTN_USERS,
                BTN_BROADCAST, BTN_STATS, BTN_MAIN}:
        return

    # -- plain text -> audiobook -----------------------------------------------
    if not db.is_approved(uid):
        return
    if len(text) < 20:
        await message.reply("✍️ Send a longer text (at least 20 characters) or upload a document.")
        return
    if uid in active_jobs:
        await message.reply("A job is already running. Use /cancel first.")
        return
    token = uuid.uuid4().hex[:12]
    now = time.monotonic()
    for old_uid in [k for k, v in pending_text.items() if v[2] <= now]:
        pending_text.pop(old_uid, None)
    pending_text[uid] = (token, text, now + 900)
    est = estimate_seconds(len(text), db.get_settings(uid)["rate"])
    await message.reply(
        f"📝 Received **{len(text):,} characters** (~{fmt_duration(est)} of audio).\n\nConvert this text to audio?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🎧 Yes, convert", callback_data=f"text_go_{token}"),
            InlineKeyboardButton("✖ No", callback_data=f"text_no_{token}"),
        ]]))


@bot.on_callback_query(filters.regex(r"^text_(go|no)_([a-f0-9]{12})$"))
async def cb_text_confirm(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    pending = pending_text.get(uid)
    if not pending or pending[0] != cq.matches[0].group(2) or pending[2] <= time.monotonic():
        await cq.answer("This confirmation expired. Send your text again.", show_alert=True)
        return
    if uid in active_jobs:
        await cq.answer("A job is already running. Use /cancel first.", show_alert=True)
        return
    _, text, _ = pending_text.pop(uid)
    if cq.matches[0].group(1) == "no":
        await safe_edit(cq.message, "✖ Cancelled.")
        await cq.answer()
        return
    await cq.answer()
    await safe_edit(cq.message, "🚀 Starting…")
    job = reserve_job(uid)
    if job is None:
        await safe_edit(cq.message, "The job queue is full. Please try again later.")
        return
    job.task = asyncio.create_task(run_audiobook_job(client, cq.message, uid, text=text,
                                                    source="text message", title="Text", job=job))
    track_job(job)


# =============================================================================
# Document handler
# =============================================================================
@bot.on_message(filters.document & filters.private)
async def handle_document(client: Client, message: Message):
    uid = message.from_user.id
    db.touch_user(message.from_user)
    admin_states.pop(uid, None)

    if not db.is_approved(uid):
        await message.reply("🔒 You do not have access. Tap **🔑 Request Access**.", reply_markup=main_keyboard(uid))
        return
    if not backend_urls(db.servers()):
        await message.reply("❌ No TTS engine is available. Please contact the administrator.")
        return

    doc = message.document
    name = doc.file_name or "file"
    ext = os.path.splitext(name)[1].lower()
    if ext not in SUPPORTED_EXT:
        await message.reply(f"⚠️ Unsupported file type `{ext or '?'}`.\nSupported: "
                            + " ".join(f"`{e}`" for e in sorted(SUPPORTED_EXT)))
        return
    if doc.file_size and doc.file_size > MAX_FILE_MB * 1024 * 1024:
        await message.reply(f"⚠️ File too large ({fmt_size(doc.file_size)}). Maximum is {MAX_FILE_MB} MB.")
        return
    if uid in active_jobs:
        await message.reply("⏳ You already have a job running. Use /cancel to stop it first.")
        return

    job = reserve_job(uid)
    if job is None:
        await message.reply("The job queue is full. Please try again later.")
        return
    job.task = asyncio.create_task(document_job(client, message, job, name, ext))
    track_job(job)


async def document_job(client, message, job, name, ext):
    status = None
    tmpdir = tempfile.mkdtemp(prefix="abp_source_")
    extraction = None
    try:
        status = await message.reply("Downloading file...")

        async def progress(current, total):
            if job.cancel.is_set() or current > MAX_FILE_MB * 1024 * 1024:
                client.stop_transmission()

        path = await message.download(file_name=os.path.join(tmpdir, "source" + ext), progress=progress)
        if not path or not os.path.isfile(path):
            raise ValueError("Download did not complete")
        if os.path.getsize(path) > MAX_FILE_MB * 1024 * 1024:
            raise ValueError("Downloaded file exceeds the size limit")
        await safe_edit(status, f"Extracting text from {fmt_size(os.path.getsize(path))} file...")
        # NOTE: must NOT be "source.txt" - a .txt upload is downloaded to exactly
        # that path, and opening it for writing would truncate the source before
        # it is read (every .txt used to fail with "No readable text found").
        text_path = os.path.join(tmpdir, "extracted.utf8")
        # Stream-extract straight to disk: EPUB/PDF are processed item by item.
        extraction = asyncio.create_task(asyncio.to_thread(extract_text_to_file, path, ext, text_path))
        chars = await asyncio.shield(extraction)
        if chars <= 0:
            if ext == ".pdf":
                raise ValueError("No readable text found. Scanned PDFs need OCR before uploading")
            raise ValueError(f"No readable text found in this {ext} file (it appears to be empty)")
        await run_audiobook_job(client, status, job.user_id, text_path=text_path, source=name,
                               title=os.path.splitext(name)[0], job=job)
    except asyncio.CancelledError:
        if status is not None:
            await safe_edit(status, "Job cancelled.")
        raise
    except Exception as exc:
        log.exception("Document processing failed")
        if status is not None:
            await safe_edit(status, f"Could not process the document: {html.escape(str(exc))[:200]}")
    finally:
        # Threads cannot be forcibly stopped. Do not delete a file a parser is still reading.
        if extraction is not None and not extraction.done():
            def cleanup(future):
                if not future.cancelled():
                    with contextlib.suppress(Exception):
                        future.result()
                shutil.rmtree(tmpdir, ignore_errors=True)
            extraction.add_done_callback(cleanup)
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)
        if active_jobs.get(job.user_id) is job:
            active_jobs.pop(job.user_id, None)


# =============================================================================
# Core audiobook job  (v4: resumable, never gives up, any size, any #servers)
# =============================================================================
def job_dir(job_id: int) -> str:
    return os.path.join(JOBS_DIR, str(job_id))


def chunk_path(jdir: str, index: int) -> str:
    return os.path.join(jdir, "chunks", f"{index:06d}.mp3")


def _read_meta(jdir: str, index: int) -> int:
    """Duration (ms) of a checkpointed chunk, stored alongside the mp3."""
    try:
        with open(chunk_path(jdir, index) + ".ms", "r") as f:
            return max(1, int(f.read().strip() or "1"))
    except Exception:  # noqa: BLE001
        return 0


def _write_chunk(jdir: str, index: int, data: bytes, dur: int) -> None:
    p = chunk_path(jdir, index)
    tmp = p + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, p)
    with open(p + ".ms", "w") as f:
        f.write(str(dur))


def _chunk_ready(jdir: str, index: int) -> bool:
    p = chunk_path(jdir, index)
    return os.path.isfile(p) and os.path.getsize(p) > 100 and os.path.isfile(p + ".ms")


async def wait_for_engines(pool: ServerPool, session: aiohttp.ClientSession, cancel: asyncio.Event,
                           status: Message, cancel_markup, stall_round: int, done: int, total: int,
                           job_id: int) -> None:
    """All engines failed in the same round: pause with escalating back-off.

    Instead of killing the job we wait, re-probe the engines and continue as
    soon as *any* of them answers again.  Only /cancel (or JOB_MAX_STALL_MINUTES)
    stops this loop.
    """
    pause = STALL_BACKOFF_STEPS[min(stall_round, len(STALL_BACKOFF_STEPS) - 1)] + random.uniform(0, 3)
    pause = max(pause, pool.soonest_available())
    reason = pool.failure_summary()[:300]
    log.warning("Job %s paused (round %d, %.0fs): %s", job_id, stall_round + 1, pause, reason)
    db.job_set_status(job_id, "paused", f"round {stall_round + 1}: {reason}")
    percent = int(done / total * 100) if total else 0
    await safe_edit(status,
                    f"**Generating audiobook**\n"
                    f"[{progress_bar(percent)}] {percent}% ({done}/{total})\n\n"
                    f"⏸ All TTS engines are busy or throttled right now.\n"
                    f"Waiting {fmt_duration(pause)} and retrying automatically (attempt {stall_round + 1}).\n"
                    f"Nothing is lost - the job continues from where it stopped.\n"
                    f"Reason: {html.escape(reason)[:180]}",
                    cancel_markup)
    await cancellable_sleep(pause, cancel)
    # Give every engine a fresh chance; the limiters will shrink again if needed.
    pool.reset_cooldowns()
    if local_limiter.throttle_events:
        local_limiter.throttle_events = max(0, local_limiter.throttle_events - 1)
    db.job_set_status(job_id, "running", "")


async def run_audiobook_job(client: Client, status: Message, uid: int, text: Optional[str] = None,
                            source: str = "", title: str = "Audiobook", job: Optional[Job] = None,
                            text_path: Optional[str] = None, resume_job_id: Optional[int] = None) -> None:
    """Drive one audiobook job from text (or a text file) to delivered MP3 parts.

    * Chunks are synthesised through a sliding-window pipeline across every
      engine (render servers + built-in Edge-TTS), each with its own adaptive
      limiter - throughput scales with the number of servers.
    * Every finished chunk is checkpointed to ``JOBS_DIR/<job>/chunks`` and the
      cursor is saved in the database, so a restart or Render sleep resumes the
      job instead of losing it (``resume_job_id``).
    * When every engine fails at once the job pauses with escalating back-off
      and keeps retrying; it never fails on its own unless
      ``JOB_MAX_STALL_MINUTES`` is set and exceeded.
    """
    if job is None:
        job = reserve_job(uid)
        if job is None:
            await safe_edit(status, "A job is already running or the queue is full.")
            return
        job.task = asyncio.current_task()
    job_id: Optional[int] = None
    jdir: Optional[str] = None
    delivered = delivered_chars = total_audio_ms = done_chunks = 0
    outcome = "failed"
    keep_files = False
    started = time.monotonic()
    cancel_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Cancel", callback_data=f"cancel_job_{uid}")]
    ])
    settings = db.get_settings(uid)
    part_ms = part_bytes = part_chars = 0
    part_label = "Audiobook"
    part_index = 0
    plan: List[Tuple[str, str]] = []
    next_write = 0
    part_start = 0  # first chunk index of the part currently being assembled
    try:
        if not db.is_approved(uid):
            raise ValueError("Your access has expired or was revoked")
        if not backend_urls(db.servers()):
            raise ValueError("No TTS engine is available")

        # ------------------------------------------------------------------
        # Job directory + source text (new job or resume)
        # ------------------------------------------------------------------
        os.makedirs(JOBS_DIR, exist_ok=True)
        if resume_job_id is not None:
            row = db.job_get(resume_job_id)
            if row is None:
                raise ValueError("Job to resume no longer exists")
            job_id = int(row["id"])
            jdir = job_dir(job_id)
            plan_path = os.path.join(jdir, "plan.jsonl")
            if not os.path.isfile(plan_path):
                raise ValueError("Job files were deleted - cannot resume, please send the file again")
            try:
                settings = {**settings, **json.loads(row["settings_json"] or "{}")}
            except ValueError:
                pass
            title = row["title"] or title
            source = row["source"] or source
            delivered = int(row["parts"] or 0)
            part_index = delivered
            total_audio_ms = int(row["audio_seconds"] or 0) * 1000
            delivered_chars = int(row["delivered_chars"] or 0)
            next_write = int(row["next_write"] or 0)
            done_chunks = next_write
            db.execute("UPDATE jobs SET resumes = COALESCE(resumes,0) + 1, status = 'running', "
                       "status_msg_id = ? WHERE id = ?", (status.id, job_id))
            await safe_edit(status, f"♻️ Resuming **{html.escape(title)[:60]}** from chunk {next_write + 1}…",
                            cancel_markup)
            plan = read_plan(plan_path)
        else:
            if text_path is None:
                if text is None:
                    raise ValueError("No text supplied")
                text = clean_text(text)
                if not text or len(text) > MAX_EXTRACTED_CHARS:
                    raise ValueError(f"Text must contain 1 to {MAX_EXTRACTED_CHARS:,} characters")
                chars = len(text)
            else:
                chars = os.path.getsize(text_path)  # bytes; refined below
            job_id = db.job_start(uid, source, chars)
            jdir = job_dir(job_id)
            os.makedirs(os.path.join(jdir, "chunks"), exist_ok=True)
            src_path = os.path.join(jdir, "source.txt")
            if text_path is not None:
                shutil.move(text_path, src_path)
            else:
                with open(src_path, "w", encoding="utf-8") as f:
                    f.write(text)
            text = None  # free memory; we re-read from disk when planning

        if job_slots.locked():
            await safe_edit(status, "Your audiobook is queued. You can cancel while waiting.", cancel_markup)

        out_path = os.path.join(jdir, "part.mp3")

        def save_checkpoint(note: str = "") -> None:
            db.job_checkpoint(job_id, next_write, done_chunks, delivered, total_audio_ms // 1000,
                              delivered_chars, note)

        async def upload_part():
            nonlocal delivered, delivered_chars, total_audio_ms, part_ms, part_bytes, part_chars, part_index, part_start
            if not part_bytes:
                return
            if job.cancel.is_set():
                raise asyncio.CancelledError
            fixed_path = await remux_mp3(out_path)
            if os.path.getsize(fixed_path) > MAX_AUDIO_BYTES:
                raise ValueError("Audio part exceeds the upload size limit")
            number = part_index + 1
            fname = f"{safe_filename(title)}_part{number:02d}.mp3"
            caption = (f"**{html.escape(title)[:80]}**\n"
                       f"{html.escape(part_label)[:60]} - part {number}\n"
                       f"{fmt_duration(part_ms / 1000)} | `{settings['voice']}`")
            await safe_edit(status, f"Uploading part {number} ({fmt_size(os.path.getsize(fixed_path))})...",
                            cancel_markup)
            for attempt in range(5):
                try:
                    await client.send_audio(status.chat.id, fixed_path, caption=caption,
                                            title=f"{title[:50]} - Part {number}", performer="AudioBook Pro",
                                            duration=max(1, math.ceil(part_ms / 1000)), file_name=fname)
                    break
                except FloodWait as exc:
                    if attempt == 4:
                        raise
                    await cancellable_sleep(float(exc.value) + 1, job.cancel)
                except (RPCError, aiohttp.ClientError, OSError) as exc:
                    if attempt == 4:
                        raise
                    log.warning("Upload of part %d failed (%s), retrying", number, exc)
                    await cancellable_sleep(5 * (attempt + 1), job.cancel)
            delivered += 1
            part_index += 1
            delivered_chars += part_chars
            total_audio_ms += part_ms  # Count delivered audio, not failed uploads.
            os.remove(fixed_path)
            part_ms = part_bytes = part_chars = 0
            # Delivered chunks are no longer needed for resume; anything left on
            # disk below ``next_write`` belongs to the *current* part.
            for i in range(part_start, next_write):
                for suffix in ("", ".ms"):
                    with contextlib.suppress(OSError):
                        os.remove(chunk_path(jdir, i) + suffix)
            part_start = next_write
            save_checkpoint()

        async with job_slots:
            if job.cancel.is_set():
                raise asyncio.CancelledError
            if not db.is_approved(uid):
                raise ValueError("Access expired while waiting in the queue")
            job.started = time.time()
            servers = backend_urls(db.servers())
            conn = aiohttp.TCPConnector(limit=MAX_TOTAL_CONCURRENCY + 5)
            async with aiohttp.ClientSession(connector=conn) as session:
                await safe_edit(status, "Checking TTS engines and planning audio...", cancel_markup)
                remote = [u for u in servers if u != LOCAL_TTS_URL]
                # Deep probe = real authenticated /tts call.  A server whose API key does not
                # match (HTTP 401) is dropped up front; a merely *busy/throttled* server is
                # kept (it will recover) but starts in cooldown.
                probes = await asyncio.gather(*(check_server(session, url, deep=True) for url in remote))
                usable: List[str] = []
                skipped: List[str] = []
                cooling: List[str] = []
                for url, info in zip(remote, probes):
                    db.server_health(url, info["ok"])
                    err = (info.get("error") or "").lower()
                    if info["ok"]:
                        usable.append(url)
                    elif any(k in err for k in ("401", "404", "api key", "invalid url", "not mp3", "not an app.py")):
                        skipped.append(f"{url} ({info.get('error') or 'offline'})")
                        log.warning("Render server %s skipped for job %s: %s", url, job_id, info.get("error"))
                    else:
                        usable.append(url)
                        cooling.append(url)
                if LOCAL_TTS_ENABLED:
                    usable.append(LOCAL_TTS_URL)
                if not usable:
                    raise ValueError("No working TTS engine. " + "; ".join(skipped)[:400])
                pool = ServerPool(usable)
                for s in pool.states:
                    if s.url in cooling:
                        s.cooldown_until = time.time() + 20
                        s.last_error = "temporarily unavailable at job start"
                if skipped:
                    await safe_edit(status, "Skipping misconfigured engine(s):\n" +
                                    "\n".join(f"- {html.escape(s)[:160]}" for s in skipped[:5]) +
                                    "\n\nPlanning audio...", cancel_markup)

                # ----------------------------------------------------------
                # Plan (new job) - chunk list is persisted for resume
                # ----------------------------------------------------------
                if resume_job_id is None:
                    limits = [r["max_text_length"] for r in probes if r["ok"] and r.get("max_text_length")]
                    if LOCAL_TTS_ENABLED:
                        limits.append(LOCAL_TTS_CHUNK_SIZE)
                    chunk_limit = min([CHUNK_SIZE] + limits)
                    src_path = os.path.join(jdir, "source.txt")

                    def _plan() -> List[Tuple[str, str]]:
                        with open(src_path, "r", encoding="utf-8") as f:
                            full = f.read()
                        if len(full) > MAX_EXTRACTED_CHARS:
                            raise ValueError(f"Document exceeds the {MAX_EXTRACTED_CHARS:,} character limit")
                        p = build_plan(full, chunk_limit, LOCAL_TTS_MAX_BYTES,
                                       settings["split_mode"] == "chapter")
                        write_plan(os.path.join(jdir, "plan.jsonl"), p)
                        return p, len(full)

                    plan, nchars = await asyncio.to_thread(_plan)
                    db.execute("UPDATE jobs SET chars = ? WHERE id = ?", (nchars, job_id))
                    db.job_init_resume(job_id, title, status.chat.id, status.id, settings, len(plan))
                total_chunks = len(plan)
                if not total_chunks:
                    raise ValueError("No readable text found in the document")
                max_part_ms = settings["max_hours"] * 3600 * 1000
                keep_files = True  # from here on a crash must leave the checkpoint intact

                # Rebuild the current (undelivered) part from checkpointed chunks on resume.
                if resume_job_id is not None:
                    if os.path.exists(out_path):
                        os.remove(out_path)
                    # Find where the last delivered part ended: everything before
                    # next_write that is still on disk belongs to the current part.
                    part_label = plan[min(next_write, total_chunks - 1)][0]
                    part_start = next_write
                    with open(out_path, "ab") as output:
                        for i in range(next_write):
                            if _chunk_ready(jdir, i):
                                part_start = min(part_start, i)
                                with open(chunk_path(jdir, i), "rb") as f:
                                    data = f.read()
                                output.write(data)
                                part_bytes += len(data)
                                part_ms += _read_meta(jdir, i)
                                part_chars += len(plan[i][1])

                parallel = pool.max_concurrency()
                window = min(MAX_TOTAL_CONCURRENCY, parallel + PIPELINE_WINDOW_EXTRA)
                sem = asyncio.Semaphore(parallel)
                gen_started = time.monotonic()
                gen_done_at_start = done_chunks
                last_edit = 0.0
                last_access_check = time.monotonic()
                last_progress = time.monotonic()
                stall_round = 0

                async def report_progress(force: bool = False) -> None:
                    nonlocal last_edit
                    now = time.monotonic()
                    if not force and now - last_edit < PROGRESS_EDIT_INTERVAL:
                        return
                    last_edit = now
                    percent = int(done_chunks / total_chunks * 100) if total_chunks else 100
                    elapsed = now - gen_started
                    fresh = done_chunks - gen_done_at_start
                    eta = elapsed / fresh * (total_chunks - done_chunks) if fresh else 0.0
                    engines = len(pool.states)
                    par = pool.concurrency()
                    await safe_edit(status,
                                    f"**Generating audiobook**\n"
                                    f"[{progress_bar(percent)}] {percent}% ({done_chunks}/{total_chunks})\n"
                                    f"Delivered: {delivered} parts | ETA {fmt_duration(eta)}\n"
                                    f"{engines} engine(s) | {par}x parallel",
                                    cancel_markup)

                await report_progress(force=True)
                pending: Dict[int, asyncio.Task] = {}
                next_submit = next_write
                try:
                    while next_write < total_chunks:
                        if job.cancel.is_set():
                            raise asyncio.CancelledError
                        now = time.monotonic()
                        if now - last_access_check >= 15:
                            last_access_check = now
                            if not db.is_approved(uid):
                                raise ValueError("Access expired or revoked during processing")
                        # Keep the window full (skip chunks already checkpointed).
                        while next_submit < total_chunks and next_submit - next_write < window:
                            idx = next_submit
                            next_submit += 1
                            if _chunk_ready(jdir, idx):
                                continue
                            pending[idx] = asyncio.create_task(
                                fetch_chunk(session, pool, plan[idx][1], idx, settings, sem, job.cancel))
                        # Consume strictly in order so the MP3 is written sequentially.
                        if next_write in pending:
                            _, data, dur = await pending.pop(next_write)
                            if not data:
                                # Every engine failed this round.  PAUSE, do not die.
                                if JOB_MAX_STALL_MINUTES and (time.monotonic() - last_progress) > JOB_MAX_STALL_MINUTES * 60:
                                    raise RuntimeError(
                                        f"No engine produced audio for {JOB_MAX_STALL_MINUTES} minutes. "
                                        f"Reason: {pool.failure_summary()}")
                                # Drain the window so we do not hammer throttled engines while paused.
                                for t in pending.values():
                                    t.cancel()
                                if pending:
                                    await asyncio.gather(*pending.values(), return_exceptions=True)
                                pending.clear()
                                next_submit = next_write
                                save_checkpoint(f"paused: {pool.failure_summary()[:150]}")
                                await wait_for_engines(pool, session, job.cancel, status, cancel_markup,
                                                       stall_round, done_chunks, total_chunks, job_id)
                                stall_round += 1
                                gen_started = time.monotonic()
                                gen_done_at_start = done_chunks
                                continue
                            _write_chunk(jdir, next_write, data, dur)
                        elif _chunk_ready(jdir, next_write):
                            with open(chunk_path(jdir, next_write), "rb") as f:
                                data = f.read()
                            dur = _read_meta(jdir, next_write) or max(1, round(
                                estimate_seconds(len(plan[next_write][1]), settings["rate"]) * 1000))
                        else:
                            # Should not happen; re-submit defensively.
                            next_submit = next_write
                            continue
                        stall_round = 0
                        last_progress = time.monotonic()
                        label, chunk_text = plan[next_write]
                        if label != part_label and part_bytes and settings["split_mode"] == "chapter":
                            await upload_part()
                        part_label = label
                        if part_bytes and (part_ms + dur > max_part_ms or
                                           part_bytes + len(data) > MAX_AUDIO_BYTES - 1024 * 1024):
                            await upload_part()
                        if len(data) > MAX_AUDIO_BYTES or dur > max_part_ms:
                            raise ValueError("A single audio chunk exceeds the part limit")
                        with open(out_path, "ab") as output:
                            output.write(data)
                        part_bytes += len(data)
                        part_ms += dur
                        part_chars += len(chunk_text)
                        next_write += 1
                        done_chunks = next_write
                        if done_chunks % 10 == 0:
                            save_checkpoint()
                        await report_progress(force=done_chunks == total_chunks)
                finally:
                    for task in pending.values():
                        if not task.done():
                            task.cancel()
                    if pending:
                        await asyncio.gather(*pending.values(), return_exceptions=True)
                    pending.clear()
                await upload_part()
        outcome = "done"
        keep_files = False
        await safe_edit(status, f"**Audiobook complete!**\n{delivered} part(s) delivered | "
                                f"{fmt_duration(total_audio_ms / 1000)} audio\n"
                                f"Took {fmt_duration(time.monotonic() - started)}")
    except asyncio.CancelledError:
        outcome = "cancelled"
        keep_files = False
        await safe_edit(status, f"Job cancelled. {delivered} complete part(s) were delivered.")
        raise
    except Exception as exc:
        outcome = "partial" if delivered else "failed"
        log.exception("Job %s failed", job_id)
        hint = ""
        if keep_files and job_id is not None:
            outcome = "interrupted"
            hint = "\n\nSend /resume to continue this job from where it stopped."
        await safe_edit(status, f"Processing stopped: {html.escape(str(exc))[:600]}\n\n"
                                f"{delivered} complete part(s) delivered. No failed passages were silently skipped."
                                f"{hint}\nTip: admins can run /servers to see each engine's status and error.")
    finally:
        if active_jobs.get(uid) is job:
            active_jobs.pop(uid, None)
        if job_id is not None:
            if keep_files:
                db.job_checkpoint(job_id, next_write, done_chunks, delivered, total_audio_ms // 1000,
                                  delivered_chars)
                db.job_set_status(job_id, "interrupted")
            else:
                db.job_finish(job_id, outcome, delivered, total_audio_ms // 1000)
                if jdir:
                    shutil.rmtree(jdir, ignore_errors=True)
            if delivered and outcome in ("done", "cancelled", "partial", "failed"):
                db.record_usage(uid, delivered_chars, total_audio_ms // 1000)
        log.info("Job %s for %s -> %s (%d parts)", job_id, uid, outcome, delivered)


async def resume_job(client: Client, row: sqlite3.Row, status: Optional[Message] = None) -> bool:
    """Restart an interrupted/paused job for its owner. Returns True if started."""
    uid = int(row["user_id"])
    if uid in active_jobs:
        return False
    chat_id = int(row["chat_id"])
    if status is None:
        status = await safe_send(client, chat_id, f"♻️ Resuming your audiobook **{html.escape(row['title'] or '')[:60]}**…")
        if status is None:
            return False
    job = reserve_job(uid)
    if job is None:
        return False
    job.task = asyncio.create_task(run_audiobook_job(client, status, uid, source=row["source"] or "",
                                                    title=row["title"] or "Audiobook", job=job,
                                                    resume_job_id=int(row["id"])))
    track_job(job)
    return True


async def resume_interrupted_jobs(client: Client) -> None:
    """Called at startup: continue every job that a restart interrupted."""
    if not RESUME_ON_START:
        return
    await asyncio.sleep(3)
    rows = db.resumable_jobs()
    for row in rows:
        jdir = job_dir(int(row["id"]))
        if not os.path.isfile(os.path.join(jdir, "plan.jsonl")):
            db.job_set_status(int(row["id"]), "failed", "job files missing after restart")
            shutil.rmtree(jdir, ignore_errors=True)
            continue
        try:
            ok = await resume_job(client, row)
            log.info("Auto-resume job %s -> %s", row["id"], "started" if ok else "skipped")
        except Exception as exc:  # noqa: BLE001
            log.warning("Auto-resume of job %s failed: %s", row["id"], exc)
        await asyncio.sleep(1)


@bot.on_message(filters.command("resume") & filters.private)
async def cmd_resume(client: Client, message: Message):
    uid = message.from_user.id
    if not db.is_approved(uid):
        await message.reply("🔒 You do not have access.")
        return
    if uid in active_jobs:
        await message.reply("⏳ A job is already running.")
        return
    row = db.user_resumable_job(uid)
    if row is None:
        await message.reply("Nothing to resume - you have no interrupted audiobook.")
        return
    status = await message.reply("♻️ Resuming…")
    if not await resume_job(client, row, status):
        await safe_edit(status, "Could not resume right now, please try again.")


@bot.on_message(filters.command("jobs") & filters.private)
async def cmd_jobs(client: Client, message: Message):
    if not _is_admin_msg(message):
        return
    rows = db.fetchall("SELECT * FROM jobs WHERE status IN ('running','paused','interrupted') ORDER BY id DESC LIMIT 20")
    if not rows:
        await message.reply("No running, paused or interrupted jobs.")
        return
    lines = ["**Jobs**"]
    for r in rows:
        total = r["total_chunks"] or 0
        done = r["done_chunks"] or 0
        pct = int(done / total * 100) if total else 0
        icon = {"running": "▶️", "paused": "⏸", "interrupted": "⚠️"}.get(r["status"], "•")
        lines.append(f"{icon} #{r['id']} user `{r['user_id']}` — {html.escape((r['title'] or r['source'] or '')[:30])} "
                     f"— {pct}% ({done}/{total}) — {r['parts'] or 0} parts"
                     + (f"\n   ↳ {html.escape((r['stall_note'] or '')[:120])}" if r["stall_note"] else ""))
    await message.reply("\n".join(lines))


# =============================================================================
# Keep-alive pinger (async, no thread / no second DB connection)
# =============================================================================
async def keep_alive_loop() -> None:
    await asyncio.sleep(15)
    while True:
        try:
            servers = db.servers()
            if servers:
                async with aiohttp.ClientSession() as session:
                    results = await asyncio.gather(*(check_server(session, s) for s in servers), return_exceptions=True)
                for r in results:
                    if isinstance(r, dict):
                        db.server_health(r["url"], r["ok"])
                online = sum(1 for r in results if isinstance(r, dict) and r["ok"])
                log.debug("Keep-alive: %d/%d servers online", online, len(servers))
        except Exception as e:  # noqa: BLE001
            log.debug("Keep-alive error: %s", e)
        await asyncio.sleep(KEEP_ALIVE_INTERVAL)


# =============================================================================
# Entrypoint
# =============================================================================
async def main() -> None:
    if not BOT_TOKEN or not API_HASH or API_ID <= 0 or OWNER_ID <= 0:
        raise SystemExit("Set API_ID, API_HASH, BOT_TOKEN and OWNER_ID environment variables before starting.")
    pinger = None
    resumer = None
    started = False
    try:
        await bot.start()
        started = True
        db.execute("UPDATE jobs SET status = 'interrupted', finished_at = ? WHERE status = 'running'", (now_str(),))
        me = await bot.get_me()
        log.info("AudioBook Pro v%s started as @%s (owner=%s, servers=%d, local_edge_tts=%s x%d..%d, api_key=%s)",
                 VERSION, me.username, OWNER_ID, len(db.servers()), LOCAL_TTS_ENABLED,
                 LOCAL_TTS_CONCURRENCY, LOCAL_TTS_MAX_CONCURRENCY, "set" if TTS_API_KEY else "NOT set")
        if db.servers() and not TTS_API_KEY:
            log.warning("Render servers are configured but TTS_API_KEY is empty - "
                        "servers started with API_KEY will reject every request with HTTP 401.")
        if not LOCAL_TTS_ENABLED and edge_tts is None:
            log.warning("edge-tts is not installed - the built-in fallback engine is unavailable "
                        "(pip install edge-tts).")
        pinger = asyncio.create_task(keep_alive_loop())
        resumer = asyncio.create_task(resume_interrupted_jobs(bot))
        engine = (f"built-in Edge-TTS (free, {LOCAL_TTS_CONCURRENCY}-{LOCAL_TTS_MAX_CONCURRENCY}x parallel)"
                  if LOCAL_TTS_ENABLED else "render servers only")
        await safe_send(bot, OWNER_ID, f"AudioBook Pro v{VERSION} is online.\nEngine: {engine}\n"
                                       f"Render servers: {len(db.servers())}")
        from pyrogram import idle
        await idle()
    finally:
        tasks = [j.task for j in list(active_jobs.values()) if j.task is not None]
        tasks.extend(list(preview_tasks.values()))
        if pinger is not None:
            tasks.append(pinger)
        if resumer is not None:
            tasks.append(resumer)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if started:
            await bot.stop()
        db.conn.close()


if __name__ == "__main__":
    try:
        bot.run(main())
    except KeyboardInterrupt:
        pass
