#!/usr/bin/env python3
"""
=============================================================================
 AudioBook Pro - Telegram TTS master bot  (bot.py)  -  v5.0.0
=============================================================================
 The *master* side of the system.  It runs on your own VPS (Oracle Cloud
 free tier is plenty) and turns text / documents sent on Telegram into MP3
 audiobooks.  All speech synthesis is delegated to one or more
 **Cloudflare Workers** (the ``worker/`` directory of this repository):

     Telegram user -> bot.py (Oracle VPS) --HTTP--> edge-tts Worker(s) on Cloudflare
                                         <--MP3---

 The bot itself never talks to Microsoft, so the VPS IP can never be
 throttled; add more Workers (Admin Panel -> Add Server) for more throughput.

 Highlights
 ----------
   * ONE self-contained file - copy it to the VPS, fill in .env, run.
   * Supports  .txt .md .docx .html .htm .epub .pdf  + plain text messages
   * Sentence-aware even chunking, duration-aware / chapter-aware part splitting
   * Multi-Worker load balancing: least-loaded scheduling, per-server adaptive
     concurrency (clamped to the ``max_concurrency`` a Worker advertises on
     /health), per-server quarantine when Microsoft throttles it, instant
     fail-over, job pauses (never dies) only when *every* Worker is out.
   * Script detection -> an English voice is switched to a Hindi/Bengali/...
     voice automatically when the text needs it.
   * Every chunk is checkpointed to disk; jobs resume after a restart.
   * Visible job queue, live progress bar with ETA, /cancel, voice preview,
     20+ voices, user approval system, admin panel, rotating logs.
   * Optional ffmpeg re-mux for perfect MP3 headers (auto-detected).

 Quick start on Oracle VPS (Ubuntu)
 ----------------------------------
   sudo apt update && sudo apt install -y python3-venv ffmpeg
   mkdir -p ~/audiobook && cd ~/audiobook && cp /path/to/bot.py .
   python3 -m venv venv && . venv/bin/activate
   pip install -U pyrogram tgcrypto aiohttp python-docx ebooklib beautifulsoup4 lxml pypdf
   cat > .env <<'EOF'
   API_ID=123456
   API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   BOT_TOKEN=123456:ABC-xxxxxxxxxxxxxxxxxxxxxxx
   OWNER_ID=123456789
   TTS_API_KEY=same-value-as-wrangler-secret-API_KEY   # leave empty if the Worker has no key
   TTS_SERVERS=https://edge-tts-worker.<account>.workers.dev
   EOF
   python bot.py

 The bot reads ``.env`` from its own directory by itself (no python-dotenv
 needed).  Run it forever with systemd:

   sudo tee /etc/systemd/system/audiobook-bot.service >/dev/null <<'EOF'
   [Unit]
   Description=AudioBook Pro Telegram bot
   After=network-online.target
   Wants=network-online.target

   [Service]
   User=ubuntu
   WorkingDirectory=/home/ubuntu/audiobook
   ExecStart=/home/ubuntu/audiobook/venv/bin/python /home/ubuntu/audiobook/bot.py
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   EOF
   sudo systemctl daemon-reload && sudo systemctl enable --now audiobook-bot
   journalctl -u audiobook-bot -f

 Environment variables
 ---------------------
   API_ID, API_HASH, BOT_TOKEN, OWNER_ID     required
   TTS_SERVERS       comma-separated Worker URLs registered at start-up (optional;
                     you can also add them from the Admin Panel)
   TTS_API_KEY       sent as X-API-Key - must equal the Worker's API_KEY secret
   CONTACT_USERNAME  shown to users who need access
   REMOTE_CHUNK_SIZE=3000  PER_SERVER_CONCURRENCY=4  PER_SERVER_MAX_CONCURRENCY=8
   MAX_TOTAL_CONCURRENCY=64  MAX_PARALLEL_JOBS=1  MAX_QUEUED_JOBS=20
   QUARANTINE_STEPS=60,120,300,600  QUARANTINE_FORGIVE_AFTER=25
   MAX_FILE_MB=200  JOBS_DIR=jobs  DB_PATH=bot_database.db  RESUME_ON_START=true
   LOG_LEVEL=INFO  PROGRESS_EDIT_INTERVAL=4
=============================================================================
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import html
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

VERSION = "5.0.1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# .env loader (tiny, dependency-free) - values already in the environment win
# ---------------------------------------------------------------------------
def _load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].strip()
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                # strip inline comments for unquoted values
                if value and value[0] not in "\"'":
                    value = re.split(r"\s+#", value, 1)[0].strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass


_load_dotenv(os.path.join(BASE_DIR, ".env"))

import aiohttp  # noqa: E402

# Pyrogram 2.x captures ``asyncio.get_event_loop()`` when ``Client()`` is
# constructed (Client.loop / Dispatcher.loop) and later creates its handler
# worker tasks on THAT loop.  If the bot is then started with ``asyncio.run()``
# a *second* loop is used, the handler tasks never run and every button /
# command is silently ignored (the bot logs "logged in" but never answers).
# So: create one loop up-front, make it current, and run everything on it.
LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)

from pyrogram import Client, filters  # noqa: E402
from pyrogram.errors import FloodWait, MessageNotModified, RPCError  # noqa: E402
from pyrogram.types import (  # noqa: E402
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
def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, "true" if default else "false").strip().lower() in ("1", "true", "yes", "on")


API_ID = _env_int("API_ID", 0)
API_HASH = _env("API_HASH")
BOT_TOKEN = _env("BOT_TOKEN")
OWNER_ID = _env_int("OWNER_ID", 0)
CONTACT_USERNAME = _env("CONTACT_USERNAME", "Niteshbhumihar")
TTS_API_KEY = _env("TTS_API_KEY", "") or _env("API_KEY", "")
TTS_SERVERS_ENV = [s.strip() for s in re.split(r"[,\s]+", _env("TTS_SERVERS", "")) if s.strip()]

# Characters per HTTP request to a Worker (further clamped to the server's
# advertised max_text_length).  The Worker splits it into websocket pieces and
# synthesises them in parallel, so big chunks = fewer round trips.
REMOTE_CHUNK_SIZE = max(500, min(_env_int("REMOTE_CHUNK_SIZE", 3000), 6000))
TARGET_CHUNK_CHARS = max(0, min(_env_int("TARGET_CHUNK_CHARS", 0), 6000))
CHUNK_MIN_RATIO = max(0.3, min(_env_int("CHUNK_MIN_PERCENT", 60), 95) / 100.0)
PER_SERVER_CONCURRENCY = max(1, min(_env_int("PER_SERVER_CONCURRENCY", 4), 16))
PER_SERVER_MAX_CONCURRENCY = max(PER_SERVER_CONCURRENCY, min(_env_int("PER_SERVER_MAX_CONCURRENCY", 8), 16))
REMOTE_MIN_GAP = max(0, _env_int("REMOTE_MIN_GAP_MS", 30)) / 1000.0
REMOTE_GROW_AFTER = max(1, min(_env_int("REMOTE_GROW_AFTER", 3), 50))
MAX_TOTAL_CONCURRENCY = max(4, min(_env_int("MAX_TOTAL_CONCURRENCY", 64), 256))
MAX_PARALLEL_JOBS = max(1, _env_int("MAX_PARALLEL_JOBS", 1))
MAX_QUEUED_JOBS = max(MAX_PARALLEL_JOBS, _env_int("MAX_QUEUED_JOBS", 20))
CHUNK_TIMEOUT = max(60, _env_int("CHUNK_TIMEOUT", 150))  # Worker SYNTH_TIMEOUT is 110 s
PIPELINE_WINDOW_EXTRA = max(2, min(_env_int("PIPELINE_WINDOW_EXTRA", 8), 64))
PROGRESS_EDIT_INTERVAL = max(2.0, float(_env_int("PROGRESS_EDIT_INTERVAL", 4)))
KEEP_ALIVE_INTERVAL = max(60, _env_int("KEEP_ALIVE_INTERVAL", 600))
MIN_FREE_DISK_MB = max(0, _env_int("MIN_FREE_DISK_MB", 400))

DB_PATH = _env("DB_PATH", os.path.join(BASE_DIR, "bot_database.db"))
SESSION_NAME = _env("SESSION_NAME", os.path.join(BASE_DIR, "audiobook_pro_bot"))
LOG_FILE = _env("LOG_FILE", os.path.join(BASE_DIR, "bot.log"))
JOBS_DIR = _env("JOBS_DIR", os.path.join(os.path.dirname(os.path.abspath(DB_PATH)) or BASE_DIR, "jobs"))
MAX_FILE_MB = max(1, _env_int("MAX_FILE_MB", 200))
MAX_EXTRACTED_CHARS = max(1000, _env_int("MAX_EXTRACTED_CHARS", 50_000_000))
MAX_ARCHIVE_BYTES = 800 * 1024 * 1024
MAX_AUDIO_BYTES = 1900 * 1024 * 1024  # below Telegram's 2 GB limit
JOB_MAX_STALL_MINUTES = max(0, _env_int("JOB_MAX_STALL_MINUTES", 0))
STALL_BACKOFF_STEPS = [5, 10, 20, 30, 60, 90, 120, 180, 300]
_q_steps = [max(15, int(x)) for x in re.findall(r"\d+", _env("QUARANTINE_STEPS", "60,120,300,600"))]
QUARANTINE_STEPS: List[int] = _q_steps or [60, 120, 300, 600]
QUARANTINE_FORGIVE_AFTER = max(5, _env_int("QUARANTINE_FORGIVE_AFTER", 25))
RESUME_ON_START = _env_bool("RESUME_ON_START", True)
PLAN_SLICE_CHARS = 200_000
SUPPORTED_EXT = {".txt", ".md", ".docx", ".html", ".htm", ".epub", ".pdf"}
FFMPEG = shutil.which("ffmpeg")

DEFAULT_VOICE = "hi-IN-MadhurNeural"
DEFAULT_HOURS = 12
DEFAULT_RATE = "+0%"
DEFAULT_PITCH = "+0Hz"
DEFAULT_VOLUME = "+0%"
DEFAULT_SPLIT = "duration"  # or "chapter"
# Edge MP3 stream is CBR 48 kbit/s -> 6000 bytes per second.
MP3_BYTES_PER_SEC = 6000

VOICE_RE = re.compile(r"^[a-z]{2,3}-[A-Za-z]{2,4}(-[A-Za-z]+)?-[A-Za-z0-9]+Neural$")
CHAPTER_RE = re.compile(
    r"^\s*(?:chapter|part|book|prologue|epilogue|अध्याय|भाग|प्रकरण|खंड)\b[^\n]{0,80}$",
    re.IGNORECASE | re.MULTILINE,
)

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

RATE_OPTIONS = [("-25%", "-25%"), ("-10%", "-10%"), ("Normal", "+0%"), ("+10%", "+10%"),
                ("+25%", "+25%"), ("+50%", "+50%")]
PITCH_OPTIONS = [("-20Hz", "-20Hz"), ("-10Hz", "-10Hz"), ("Normal", "+0Hz"), ("+10Hz", "+10Hz"), ("+20Hz", "+20Hz")]
VOLUME_OPTIONS = [("-20%", "-20%"), ("Normal", "+0%"), ("+20%", "+20%"), ("+50%", "+50%")]
HOURS_OPTIONS = [("1 h", "1"), ("2 h", "2"), ("4 h", "4"), ("6 h", "6"), ("8 h", "8"), ("12 h", "12")]

# ---------------------------------------------------------------------------
# Script <-> voice compatibility
# ---------------------------------------------------------------------------
_SCRIPT_BLOCKS: List[Tuple[str, int, int]] = [
    ("hi", 0x0900, 0x097F), ("bn", 0x0980, 0x09FF), ("pa", 0x0A00, 0x0A7F), ("gu", 0x0A80, 0x0AFF),
    ("or", 0x0B00, 0x0B7F), ("ta", 0x0B80, 0x0BFF), ("te", 0x0C00, 0x0C7F), ("kn", 0x0C80, 0x0CFF),
    ("ml", 0x0D00, 0x0D7F), ("ur", 0x0600, 0x06FF), ("ur", 0x0750, 0x077F),
]
_SCRIPT_VOICE_LANGS: Dict[str, Tuple[str, ...]] = {
    "hi": ("hi", "mr", "ne"), "bn": ("bn",), "gu": ("gu",), "ta": ("ta",), "te": ("te",), "kn": ("kn",),
    "ml": ("ml",), "ur": ("ur",), "pa": ("hi", "mr", "ne"), "or": ("hi", "mr", "ne"),
}
_SCRIPT_FALLBACK_VOICES: Dict[str, Tuple[str, str]] = {
    "hi": ("hi-IN-MadhurNeural", "hi-IN-SwaraNeural"), "bn": ("bn-IN-BashkarNeural", "bn-IN-TanishaaNeural"),
    "gu": ("gu-IN-NiranjanNeural", "gu-IN-DhwaniNeural"), "ta": ("ta-IN-ValluvarNeural", "ta-IN-PallaviNeural"),
    "te": ("te-IN-MohanNeural", "te-IN-ShrutiNeural"), "kn": ("kn-IN-GaganNeural", "kn-IN-SapnaNeural"),
    "ml": ("ml-IN-MidhunNeural", "ml-IN-SobhanaNeural"), "ur": ("ur-IN-SalmanNeural", "ur-IN-GulNeural"),
    "pa": ("hi-IN-MadhurNeural", "hi-IN-SwaraNeural"), "or": ("hi-IN-MadhurNeural", "hi-IN-SwaraNeural"),
}
_SCRIPT_NAMES = {"hi": "Devanagari (Hindi)", "bn": "Bengali", "gu": "Gujarati", "ta": "Tamil", "te": "Telugu",
                 "kn": "Kannada", "ml": "Malayalam", "ur": "Urdu", "pa": "Punjabi (Gurmukhi)", "or": "Odia",
                 "en": "Latin (English)"}
_FEMALE_HINTS = ("swara", "neerja", "aria", "jenny", "sonia", "tanishaa", "pallavi", "shruti", "aarohi",
                 "dhwani", "gul", "sapna", "sobhana", "michelle", "ana", "libby", "maisie")
SCRIPT_SAMPLE_CHARS = 20_000
_WORD_CHAR_RE = re.compile(r"\w", re.UNICODE)


def detect_script(text: str, sample: int = SCRIPT_SAMPLE_CHARS) -> Optional[str]:
    """Dominant script language of ``text`` ("hi", "bn", "en", ...) or None if no letters."""
    counts: Dict[str, int] = {}
    for ch in text[:sample]:
        if not ch.isalpha():
            continue
        cp = ord(ch)
        lang = "en"
        if cp >= 0x0590:
            for code, lo, hi in _SCRIPT_BLOCKS:
                if lo <= cp <= hi:
                    lang = code
                    break
            else:
                lang = "other"
        counts[lang] = counts.get(lang, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def voice_language(voice: str) -> str:
    return voice.split("-", 1)[0].lower()


def voice_is_female(voice: str) -> bool:
    low = voice.lower()
    return any(h in low for h in _FEMALE_HINTS)


def voice_can_read(voice: str, script: Optional[str]) -> bool:
    if script is None or script in ("en", "other"):
        return True
    return voice_language(voice) in _SCRIPT_VOICE_LANGS.get(script, ())


def compatible_voice(voice: str, text: str) -> Tuple[str, Optional[str]]:
    """Return (voice_to_use, detected_script). Switches to a same-gender voice when needed."""
    script = detect_script(text)
    if voice_can_read(voice, script):
        return voice, script
    male, female = _SCRIPT_FALLBACK_VOICES.get(script or "hi", _SCRIPT_FALLBACK_VOICES["hi"])
    return (female if voice_is_female(voice) else male), script


def script_name(script: Optional[str]) -> str:
    return _SCRIPT_NAMES.get(script or "", script or "unknown")


def is_speakable(text: str) -> bool:
    return bool(_WORD_CHAR_RE.search(text))


# =============================================================================
# Logging
# =============================================================================
def _setup_logging() -> logging.Logger:
    level = getattr(logging, _env("LOG_LEVEL", "INFO").upper(), logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(level)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    try:
        fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError:
        pass
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    return logging.getLogger("audiobook")


log = _setup_logging()


# =============================================================================
# Database
# =============================================================================
class Database:
    """Small thread-safe SQLite wrapper."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init()

    def _init(self) -> None:
        with self._lock:
            c = self.conn
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT, first_name TEXT,
                    approved INTEGER DEFAULT 0, banned INTEGER DEFAULT 0,
                    expiry TEXT, joined TEXT,
                    voice TEXT, rate TEXT, pitch TEXT, volume TEXT,
                    hours INTEGER, split_mode TEXT,
                    total_jobs INTEGER DEFAULT 0, total_chars INTEGER DEFAULT 0,
                    total_seconds REAL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS servers (
                    url TEXT PRIMARY KEY, added TEXT, enabled INTEGER DEFAULT 1, note TEXT
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER, title TEXT, chars INTEGER, chunks INTEGER,
                    voice TEXT, status TEXT, created TEXT, finished TEXT,
                    duration REAL DEFAULT 0, parts INTEGER DEFAULT 0, error TEXT,
                    chat_id INTEGER, settings TEXT, text_path TEXT,
                    cursor INTEGER DEFAULT 0, delivered_parts INTEGER DEFAULT 0,
                    delivered_cursor INTEGER DEFAULT 0,
                    last_error TEXT, stall_since TEXT, updated TEXT
                );
                CREATE TABLE IF NOT EXISTS requests (
                    user_id INTEGER PRIMARY KEY, requested TEXT, note TEXT
                );
                CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
                """
            )
            self._migrate()

    def _migrate(self) -> None:
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(jobs)")}
        for col, typ in (("chat_id", "INTEGER"), ("settings", "TEXT"), ("text_path", "TEXT"),
                         ("cursor", "INTEGER DEFAULT 0"), ("delivered_parts", "INTEGER DEFAULT 0"),
                         ("delivered_cursor", "INTEGER DEFAULT 0"),
                         ("last_error", "TEXT"), ("stall_since", "TEXT"), ("updated", "TEXT")):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {typ}")

    # --- generic -----------------------------------------------------------
    def q(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(sql, params).fetchall())

    def one(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(sql, params).fetchone()

    def x(self, sql: str, params: Tuple = ()) -> int:
        with self._lock:
            cur = self.conn.execute(sql, params)
            return cur.lastrowid or cur.rowcount

    # --- users -------------------------------------------------------------
    def ensure_user(self, uid: int, username: Optional[str], first_name: Optional[str]) -> sqlite3.Row:
        row = self.one("SELECT * FROM users WHERE user_id=?", (uid,))
        if row is None:
            approved = 1 if uid == OWNER_ID else 0
            self.x(
                "INSERT INTO users(user_id, username, first_name, approved, joined, voice, rate, pitch, volume, hours, split_mode)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (uid, username or "", first_name or "", approved, now_str(), DEFAULT_VOICE, DEFAULT_RATE,
                 DEFAULT_PITCH, DEFAULT_VOLUME, DEFAULT_HOURS, DEFAULT_SPLIT),
            )
            row = self.one("SELECT * FROM users WHERE user_id=?", (uid,))
        elif (username or "") != (row["username"] or "") or (first_name or "") != (row["first_name"] or ""):
            self.x("UPDATE users SET username=?, first_name=? WHERE user_id=?", (username or "", first_name or "", uid))
            row = self.one("SELECT * FROM users WHERE user_id=?", (uid,))
        return row  # type: ignore[return-value]

    def user(self, uid: int) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM users WHERE user_id=?", (uid,))

    def is_approved(self, uid: int) -> bool:
        if uid == OWNER_ID:
            return True
        row = self.user(uid)
        if not row or row["banned"] or not row["approved"]:
            return False
        if row["expiry"] and parse_dt(row["expiry"]) < datetime.now():
            self.x("UPDATE users SET approved=0 WHERE user_id=?", (uid,))
            return False
        return True

    def approve(self, uid: int, days: int) -> None:
        expiry = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S") if days > 0 else None
        self.x("UPDATE users SET approved=1, banned=0, expiry=? WHERE user_id=?", (expiry, uid))
        self.x("DELETE FROM requests WHERE user_id=?", (uid,))

    def revoke(self, uid: int, ban: bool = False) -> None:
        self.x("UPDATE users SET approved=0, banned=? WHERE user_id=?", (1 if ban else 0, uid))

    def settings(self, uid: int) -> Dict[str, Any]:
        row = self.user(uid)
        if not row:
            return {"voice": DEFAULT_VOICE, "rate": DEFAULT_RATE, "pitch": DEFAULT_PITCH, "volume": DEFAULT_VOLUME,
                    "hours": DEFAULT_HOURS, "split_mode": DEFAULT_SPLIT}
        return {
            "voice": row["voice"] or DEFAULT_VOICE, "rate": row["rate"] or DEFAULT_RATE,
            "pitch": row["pitch"] or DEFAULT_PITCH, "volume": row["volume"] or DEFAULT_VOLUME,
            "hours": row["hours"] or DEFAULT_HOURS, "split_mode": row["split_mode"] or DEFAULT_SPLIT,
        }

    def set_setting(self, uid: int, key: str, value: Any) -> None:
        if key not in ("voice", "rate", "pitch", "volume", "hours", "split_mode"):
            raise ValueError(key)
        self.x(f"UPDATE users SET {key}=? WHERE user_id=?", (value, uid))

    def add_usage(self, uid: int, chars: int, seconds: float) -> None:
        self.x("UPDATE users SET total_jobs=total_jobs+1, total_chars=total_chars+?, total_seconds=total_seconds+?"
               " WHERE user_id=?", (chars, seconds, uid))

    # --- servers -----------------------------------------------------------
    def servers(self, enabled_only: bool = True) -> List[str]:
        sql = "SELECT url FROM servers" + (" WHERE enabled=1" if enabled_only else "") + " ORDER BY added"
        return [r["url"] for r in self.q(sql)]

    def add_server(self, url: str, note: str = "") -> bool:
        if self.one("SELECT 1 FROM servers WHERE url=?", (url,)):
            return False
        self.x("INSERT INTO servers(url, added, enabled, note) VALUES (?,?,1,?)", (url, now_str(), note))
        return True

    def remove_server(self, url: str) -> None:
        self.x("DELETE FROM servers WHERE url=?", (url,))

    # --- jobs --------------------------------------------------------------
    def create_job(self, uid: int, chat_id: int, title: str, chars: int, voice: str, settings: Dict[str, Any],
                   text_path: str) -> int:
        return self.x(
            "INSERT INTO jobs(user_id, chat_id, title, chars, chunks, voice, status, created, settings, text_path, updated)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (uid, chat_id, title, chars, 0, voice, "queued", now_str(), json.dumps(settings), text_path, now_str()),
        )

    def update_job(self, job_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated"] = now_str()
        cols = ", ".join(f"{k}=?" for k in fields)
        self.x(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))

    def job(self, job_id: int) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM jobs WHERE id=?", (job_id,))

    def unfinished_jobs(self) -> List[sqlite3.Row]:
        return self.q("SELECT * FROM jobs WHERE status IN ('queued','running','paused','interrupted') ORDER BY id")

    def user_history(self, uid: int, limit: int = 10) -> List[sqlite3.Row]:
        return self.q("SELECT * FROM jobs WHERE user_id=? ORDER BY id DESC LIMIT ?", (uid, limit))


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
    except ValueError:
        return datetime.min


def normalize_server_url(url: str) -> str:
    url = (url or "").strip().rstrip("/")
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    parts = urlsplit(url)
    if not parts.netloc or parts.scheme not in ("http", "https"):
        return ""
    path = parts.path.rstrip("/")
    for suffix in ("/tts", "/health"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
    return urlunsplit((parts.scheme, parts.netloc, path, "", "")).rstrip("/")


def short_server(url: str) -> str:
    host = urlsplit(url).netloc or url
    host = host.split(":")[0]
    if host.endswith(".workers.dev"):
        host = host[: -len(".workers.dev")]
    return host[:40]


def fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def fmt_size(nbytes: int) -> str:
    n = float(nbytes)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} GB"


def progress_bar(percent: int, width: int = 12) -> str:
    percent = max(0, min(100, percent))
    filled = int(round(width * percent / 100))
    return "█" * filled + "░" * (width - filled)


def voice_label(voice: str) -> str:
    return VOICE_LABELS.get(voice, voice)


def safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", name).strip(" .")
    return (name or "audiobook")[:80]


def estimate_seconds(chars: int, rate: str) -> float:
    """Rough spoken duration: ~15 chars/s for Hindi/English at normal rate."""
    m = re.match(r"^([+-]?\d+)%$", rate or "+0%")
    factor = 1 + (int(m.group(1)) / 100.0 if m else 0)
    factor = max(0.4, factor)
    return chars / (15.0 * factor)


def free_disk_mb(path: str) -> float:
    try:
        st = os.statvfs(path)
        return st.f_bavail * st.f_frsize / (1024 * 1024)
    except OSError:
        return float("inf")


async def safe_edit(msg: Optional[Message], text: str, reply_markup=None) -> None:
    if msg is None:
        return
    try:
        await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(min(60, int(getattr(e, "value", 5)) + 1))
    except RPCError as e:
        log.debug("edit failed: %s", e)
    except Exception as e:  # noqa: BLE001
        log.debug("edit failed: %s", e)


async def safe_send(client: Client, chat_id: int, text: str, **kwargs) -> Optional[Message]:
    for attempt in range(3):
        try:
            return await client.send_message(chat_id, text, disable_web_page_preview=True, **kwargs)
        except FloodWait as e:
            await asyncio.sleep(min(60, int(getattr(e, "value", 5)) + 1))
        except RPCError as e:
            log.warning("send_message to %s failed: %s", chat_id, e)
            return None
        except Exception as e:  # noqa: BLE001
            log.warning("send_message to %s failed: %s", chat_id, e)
            if attempt == 2:
                return None
            await asyncio.sleep(1)
    return None


async def cancellable_sleep(seconds: float, cancel: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(cancel.wait(), timeout=max(0.0, seconds))
    except asyncio.TimeoutError:
        pass


class JobCancelled(Exception):
    pass


# =============================================================================
# Text extraction
# =============================================================================
def _read_text_file(path: str) -> str:
    with open(path, "rb") as f:
        raw = f.read()
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
    for br in soup.find_all("br"):
        br.replace_with("\n")
    return soup.get_text(separator="\n")


def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_text_to_file(path: str, ext: str, out_path: str) -> int:
    """Stream-extract a document into ``out_path`` (UTF-8); return char count.  Runs in a thread."""
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
                raise RuntimeError("ebooklib is not installed on the bot server (pip install ebooklib)")
            try:
                book = epub.read_epub(path, options={"ignore_ncx": True})
            except TypeError:
                book = epub.read_epub(path)
            spine_ids = [item[0] for item in getattr(book, "spine", [])]
            items = {it.get_id(): it for it in book.get_items() if it.get_type() == ebooklib.ITEM_DOCUMENT}
            ordered = [items[i] for i in spine_ids if i in items] + [it for k, it in items.items() if k not in spine_ids]
            for it in ordered:
                emit(_html_to_text(it.get_body_content()))
            return total
        if ext == ".pdf":
            if PdfReader is None:
                raise RuntimeError("pypdf is not installed on the bot server (pip install pypdf)")
            reader = PdfReader(path)
            for page in reader.pages:
                emit(page.extract_text() or "")
            return total
        if ext in (".txt", ".md"):
            emit(_read_text_file(path))
        elif ext == ".docx":
            if docx is None:
                raise RuntimeError("python-docx is not installed on the bot server (pip install python-docx)")
            d = docx.Document(path)
            parts = [p.text for p in d.paragraphs]
            for table in d.tables:
                for row in table.rows:
                    parts.append(" | ".join(c.text for c in row.cells))
            emit("\n".join(parts))
        elif ext in (".html", ".htm"):
            emit(_html_to_text(_read_text_file(path)))
        else:
            raise RuntimeError(f"Unsupported file type: {ext}")
    return total


# =============================================================================
# Chunk planner
# =============================================================================
_SENTENCE_END = re.compile(r"(?<=[.!?।؟。])\s+")


def _sentences(text: str, max_len: int) -> List[str]:
    """Sentences of ``text`` none longer than ``max_len`` (long ones split on spaces)."""
    out: List[str] = []
    for para in text.split("\n\n"):
        para = para.strip()
        if not para:
            continue
        for sentence in _SENTENCE_END.split(para):
            sentence = sentence.strip()
            if not sentence:
                continue
            while len(sentence) > max_len:
                cut = sentence.rfind(" ", 0, max_len)
                cut = cut if cut > max_len // 3 else max_len
                out.append(sentence[:cut].strip())
                sentence = sentence[cut:].strip()
            if sentence:
                out.append(sentence)
        out.append("\n")  # paragraph marker
    return out


def even_chunks(text: str, target: int, max_len: int, min_ratio: float = CHUNK_MIN_RATIO) -> List[str]:
    """Pack sentences into chunks of ~``target`` chars (never > ``max_len``).

    Only the final chunk may be shorter than ``target * min_ratio``; a trailing
    runt is merged into the previous chunk or the last two are re-balanced.
    """
    target = max(100, min(target, max_len))
    chunks: List[str] = []
    buf = ""
    for sent in _sentences(text, max_len):
        if sent == "\n":
            if buf and not buf.endswith("\n"):
                buf += "\n"
            continue
        sep = "" if (not buf or buf.endswith("\n")) else " "
        if buf and len(buf) + len(sep) + len(sent) > target:
            chunks.append(buf.strip())
            buf = sent
        else:
            buf = f"{buf}{sep}{sent}"
    if buf.strip():
        chunks.append(buf.strip())
    if len(chunks) >= 2 and len(chunks[-1]) < target * min_ratio:
        last, prev = chunks[-1], chunks[-2]
        if len(prev) + 1 + len(last) <= max_len:
            chunks[-2:] = [f"{prev}\n{last}"]
        else:
            merged = f"{prev}\n{last}"
            half = len(merged) // 2
            cut = merged.rfind(" ", 0, half)
            cut = cut if cut > half // 2 else half
            chunks[-2:] = [merged[:cut].strip(), merged[cut:].strip()]
    return [c for c in chunks if c.strip()]


def chunk_target(chunk_limit: int) -> int:
    if TARGET_CHUNK_CHARS:
        return min(TARGET_CHUNK_CHARS, chunk_limit)
    return max(300, int(chunk_limit * 0.9))


def split_chapters(text: str) -> List[Tuple[str, str]]:
    """[(title, body)] using chapter headings; single 'Full text' entry when none found."""
    positions = [m.start() for m in CHAPTER_RE.finditer(text)]
    if len(positions) < 2:
        return [("", text)]
    out: List[Tuple[str, str]] = []
    if positions[0] > 0:
        head = text[: positions[0]].strip()
        if head:
            out.append(("Introduction", head))
    for i, pos in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        seg = text[pos:end].strip()
        if not seg:
            continue
        title = seg.split("\n", 1)[0].strip()[:80]
        out.append((title, seg))
    return out or [("", text)]


def iter_text_slices(text: str, slice_chars: int = PLAN_SLICE_CHARS):
    """Yield ``text`` in slices that end on a paragraph boundary (memory-friendly planner)."""
    pos = 0
    n = len(text)
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


def build_plan(text: str, chunk_limit: int, chapter_mode: bool) -> List[Tuple[str, str]]:
    """Return [(chapter_title, chunk_text)] - separator-only chunks are dropped."""
    target = chunk_target(chunk_limit)
    plan: List[Tuple[str, str]] = []
    sections = split_chapters(text) if chapter_mode else [("", text)]
    for title, body in sections:
        for sl in iter_text_slices(body):
            for ch in even_chunks(sl, target, chunk_limit):
                if is_speakable(ch):
                    plan.append((title, ch))
    return plan


def write_plan(plan_path: str, plan: List[Tuple[str, str]]) -> None:
    with open(plan_path, "w", encoding="utf-8") as fh:
        for title, chunk in plan:
            fh.write(json.dumps({"t": title, "c": chunk}, ensure_ascii=False) + "\n")


def read_plan(plan_path: str) -> List[Tuple[str, str]]:
    plan: List[Tuple[str, str]] = []
    with open(plan_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                d = json.loads(line)
                plan.append((d.get("t", ""), d["c"]))
    return plan


# =============================================================================
# Cloudflare Worker pool: health, adaptive limiter, quarantine, scheduling
# =============================================================================
class ThrottleError(RuntimeError):
    """The Worker reported that Microsoft is throttling it (503/403/NoAudioReceived)."""


class BusyError(RuntimeError):
    """The Worker refused because all its slots are busy - try another one."""


class VoiceError(RuntimeError):
    """HTTP 400: the voice cannot read this text (or another permanent input problem)."""


class FatalServerError(RuntimeError):
    """401/404 - misconfiguration; retrying will not help."""


def _headers() -> Dict[str, str]:
    h = {"User-Agent": f"AudioBookPro/{VERSION}"}
    if TTS_API_KEY:
        h["X-API-Key"] = TTS_API_KEY
    return h


async def _describe_http_error(resp: aiohttp.ClientResponse) -> Tuple[str, str]:
    """(kind, message) for a non-200 Worker reply. kind in throttle/busy/voice/fatal/retry."""
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
    detail = re.sub(r"\s+", " ", detail)[:160]
    low = detail.lower()
    st = resp.status
    if st == 401:
        hint = ("API key rejected - TTS_API_KEY on the bot must equal the Worker's API_KEY secret"
                if TTS_API_KEY else "Worker requires an API key - set TTS_API_KEY on the bot")
        return "fatal", f"HTTP 401: {hint}"
    if st == 404:
        return "fatal", "HTTP 404: /tts not found (is this really the edge-tts Worker URL?)"
    if st == 400:
        return "voice", f"HTTP 400: {detail or 'bad request'}"
    if st == 413:
        return "voice", f"HTTP 413: {detail or 'chunk too large'}"
    if st == 429:
        return "busy", f"HTTP 429: rate limited{(' - ' + detail) if detail else ''}"
    if st in (502, 503, 504):
        if "slots in use" in low or low.startswith("server busy"):
            return "busy", f"HTTP {st}: server busy - trying another server"
        if ("noaudio" in low.replace(" ", "") or "no audio" in low or "403" in low or "throttl" in low
                or "too many" in low or "handshake" in low or st == 503):
            return "throttle", f"HTTP {st}: Microsoft is throttling this Worker{(' - ' + detail) if detail else ''}"
        return "retry", f"HTTP {st}: upstream error{(' - ' + detail) if detail else ''}"
    return "retry", f"HTTP {st}{(': ' + detail) if detail else ''}"


PROBE_TEXT = "यह एक परीक्षण वाक्य है। इसका उपयोग यह जाँचने के लिए किया जाता है कि सर्वर सही ढंग से ऑडियो बना रहा है या नहीं।"


async def probe_tts(session: aiohttp.ClientSession, url: str) -> Tuple[bool, str]:
    """Authenticated end-to-end /tts probe."""
    payload = {"text": PROBE_TEXT, "voice": DEFAULT_VOICE, "rate": "+0%", "pitch": "+0Hz", "volume": "+0%"}
    try:
        async with session.post(f"{url}/tts", json=payload, headers=_headers(), allow_redirects=False,
                                timeout=aiohttp.ClientTimeout(total=90)) as r:
            if r.status != 200:
                _, msg = await _describe_http_error(r)
                return False, msg
            data = await r.content.read(4096)
            mp3 = data.startswith(b"ID3") or (len(data) >= 2 and data[0] == 255 and data[1] & 224 == 224)
            if not mp3:
                return False, "HTTP 200 but response is not MP3 audio (proxy / login page?)"
            return True, ""
    except asyncio.TimeoutError:
        return False, "timed out waiting for /tts"
    except aiohttp.ClientError as exc:
        return False, f"connection error: {type(exc).__name__}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {str(exc)[:80]}"


async def check_server(session: aiohttp.ClientSession, url: str, deep: bool = False) -> Dict[str, Any]:
    """Probe /health of a Worker; with ``deep`` also run a real /tts synthesis."""
    info: Dict[str, Any] = {"url": url, "ok": False, "latency": None, "version": None, "active": None,
                            "error": "", "auth_required": None, "throttled": False}
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
                except (ValueError, TypeError):
                    info["error"] = "/health returned invalid JSON"
                    return info
                if not isinstance(j, dict) or j.get("status") != "ok":
                    info["error"] = "/health did not return status=ok"
                    return info
                info["version"] = j.get("version")
                info["active"] = j.get("active_jobs")
                info["auth_required"] = j.get("auth_required")
                info["throttled"] = bool(j.get("throttled"))
                info["runtime"] = j.get("runtime")
                if isinstance(j.get("max_text_length"), int) and j["max_text_length"] > 0:
                    info["max_text_length"] = j["max_text_length"]
                if isinstance(j.get("max_concurrency"), int) and j["max_concurrency"] > 0:
                    info["max_concurrency"] = j["max_concurrency"]
                info["ok"] = True
            else:
                _, info["error"] = await _describe_http_error(r)
    except asyncio.TimeoutError:
        info["error"] = "unreachable (timeout)"
    except Exception as exc:  # noqa: BLE001
        info["error"] = f"unreachable ({type(exc).__name__})"
    if info["ok"] and info.get("auth_required") and not TTS_API_KEY:
        info["ok"] = False
        info["error"] = "Worker requires an API key but TTS_API_KEY is not set on the bot"
        return info
    if info["ok"] and deep:
        ok, reason = await probe_tts(session, url)
        if not ok:
            info["ok"] = False
            info["error"] = reason
    return info


class AdaptiveLimiter:
    """Per-server concurrency limiter: grows while healthy, halves on throttling."""

    def __init__(self, start: int, ceiling: int, grow_after: int, min_gap: float):
        self.limit = max(1, min(start, ceiling))
        self.ceiling = max(1, ceiling)
        self.grow_after = grow_after
        self.min_gap = min_gap
        self.active = 0
        self.clean = 0
        self.throttles = 0
        self._last_start = 0.0
        self._cond = asyncio.Condition()

    def set_ceiling(self, ceiling: int) -> None:
        self.ceiling = max(1, ceiling)
        self.limit = max(1, min(self.limit, self.ceiling))

    @property
    def free(self) -> int:
        return max(0, self.limit - self.active)

    async def acquire(self) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: self.active < self.limit)
            self.active += 1
        gap = self.min_gap - (time.monotonic() - self._last_start)
        if gap > 0:
            await asyncio.sleep(gap)
        self._last_start = time.monotonic()

    async def release(self) -> None:
        async with self._cond:
            self.active = max(0, self.active - 1)
            self._cond.notify_all()

    async def success(self) -> None:
        async with self._cond:
            self.clean += 1
            if self.clean >= self.grow_after and self.limit < self.ceiling:
                self.limit += 1
                self.clean = 0
                self._cond.notify_all()

    async def busy(self) -> None:
        """One request too many - trim by one, do not count as throttling."""
        async with self._cond:
            self.clean = 0
            if self.limit > 1:
                self.limit -= 1

    async def throttled(self) -> None:
        async with self._cond:
            self.throttles += 1
            self.clean = 0
            self.limit = max(1, self.limit // 2)


class ServerState:
    def __init__(self, url: str):
        self.url = url
        self.limiter = AdaptiveLimiter(PER_SERVER_CONCURRENCY, PER_SERVER_MAX_CONCURRENCY, REMOTE_GROW_AFTER,
                                       REMOTE_MIN_GAP)
        self.max_text_length = REMOTE_CHUNK_SIZE
        self.latency = 1.0
        self.ok = True
        self.error = ""
        self.version: Optional[str] = None
        # quarantine
        self.q_until = 0.0
        self.q_set_at = 0.0
        self.q_level = 0
        self.q_count = 0
        self.q_reason = ""
        self.clean_since_q = 0
        self.chunks_done = 0
        self.chunks_failed = 0

    @property
    def benched(self) -> bool:
        return time.monotonic() < self.q_until

    @property
    def bench_left(self) -> float:
        return max(0.0, self.q_until - time.monotonic())

    def quarantine(self, reason: str) -> float:
        now = time.monotonic()
        if self.benched and now - self.q_set_at < 10:
            # Requests that were already in flight when the server got benched all
            # fail together - that is one incident, not several escalations.
            return self.bench_left
        secs = QUARANTINE_STEPS[min(self.q_level, len(QUARANTINE_STEPS) - 1)]
        self.q_until = now + secs
        self.q_set_at = now
        self.q_level = min(self.q_level + 1, len(QUARANTINE_STEPS))
        self.q_count += 1
        self.q_reason = reason[:120]
        self.clean_since_q = 0
        self.limiter.limit = 1
        self.limiter.clean = 0
        return secs

    def record_success(self) -> None:
        self.chunks_done += 1
        self.clean_since_q += 1
        if self.q_level and self.clean_since_q >= QUARANTINE_FORGIVE_AFTER:
            self.q_level -= 1
            self.clean_since_q = 0

    def apply_health(self, info: Dict[str, Any]) -> None:
        self.ok = bool(info.get("ok"))
        self.error = info.get("error", "")
        self.version = info.get("version")
        if info.get("latency") is not None:
            self.latency = float(info["latency"])
        if info.get("max_text_length"):
            self.max_text_length = max(500, min(int(info["max_text_length"]), 6000))
        if info.get("max_concurrency"):
            self.limiter.set_ceiling(min(PER_SERVER_MAX_CONCURRENCY, int(info["max_concurrency"])))

    def describe(self) -> str:
        name = short_server(self.url)
        if self.benched:
            return f"⛔ {name} benched {fmt_duration(self.bench_left)} (throttle #{self.q_count})"
        if not self.ok:
            return f"❌ {name} {self.error[:50]}"
        return f"✅ {name} {self.limiter.active}/{self.limiter.limit}"


class ServerPool:
    """All registered Workers with their live state; least-loaded scheduling."""

    def __init__(self):
        self.servers: Dict[str, ServerState] = {}
        self._lock = asyncio.Lock()

    def sync_from_db(self) -> None:
        urls = db.servers()
        for u in urls:
            if u not in self.servers:
                self.servers[u] = ServerState(u)
        for u in list(self.servers):
            if u not in urls:
                del self.servers[u]

    def states(self) -> List[ServerState]:
        return list(self.servers.values())

    def usable(self) -> List[ServerState]:
        return [s for s in self.servers.values() if s.ok and not s.benched]

    def chunk_limit(self) -> int:
        sts = [s for s in self.servers.values() if s.ok]
        if not sts:
            return REMOTE_CHUNK_SIZE
        return max(500, min(REMOTE_CHUNK_SIZE, min(s.max_text_length for s in sts)))

    def pick(self, exclude: set) -> Optional[ServerState]:
        cands = [s for s in self.usable() if s.url not in exclude]
        if not cands:
            return None
        cands.sort(key=lambda s: (-s.limiter.free, s.latency, random.random()))
        return cands[0]

    def any_free(self, exclude: set) -> bool:
        return any(s.limiter.free > 0 for s in self.usable() if s.url not in exclude)

    def next_bench_end(self) -> float:
        benched = [s.bench_left for s in self.servers.values() if s.benched]
        return min(benched) if benched else 0.0

    async def refresh(self, session: aiohttp.ClientSession, deep: bool = False) -> List[Dict[str, Any]]:
        self.sync_from_db()
        sts = self.states()
        if not sts:
            return []
        results = await asyncio.gather(*(check_server(session, s.url, deep=deep) for s in sts),
                                       return_exceptions=True)
        out: List[Dict[str, Any]] = []
        for s, info in zip(sts, results):
            if isinstance(info, Exception):
                info = {"url": s.url, "ok": False, "error": f"{type(info).__name__}"}
            s.apply_health(info)
            if info.get("ok") and info.get("throttled") and not s.benched:
                s.quarantine("Worker reports Microsoft throttling")
            out.append(info)
        return out

    def summary(self) -> str:
        sts = self.states()
        if not sts:
            return "no servers"
        return " · ".join(s.describe() for s in sts)


_network_slots = asyncio.Semaphore(MAX_TOTAL_CONCURRENCY)


async def _tts_request(session: aiohttp.ClientSession, srv: ServerState, text: str,
                       settings: Dict[str, Any]) -> Tuple[bytes, int]:
    payload = {"text": text, "voice": settings["voice"], "rate": settings["rate"],
               "pitch": settings["pitch"], "volume": settings["volume"]}
    t0 = time.monotonic()
    async with session.post(f"{srv.url}/tts", json=payload, headers=_headers(), allow_redirects=False,
                            timeout=aiohttp.ClientTimeout(total=CHUNK_TIMEOUT)) as r:
        if r.status != 200:
            kind, msg = await _describe_http_error(r)
            if kind == "throttle":
                raise ThrottleError(msg)
            if kind == "busy":
                raise BusyError(msg)
            if kind == "voice":
                raise VoiceError(msg)
            if kind == "fatal":
                raise FatalServerError(msg)
            raise RuntimeError(msg)
        data = await r.read()
        if len(data) < 200:
            raise ThrottleError("Worker returned an empty audio stream (Microsoft throttling)")
        try:
            dur = int(r.headers.get("X-Duration-Ms") or 0)
        except ValueError:
            dur = 0
        if dur <= 0:
            dur = int(len(data) * 1000 / MP3_BYTES_PER_SEC)
    srv.latency = 0.8 * srv.latency + 0.2 * (time.monotonic() - t0)
    return data, dur


async def fetch_chunk(session: aiohttp.ClientSession, pool: ServerPool, chunk: str, index: int,
                      settings: Dict[str, Any], cancel: asyncio.Event) -> Tuple[bytes, int, str]:
    """Synthesise one chunk on the best available Worker; fail over across servers.

    Returns (mp3_bytes, duration_ms, server_url). Raises VoiceError for a
    permanent input problem and RuntimeError when every engine failed this round.
    """
    tried: set = set()
    last_err = "no servers available"
    attempts = max(2, len(pool.servers) + 1)
    second_pass = False
    for attempt in range(attempts):
        if cancel.is_set():
            raise JobCancelled()
        srv = pool.pick(tried)
        if srv is None and tried and not second_pass:
            second_pass = True  # every healthy server tried once; allow one more pass
            tried = set()
            srv = pool.pick(tried)
        if srv is None:
            break
        text = chunk
        if len(text) > srv.max_text_length:
            text = text[: srv.max_text_length]  # planner already respects limits; safety only
        async with _network_slots:
            await srv.limiter.acquire()
            try:
                data, dur = await _tts_request(session, srv, text, settings)
                await srv.limiter.success()
                srv.record_success()
                return data, dur, srv.url
            except BusyError as e:
                last_err = f"{short_server(srv.url)} -> {e}"
                await srv.limiter.busy()
                tried.add(srv.url)
                await cancellable_sleep(1.0, cancel)
            except ThrottleError as e:
                last_err = f"{short_server(srv.url)} -> {e}"
                await srv.limiter.throttled()
                secs = srv.quarantine(str(e))
                srv.chunks_failed += 1
                log.warning("chunk %d: %s -> benched %ds", index + 1, last_err, secs)
                tried.add(srv.url)
            except VoiceError:
                raise
            except FatalServerError as e:
                last_err = f"{short_server(srv.url)} -> {e}"
                srv.ok = False
                srv.error = str(e)
                srv.chunks_failed += 1
                log.error("chunk %d: %s (server disabled for this session)", index + 1, last_err)
                tried.add(srv.url)
            except asyncio.TimeoutError:
                last_err = f"{short_server(srv.url)} -> timed out after {CHUNK_TIMEOUT}s"
                srv.chunks_failed += 1
                await srv.limiter.busy()
                tried.add(srv.url)
            except (aiohttp.ClientError, OSError) as e:
                last_err = f"{short_server(srv.url)} -> {type(e).__name__}: {str(e)[:60]}"
                srv.chunks_failed += 1
                tried.add(srv.url)
                await cancellable_sleep(0.5 + random.random(), cancel)
            except RuntimeError as e:
                last_err = f"{short_server(srv.url)} -> {e}"
                srv.chunks_failed += 1
                tried.add(srv.url)
                await cancellable_sleep(0.5 + random.random(), cancel)
            finally:
                await srv.limiter.release()
    raise RuntimeError(last_err)


# =============================================================================
# ffmpeg remux (optional)
# =============================================================================
async def remux_mp3(path: str) -> str:
    """Re-mux a concatenated MP3 with ffmpeg so players show the correct duration."""
    if not FFMPEG:
        return path
    out = path[:-4] + "_fixed.mp3"
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            FFMPEG, "-y", "-loglevel", "error", "-i", path, "-c:a", "copy", "-map_metadata", "-1", out,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.wait_for(proc.wait(), timeout=600)
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
    except Exception as e:  # noqa: BLE001
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
pool = ServerPool()
http_session: Optional[aiohttp.ClientSession] = None

admin_states: Dict[int, str] = {}                      # admin_id -> pending input state
pending_text: Dict[int, Tuple[str, str, float]] = {}   # uid -> (token, text, expiry)
pending_servers: Dict[str, Tuple[List[str], float]] = {}  # token -> (urls that failed the probe, expiry)
preview_tasks: Dict[int, asyncio.Task] = {}


@dataclass
class Job:
    user_id: int
    title: str = ""
    chars: int = 0
    job_id: int = 0
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    task: Optional[asyncio.Task] = None
    started: float = field(default_factory=time.time)
    queued_at: float = field(default_factory=time.time)
    done_chunks: int = 0
    total_chunks: int = 0
    done_chars: int = 0
    running: bool = False
    status_line: str = ""


class JobQueue:
    """Ordered queue with visible positions; MAX_PARALLEL_JOBS run at once."""

    def __init__(self, parallel: int):
        self.parallel = parallel
        self.waiting: List[Job] = []
        self.running: List[Job] = []
        self._cond = asyncio.Condition()

    def by_user(self, uid: int) -> Optional[Job]:
        for j in self.running + self.waiting:
            if j.user_id == uid:
                return j
        return None

    def position(self, job: Job) -> int:
        try:
            return self.waiting.index(job) + 1
        except ValueError:
            return 0

    def eta_seconds(self, job: Job) -> float:
        """Rough start ETA for a waiting job based on characters ahead of it."""
        ahead = 0
        for j in self.running:
            ahead += max(0, j.chars - j.done_chars)
        for j in self.waiting:
            if j is job:
                break
            ahead += j.chars
        speed = 400.0 * max(1, len(pool.usable()))  # chars/s, conservative
        return ahead / speed

    async def enter(self, job: Job) -> None:
        async with self._cond:
            self.waiting.append(job)
            await self._cond.wait_for(lambda: job.cancel.is_set() or
                                      (len(self.running) < self.parallel and self.waiting and self.waiting[0] is job))
            if job in self.waiting:
                self.waiting.remove(job)
            if job.cancel.is_set():
                self._cond.notify_all()
                raise JobCancelled()
            self.running.append(job)
            job.running = True
            job.started = time.time()

    async def leave(self, job: Job) -> None:
        async with self._cond:
            if job in self.running:
                self.running.remove(job)
            if job in self.waiting:
                self.waiting.remove(job)
            job.running = False
            self._cond.notify_all()

    async def kick(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    def counts(self) -> Tuple[int, int]:
        return len(self.running), len(self.waiting)


job_queue = JobQueue(MAX_PARALLEL_JOBS)
active_jobs: Dict[int, Job] = {}  # user_id -> Job


def reserve_job(uid: int, title: str = "", chars: int = 0) -> Optional[Job]:
    if uid in active_jobs:
        return None
    if len(job_queue.waiting) >= MAX_QUEUED_JOBS:
        return None
    job = Job(user_id=uid, title=title, chars=chars)
    active_jobs[uid] = job
    return job


def release_job(job: Job) -> None:
    if active_jobs.get(job.user_id) is job:
        del active_jobs[job.user_id]


# =============================================================================
# Job checkpoints on disk
# =============================================================================
def job_dir(job_id: int) -> str:
    d = os.path.join(JOBS_DIR, str(job_id))
    os.makedirs(os.path.join(d, "chunks"), exist_ok=True)
    return d


def chunk_path(jdir: str, index: int) -> str:
    return os.path.join(jdir, "chunks", f"{index:06d}.mp3")


def _write_chunk(jdir: str, index: int, data: bytes, dur: int) -> None:
    p = chunk_path(jdir, index)
    tmp = p + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, p)
    with open(p + ".meta", "w") as fh:
        fh.write(str(dur))


def _read_meta(jdir: str, index: int) -> int:
    try:
        with open(chunk_path(jdir, index) + ".meta") as fh:
            return int(fh.read().strip() or 0)
    except (OSError, ValueError):
        try:
            return int(os.path.getsize(chunk_path(jdir, index)) * 1000 / MP3_BYTES_PER_SEC)
        except OSError:
            return 0


def cleanup_job_dir(job_id: int) -> None:
    shutil.rmtree(os.path.join(JOBS_DIR, str(job_id)), ignore_errors=True)


# =============================================================================
# Job runner
# =============================================================================
def cancel_markup(job_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Cancel", callback_data=f"canceljob:{job_id}")]])


async def wait_for_engines(session: aiohttp.ClientSession, cancel: asyncio.Event, status: Optional[Message],
                           job_id: int, header: str, stall_round: int, reason: str) -> None:
    """Every engine failed: pause with escalating back-off, re-probe, continue."""
    delay = STALL_BACKOFF_STEPS[min(stall_round, len(STALL_BACKOFF_STEPS) - 1)]
    bench = pool.next_bench_end()
    if bench and not pool.usable():
        delay = max(5, min(delay, int(bench) + 1))
    all_benched = bool(pool.states()) and all(s.benched or not s.ok for s in pool.states())
    why = ("Microsoft is throttling every TTS Worker" if all_benched and any(s.benched for s in pool.states())
           else "All TTS Workers are failing")
    await safe_edit(status,
                    f"{header}\n\n⏸ <b>Paused</b> - {why}.\n"
                    f"Last error: <code>{html.escape(reason[:160])}</code>\n"
                    f"Engines: {html.escape(pool.summary())}\n"
                    f"Retrying in {delay}s (round {stall_round + 1}). The job does not give up - /cancel to stop.",
                    cancel_markup(job_id))
    await cancellable_sleep(delay, cancel)
    if cancel.is_set():
        raise JobCancelled()
    await pool.refresh(session)


async def run_audiobook_job(client: Client, status: Optional[Message], uid: int, job: Job,
                            text_path: str, title: str, settings: Dict[str, Any],
                            job_id: Optional[int] = None, chat_id: Optional[int] = None) -> None:
    """Full pipeline: plan -> parallel synthesis (checkpointed) -> parts -> upload."""
    session = http_session
    assert session is not None
    chat_id = chat_id or uid
    cancel = job.cancel
    settings = dict(settings)
    voice_note = ""
    parts_dir: Optional[str] = None
    stall_round = 0
    stall_since: Optional[float] = None
    uploader_task: Optional[asyncio.Task] = None
    in_flight: Dict[int, asyncio.Task] = {}

    try:
        # ---------------- queue -------------------------------------------
        pos = job_queue.position(job)
        if job_queue.running and len(job_queue.running) >= job_queue.parallel:
            await safe_edit(status,
                            f"🕒 <b>Queued</b> - position #{len(job_queue.waiting) + 1}\n"
                            f"Running {len(job_queue.running)}/{job_queue.parallel}. /queue for details.",
                            cancel_markup(job_id or 0))
        enter = asyncio.ensure_future(job_queue.enter(job))
        while not enter.done():
            await asyncio.wait({enter}, timeout=15)
            if not enter.done():
                pos = job_queue.position(job)
                eta = job_queue.eta_seconds(job)
                await safe_edit(status,
                                f"🕒 <b>Queued</b> - position #{pos} of {len(job_queue.waiting)}\n"
                                f"Running {len(job_queue.running)}/{job_queue.parallel} · estimated start ~{fmt_duration(eta)}",
                                cancel_markup(job_id or 0))
        enter.result()  # raises JobCancelled

        # ---------------- prepare -----------------------------------------
        if MIN_FREE_DISK_MB and free_disk_mb(JOBS_DIR if os.path.isdir(JOBS_DIR) else BASE_DIR) < MIN_FREE_DISK_MB:
            raise RuntimeError(f"Not enough free disk space on the bot server (< {MIN_FREE_DISK_MB} MB)")
        await safe_edit(status, "🔍 Checking TTS Workers...", cancel_markup(job_id or 0))
        await pool.refresh(session)
        if not pool.states():
            raise RuntimeError("No TTS Worker registered. Admin: /admin -> ➕ Add Server (or set TTS_SERVERS).")

        with open(text_path, "r", encoding="utf-8") as fh:
            text = fh.read()
        orig_voice = settings.get("voice_requested") or settings.get("voice", DEFAULT_VOICE)
        settings["voice_requested"] = orig_voice
        settings["voice"], script = compatible_voice(orig_voice, text)
        if orig_voice != settings["voice"]:
            voice_note = (f"ℹ️ {voice_label(orig_voice)} cannot read {script_name(script)} text - "
                          f"switched to {voice_label(settings['voice'])}.")

        if job_id is None:
            job_id = db.create_job(uid, chat_id, title, len(text), settings["voice"], settings, text_path)
        job.job_id = job_id
        jdir = job_dir(job_id)
        plan_path = os.path.join(jdir, "plan.jsonl")
        if os.path.exists(plan_path):
            plan = read_plan(plan_path)
        else:
            chunk_limit = pool.chunk_limit()
            plan = await asyncio.to_thread(build_plan, text, chunk_limit, settings.get("split_mode") == "chapter")
            if not plan:
                raise RuntimeError("Nothing to read - the document has no pronounceable text.")
            write_plan(plan_path, plan)
        del text
        total = len(plan)
        total_chars = sum(len(c) for _, c in plan)
        job.total_chunks = total
        job.chars = total_chars
        db.update_job(job_id, status="running", chunks=total, chars=total_chars, voice=settings["voice"],
                      settings=json.dumps(settings), stall_since=None)

        # ---------------- synthesis pipeline ------------------------------
        header = f"🎧 <b>{html.escape(title[:60])}</b>\n🗣 {html.escape(voice_label(settings['voice']))}"
        if voice_note:
            header += f"\n{voice_note}"
        row = db.job(job_id)
        delivered_parts = int(row["delivered_parts"] or 0) if row else 0
        delivered_cursor = int(row["delivered_cursor"] or 0) if row else 0
        parts_dir = os.path.join(jdir, "parts")
        os.makedirs(parts_dir, exist_ok=True)
        max_part_sec = max(1, int(settings.get("hours") or DEFAULT_HOURS)) * 3600
        chapter_mode = settings.get("split_mode") == "chapter"

        done = set(i for i in range(total) if os.path.exists(chunk_path(jdir, i)))  # empty file = skipped chunk
        job.done_chunks = len(done)
        job.done_chars = sum(len(plan[i][1]) for i in done)
        next_index = 0
        writer_index = 0  # next chunk to be appended to the current part
        window = 0
        last_edit = 0.0
        t_start = time.time()
        chars_at_start = job.done_chars
        failures: List[str] = []
        round_failed: List[int] = []
        pending_retry: List[int] = []
        consecutive_failures = 0  # chunk attempts failed in a row (no success in between)
        upload_queue: asyncio.Queue = asyncio.Queue()
        upload_errors: List[str] = []
        parts_sent = delivered_parts
        total_duration_ms = 0

        # current part state
        part_no = delivered_parts + 1
        part_fh = None
        part_path = ""
        part_dur = 0
        part_title = ""
        part_chunks = 0
        part_start = 0

        def open_part(chapter: str, start_index: int) -> None:
            nonlocal part_fh, part_path, part_dur, part_title, part_chunks, part_start
            part_path = os.path.join(parts_dir, f"part_{part_no:03d}.mp3")
            part_fh = open(part_path, "wb")
            part_dur = 0
            part_title = chapter
            part_chunks = 0
            part_start = start_index

        async def uploader() -> None:
            nonlocal parts_sent
            while True:
                item = await upload_queue.get()
                if item is None:
                    upload_queue.task_done()
                    return
                p_path, p_no, p_dur, p_title, p_start, p_end = item
                try:
                    await deliver_part(client, chat_id, p_path, p_no, p_dur, title, p_title, settings, cancel)
                    parts_sent = max(parts_sent, p_no)
                    db.update_job(job_id, delivered_parts=parts_sent, delivered_cursor=p_end)
                    # chunk files are only removed once their part is safely delivered
                    for k in range(p_start, p_end):
                        with contextlib.suppress(OSError):
                            os.remove(chunk_path(jdir, k))
                            os.remove(chunk_path(jdir, k) + ".meta")
                except JobCancelled:
                    pass
                except Exception as e:  # noqa: BLE001
                    log.exception("upload of part %d failed", p_no)
                    upload_errors.append(f"part {p_no}: {type(e).__name__}: {str(e)[:80]}")
                finally:
                    with contextlib.suppress(OSError):
                        if os.path.exists(p_path):
                            os.remove(p_path)
                    upload_queue.task_done()

        async def close_part(final: bool) -> None:
            nonlocal part_fh, part_no, total_duration_ms
            if part_fh is None:
                return
            part_fh.close()
            part_fh = None
            if part_chunks == 0 or os.path.getsize(part_path) == 0:
                with contextlib.suppress(OSError):
                    os.remove(part_path)
                return
            total_duration_ms += part_dur
            await upload_queue.put((part_path, part_no, part_dur, part_title, part_start, writer_index))
            part_no += 1

        uploader_task = asyncio.create_task(uploader())

        async def progress(force: bool = False) -> None:
            nonlocal last_edit
            now = time.time()
            if not force and now - last_edit < PROGRESS_EDIT_INTERVAL:
                return
            last_edit = now
            pct = int(100 * job.done_chunks / max(1, total))
            elapsed = max(1e-6, now - t_start)
            cps = (job.done_chars - chars_at_start) / elapsed
            remaining = max(0, total_chars - job.done_chars)
            eta = remaining / cps if cps > 0 else 0
            spoken = estimate_seconds(job.done_chars - chars_at_start, settings["rate"])
            rt = spoken / elapsed if elapsed > 0 else 0
            inflight = sum(s.limiter.active for s in pool.states())
            limit = sum(s.limiter.limit for s in pool.usable())
            r_cnt, w_cnt = job_queue.counts()
            txt = (f"{header}\n\n"
                   f"{progress_bar(pct)} <b>{pct}%</b>\n"
                   f"Chunks {job.done_chunks}/{total} · {job.done_chars:,}/{total_chars:,} chars\n"
                   f"Speed {cps:,.0f} chars/s (~{rt:.0f}× realtime) · in flight {inflight}/{limit}\n"
                   f"Elapsed {fmt_duration(elapsed)} · ETA {fmt_duration(eta) if cps > 0 else '…'}\n"
                   f"Parts sent {parts_sent}\n"
                   f"Engines: {html.escape(pool.summary())}\n"
                   f"Queue: running {r_cnt} · waiting {w_cnt}")
            job.status_line = f"{pct}% · {job.done_chunks}/{total}"
            await safe_edit(status, txt, cancel_markup(job_id))

        # Resume: chunks of delivered parts are gone for good, the writer restarts
        # at the first chunk of the first undelivered part (its chunk files are
        # still on disk, so nothing is synthesised twice).
        if delivered_cursor > 0:
            writer_index = min(delivered_cursor, total)
            next_index = writer_index
            for i in range(writer_index):
                done.add(i)
            job.done_chunks = len(done)
            job.done_chars = sum(len(plan[i][1]) for i in done)
            chars_at_start = job.done_chars

        open_part(plan[writer_index][0] if writer_index < total else "", writer_index)
        await progress(force=True)

        while writer_index < total:
            if cancel.is_set():
                raise JobCancelled()
            # fill the window
            window = sum(s.limiter.limit for s in pool.usable()) + PIPELINE_WINDOW_EXTRA
            if consecutive_failures >= max(6, 3 * max(1, len(pool.servers))):
                window = 0  # let the in-flight requests drain, then pause
            while (len(in_flight) < window and (pending_retry or next_index < total)):
                if pending_retry:
                    i = pending_retry.pop(0)
                else:
                    i = next_index
                    next_index += 1
                if i in done or i in in_flight:
                    continue
                in_flight[i] = asyncio.create_task(fetch_chunk(session, pool, plan[i][1], i, settings, cancel))
            if not in_flight:
                if writer_index in done:
                    pass
                elif not pool.usable() or consecutive_failures >= max(6, 3 * max(1, len(pool.servers))):
                    if stall_since is None:
                        stall_since = time.time()
                    if JOB_MAX_STALL_MINUTES and time.time() - stall_since > JOB_MAX_STALL_MINUTES * 60:
                        raise RuntimeError(f"Every TTS Worker has been failing for {JOB_MAX_STALL_MINUTES} min")
                    reason = failures[-1] if failures else "no usable engine"
                    db.update_job(job_id, status="paused", last_error=reason, cursor=writer_index,
                                  stall_since=now_str())
                    await wait_for_engines(session, cancel, status, job_id, header, stall_round, reason)
                    stall_round += 1
                    consecutive_failures = 0
                    db.update_job(job_id, status="running")
                    pending_retry.extend(round_failed)
                    round_failed.clear()
                    continue
                else:
                    pending_retry.extend(round_failed)
                    round_failed.clear()
                    if not pending_retry and writer_index not in done:
                        pending_retry.append(writer_index)
                    continue
            else:
                finished, _ = await asyncio.wait(set(in_flight.values()), timeout=PROGRESS_EDIT_INTERVAL,
                                                 return_when=asyncio.FIRST_COMPLETED)
                for i, t in list(in_flight.items()):
                    if not t.done():
                        continue
                    del in_flight[i]
                    try:
                        data, dur, url = t.result()
                    except JobCancelled:
                        raise
                    except VoiceError as e:
                        # Try a compatible voice for this chunk only; if still failing, skip it.
                        alt, sc = compatible_voice(settings["voice"], plan[i][1])
                        if alt != settings["voice"]:
                            alt_settings = dict(settings, voice=alt)
                            try:
                                data, dur, url = await fetch_chunk(session, pool, plan[i][1], i, alt_settings, cancel)
                            except Exception as e2:  # noqa: BLE001
                                log.warning("chunk %d skipped (%s / %s)", i + 1, e, e2)
                                _write_chunk(jdir, i, b"", 0)
                                done.add(i)
                                job.done_chunks = len(done)
                                job.done_chars += len(plan[i][1])
                                continue
                        else:
                            log.warning("chunk %d skipped: %s", i + 1, e)
                            _write_chunk(jdir, i, b"", 0)
                            done.add(i)
                            job.done_chunks = len(done)
                            job.done_chars += len(plan[i][1])
                            continue
                    except Exception as e:  # noqa: BLE001
                        msg = str(e) or type(e).__name__
                        failures.append(msg)
                        if len(failures) > 20:
                            failures.pop(0)
                        round_failed.append(i)
                        consecutive_failures += 1
                        continue
                    stall_round = 0
                    stall_since = None
                    consecutive_failures = 0
                    _write_chunk(jdir, i, data, dur)
                    done.add(i)
                    job.done_chunks = len(done)
                    job.done_chars += len(plan[i][1])
                # re-queue failed chunks immediately while engines are still healthy; once
                # too many attempts fail in a row we drain the window and pause instead
                if round_failed and pool.usable() and consecutive_failures < max(6, 3 * max(1, len(pool.servers))):
                    pending_retry.extend(round_failed)
                    round_failed.clear()

            # writer: append ready chunks in order
            wrote = False
            while writer_index < total and writer_index in done:
                ch_title, ch_text = plan[writer_index]
                d = _read_meta(jdir, writer_index)
                boundary = (chapter_mode and ch_title != part_title and part_chunks > 0) or \
                           (part_chunks > 0 and part_dur + d > max_part_sec * 1000)
                if boundary:
                    await close_part(final=False)
                    open_part(ch_title, writer_index)
                p = chunk_path(jdir, writer_index)
                if os.path.getsize(p) > 0 and part_fh is not None:
                    with open(p, "rb") as src:
                        shutil.copyfileobj(src, part_fh, 1024 * 1024)
                    part_fh.flush()
                    part_dur += d
                    part_chunks += 1
                writer_index += 1
                wrote = True
            if wrote:
                db.update_job(job_id, cursor=writer_index)
            await progress()

        await close_part(final=True)
        await progress(force=True)
        await safe_edit(status, f"{header}\n\n📤 Uploading remaining part(s)...", cancel_markup(job_id))
        await upload_queue.put(None)
        await uploader_task
        uploader_task = None

        if upload_errors:
            raise RuntimeError("Upload failed: " + "; ".join(upload_errors[:3]))
        elapsed = time.time() - t_start
        dur_s = total_duration_ms / 1000.0
        db.update_job(job_id, status="done", finished=now_str(), duration=dur_s, parts=parts_sent)
        db.add_usage(uid, total_chars, dur_s)
        cleanup_job_dir(job_id)
        with contextlib.suppress(OSError):
            os.remove(text_path)
        await safe_edit(status,
                        f"✅ <b>Done</b> - {html.escape(title[:60])}\n"
                        f"{parts_sent} part(s) · {fmt_duration(dur_s)} of audio · {total_chars:,} chars\n"
                        f"Generated in {fmt_duration(elapsed)} · voice {html.escape(voice_label(settings['voice']))}")
    except JobCancelled:
        if job_id:
            db.update_job(job_id, status="cancelled", finished=now_str())
            cleanup_job_dir(job_id)
        with contextlib.suppress(OSError):
            os.remove(text_path)
        await safe_edit(status, "⛔ Job cancelled.")
    except asyncio.CancelledError:
        if job_id:
            db.update_job(job_id, status="interrupted", last_error="bot shut down")
        raise
    except Exception as e:  # noqa: BLE001
        log.exception("job for %s failed", uid)
        if job_id:
            db.update_job(job_id, status="failed", finished=now_str(), error=str(e)[:300])
            cleanup_job_dir(job_id)
        with contextlib.suppress(OSError):
            os.remove(text_path)
        await safe_edit(status, f"❌ <b>Failed</b>: {html.escape(str(e)[:400])}")
    finally:
        for t in in_flight.values():
            if not t.done():
                t.cancel()
            else:
                with contextlib.suppress(BaseException):
                    t.exception()  # mark retrieved (JobCancelled etc.)
        if in_flight:
            await asyncio.gather(*in_flight.values(), return_exceptions=True)
            in_flight.clear()
        if uploader_task is not None and not uploader_task.done():
            uploader_task.cancel()
            with contextlib.suppress(BaseException):
                await uploader_task
        await job_queue.leave(job)
        release_job(job)


async def deliver_part(client: Client, chat_id: int, path: str, part_no: int, dur_ms: int, title: str,
                       chapter: str, settings: Dict[str, Any], cancel: asyncio.Event) -> None:
    if cancel.is_set():
        raise JobCancelled()
    path = await remux_mp3(path)
    size = os.path.getsize(path)
    if size > MAX_AUDIO_BYTES:
        raise RuntimeError(f"part {part_no} is larger than Telegram allows ({fmt_size(size)}); lower 'Max hours per part'")
    base = safe_filename(title)
    fname = f"{base} - Part {part_no:02d}.mp3"
    cap_title = f"{title[:60]} - Part {part_no}"
    if chapter:
        cap_title += f"\n📖 {chapter[:80]}"
    caption = (f"🎧 <b>{html.escape(cap_title)}</b>\n"
               f"⏱ {fmt_duration(dur_ms / 1000)} · {fmt_size(size)} · {html.escape(voice_label(settings['voice']))}")
    for attempt in range(4):
        try:
            await client.send_audio(chat_id, path, caption=caption, title=f"{title[:60]} - Part {part_no}",
                                    performer="AudioBook Pro", duration=int(dur_ms / 1000), file_name=fname)
            return
        except FloodWait as e:
            await asyncio.sleep(min(120, int(getattr(e, "value", 10)) + 1))
        except (RPCError, OSError, asyncio.TimeoutError) as e:
            log.warning("send_audio attempt %d failed: %s", attempt + 1, e)
            if attempt == 3:
                raise
            await asyncio.sleep(5 * (attempt + 1))


# =============================================================================
# Keyboards
# =============================================================================
BTN_CREATE = "🎧 Create Audio"
BTN_SETTINGS = "⚙️ Settings"
BTN_PREVIEW = "🔊 Voice Preview"
BTN_HISTORY = "📜 History"
BTN_ACCOUNT = "👤 My Account"
BTN_STATUS = "📊 Status"
BTN_HELP = "❓ Help"
BTN_REQUEST = "🔑 Request Access"
BTN_ADMIN = "🛠 Admin Panel"
BTN_BACK = "🔙 Back"
BTN_ADD_SERVER = "➕ Add Server"
BTN_DEL_SERVER = "➖ Remove Server"
BTN_SERVERS = "🖥 Server Status"
BTN_APPROVE = "✅ Approve User"
BTN_REVOKE = "🚫 Revoke / Ban"
BTN_USERS = "👥 Users"
BTN_BROADCAST = "📢 Broadcast"
BTN_STATS = "📈 Stats"
BTN_JOBS = "🧾 Jobs"
ALL_BUTTONS = {BTN_CREATE, BTN_SETTINGS, BTN_PREVIEW, BTN_HISTORY, BTN_ACCOUNT, BTN_STATUS, BTN_HELP, BTN_REQUEST,
               BTN_ADMIN, BTN_BACK, BTN_ADD_SERVER, BTN_DEL_SERVER, BTN_SERVERS, BTN_APPROVE, BTN_REVOKE, BTN_USERS,
               BTN_BROADCAST, BTN_STATS, BTN_JOBS}


def main_keyboard(uid: int) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(BTN_CREATE), KeyboardButton(BTN_SETTINGS)],
            [KeyboardButton(BTN_PREVIEW), KeyboardButton(BTN_HISTORY)],
            [KeyboardButton(BTN_ACCOUNT), KeyboardButton(BTN_STATUS), KeyboardButton(BTN_HELP)]]
    if not db.is_approved(uid):
        rows.insert(0, [KeyboardButton(BTN_REQUEST)])
    if uid == OWNER_ID:
        rows.append([KeyboardButton(BTN_ADMIN)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([
        [KeyboardButton(BTN_ADD_SERVER), KeyboardButton(BTN_DEL_SERVER), KeyboardButton(BTN_SERVERS)],
        [KeyboardButton(BTN_APPROVE), KeyboardButton(BTN_REVOKE), KeyboardButton(BTN_USERS)],
        [KeyboardButton(BTN_BROADCAST), KeyboardButton(BTN_STATS), KeyboardButton(BTN_JOBS)],
        [KeyboardButton(BTN_BACK)],
    ], resize_keyboard=True)


def settings_text(uid: int) -> str:
    s = db.settings(uid)
    return ("⚙️ <b>Settings</b>\n\n"
            f"🗣 Voice: <b>{html.escape(voice_label(s['voice']))}</b>\n"
            f"⏩ Rate: <b>{s['rate']}</b> · 🎚 Pitch: <b>{s['pitch']}</b> · 🔉 Volume: <b>{s['volume']}</b>\n"
            f"⏱ Max hours per part: <b>{s['hours']} h</b>\n"
            f"✂️ Split: <b>{'by chapter' if s['split_mode'] == 'chapter' else 'by duration'}</b>")


def settings_markup(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗣 Voice", callback_data="set:voice"),
         InlineKeyboardButton("⏩ Rate", callback_data="set:rate")],
        [InlineKeyboardButton("🎚 Pitch", callback_data="set:pitch"),
         InlineKeyboardButton("🔉 Volume", callback_data="set:volume")],
        [InlineKeyboardButton("⏱ Hours/part", callback_data="set:hours"),
         InlineKeyboardButton("✂️ Split mode", callback_data="set:split")],
        [InlineKeyboardButton("🔊 Preview", callback_data="preview"),
         InlineKeyboardButton("↩️ Reset", callback_data="reset_settings")],
        [InlineKeyboardButton("✖️ Close", callback_data="close")],
    ])


def voice_groups_markup() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(g, callback_data=f"vg:{i}")] for i, g in enumerate(VOICE_GROUPS)]
    rows.append([InlineKeyboardButton("✏️ Custom voice id", callback_data="voice_custom")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_settings")])
    return InlineKeyboardMarkup(rows)


def voice_list_markup(group_index: int, current: str) -> InlineKeyboardMarkup:
    group = list(VOICE_GROUPS.keys())[group_index]
    rows = []
    for lbl, vid in VOICE_GROUPS[group]:
        mark = "✅ " if vid == current else ""
        rows.append([InlineKeyboardButton(f"{mark}{lbl}", callback_data=f"voice:{vid}")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="set:voice")])
    return InlineKeyboardMarkup(rows)


def options_markup(prefix: str, options: List[Tuple[str, str]], current: str, cols: int = 3) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for lbl, val in options:
        mark = "✅ " if val == current else ""
        row.append(InlineKeyboardButton(f"{mark}{lbl}", callback_data=f"{prefix}:{val}"))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_settings")])
    return InlineKeyboardMarkup(rows)


def approval_markup(target: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("7 days", callback_data=f"approve:{target}:7"),
         InlineKeyboardButton("30 days", callback_data=f"approve:{target}:30"),
         InlineKeyboardButton("90 days", callback_data=f"approve:{target}:90")],
        [InlineKeyboardButton("♾ Lifetime", callback_data=f"approve:{target}:0"),
         InlineKeyboardButton("🚫 Ban", callback_data=f"ban:{target}")],
    ])


HELP_TEXT = (
    "❓ <b>How to use</b>\n\n"
    "1. Send a document (.txt .md .docx .html .epub .pdf, up to {mb} MB) or paste text.\n"
    "2. Confirm with <b>🎧 Create Audio</b>.\n"
    "3. Receive MP3 part(s) - long books are split into parts of at most N hours "
    "(Settings → Hours/part) or by chapter.\n\n"
    "<b>Commands</b>\n"
    "/settings - voice, rate, pitch, volume, split mode\n"
    "/preview - short voice sample\n"
    "/cancel - stop your current or queued job\n"
    "/queue - job queue and your position\n"
    "/resume - continue an interrupted job\n"
    "/history · /account · /status · /help\n"
)


def is_admin(uid: int) -> bool:
    return uid == OWNER_ID


async def deny(message: Message) -> None:
    await message.reply_text(
        "🔒 You are not approved to use this bot yet.\n"
        f"Press <b>{BTN_REQUEST}</b> or contact @{CONTACT_USERNAME}.",
        reply_markup=main_keyboard(message.from_user.id))


# =============================================================================
# Basic commands
# =============================================================================
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    u = message.from_user
    db.ensure_user(u.id, u.username, u.first_name)
    approved = db.is_approved(u.id)
    txt = (f"👋 Hello <b>{html.escape(u.first_name or 'there')}</b>!\n\n"
           "I turn books, stories and documents into <b>MP3 audiobooks</b> with natural neural voices "
           "(Hindi, English and other Indian languages).\n\n")
    txt += ("Send me a document or some text to begin." if approved
            else f"Access is by approval - press <b>{BTN_REQUEST}</b> to ask the owner.")
    await message.reply_text(txt, reply_markup=main_keyboard(u.id))


@bot.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, message: Message):
    await message.reply_text(HELP_TEXT.format(mb=MAX_FILE_MB), reply_markup=main_keyboard(message.from_user.id))


@bot.on_message(filters.regex(f"^{re.escape(BTN_HELP)}$") & filters.private)
async def btn_help(client: Client, message: Message):
    await cmd_help(client, message)


@bot.on_message((filters.command("status") | filters.regex(f"^{re.escape(BTN_STATUS)}$")) & filters.private)
async def cmd_status(client: Client, message: Message):
    uid = message.from_user.id
    r_cnt, w_cnt = job_queue.counts()
    srv = pool.states()
    usable = len(pool.usable())
    txt = (f"📊 <b>Status</b> · bot v{VERSION}\n\n"
           f"TTS Workers: {usable}/{len(srv)} usable\n"
           f"Jobs: running {r_cnt} · waiting {w_cnt}\n"
           f"ffmpeg: {'yes' if FFMPEG else 'no'}\n")
    mine = active_jobs.get(uid)
    if mine:
        state = "running" if mine.running else f"queued #{job_queue.position(mine)}"
        txt += f"\nYour job: <b>{html.escape(mine.title[:40])}</b> - {state} {mine.status_line}"
    if is_admin(uid) and srv:
        txt += "\n\n" + "\n".join(html.escape(s.describe()) for s in srv)
    await message.reply_text(txt)


@bot.on_message(filters.command("queue") & filters.private)
async def cmd_queue(client: Client, message: Message):
    uid = message.from_user.id
    r_cnt, w_cnt = job_queue.counts()
    lines = [f"🧾 <b>Job queue</b> - running {r_cnt}/{job_queue.parallel} · waiting {w_cnt}"]
    for j in job_queue.running:
        who = f"user {j.user_id}" if is_admin(uid) else ("you" if j.user_id == uid else "another user")
        lines.append(f"▶️ {who}: {html.escape(j.title[:30])} {j.status_line}")
    for n, j in enumerate(job_queue.waiting, 1):
        who = f"user {j.user_id}" if is_admin(uid) else ("you" if j.user_id == uid else "another user")
        eta = fmt_duration(job_queue.eta_seconds(j))
        lines.append(f"#{n} {who}: {html.escape(j.title[:30])} · {j.chars:,} chars · ~{eta}")
    await message.reply_text("\n".join(lines))


@bot.on_message(filters.regex(f"^{re.escape(BTN_REQUEST)}$") & filters.private)
async def request_access(client: Client, message: Message):
    u = message.from_user
    db.ensure_user(u.id, u.username, u.first_name)
    if db.is_approved(u.id):
        await message.reply_text("✅ You are already approved.", reply_markup=main_keyboard(u.id))
        return
    row = db.user(u.id)
    if row and row["banned"]:
        await message.reply_text("🚫 Your access has been blocked.")
        return
    if db.one("SELECT 1 FROM requests WHERE user_id=?", (u.id,)):
        await message.reply_text("⏳ Your request is already pending. Please wait for the owner.")
        return
    db.x("INSERT OR REPLACE INTO requests(user_id, requested) VALUES (?,?)", (u.id, now_str()))
    await message.reply_text("📨 Request sent. You will be notified when approved.")
    if OWNER_ID:
        await safe_send(client, OWNER_ID,
                        f"🔑 <b>Access request</b>\n{html.escape(u.first_name or '')} "
                        f"(@{u.username or '-'}) · <code>{u.id}</code>",
                        reply_markup=approval_markup(u.id))


async def grant_access(client: Client, target: int, days: int) -> str:
    db.ensure_user(target, None, None)
    db.approve(target, days)
    until = "lifetime" if days == 0 else f"{days} days"
    await safe_send(client, target, f"✅ Your access has been approved ({until}). Send /start to begin.")
    return f"✅ User {target} approved for {until}."


@bot.on_callback_query(filters.regex(r"^(approve|ban):"))
async def cb_approval(client: Client, cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Owner only", show_alert=True)
        return
    parts = cq.data.split(":")
    target = int(parts[1])
    if parts[0] == "ban":
        db.ensure_user(target, None, None)
        db.revoke(target, ban=True)
        db.x("DELETE FROM requests WHERE user_id=?", (target,))
        await safe_send(client, target, "🚫 Your access request was declined.")
        await cq.message.edit_text(f"🚫 User {target} banned.")
    else:
        msg = await grant_access(client, target, int(parts[2]))
        await cq.message.edit_text(msg)
    await cq.answer()


@bot.on_message((filters.command("account") | filters.regex(f"^{re.escape(BTN_ACCOUNT)}$")) & filters.private)
async def my_account(client: Client, message: Message):
    u = message.from_user
    row = db.ensure_user(u.id, u.username, u.first_name)
    if db.is_approved(u.id):
        exp = row["expiry"] or ("lifetime" if u.id != OWNER_ID else "owner")
        status = f"✅ approved (until {exp})"
    elif row["banned"]:
        status = "🚫 banned"
    else:
        status = "⏳ not approved"
    await message.reply_text(
        f"👤 <b>Account</b>\nID: <code>{u.id}</code>\nStatus: {status}\n"
        f"Joined: {row['joined']}\n\n"
        f"Audiobooks: {row['total_jobs']} · {row['total_chars']:,} chars · {fmt_duration(row['total_seconds'] or 0)}")


@bot.on_message((filters.command("history") | filters.regex(f"^{re.escape(BTN_HISTORY)}$")) & filters.private)
async def my_history(client: Client, message: Message):
    rows = db.user_history(message.from_user.id)
    if not rows:
        await message.reply_text("📜 No audiobooks yet.")
        return
    lines = ["📜 <b>Recent audiobooks</b>"]
    for r in rows:
        icon = {"done": "✅", "failed": "❌", "cancelled": "⛔", "running": "▶️", "paused": "⏸",
                "queued": "🕒", "interrupted": "💤"}.get(r["status"], "•")
        lines.append(f"{icon} #{r['id']} {html.escape((r['title'] or '')[:35])} · {(r['chars'] or 0):,} chars"
                     f" · {fmt_duration(r['duration'] or 0)} · {r['created'][:16]}")
    await message.reply_text("\n".join(lines))


# =============================================================================
# Settings
# =============================================================================
@bot.on_message((filters.command("settings") | filters.regex(f"^{re.escape(BTN_SETTINGS)}$")) & filters.private)
async def open_settings(client: Client, message: Message):
    uid = message.from_user.id
    db.ensure_user(uid, message.from_user.username, message.from_user.first_name)
    await message.reply_text(settings_text(uid), reply_markup=settings_markup(uid))


@bot.on_callback_query(filters.regex(r"^back_settings$"))
async def cb_back_settings(client: Client, cq: CallbackQuery):
    admin_states.pop(cq.from_user.id, None)
    await safe_edit(cq.message, settings_text(cq.from_user.id), settings_markup(cq.from_user.id))
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^close$"))
async def cb_close(client: Client, cq: CallbackQuery):
    with contextlib.suppress(Exception):
        await cq.message.delete()
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^set:"))
async def cb_set(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    what = cq.data.split(":", 1)[1]
    s = db.settings(uid)
    if what == "voice":
        await safe_edit(cq.message, "🗣 <b>Choose a voice group</b>", voice_groups_markup())
    elif what == "rate":
        await safe_edit(cq.message, "⏩ <b>Speaking rate</b>", options_markup("rate", RATE_OPTIONS, s["rate"]))
    elif what == "pitch":
        await safe_edit(cq.message, "🎚 <b>Pitch</b>", options_markup("pitch", PITCH_OPTIONS, s["pitch"]))
    elif what == "volume":
        await safe_edit(cq.message, "🔉 <b>Volume</b>", options_markup("volume", VOLUME_OPTIONS, s["volume"]))
    elif what == "hours":
        await safe_edit(cq.message, "⏱ <b>Maximum hours per MP3 part</b>",
                        options_markup("hours", HOURS_OPTIONS, str(s["hours"])))
    elif what == "split":
        await safe_edit(cq.message, "✂️ <b>How to split long books</b>",
                        options_markup("split_mode", [("By duration", "duration"), ("By chapter", "chapter")],
                                       s["split_mode"], cols=2))
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^vg:\d+$"))
async def cb_voice_group(client: Client, cq: CallbackQuery):
    idx = int(cq.data.split(":")[1])
    if idx >= len(VOICE_GROUPS):
        await cq.answer()
        return
    await safe_edit(cq.message, f"🗣 <b>{list(VOICE_GROUPS)[idx]}</b>",
                    voice_list_markup(idx, db.settings(cq.from_user.id)["voice"]))
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^voice:"))
async def cb_voice(client: Client, cq: CallbackQuery):
    vid = cq.data.split(":", 1)[1]
    if not VOICE_RE.match(vid):
        await cq.answer("Invalid voice", show_alert=True)
        return
    db.set_setting(cq.from_user.id, "voice", vid)
    await safe_edit(cq.message, settings_text(cq.from_user.id), settings_markup(cq.from_user.id))
    await cq.answer(f"Voice: {voice_label(vid)}")


@bot.on_callback_query(filters.regex(r"^voice_custom$"))
async def cb_voice_custom(client: Client, cq: CallbackQuery):
    admin_states[cq.from_user.id] = "custom_voice"
    await safe_edit(cq.message,
                    "✏️ Send a voice id, e.g. <code>en-US-AndrewNeural</code> or <code>hi-IN-SwaraNeural</code>.\n"
                    "Full list: the Worker's <code>/voices</code> endpoint. Send /cancel to abort.",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_settings")]]))
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^(rate|pitch|volume|hours|split_mode):"))
async def cb_apply_option(client: Client, cq: CallbackQuery):
    key, val = cq.data.split(":", 1)
    valid = {"rate": [v for _, v in RATE_OPTIONS], "pitch": [v for _, v in PITCH_OPTIONS],
             "volume": [v for _, v in VOLUME_OPTIONS], "hours": [v for _, v in HOURS_OPTIONS],
             "split_mode": ["duration", "chapter"]}
    if val not in valid[key]:
        await cq.answer("Invalid option", show_alert=True)
        return
    db.set_setting(cq.from_user.id, key, int(val) if key == "hours" else val)
    await safe_edit(cq.message, settings_text(cq.from_user.id), settings_markup(cq.from_user.id))
    await cq.answer("Saved")


@bot.on_callback_query(filters.regex(r"^reset_settings$"))
async def cb_reset_settings(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    for k, v in (("voice", DEFAULT_VOICE), ("rate", DEFAULT_RATE), ("pitch", DEFAULT_PITCH),
                 ("volume", DEFAULT_VOLUME), ("hours", DEFAULT_HOURS), ("split_mode", DEFAULT_SPLIT)):
        db.set_setting(uid, k, v)
    await safe_edit(cq.message, settings_text(uid), settings_markup(uid))
    await cq.answer("Settings reset")


# =============================================================================
# Voice preview
# =============================================================================
PREVIEW_TEXTS = {
    "hi": "नमस्ते! यह आपकी चुनी हुई आवाज़ का एक छोटा सा नमूना है। कहानी शुरू करने के लिए तैयार हैं?",
    "en": "Hello! This is a short sample of the voice you selected. Ready to start your story?",
    "bn": "নমস্কার! এটি আপনার নির্বাচিত কণ্ঠস্বরের একটি ছোট নমুনা।",
    "ta": "வணக்கம்! இது நீங்கள் தேர்ந்தெடுத்த குரலின் ஒரு சிறிய மாதிரி.",
    "te": "నమస్కారం! ఇది మీరు ఎంచుకున్న స్వరం యొక్క చిన్న నమూనా.",
    "mr": "नमस्कार! हा तुम्ही निवडलेल्या आवाजाचा एक छोटा नमुना आहे.",
    "gu": "નમસ્તે! આ તમે પસંદ કરેલા અવાજનો એક નાનો નમૂનો છે.",
    "ur": "السلام علیکم! یہ آپ کی منتخب کردہ آواز کا ایک مختصر نمونہ ہے۔",
}


async def send_voice_preview(client: Client, chat_id: int, uid: int, status: Optional[Message] = None) -> None:
    s = db.settings(uid)
    text = PREVIEW_TEXTS.get(voice_language(s["voice"]), PREVIEW_TEXTS["en"])
    if status is None:
        status = await safe_send(client, chat_id, "🔊 Generating preview...")
    if http_session is None:
        await safe_edit(status, "❌ Bot is still starting, try again in a moment.")
        return
    try:
        if not pool.states():
            pool.sync_from_db()
        if not pool.usable():
            await pool.refresh(http_session)
        if not pool.states():
            raise RuntimeError("No TTS Worker registered yet.")
        data, dur, _ = await fetch_chunk(http_session, pool, text, 0, s, asyncio.Event())
        tmp = os.path.join(tempfile.gettempdir(), f"preview_{uid}_{uuid.uuid4().hex[:6]}.mp3")
        with open(tmp, "wb") as fh:
            fh.write(data)
        try:
            await client.send_audio(chat_id, tmp, caption=f"🔊 {html.escape(voice_label(s['voice']))} · rate {s['rate']}",
                                    title="Voice preview", performer="AudioBook Pro", duration=int(dur / 1000))
        finally:
            with contextlib.suppress(OSError):
                os.remove(tmp)
        with contextlib.suppress(Exception):
            await status.delete()
    except Exception as e:  # noqa: BLE001
        await safe_edit(status, f"❌ Preview failed: {html.escape(str(e)[:200])}")


async def start_preview(client: Client, chat_id: int, uid: int) -> None:
    old = preview_tasks.get(uid)
    if old and not old.done():
        await safe_send(client, chat_id, "⏳ A preview is already being generated.")
        return
    preview_tasks[uid] = asyncio.create_task(send_voice_preview(client, chat_id, uid))


@bot.on_callback_query(filters.regex(r"^preview$"))
async def cb_preview(client: Client, cq: CallbackQuery):
    if not db.is_approved(cq.from_user.id):
        await cq.answer("Not approved", show_alert=True)
        return
    await cq.answer("Generating…")
    await start_preview(client, cq.message.chat.id, cq.from_user.id)


@bot.on_message((filters.command("preview") | filters.regex(f"^{re.escape(BTN_PREVIEW)}$")) & filters.private)
async def cmd_preview(client: Client, message: Message):
    if not db.is_approved(message.from_user.id):
        await deny(message)
        return
    await start_preview(client, message.chat.id, message.from_user.id)


# =============================================================================
# Create / cancel
# =============================================================================
@bot.on_message(filters.regex(f"^{re.escape(BTN_CREATE)}$") & filters.private)
async def btn_create(client: Client, message: Message):
    if not db.is_approved(message.from_user.id):
        await deny(message)
        return
    await message.reply_text(
        f"📄 Send me a document (.txt .md .docx .html .epub .pdf, up to {MAX_FILE_MB} MB) "
        "or paste the text you want narrated.")


@bot.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(client: Client, message: Message):
    uid = message.from_user.id
    if admin_states.pop(uid, None):
        await message.reply_text("Input cancelled.", reply_markup=main_keyboard(uid))
        return
    if pending_text.pop(uid, None):
        await message.reply_text("Pending text discarded.")
        return
    job = active_jobs.get(uid)
    if not job:
        await message.reply_text("Nothing to cancel.")
        return
    job.cancel.set()
    await job_queue.kick()
    await message.reply_text("⛔ Cancelling your job...")


@bot.on_callback_query(filters.regex(r"^canceljob:"))
async def cb_cancel_job(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    job = active_jobs.get(uid)
    if not job and is_admin(uid):
        jid = int(cq.data.split(":")[1] or 0)
        job = next((j for j in active_jobs.values() if j.job_id == jid), None)
    if not job:
        await cq.answer("No active job", show_alert=True)
        return
    job.cancel.set()
    await job_queue.kick()
    await cq.answer("Cancelling…")


# =============================================================================
# Text & document intake
# =============================================================================
def _title_from_text(text: str) -> str:
    first = text.strip().split("\n", 1)[0].strip()
    first = re.sub(r"\s+", " ", first)
    return (first[:50] or "Text").strip()


async def start_job_from_file(client: Client, chat_id: int, uid: int, text_path: str, title: str,
                              chars: int) -> None:
    job = reserve_job(uid, title, chars)
    if job is None:
        with contextlib.suppress(OSError):
            os.remove(text_path)
        if uid in active_jobs:
            await safe_send(client, chat_id, "⏳ You already have a job running or queued. /cancel it first.")
        else:
            await safe_send(client, chat_id, "🚦 The queue is full right now, please try again later.")
        return
    settings = db.settings(uid)
    settings["voice_requested"] = settings["voice"]
    est = fmt_duration(estimate_seconds(chars, settings["rate"]))
    status = await safe_send(
        client, chat_id,
        f"📚 <b>{html.escape(title[:60])}</b>\n{chars:,} characters · ≈ {est} of audio\n"
        f"🗣 {html.escape(voice_label(settings['voice']))}\n\nStarting...",
        reply_markup=cancel_markup(0))
    job.task = asyncio.create_task(
        run_audiobook_job(client, status, uid, job, text_path, title, settings, chat_id=chat_id))


@bot.on_message(filters.document & filters.private)
async def handle_document(client: Client, message: Message):
    uid = message.from_user.id
    db.ensure_user(uid, message.from_user.username, message.from_user.first_name)
    if not db.is_approved(uid):
        await deny(message)
        return
    doc = message.document
    name = doc.file_name or "document.txt"
    ext = os.path.splitext(name)[1].lower()
    if ext not in SUPPORTED_EXT:
        await message.reply_text(f"❌ Unsupported file type <b>{html.escape(ext or '?')}</b>. "
                                 f"Supported: {' '.join(sorted(SUPPORTED_EXT))}")
        return
    if (doc.file_size or 0) > MAX_FILE_MB * 1024 * 1024:
        await message.reply_text(f"❌ File is larger than {MAX_FILE_MB} MB.")
        return
    if uid in active_jobs:
        await message.reply_text("⏳ You already have a job running or queued. /cancel it first.")
        return
    status = await message.reply_text("⬇️ Downloading...")
    os.makedirs(JOBS_DIR, exist_ok=True)
    src = os.path.join(JOBS_DIR, f"src_{uid}_{uuid.uuid4().hex[:8]}{ext}")
    txt_path = src + ".txt"
    try:
        await message.download(file_name=src)
        await safe_edit(status, "📖 Extracting text...")
        chars = await asyncio.to_thread(extract_text_to_file, src, ext, txt_path)
        if chars < 5:
            raise ValueError("No readable text found in this file (scanned PDF?)")
    except Exception as e:  # noqa: BLE001
        log.warning("extract failed for %s: %s", name, e)
        for p in (src, txt_path):
            with contextlib.suppress(OSError):
                os.remove(p)
        await safe_edit(status, f"❌ Could not read the file: {html.escape(str(e)[:200])}")
        return
    finally:
        with contextlib.suppress(OSError):
            os.remove(src)
    with contextlib.suppress(Exception):
        await status.delete()
    title = os.path.splitext(name)[0][:60] or "Document"
    await start_job_from_file(client, message.chat.id, uid, txt_path, title, chars)


@bot.on_message(filters.text & filters.private & ~filters.command(
    ["start", "help", "status", "queue", "account", "history", "settings", "preview", "cancel", "resume", "jobs",
     "admin", "servers", "users", "stats", "addserver", "add_server"]))
async def text_handler(client: Client, message: Message):
    uid = message.from_user.id
    text = message.text or ""
    db.ensure_user(uid, message.from_user.username, message.from_user.first_name)
    state = admin_states.get(uid)
    if state == "custom_voice":
        admin_states.pop(uid, None)
        vid = text.strip()
        if not VOICE_RE.match(vid):
            await message.reply_text("❌ That does not look like a voice id (e.g. en-US-AndrewNeural).")
            return
        db.set_setting(uid, "voice", vid)
        await message.reply_text(f"✅ Voice set to <code>{html.escape(vid)}</code>. Use /preview to test it.",
                                 reply_markup=main_keyboard(uid))
        return
    if is_admin(uid) and state and await _handle_admin_state(client, message, state, text):
        return
    # reply-keyboard buttons have their own handlers (some registered after this one)
    if text in ALL_BUTTONS:
        admin_states.pop(uid, None)
        message.continue_propagation()
    if not db.is_approved(uid):
        await deny(message)
        return
    clean = clean_text(text)
    if len(clean) < 5 or not is_speakable(clean):
        await message.reply_text("Send a longer text or a document to narrate.")
        return
    if uid in active_jobs:
        await message.reply_text("⏳ You already have a job running or queued. /cancel it first.")
        return
    token = uuid.uuid4().hex[:8]
    pending_text[uid] = (token, clean, time.time() + 600)
    s = db.settings(uid)
    await message.reply_text(
        f"📝 {len(clean):,} characters · ≈ {fmt_duration(estimate_seconds(len(clean), s['rate']))} of audio\n"
        f"🗣 {html.escape(voice_label(s['voice']))}\n\nCreate the audiobook?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🎧 Create Audio", callback_data=f"mk:{token}"),
            InlineKeyboardButton("⚙️ Settings", callback_data="back_settings"),
            InlineKeyboardButton("✖️", callback_data=f"mkno:{token}")]]))


@bot.on_callback_query(filters.regex(r"^mk(no)?:"))
async def cb_text_confirm(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    action, token = cq.data.split(":", 1)
    entry = pending_text.get(uid)
    if not entry or entry[0] != token or entry[2] < time.time():
        pending_text.pop(uid, None)
        await cq.answer("This text has expired - send it again.", show_alert=True)
        with contextlib.suppress(Exception):
            await cq.message.delete()
        return
    pending_text.pop(uid, None)
    if action == "mkno":
        await cq.answer("Discarded")
        with contextlib.suppress(Exception):
            await cq.message.delete()
        return
    if not db.is_approved(uid):
        await cq.answer("Not approved", show_alert=True)
        return
    await cq.answer()
    text = entry[1]
    os.makedirs(JOBS_DIR, exist_ok=True)
    txt_path = os.path.join(JOBS_DIR, f"src_{uid}_{uuid.uuid4().hex[:8]}.txt")
    with open(txt_path, "w", encoding="utf-8") as fh:
        fh.write(text)
    with contextlib.suppress(Exception):
        await cq.message.delete()
    await start_job_from_file(client, cq.message.chat.id, uid, txt_path, _title_from_text(text), len(text))


# =============================================================================
# Resume interrupted jobs
# =============================================================================
async def resume_job(client: Client, row: sqlite3.Row, status: Optional[Message] = None) -> bool:
    uid = int(row["user_id"])
    text_path = row["text_path"] or ""
    if not text_path or not os.path.exists(text_path):
        db.update_job(row["id"], status="failed", error="source text missing after restart", finished=now_str())
        return False
    try:
        settings = json.loads(row["settings"] or "{}")
    except ValueError:
        settings = {}
    base = db.settings(uid)
    base.update({k: v for k, v in settings.items() if v is not None})
    job = reserve_job(uid, row["title"] or "Audiobook", int(row["chars"] or 0))
    if job is None:
        return False
    chat_id = int(row["chat_id"] or uid)
    if status is None:
        status = await safe_send(client, chat_id,
                                 f"🔄 Resuming <b>{html.escape((row['title'] or '')[:60])}</b> "
                                 f"(job #{row['id']}) after a restart...", reply_markup=cancel_markup(row["id"]))
    job.task = asyncio.create_task(
        run_audiobook_job(client, status, uid, job, text_path, row["title"] or "Audiobook", base,
                          job_id=int(row["id"]), chat_id=chat_id))
    return True


async def resume_interrupted_jobs(client: Client) -> None:
    rows = db.unfinished_jobs()
    if not rows:
        return
    log.info("resuming %d unfinished job(s)", len(rows))
    for row in rows:
        try:
            await resume_job(client, row)
        except Exception as e:  # noqa: BLE001
            log.warning("resume of job %s failed: %s", row["id"], e)


@bot.on_message(filters.command("resume") & filters.private)
async def cmd_resume(client: Client, message: Message):
    uid = message.from_user.id
    if not db.is_approved(uid):
        await deny(message)
        return
    if uid in active_jobs:
        await message.reply_text("⏳ You already have a job running or queued.")
        return
    rows = [r for r in db.unfinished_jobs() if r["user_id"] == uid]
    if not rows:
        await message.reply_text("Nothing to resume.")
        return
    ok = await resume_job(client, rows[0])
    if not ok:
        await message.reply_text("❌ Could not resume - the source text is no longer available.")


@bot.on_message((filters.command("jobs") | filters.regex(f"^{re.escape(BTN_JOBS)}$")) & filters.private)
async def cmd_jobs(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    lines = ["🧾 <b>Jobs</b>"]
    for j in job_queue.running:
        lines.append(f"▶️ #{j.job_id} user {j.user_id} · {html.escape(j.title[:30])} · {j.status_line}")
    for n, j in enumerate(job_queue.waiting, 1):
        lines.append(f"🕒 #{n} user {j.user_id} · {html.escape(j.title[:30])} · {j.chars:,} chars")
    rows = db.q("SELECT * FROM jobs WHERE status IN ('paused','interrupted') ORDER BY id DESC LIMIT 10")
    for r in rows:
        lines.append(f"⏸ #{r['id']} user {r['user_id']} · {html.escape((r['title'] or '')[:30])} · "
                     f"cursor {r['cursor']}/{r['chunks']} · {html.escape((r['last_error'] or '')[:60])}")
    rows = db.q("SELECT * FROM jobs WHERE status IN ('done','failed','cancelled') ORDER BY id DESC LIMIT 5")
    for r in rows:
        lines.append(f"• #{r['id']} {r['status']} · {html.escape((r['title'] or '')[:30])} · {r['created'][:16]}")
    await message.reply_text("\n".join(lines))


# =============================================================================
# Admin panel
# =============================================================================
@bot.on_message((filters.command("admin") | filters.regex(f"^{re.escape(BTN_ADMIN)}$")) & filters.private)
async def admin_panel(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    pend = db.q("SELECT COUNT(*) AS n FROM requests")[0]["n"]
    await message.reply_text(
        f"🛠 <b>Admin panel</b>\nWorkers: {len(db.servers())} · pending requests: {pend}\n"
        f"TTS_API_KEY: {'set' if TTS_API_KEY else 'not set'}\n\n"
        "Commands: /addserver &lt;url&gt; · /servers · /users · /stats · /jobs",
        reply_markup=admin_keyboard())


@bot.on_message(filters.regex(f"^{re.escape(BTN_BACK)}$") & filters.private)
async def back_main(client: Client, message: Message):
    admin_states.pop(message.from_user.id, None)
    await message.reply_text("Main menu", reply_markup=main_keyboard(message.from_user.id))


ADD_SERVER_PROMPT = (
    "➕ <b>Add Worker</b>\nSend the Worker URL, e.g. <code>https://edge-tts-worker.your-account.workers.dev</code>\n"
    "(one per line / comma separated for several).\n"
    "I will run a real synthesis test before adding it.  /cancel to abort.\n\n"
    "Tip: <code>/addserver &lt;url&gt;</code> works from anywhere.")


async def add_servers(message: Message, text: str) -> None:
    """Probe every URL in ``text`` (health + real /tts) and register the good ones."""
    if http_session is None:
        await message.reply_text("Starting up, try again in a moment.")
        return
    urls: List[str] = []
    for raw in re.split(r"[\s,]+", text):
        nu = normalize_server_url(raw)
        if nu and nu not in urls:
            urls.append(nu)
    if not urls:
        await message.reply_text("❌ No valid URL found. Example: <code>https://name.account.workers.dev</code>")
        return
    status = await message.reply_text(f"🔍 Testing {len(urls)} Worker(s) (health + real synthesis)...")
    out: List[str] = []
    failed: List[str] = []
    for u in urls:
        info = await check_server(http_session, u, deep=True)
        if info["ok"]:
            added = db.add_server(u)
            out.append(f"✅ {html.escape(short_server(u))} v{info.get('version') or '?'}"
                       f" · {info.get('latency')}s · max_conc {info.get('max_concurrency', '?')}"
                       + ("" if added else " (already registered)"))
        else:
            failed.append(u)
            out.append(f"❌ {html.escape(short_server(u))}: {html.escape(info['error'] or 'unknown error')}")
    pool.sync_from_db()
    markup = None
    if failed:
        # let the admin force-register a Worker that is temporarily down / throttled
        token = uuid.uuid4().hex[:8]
        pending_servers[token] = (failed, time.time() + 900)
        out.append("\nWorker failed the test. Check the URL / API key, or add it anyway (it will be retried "
                   "automatically by the health loop).")
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add anyway", callback_data=f"addsrv:{token}"),
                                        InlineKeyboardButton("✖️", callback_data="close")]])
    n = len(db.servers())
    out.append(f"\nRegistered Workers: <b>{n}</b>")
    await safe_edit(status, "\n".join(out), reply_markup=markup)


@bot.on_message(filters.regex(f"^{re.escape(BTN_ADD_SERVER)}$") & filters.private)
async def btn_add_server(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = "add_server"
    await message.reply_text(ADD_SERVER_PROMPT, reply_markup=admin_keyboard())


@bot.on_message(filters.command(["addserver", "add_server"]) & filters.private)
async def cmd_add_server(client: Client, message: Message):
    """/addserver <url> [<url> ...] - register Worker(s) directly (owner only)."""
    if not is_admin(message.from_user.id):
        return
    arg = (message.text or "").split(None, 1)
    if len(arg) < 2 or not arg[1].strip():
        admin_states[message.from_user.id] = "add_server"
        await message.reply_text(ADD_SERVER_PROMPT, reply_markup=admin_keyboard())
        return
    admin_states.pop(message.from_user.id, None)
    await add_servers(message, arg[1])


@bot.on_callback_query(filters.regex(r"^addsrv:"))
async def cb_add_server_anyway(client: Client, cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Owner only", show_alert=True)
        return
    token = cq.data.split(":", 1)[1]
    entry = pending_servers.pop(token, None)
    if not entry or entry[1] < time.time():
        await cq.answer("Expired - send the URL again.", show_alert=True)
        return
    added = [u for u in entry[0] if db.add_server(u, note="added without passing the probe")]
    pool.sync_from_db()
    lines = [f"➕ Registered <code>{html.escape(u)}</code>" for u in added] or ["Already registered."]
    lines.append(f"Registered Workers: <b>{len(db.servers())}</b> · use 🖥 Server Status to re-test.")
    with contextlib.suppress(Exception):
        await cq.message.edit_text("\n".join(lines))
    await cq.answer("Added")


@bot.on_message(filters.regex(f"^{re.escape(BTN_DEL_SERVER)}$") & filters.private)
async def btn_remove_server(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    urls = db.servers(enabled_only=False)
    if not urls:
        await message.reply_text("No Workers registered.")
        return
    rows = [[InlineKeyboardButton(f"🗑 {short_server(u)}", callback_data=f"delsrv:{i}")] for i, u in enumerate(urls)]
    await message.reply_text("Select the Worker to remove:", reply_markup=InlineKeyboardMarkup(rows))


@bot.on_callback_query(filters.regex(r"^delsrv:\d+$"))
async def cb_del_server(client: Client, cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Owner only", show_alert=True)
        return
    urls = db.servers(enabled_only=False)
    idx = int(cq.data.split(":")[1])
    if idx >= len(urls):
        await cq.answer("Gone")
        return
    db.remove_server(urls[idx])
    pool.sync_from_db()
    await cq.message.edit_text(f"🗑 Removed <code>{html.escape(urls[idx])}</code>")
    await cq.answer()


@bot.on_message((filters.command("servers") | filters.regex(f"^{re.escape(BTN_SERVERS)}$")) & filters.private)
async def btn_server_status(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    if http_session is None:
        await message.reply_text("Starting up, try again in a moment.")
        return
    urls = db.servers(enabled_only=False)
    if not urls:
        await message.reply_text("No Workers registered. Use ➕ Add Server.")
        return
    status = await message.reply_text("🔍 Probing Workers (real synthesis test)...")
    infos = await pool.refresh(http_session, deep=True)
    lines = ["🖥 <b>TTS Workers</b>"]
    for info in infos:
        s = pool.servers.get(info["url"])
        name = short_server(info["url"])
        if info.get("ok"):
            lines.append(f"✅ <b>{html.escape(name)}</b> · v{info.get('version') or '?'} · {info.get('latency')}s · "
                         f"limit {s.limiter.limit}/{s.limiter.ceiling} · max_text {s.max_text_length} · "
                         f"done {s.chunks_done} · failed {s.chunks_failed}"
                         + (f" · ⛔ benched {fmt_duration(s.bench_left)} (#{s.q_count})" if s and s.benched else ""))
        else:
            lines.append(f"❌ <b>{html.escape(name)}</b> · {html.escape(info.get('error') or 'unknown error')}")
    lines.append(f"\nTTS_API_KEY on bot: {'set' if TTS_API_KEY else 'not set'}")
    await safe_edit(status, "\n".join(lines))


@bot.on_message(filters.regex(f"^{re.escape(BTN_APPROVE)}$") & filters.private)
async def btn_approve(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    pend = db.q("SELECT r.user_id, r.requested, u.first_name, u.username FROM requests r "
                "LEFT JOIN users u ON u.user_id=r.user_id ORDER BY r.requested")
    for r in pend[:20]:
        await message.reply_text(
            f"🔑 {html.escape(r['first_name'] or '')} (@{r['username'] or '-'}) · <code>{r['user_id']}</code>"
            f" · requested {r['requested']}", reply_markup=approval_markup(r["user_id"]))
    admin_states[message.from_user.id] = "approve"
    await message.reply_text(
        f"{len(pend)} pending request(s) above.\nOr send <code>user_id [days]</code> to approve manually "
        "(days 0 = lifetime). /cancel to abort.")


@bot.on_message(filters.regex(f"^{re.escape(BTN_REVOKE)}$") & filters.private)
async def btn_revoke(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = "revoke"
    await message.reply_text("🚫 Send <code>user_id</code> to revoke access, or <code>user_id ban</code> to ban. "
                             "<code>user_id unban</code> lifts a ban. /cancel to abort.")


@bot.on_message((filters.command("users") | filters.regex(f"^{re.escape(BTN_USERS)}$")) & filters.private)
async def btn_users(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    rows = db.q("SELECT * FROM users ORDER BY joined DESC LIMIT 60")
    lines = [f"👥 <b>Users</b> ({db.q('SELECT COUNT(*) AS n FROM users')[0]['n']})"]
    for r in rows:
        if r["banned"]:
            st = "🚫"
        elif r["approved"]:
            st = "✅"
        else:
            st = "⏳"
        lines.append(f"{st} <code>{r['user_id']}</code> {html.escape(r['first_name'] or '')} "
                     f"@{r['username'] or '-'} · {r['total_jobs']} jobs"
                     + (f" · until {r['expiry'][:10]}" if r["expiry"] else ""))
    await message.reply_text("\n".join(lines)[:4000])


@bot.on_message(filters.regex(f"^{re.escape(BTN_BROADCAST)}$") & filters.private)
async def btn_broadcast(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = "broadcast"
    await message.reply_text("📢 Send the message to broadcast to all approved users. /cancel to abort.")


@bot.on_message((filters.command("stats") | filters.regex(f"^{re.escape(BTN_STATS)}$")) & filters.private)
async def btn_stats(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    users = db.q("SELECT COUNT(*) AS n, SUM(approved) AS a, SUM(banned) AS b FROM users")[0]
    jobs = db.q("SELECT COUNT(*) AS n, SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS d, "
                "SUM(chars) AS c, SUM(duration) AS s FROM jobs")[0]
    r_cnt, w_cnt = job_queue.counts()
    await message.reply_text(
        f"📈 <b>Stats</b>\nUsers {users['n']} · approved {users['a'] or 0} · banned {users['b'] or 0}\n"
        f"Jobs {jobs['n']} · done {jobs['d'] or 0} · {(jobs['c'] or 0):,} chars · {fmt_duration(jobs['s'] or 0)} audio\n"
        f"Now: running {r_cnt} · waiting {w_cnt}\n"
        f"Workers: {len(pool.usable())}/{len(pool.states())} usable · free disk {free_disk_mb(BASE_DIR):,.0f} MB")


async def _handle_admin_state(client: Client, message: Message, state: str, text: str) -> bool:
    uid = message.from_user.id
    if state == "add_server":
        if text in ALL_BUTTONS:
            return False  # admin pressed another button - let its handler run
        admin_states.pop(uid, None)
        await add_servers(message, text)
        return True
    if state == "approve":
        m = re.match(r"^\s*(\d{5,})\s*(\d+)?\s*$", text)
        if not m:
            return False
        admin_states.pop(uid, None)
        msg = await grant_access(client, int(m.group(1)), int(m.group(2) or 0))
        await message.reply_text(msg, reply_markup=admin_keyboard())
        return True
    if state == "revoke":
        m = re.match(r"^\s*(\d{5,})\s*(ban|unban)?\s*$", text, re.I)
        if not m:
            return False
        admin_states.pop(uid, None)
        target = int(m.group(1))
        mode = (m.group(2) or "").lower()
        db.ensure_user(target, None, None)
        if mode == "unban":
            db.x("UPDATE users SET banned=0 WHERE user_id=?", (target,))
            await message.reply_text(f"✅ User {target} unbanned (not approved yet).", reply_markup=admin_keyboard())
        else:
            db.revoke(target, ban=(mode == "ban"))
            j = active_jobs.get(target)
            if j:
                j.cancel.set()
            await safe_send(client, target, "🚫 Your access has been revoked.")
            await message.reply_text(f"🚫 User {target} {'banned' if mode == 'ban' else 'revoked'}.",
                                     reply_markup=admin_keyboard())
        return True
    if state == "broadcast":
        admin_states.pop(uid, None)
        rows = db.q("SELECT user_id FROM users WHERE approved=1 AND banned=0")
        sent = 0
        for r in rows:
            if await safe_send(client, int(r["user_id"]), f"📢 {text}"):
                sent += 1
            await asyncio.sleep(0.05)
        await message.reply_text(f"📢 Sent to {sent}/{len(rows)} users.", reply_markup=admin_keyboard())
        return True
    return False


# =============================================================================
# Background tasks & main
# =============================================================================
async def keep_alive_loop() -> None:
    """Periodic /health refresh so /status is accurate and benched Workers recover."""
    while True:
        try:
            await asyncio.sleep(KEEP_ALIVE_INTERVAL)
            if http_session is not None and db.servers():
                await pool.refresh(http_session)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("keep-alive: %s", e)


def _check_config() -> None:
    missing = [k for k, v in (("API_ID", API_ID), ("API_HASH", API_HASH), ("BOT_TOKEN", BOT_TOKEN),
                              ("OWNER_ID", OWNER_ID)) if not v]
    if missing:
        sys.stderr.write("Missing required settings: " + ", ".join(missing) +
                         f"\nPut them in {os.path.join(BASE_DIR, '.env')} or the environment.\n")
        sys.exit(2)


async def main() -> None:
    global http_session
    _check_config()
    os.makedirs(JOBS_DIR, exist_ok=True)
    for u in TTS_SERVERS_ENV:
        nu = normalize_server_url(u)
        if nu and db.add_server(nu, note="from TTS_SERVERS"):
            log.info("registered Worker from TTS_SERVERS: %s", nu)
    connector = aiohttp.TCPConnector(limit=MAX_TOTAL_CONCURRENCY + 16, ttl_dns_cache=300)
    http_session = aiohttp.ClientSession(connector=connector)
    pool.sync_from_db()
    log.info("AudioBook Pro bot v%s starting - %d Worker(s), ffmpeg=%s, jobs dir=%s",
             VERSION, len(pool.states()), bool(FFMPEG), JOBS_DIR)
    if asyncio.get_running_loop() is not getattr(bot, "loop", None):
        # would make every handler silently dead - fail loudly instead
        raise RuntimeError("event loop mismatch: bot.py must be run with LOOP.run_until_complete(main())")
    await bot.start()
    me = await bot.get_me()
    log.info("logged in as @%s", me.username)
    if pool.states():
        with contextlib.suppress(Exception):
            await pool.refresh(http_session)
            log.info("Workers: %s", pool.summary())
    else:
        log.warning("No TTS Worker registered yet - add one with /admin -> Add Server or TTS_SERVERS")
    ka = asyncio.create_task(keep_alive_loop())
    if RESUME_ON_START:
        await resume_interrupted_jobs(bot)
    if OWNER_ID:
        await safe_send(bot, OWNER_ID, f"🤖 Bot v{VERSION} started · Workers: {html.escape(pool.summary())}")
    stop = asyncio.Event()
    try:
        import signal
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)
    except Exception:  # noqa: BLE001
        pass
    try:
        await stop.wait()
    finally:
        log.info("shutting down...")
        ka.cancel()
        for j in list(active_jobs.values()):
            if j.task and not j.task.done():
                if j.job_id:
                    db.update_job(j.job_id, status="interrupted", last_error="bot restarted")
                j.task.cancel()
        await asyncio.sleep(0.5)
        with contextlib.suppress(Exception):
            await bot.stop()
        with contextlib.suppress(Exception):
            await http_session.close()


if __name__ == "__main__":
    try:
        # Must be the same loop Pyrogram captured at Client() construction (see LOOP above).
        LOOP.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        with contextlib.suppress(Exception):
            LOOP.run_until_complete(LOOP.shutdown_asyncgens())
        LOOP.close()
