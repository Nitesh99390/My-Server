#!/usr/bin/env python3
"""
=============================================================================
 AudioBook Pro - Telegram TTS master bot  (bot.py)  -  v5.1.0
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
   * One live status card per job through every stage
     (Queue > Prepare > Voice > Send > Done) with download %, extraction,
     chunking, synthesis %, speed, ETA, audio-ready time and upload % per part.
   * Tiny two-row reply keyboard; everything else is an inline menu
     (My Status, Settings, Admin panel) that edits itself in place.
   * Visible job queue, /cancel, voice preview, 20+ voices, user approval
     system, rotating logs.  TTS_API_KEY is optional.
   * Databases from bot.py 4.x are upgraded in place on first start.
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
   -- fleet tuning (defaults assume MANY Workers; Microsoft limits per egress) --
   REMOTE_CHUNK_SIZE=2000  PER_SERVER_CONCURRENCY=2  PER_SERVER_MAX_CONCURRENCY=3
   REMOTE_MIN_GAP_MS=250  MAX_TOTAL_CONCURRENCY=160  HEALTH_PARALLELISM=24
   MAX_PARALLEL_JOBS=1  MAX_QUEUED_JOBS=20
   QUARANTINE_STEPS=30,60,120,240  QUARANTINE_FORGIVE_AFTER=8  QUARANTINE_DECAY_SECS=300
   SOFT_FAIL_LIMIT=4  SOFT_FAIL_WINDOW=120
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

VERSION = "5.2.0"
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
# advertised max_text_length).  The Worker splits it into ~3.8 KB websocket
# pieces; Devanagari is 3 bytes/char so 2000 chars = ~2 pieces = 2 sockets.
#
# Fleet model (100+ Workers): every Worker gets a LOW, steady load - Microsoft
# rate-limits per egress, so "many Workers x 2 requests" is both faster and
# far safer than "few Workers x 8 requests".  Total throughput comes from the
# fleet size (MAX_TOTAL_CONCURRENCY), not from squeezing one Worker.
REMOTE_CHUNK_SIZE = max(500, min(_env_int("REMOTE_CHUNK_SIZE", 2000), 6000))
TARGET_CHUNK_CHARS = max(0, min(_env_int("TARGET_CHUNK_CHARS", 0), 6000))
CHUNK_MIN_RATIO = max(0.3, min(_env_int("CHUNK_MIN_PERCENT", 60), 95) / 100.0)
PER_SERVER_CONCURRENCY = max(1, min(_env_int("PER_SERVER_CONCURRENCY", 2), 16))
PER_SERVER_MAX_CONCURRENCY = max(PER_SERVER_CONCURRENCY, min(_env_int("PER_SERVER_MAX_CONCURRENCY", 3), 16))
REMOTE_MIN_GAP = max(0, _env_int("REMOTE_MIN_GAP_MS", 250)) / 1000.0
REMOTE_GROW_AFTER = max(1, min(_env_int("REMOTE_GROW_AFTER", 6), 50))
MAX_TOTAL_CONCURRENCY = max(4, min(_env_int("MAX_TOTAL_CONCURRENCY", 160), 512))
# Soft failures (timeouts, resets, empty audio) never bench a Worker on their
# own; only this many within SOFT_FAIL_WINDOW seconds do.
SOFT_FAIL_LIMIT = max(2, _env_int("SOFT_FAIL_LIMIT", 4))
SOFT_FAIL_WINDOW = max(30, _env_int("SOFT_FAIL_WINDOW", 120))
# /health probes of the fleet run at most this many at once (100+ Workers).
HEALTH_PARALLELISM = max(4, min(_env_int("HEALTH_PARALLELISM", 24), 128))
MAX_PARALLEL_JOBS = max(1, _env_int("MAX_PARALLEL_JOBS", 1))
MAX_QUEUED_JOBS = max(MAX_PARALLEL_JOBS, _env_int("MAX_QUEUED_JOBS", 20))
CHUNK_TIMEOUT = max(60, _env_int("CHUNK_TIMEOUT", 150))  # Worker SYNTH_TIMEOUT is 110 s
PIPELINE_WINDOW_EXTRA = max(2, min(_env_int("PIPELINE_WINDOW_EXTRA", 16), 256))
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
# Bench ladder for a Worker Microsoft really throttled (403/429).  With a big
# fleet a short bench is enough - the work simply flows to the other Workers -
# and the level decays with clean chunks AND with time (QUARANTINE_DECAY_SECS).
_q_steps = [max(10, int(x)) for x in re.findall(r"\d+", _env("QUARANTINE_STEPS", "30,60,120,240"))]
QUARANTINE_STEPS: List[int] = _q_steps or [30, 60, 120, 240]
QUARANTINE_FORGIVE_AFTER = max(3, _env_int("QUARANTINE_FORGIVE_AFTER", 8))
QUARANTINE_DECAY_SECS = max(60, _env_int("QUARANTINE_DECAY_SECS", 300))
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

    # Every column the v5 code reads.  ``CREATE TABLE IF NOT EXISTS`` does nothing
    # for a database created by an older bot, so each column is checked one by
    # one (a missing column makes ``row["voice"]`` raise IndexError and kills
    # every handler).
    _SCHEMA: Dict[str, List[Tuple[str, str]]] = {
        "users": [("username", "TEXT"), ("first_name", "TEXT"), ("approved", "INTEGER DEFAULT 0"),
                  ("banned", "INTEGER DEFAULT 0"), ("expiry", "TEXT"), ("joined", "TEXT"), ("voice", "TEXT"),
                  ("rate", "TEXT"), ("pitch", "TEXT"), ("volume", "TEXT"), ("hours", "INTEGER"),
                  ("split_mode", "TEXT"), ("total_jobs", "INTEGER DEFAULT 0"), ("total_chars", "INTEGER DEFAULT 0"),
                  ("total_seconds", "REAL DEFAULT 0")],
        "servers": [("added", "TEXT"), ("enabled", "INTEGER DEFAULT 1"), ("note", "TEXT")],
        "jobs": [("user_id", "INTEGER"), ("title", "TEXT"), ("chars", "INTEGER"), ("chunks", "INTEGER"),
                 ("voice", "TEXT"), ("status", "TEXT"), ("created", "TEXT"), ("finished", "TEXT"),
                 ("duration", "REAL DEFAULT 0"), ("parts", "INTEGER DEFAULT 0"), ("error", "TEXT"),
                 ("chat_id", "INTEGER"), ("settings", "TEXT"), ("text_path", "TEXT"),
                 ("cursor", "INTEGER DEFAULT 0"), ("delivered_parts", "INTEGER DEFAULT 0"),
                 ("delivered_cursor", "INTEGER DEFAULT 0"), ("last_error", "TEXT"), ("stall_since", "TEXT"),
                 ("updated", "TEXT")],
        "requests": [("requested", "TEXT"), ("note", "TEXT")],
    }

    def _cols(self, table: str) -> set:
        return {r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")}

    def _has_table(self, table: str) -> bool:
        return self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None

    def _migrate(self) -> None:
        added: List[str] = []
        for table, columns in self._SCHEMA.items():
            have = self._cols(table)
            for col, typ in columns:
                if col not in have:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
                    added.append(f"{table}.{col}")
        if added:
            logging.getLogger("audiobook").info("database upgraded: added %s", ", ".join(added))
        self._import_legacy()

    def _import_legacy(self) -> None:
        """One-time import of data written by bot.py <= 4.3 (different table layout)."""
        if self.conn.execute("SELECT value FROM kv WHERE key='legacy_import_v5'").fetchone():
            return
        log_ = logging.getLogger("audiobook")
        now = now_str()
        ucols = self._cols("users")
        try:
            if "expiry_date" in ucols:
                # v4: approved == expiry_date in the future, is_admin == lifetime
                self.conn.execute(
                    "UPDATE users SET "
                    "  expiry = CASE WHEN expiry IS NULL AND COALESCE(is_admin,0)=0 THEN expiry_date ELSE expiry END,"
                    "  approved = CASE WHEN approved=1 OR COALESCE(is_admin,0)=1 "
                    "                   OR (expiry_date IS NOT NULL AND expiry_date > ?) THEN 1 ELSE 0 END",
                    (now,))
            if "is_banned" in ucols:
                self.conn.execute("UPDATE users SET banned = COALESCE(banned, 0) | COALESCE(is_banned, 0)")
            if "joined_at" in ucols:
                self.conn.execute("UPDATE users SET joined = COALESCE(joined, joined_at)")
            if "usage_count" in ucols:
                self.conn.execute("UPDATE users SET total_jobs = COALESCE(NULLIF(total_jobs,0), usage_count, 0)")
            if "chars_total" in ucols:
                self.conn.execute("UPDATE users SET total_chars = COALESCE(NULLIF(total_chars,0), chars_total, 0)")
            if "audio_seconds" in ucols:
                self.conn.execute("UPDATE users SET total_seconds = COALESCE(NULLIF(total_seconds,0), audio_seconds, 0)")
            if self._has_table("user_settings"):
                scols = self._cols("user_settings")
                for r in self.conn.execute("SELECT * FROM user_settings").fetchall():
                    self.conn.execute("INSERT OR IGNORE INTO users(user_id, joined) VALUES (?, ?)", (r["user_id"], now))
                    self.conn.execute(
                        "UPDATE users SET voice=COALESCE(voice,?), rate=COALESCE(rate,?), pitch=COALESCE(pitch,?),"
                        " volume=COALESCE(volume,?), hours=COALESCE(hours,?), split_mode=COALESCE(split_mode,?)"
                        " WHERE user_id=?",
                        (r["voice"] if "voice" in scols else None, r["rate"] if "rate" in scols else None,
                         r["pitch"] if "pitch" in scols else None, r["volume"] if "volume" in scols else None,
                         r["max_hours"] if "max_hours" in scols else None,
                         r["split_mode"] if "split_mode" in scols else None, r["user_id"]))
            if self._has_table("render_servers"):
                n = 0
                for r in self.conn.execute("SELECT url FROM render_servers").fetchall():
                    nu = normalize_server_url(r["url"] or "")
                    if nu:
                        cur = self.conn.execute("INSERT OR IGNORE INTO servers(url, added, enabled, note) VALUES (?,?,1,?)",
                                                (nu, now, "imported from v4 database"))
                        n += cur.rowcount
                if n:
                    log_.info("imported %d Worker URL(s) from the v4 database", n)
            jcols = self._cols("jobs")
            if "created_at" in jcols:
                self.conn.execute("UPDATE jobs SET created = COALESCE(created, created_at)")
            if "finished_at" in jcols:
                self.conn.execute("UPDATE jobs SET finished = COALESCE(finished, finished_at)")
            if "audio_seconds" in jcols:
                self.conn.execute("UPDATE jobs SET duration = COALESCE(NULLIF(duration,0), audio_seconds, 0)")
            if "source" in jcols:
                self.conn.execute("UPDATE jobs SET title = COALESCE(title, source)")
            # v4 jobs cannot be resumed by v5 (different checkpoint layout) - close them
            self.conn.execute("UPDATE jobs SET status='failed', error='not resumable after upgrade', finished=? "
                              "WHERE status IN ('queued','running','paused','interrupted') AND text_path IS NULL", (now,))
        except sqlite3.Error as e:
            log_.warning("legacy import skipped: %s", e)
        self.conn.execute("INSERT OR REPLACE INTO kv(key, value) VALUES ('legacy_import_v5', ?)", (now,))

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
        keys = row.keys()

        def g(k: str, default: Any) -> Any:
            return (row[k] if k in keys else None) or default

        return {
            "voice": g("voice", DEFAULT_VOICE), "rate": g("rate", DEFAULT_RATE),
            "pitch": g("pitch", DEFAULT_PITCH), "volume": g("volume", DEFAULT_VOLUME),
            "hours": g("hours", DEFAULT_HOURS), "split_mode": g("split_mode", DEFAULT_SPLIT),
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

    def counts(self) -> Dict[str, int]:
        u = self.one("SELECT COUNT(*) AS n, COALESCE(SUM(approved),0) AS a, COALESCE(SUM(banned),0) AS b FROM users")
        p = self.one("SELECT COUNT(*) AS n FROM requests")
        return {"users": int(u["n"]), "approved": int(u["a"]), "banned": int(u["b"]), "pending": int(p["n"])}


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
    """The Worker reported that Microsoft is really rate-limiting it (403/429 -> 503 kind=throttle)."""

    def __init__(self, msg: str, retry_after: int = 0):
        super().__init__(msg)
        self.retry_after = max(0, int(retry_after or 0))


class TransientError(RuntimeError):
    """Upstream hiccup (timeout, reset, empty audio) - retry on another Worker, do not bench this one."""


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


def _retry_after(resp: aiohttp.ClientResponse) -> int:
    try:
        return max(0, int(float(resp.headers.get("Retry-After") or 0)))
    except ValueError:
        return 0


async def _describe_http_error(resp: aiohttp.ClientResponse) -> Tuple[str, str]:
    """(kind, message) for a non-200 Worker reply. kind in throttle/busy/voice/fatal/retry."""
    detail = ""
    declared = (resp.headers.get("X-Error-Kind") or "").strip().lower()
    try:
        body = await resp.text()
        try:
            j = json.loads(body)
            if isinstance(j, dict):
                detail = str(j.get("error") or j.get("message") or "")
                declared = declared or str(j.get("kind") or "").strip().lower()
        except ValueError:
            detail = body.strip()
    except Exception:  # noqa: BLE001
        pass
    detail = re.sub(r"\s+", " ", detail)[:160]
    low = detail.lower()
    st = resp.status
    # Worker >= 5.2 tells us exactly what happened - no guessing needed.
    if declared == "throttle" and st in (429, 502, 503):
        return "throttle", f"HTTP {st}: Microsoft is rate-limiting this Worker{(' - ' + detail) if detail else ''}"
    if declared == "transient" and st in (502, 503, 504):
        return "retry", f"HTTP {st}: upstream hiccup{(' - ' + detail) if detail else ''}"
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
        # Older Workers (< 5.2): only an explicit rate-limit signal is throttling.
        # Timeouts, resets and "no audio" are transient and must NOT bench the Worker.
        if re.search(r"\b(403|429)\b", low) or "throttl" in low or "too many" in low or "rate limit" in low:
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
        # The Worker has an API_KEY secret but the bot has none - every /tts would be 401.
        info["ok"] = False
        info["error"] = ("Worker has an API_KEY secret - put the same value in TTS_API_KEY on the bot "
                         "(or remove the secret from the Worker)")
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
        # transient failures (timeouts / resets / empty audio) in a sliding window
        self.soft_fails: List[float] = []
        # round-robin bookkeeping: when many Workers are equally free we rotate
        # through them so the load (and Microsoft's attention) is spread thin
        self.last_used = 0.0

    @property
    def benched(self) -> bool:
        return time.monotonic() < self.q_until

    @property
    def bench_left(self) -> float:
        return max(0.0, self.q_until - time.monotonic())

    def _decay(self) -> None:
        """Quarantine level fades with time: every QUARANTINE_DECAY_SECS without a new incident = one level."""
        if self.q_level and self.q_set_at:
            idle = time.monotonic() - self.q_set_at
            drop = int(idle // QUARANTINE_DECAY_SECS)
            if drop > 0:
                self.q_level = max(0, self.q_level - drop)
                self.q_set_at += drop * QUARANTINE_DECAY_SECS

    def quarantine(self, reason: str, hint: int = 0) -> float:
        now = time.monotonic()
        if self.benched and now - self.q_set_at < 10:
            # Requests that were already in flight when the server got benched all
            # fail together - that is one incident, not several escalations.
            return self.bench_left
        self._decay()
        secs = QUARANTINE_STEPS[min(self.q_level, len(QUARANTINE_STEPS) - 1)]
        if hint:
            # the Worker's Retry-After knows best, but never longer than our ladder step
            secs = max(10, min(secs, hint))
        self.q_until = now + secs
        self.q_set_at = now
        self.q_level = min(self.q_level + 1, len(QUARANTINE_STEPS))
        self.q_count += 1
        self.q_reason = reason[:120]
        self.clean_since_q = 0
        self.soft_fails.clear()
        self.limiter.limit = 1
        self.limiter.clean = 0
        return secs

    def soft_fail(self, reason: str) -> Optional[float]:
        """A transient failure. Returns the bench length only if the Worker keeps failing."""
        now = time.monotonic()
        self.chunks_failed += 1
        self.soft_fails = [t for t in self.soft_fails if now - t < SOFT_FAIL_WINDOW]
        self.soft_fails.append(now)
        if len(self.soft_fails) >= SOFT_FAIL_LIMIT:
            return self.quarantine(f"{len(self.soft_fails)} failures in {SOFT_FAIL_WINDOW}s: {reason}")
        return None

    def record_success(self) -> None:
        self.chunks_done += 1
        self.clean_since_q += 1
        if self.soft_fails:
            self.soft_fails.pop(0)
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


async def _gather_limited(coros: List[Any], limit: int) -> List[Any]:
    """asyncio.gather(return_exceptions=True) with at most ``limit`` coroutines running."""
    sem = asyncio.Semaphore(max(1, limit))

    async def run(c):
        async with sem:
            try:
                return await c
            except Exception as e:  # noqa: BLE001
                return e

    return list(await asyncio.gather(*(run(c) for c in coros)))


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
        """Least-loaded Worker; ties broken by *least recently used* so a big fleet
        is rotated evenly instead of the same few fast Workers being hammered."""
        cands = [s for s in self.usable() if s.url not in exclude and s.limiter.free > 0]
        if not cands:
            cands = [s for s in self.usable() if s.url not in exclude]
            if not cands:
                return None
        cands.sort(key=lambda s: (s.limiter.active, s.last_used, s.latency))
        best = cands[0]
        best.last_used = time.monotonic()
        return best

    def any_free(self, exclude: set) -> bool:
        return any(s.limiter.free > 0 for s in self.usable() if s.url not in exclude)

    def next_bench_end(self) -> float:
        benched = [s.bench_left for s in self.servers.values() if s.benched]
        return min(benched) if benched else 0.0

    def capacity(self) -> int:
        return sum(s.limiter.limit for s in self.usable())

    async def refresh(self, session: aiohttp.ClientSession, deep: bool = False,
                      only: Optional[List[ServerState]] = None) -> List[Dict[str, Any]]:
        self.sync_from_db()
        sts = only if only is not None else self.states()
        if not sts:
            return []
        results = await _gather_limited([check_server(session, s.url, deep=deep) for s in sts], HEALTH_PARALLELISM)
        out: List[Dict[str, Any]] = []
        for s, info in zip(sts, results):
            if isinstance(info, Exception):
                info = {"url": s.url, "ok": False, "error": f"{type(info).__name__}"}
            s.apply_health(info)
            if info.get("ok") and info.get("throttled") and not s.benched:
                s.quarantine("Worker reports Microsoft throttling")
            out.append(info)
        return out

    def counts(self) -> Dict[str, int]:
        sts = self.states()
        benched = sum(1 for s in sts if s.benched)
        down = sum(1 for s in sts if not s.ok and not s.benched)
        return {"total": len(sts), "ok": len(sts) - benched - down, "benched": benched, "down": down,
                "active": sum(s.limiter.active for s in sts), "capacity": self.capacity()}

    def summary(self, max_names: int = 3) -> str:
        """One line that stays short even with 100+ Workers."""
        sts = self.states()
        if not sts:
            return "no servers"
        if len(sts) <= max_names:
            return " · ".join(s.describe() for s in sts)
        c = self.counts()
        txt = f"✅ {c['ok']}/{c['total']} Workers · {c['active']}/{c['capacity']} in flight"
        if c["benched"]:
            txt += f" · ⛔ {c['benched']} benched"
        if c["down"]:
            txt += f" · ❌ {c['down']} down"
        return txt


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
                raise ThrottleError(msg, _retry_after(r))
            if kind == "busy":
                raise BusyError(msg)
            if kind == "voice":
                raise VoiceError(msg)
            if kind == "fatal":
                raise FatalServerError(msg)
            raise TransientError(msg)
        data = await r.read()
        if len(data) < 200:
            # not a rate limit - just a bad reply; another Worker will do it
            raise TransientError("Worker returned an empty audio stream")
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
    # With a big fleet one chunk should hop across a handful of Workers, not all
    # 100 of them - the pipeline re-queues it after a short pause anyway.
    attempts = max(2, min(len(pool.servers) + 1, 6))
    second_pass = False

    def soft(srv: ServerState, reason: str) -> None:
        secs = srv.soft_fail(reason)
        if secs:
            log.warning("chunk %d: %s -> keeps failing, benched %ds", index + 1, reason, secs)

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
        # Reserve the Worker slot FIRST (cheap, spreads load), then a global
        # network slot - the other way round a queued request could pin a
        # network slot while waiting for a Worker that never frees up.
        await srv.limiter.acquire()
        try:
            async with _network_slots:
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
                    secs = srv.quarantine(str(e), hint=e.retry_after)
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
                    await srv.limiter.busy()
                    soft(srv, last_err)
                    tried.add(srv.url)
                except (aiohttp.ClientError, OSError) as e:
                    last_err = f"{short_server(srv.url)} -> {type(e).__name__}: {str(e)[:60]}"
                    soft(srv, last_err)
                    tried.add(srv.url)
                    await cancellable_sleep(0.3 + random.random() * 0.7, cancel)
                except TransientError as e:
                    last_err = f"{short_server(srv.url)} -> {e}"
                    soft(srv, last_err)
                    tried.add(srv.url)
                    await cancellable_sleep(0.3 + random.random() * 0.7, cancel)
                except RuntimeError as e:
                    last_err = f"{short_server(srv.url)} -> {e}"
                    soft(srv, last_err)
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
    stage: str = "starting"      # starting / queued / preparing / synth / uploading / paused / done
    upload_note: str = ""        # "part 2 · 45%" while an MP3 is being sent
    parts_sent: int = 0

    @property
    def stale(self) -> bool:
        """True when the job can no longer make progress (task finished/crashed or never started)."""
        if self.task is not None:
            return self.task.done()
        return time.time() - self.queued_at > 180


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
    old = active_jobs.get(uid)
    if old is not None and old.stale:
        # a crashed / never-started job must not block the user forever
        log.warning("dropping stale job of user %s (%s)", uid, old.title[:30])
        release_job(old)
        old = None
    if old is not None:
        return None
    if len(job_queue.waiting) >= MAX_QUEUED_JOBS:
        return None
    job = Job(user_id=uid, title=title, chars=chars)
    active_jobs[uid] = job
    return job


def current_job(uid: int) -> Optional[Job]:
    """The user's live job, dropping a stale one on the way."""
    job = active_jobs.get(uid)
    if job is not None and job.stale:
        release_job(job)
        return None
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


# The five stages every job walks through, shown as a compact step line so the
# user always knows *where* the job is, not only what percentage it has reached.
JOB_STAGES = [("queued", "Queue"), ("preparing", "Prepare"), ("synth", "Voice"), ("uploading", "Send"),
              ("done", "Done")]


def stage_line(current: str) -> str:
    order = [k for k, _ in JOB_STAGES]
    cur = current if current in order else ("synth" if current == "paused" else "queued")
    idx = order.index(cur)
    parts = []
    for i, (key, label) in enumerate(JOB_STAGES):
        if i < idx:
            parts.append(f"✅ {label}")
        elif i == idx:
            parts.append(f"{'⏸' if current == 'paused' else '🔵'} <b>{label}</b>")
        else:
            parts.append(f"⚪ {label}")
    return " › ".join(parts)


def failure_budget() -> int:
    """Chunk attempts that may fail in a row before the job drains and pauses.

    Scales with the fleet (a 100-Worker fleet legitimately produces more scattered
    hiccups) but is capped so a real outage is noticed within a minute or two.
    """
    return max(6, min(3 * max(1, len(pool.servers)), 40))


def job_card(header: str, job: Job, body: str, footer: str = "") -> str:
    txt = f"{header}\n{stage_line(job.stage)}\n\n{body}"
    if footer:
        txt += f"\n\n{footer}"
    return txt


async def wait_for_engines(session: aiohttp.ClientSession, cancel: asyncio.Event, status: Optional[Message],
                           job_id: int, header: str, stall_round: int, reason: str) -> None:
    """Every engine failed: pause with escalating back-off, re-probe, continue."""
    delay = STALL_BACKOFF_STEPS[min(stall_round, len(STALL_BACKOFF_STEPS) - 1)]
    bench = pool.next_bench_end()
    if bench and not pool.usable():
        delay = max(5, min(delay, int(bench) + 1))
    c = pool.counts()
    if c["total"] and c["benched"] == c["total"]:
        why = "Microsoft is rate-limiting every TTS Worker"
    elif c["total"] and c["ok"] == 0:
        why = "No TTS Worker is reachable"
    else:
        why = "Every attempt failed in a row (upstream hiccups)"
    tip = ""
    if c["total"] < 5:
        tip = ("\n💡 Only " + str(c["total"]) + " Worker(s) registered - deploy more (free Cloudflare accounts) "
               "and add them in 🛠 Admin → Workers; the job spreads over all of them.")
    await safe_edit(status,
                    f"{header}\n{stage_line('paused')}\n\n"
                    f"⏸ <b>Paused</b> - {why}.\n"
                    f"Last error: <code>{html.escape(reason[:160])}</code>\n"
                    f"Workers: {html.escape(pool.summary())}{tip}\n"
                    f"Retrying in {delay}s (round {stall_round + 1}). The job does not give up - /cancel to stop.",
                    cancel_markup(job_id))
    await cancellable_sleep(delay, cancel)
    if cancel.is_set():
        raise JobCancelled()
    # Re-probe only the Workers that are in trouble; a healthy fleet of 100 does
    # not need 100 /health calls every time one chunk round failed.
    trouble = [s for s in pool.states() if (not s.ok) or s.benched]
    await pool.refresh(session, only=trouble if trouble and len(trouble) < len(pool.states()) else None)


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

    base_header = f"🎧 <b>{html.escape(title[:60])}</b>"
    try:
        # ---------------- queue -------------------------------------------
        job.stage = "queued"
        if job_queue.running and len(job_queue.running) >= job_queue.parallel:
            await safe_edit(status,
                            job_card(base_header, job,
                                     f"🕒 <b>Waiting in queue</b> - position #{len(job_queue.waiting) + 1}\n"
                                     f"Running {len(job_queue.running)}/{job_queue.parallel} · /queue for details"),
                            cancel_markup(job_id or 0))
        enter = asyncio.ensure_future(job_queue.enter(job))
        while not enter.done():
            await asyncio.wait({enter}, timeout=15)
            if not enter.done():
                pos = job_queue.position(job)
                eta = job_queue.eta_seconds(job)
                await safe_edit(status,
                                job_card(base_header, job,
                                         f"🕒 <b>Waiting in queue</b> - position #{pos} of {len(job_queue.waiting)}\n"
                                         f"Running {len(job_queue.running)}/{job_queue.parallel} · "
                                         f"estimated start ~{fmt_duration(eta)}"),
                                cancel_markup(job_id or 0))
        enter.result()  # raises JobCancelled

        # ---------------- prepare -----------------------------------------
        job.stage = "preparing"
        if MIN_FREE_DISK_MB and free_disk_mb(JOBS_DIR if os.path.isdir(JOBS_DIR) else BASE_DIR) < MIN_FREE_DISK_MB:
            raise RuntimeError(f"Not enough free disk space on the bot server (< {MIN_FREE_DISK_MB} MB)")
        await safe_edit(status, job_card(base_header, job, "🔍 Checking TTS Workers…"), cancel_markup(job_id or 0))
        await pool.refresh(session)
        if not pool.states():
            raise RuntimeError("No TTS Worker registered yet. Admin: 🛠 Admin → Workers → ➕ Add "
                               "(or put the URL in TTS_SERVERS).")
        if not pool.usable():
            await safe_edit(status,
                            job_card(base_header, job,
                                     "⚠️ No Worker is answering right now - I will keep retrying.\n"
                                     f"Workers: {html.escape(pool.summary())}"),
                            cancel_markup(job_id or 0))

        await safe_edit(status, job_card(base_header, job, "📖 Reading the text…"), cancel_markup(job_id or 0))
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
            await safe_edit(status,
                            job_card(base_header, job,
                                     f"✂️ Splitting {len(text):,} characters into chunks of ~{chunk_limit:,}…"),
                            cancel_markup(job_id))
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
        header = f"{base_header}\n🗣 {html.escape(voice_label(settings['voice']))}"
        if voice_note:
            header += f"\n{voice_note}"
        job.stage = "synth"
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
                    def on_upload(sent: int, total_b: int, _no: int = p_no) -> None:
                        pct_u = int(100 * sent / max(1, total_b))
                        job.upload_note = f"part {_no} · {pct_u}%"

                    job.upload_note = f"part {p_no} · preparing"
                    await deliver_part(client, chat_id, p_path, p_no, p_dur, title, p_title, settings, cancel,
                                       progress=on_upload)
                    parts_sent = max(parts_sent, p_no)
                    job.parts_sent = parts_sent
                    job.upload_note = ""
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
                    job.upload_note = ""
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

        def audio_so_far() -> float:
            # seconds of audio already synthesised (chunk metas of finished chunks + open part)
            return (total_duration_ms + part_dur) / 1000.0

        async def progress(force: bool = False) -> None:
            nonlocal last_edit
            now = time.time()
            if not force and now - last_edit < PROGRESS_EDIT_INTERVAL:
                return
            last_edit = now
            pct = int(100 * job.done_chars / max(1, total_chars))
            elapsed = max(1e-6, now - t_start)
            cps = (job.done_chars - chars_at_start) / elapsed
            remaining = max(0, total_chars - job.done_chars)
            eta = remaining / cps if cps > 0 else 0
            spoken = estimate_seconds(job.done_chars - chars_at_start, settings["rate"])
            rt = spoken / elapsed if elapsed > 0 else 0
            inflight = sum(s.limiter.active for s in pool.states())
            limit = pool.capacity()
            body = (f"{progress_bar(pct)} <b>{pct}%</b>\n"
                    f"📝 {job.done_chars:,} / {total_chars:,} chars · chunk {job.done_chunks}/{total}\n"
                    f"🎵 {fmt_duration(audio_so_far())} of audio ready\n"
                    f"⚡ {cps:,.0f} chars/s (~{rt:.0f}× realtime) · {inflight}/{limit} requests in flight\n"
                    f"⏱ {fmt_duration(elapsed)} elapsed · ETA {fmt_duration(eta) if cps > 0 else '…'}")
            up = f"📤 Sending {job.upload_note}" if job.upload_note else f"📤 Parts sent: {parts_sent}"
            if upload_queue.qsize() > 0 and not job.upload_note:
                up += f" · {upload_queue.qsize()} waiting"
            footer = f"{up}\n🖥 {html.escape(pool.summary())}"
            r_cnt, w_cnt = job_queue.counts()
            if w_cnt:
                footer += f"\n🧾 Queue: {r_cnt} running · {w_cnt} waiting"
            job.status_line = f"{pct}% · {job.done_chunks}/{total}"
            await safe_edit(status, job_card(header, job, body, footer), cancel_markup(job_id))

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
            window = min(pool.capacity() + PIPELINE_WINDOW_EXTRA, MAX_TOTAL_CONCURRENCY + PIPELINE_WINDOW_EXTRA)
            if consecutive_failures >= failure_budget():
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
                elif not pool.usable() or consecutive_failures >= failure_budget():
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
                if round_failed and pool.usable() and consecutive_failures < failure_budget():
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
        job.stage = "uploading"
        await upload_queue.put(None)
        total_parts = part_no - 1
        # live upload progress while the last part(s) are being sent
        first = True
        while not uploader_task.done():
            if not first:
                await asyncio.wait({uploader_task}, timeout=PROGRESS_EDIT_INTERVAL)
            first = False
            if uploader_task.done():
                break
            note = job.upload_note or (f"part {parts_sent + 1} · preparing" if parts_sent < total_parts else "finishing…")
            waiting = max(0, total_parts - parts_sent - 1)
            if waiting:
                note += f" · {waiting} more waiting"
            await safe_edit(status,
                            job_card(header, job,
                                     f"{progress_bar(100)} <b>100%</b>\n"
                                     f"🎵 {fmt_duration(total_duration_ms / 1000)} of audio · {total_parts} part(s)\n"
                                     f"📤 Sending {html.escape(note)} · {parts_sent}/{total_parts} delivered"),
                            cancel_markup(job_id))
        await uploader_task
        uploader_task = None

        if upload_errors:
            raise RuntimeError("Upload failed: " + "; ".join(upload_errors[:3]))
        elapsed = time.time() - t_start
        dur_s = total_duration_ms / 1000.0
        job.stage = "done"
        db.update_job(job_id, status="done", finished=now_str(), duration=dur_s, parts=parts_sent)
        db.add_usage(uid, total_chars, dur_s)
        cleanup_job_dir(job_id)
        with contextlib.suppress(OSError):
            os.remove(text_path)
        speed = (dur_s / elapsed) if elapsed > 0 else 0
        await safe_edit(status,
                        f"✅ <b>Done</b> - {html.escape(title[:60])}\n{stage_line('done')}\n\n"
                        f"🎵 {fmt_duration(dur_s)} of audio in {parts_sent} part(s)\n"
                        f"📝 {total_chars:,} characters · 🗣 {html.escape(voice_label(settings['voice']))}\n"
                        f"⏱ Generated in {fmt_duration(elapsed)} (~{speed:.0f}× realtime)",
                        InlineKeyboardMarkup([[InlineKeyboardButton("🎧 New audiobook", callback_data="new"),
                                               InlineKeyboardButton("⚙️ Settings", callback_data="back_settings")]]))
    except JobCancelled:
        if job_id:
            db.update_job(job_id, status="cancelled", finished=now_str())
            cleanup_job_dir(job_id)
        with contextlib.suppress(OSError):
            os.remove(text_path)
        await safe_edit(status, f"⛔ <b>Cancelled</b> - {html.escape(title[:60])}")
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
        await safe_edit(status, f"❌ <b>Failed</b> - {html.escape(title[:60])}\n\n{html.escape(str(e)[:400])}",
                        InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Try again", callback_data="new")]]))
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
                       chapter: str, settings: Dict[str, Any], cancel: asyncio.Event,
                       progress: Optional[Any] = None) -> None:
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
                                    performer="AudioBook Pro", duration=int(dur_ms / 1000), file_name=fname,
                                    progress=progress)
            return
        except FloodWait as e:
            await asyncio.sleep(min(120, int(getattr(e, "value", 10)) + 1))
        except (RPCError, OSError, asyncio.TimeoutError) as e:
            log.warning("send_audio attempt %d failed: %s", attempt + 1, e)
            if attempt == 3:
                raise
            await asyncio.sleep(5 * (attempt + 1))


# =============================================================================
# Keyboards & UI helpers
# =============================================================================
# The reply keyboard is deliberately tiny (two rows) so it never covers the
# chat; everything else lives in inline menus that edit themselves in place.
BTN_CREATE = "🎧 Create Audio"
BTN_SETTINGS = "⚙️ Settings"
BTN_STATUS = "📊 My Status"
BTN_HELP = "❓ Help"
BTN_REQUEST = "🔑 Request Access"
BTN_ADMIN = "🛠 Admin"
MAIN_BUTTONS: Dict[str, str] = {BTN_CREATE: "create", BTN_SETTINGS: "settings", BTN_STATUS: "status",
                                BTN_HELP: "help", BTN_REQUEST: "request", BTN_ADMIN: "admin"}
# Buttons of older keyboards that may still be cached in a user's Telegram app.
LEGACY_BUTTONS: Dict[str, str] = {
    "🔊 Voice Preview": "preview", "📜 History": "history", "👤 My Account": "status", "📊 Status": "status",
    "🛠 Admin Panel": "admin", "🔙 Back": "home", "➕ Add Server": "adm_add", "➖ Remove Server": "adm_workers",
    "🖥 Server Status": "adm_workers", "✅ Approve User": "adm_users", "🚫 Revoke / Ban": "adm_users",
    "👥 Users": "adm_users", "📢 Broadcast": "adm_broadcast", "📈 Stats": "adm_stats", "🧾 Jobs": "adm_jobs",
}
ALL_BUTTONS = set(MAIN_BUTTONS) | set(LEGACY_BUTTONS)
USER_COMMANDS = ["start", "help", "status", "queue", "account", "history", "settings", "preview", "cancel", "resume",
                 "jobs", "admin", "servers", "users", "stats", "addserver", "add_server", "menu"]


def main_keyboard(uid: int) -> ReplyKeyboardMarkup:
    if not db.is_approved(uid):
        rows = [[KeyboardButton(BTN_REQUEST), KeyboardButton(BTN_HELP)]]
    else:
        second = [KeyboardButton(BTN_STATUS)]
        if uid == OWNER_ID:
            second.append(KeyboardButton(BTN_ADMIN))
        second.append(KeyboardButton(BTN_HELP))
        rows = [[KeyboardButton(BTN_CREATE), KeyboardButton(BTN_SETTINGS)], second]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True,
                               placeholder="Send a document or paste text…")


def ikb(rows: List[List[Tuple[str, str]]]) -> InlineKeyboardMarkup:
    """Inline keyboard from [[(label, callback_data), ...], ...]."""
    return InlineKeyboardMarkup([[InlineKeyboardButton(t, callback_data=d) for t, d in row] for row in rows])


CLOSE_ROW = [("✖️ Close", "close")]


def settings_text(uid: int) -> str:
    s = db.settings(uid)
    return ("⚙️ <b>Settings</b>\n\n"
            f"🗣 Voice: <b>{html.escape(voice_label(s['voice']))}</b>\n"
            f"⏩ Rate <b>{s['rate']}</b> · 🎚 Pitch <b>{s['pitch']}</b> · 🔉 Volume <b>{s['volume']}</b>\n"
            f"⏱ Max <b>{s['hours']} h</b> per MP3 part · "
            f"✂️ split <b>{'by chapter' if s['split_mode'] == 'chapter' else 'by duration'}</b>")


def settings_markup(uid: int) -> InlineKeyboardMarkup:
    return ikb([
        [("🗣 Voice", "set:voice"), ("⏩ Rate", "set:rate")],
        [("🎚 Pitch", "set:pitch"), ("🔉 Volume", "set:volume")],
        [("⏱ Hours/part", "set:hours"), ("✂️ Split mode", "set:split")],
        [("🔊 Preview voice", "preview"), ("↩️ Reset", "reset_settings")],
        CLOSE_ROW,
    ])


def voice_groups_markup() -> InlineKeyboardMarkup:
    names = list(VOICE_GROUPS)
    rows: List[List[Tuple[str, str]]] = []
    for i in range(0, len(names), 2):
        rows.append([(g, f"vg:{names.index(g)}") for g in names[i:i + 2]])
    rows.append([("✏️ Custom voice id", "voice_custom"), ("🔙 Back", "back_settings")])
    return ikb(rows)


def voice_list_markup(group_index: int, current: str) -> InlineKeyboardMarkup:
    group = list(VOICE_GROUPS.keys())[group_index]
    items = VOICE_GROUPS[group]
    rows: List[List[Tuple[str, str]]] = []
    for i in range(0, len(items), 2):
        rows.append([(("✅ " if vid == current else "") + lbl, f"voice:{vid}") for lbl, vid in items[i:i + 2]])
    rows.append([("🔙 Back", "set:voice")])
    return ikb(rows)


def options_markup(prefix: str, options: List[Tuple[str, str]], current: str, cols: int = 3) -> InlineKeyboardMarkup:
    rows: List[List[Tuple[str, str]]] = []
    row: List[Tuple[str, str]] = []
    for lbl, val in options:
        row.append((("✅ " if val == current else "") + lbl, f"{prefix}:{val}"))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([("🔙 Back", "back_settings")])
    return ikb(rows)


def approval_markup(target: int, prefix: str = "approve", ban_prefix: str = "ban") -> InlineKeyboardMarkup:
    return ikb([
        [("7 days", f"{prefix}:{target}:7"), ("30 days", f"{prefix}:{target}:30"),
         ("90 days", f"{prefix}:{target}:90")],
        [("♾ Lifetime", f"{prefix}:{target}:0"), ("🚫 Ban", f"{ban_prefix}:{target}")],
    ])


HELP_TEXT = (
    "❓ <b>How it works</b>\n\n"
    "1️⃣ Send a document (<code>.txt .md .docx .html .epub .pdf</code>, up to {mb} MB) or paste text.\n"
    "2️⃣ Confirm - I split the book, voice it in parallel and send MP3 part(s) "
    "(max N hours each, or one per chapter - see Settings).\n"
    "3️⃣ Watch the live card: Queue › Prepare › Voice › Send › Done, with %, speed and ETA.\n\n"
    "<b>Commands</b>\n"
    "/settings voice, rate, pitch, volume, split\n"
    "/preview hear the selected voice\n"
    "/status your job, account, history\n"
    "/cancel stop your current job\n"
    "/resume continue an interrupted job\n"
    "/queue · /history · /help"
)


def help_markup(uid: int) -> InlineKeyboardMarkup:
    rows = [[("⚙️ Settings", "back_settings"), ("🔊 Preview voice", "preview")]]
    if not db.is_approved(uid):
        rows = [[("🔑 Request access", "request")]]
    rows.append(CLOSE_ROW)
    return ikb(rows)


def is_admin(uid: int) -> bool:
    return uid == OWNER_ID


async def deny(message: Message) -> None:
    await message.reply_text(
        "🔒 You are not approved to use this bot yet.\n"
        f"Press <b>{BTN_REQUEST}</b> or contact @{CONTACT_USERNAME}.",
        reply_markup=main_keyboard(message.from_user.id))


def _user_line(u) -> str:
    return html.escape(u.first_name or u.username or str(u.id))


# =============================================================================
# Status card (account + current job + queue) - one message, inline actions
# =============================================================================
def job_summary_line(job: Job) -> str:
    stage = dict(JOB_STAGES).get(job.stage, job.stage)
    if job.running:
        st = f"{job.status_line or 'starting'} · {stage}"
    else:
        st = f"queued #{job_queue.position(job)} · starts in ~{fmt_duration(job_queue.eta_seconds(job))}"
    return f"▶️ <b>{html.escape(job.title[:40])}</b> - {st}"


def status_text(uid: int, username: Optional[str] = None, first_name: Optional[str] = None) -> str:
    row = db.ensure_user(uid, username, first_name)
    if uid == OWNER_ID:
        access = "👑 owner"
    elif db.is_approved(uid):
        access = f"✅ approved until {row['expiry'][:10]}" if row["expiry"] else "✅ approved (lifetime)"
    elif row["banned"]:
        access = "🚫 banned"
    elif db.one("SELECT 1 FROM requests WHERE user_id=?", (uid,)):
        access = "⏳ request pending"
    else:
        access = "🔒 not approved"
    r_cnt, w_cnt = job_queue.counts()
    lines = ["📊 <b>My Status</b>",
             f"👤 <code>{uid}</code> · {access}",
             f"📚 {row['total_jobs'] or 0} audiobooks · {(row['total_chars'] or 0):,} chars · "
             f"{fmt_duration(row['total_seconds'] or 0)} audio", ""]
    job = current_job(uid)
    if job:
        lines.append(job_summary_line(job))
    else:
        resumable = [r for r in db.unfinished_jobs() if r["user_id"] == uid]
        if resumable:
            r = resumable[0]
            lines.append(f"💤 Interrupted: <b>{html.escape((r['title'] or '')[:40])}</b> "
                         f"({r['cursor'] or 0}/{r['chunks'] or '?'} chunks) - press Resume")
        else:
            lines.append("💤 No job running - send a document or text to start.")
    lines.append("")
    lines.append(f"🖥 Workers online {len(pool.usable())}/{len(pool.states())} · "
                 f"🧾 queue {r_cnt} running · {w_cnt} waiting")
    return "\n".join(lines)


def status_markup(uid: int) -> InlineKeyboardMarkup:
    rows: List[List[Tuple[str, str]]] = []
    job = current_job(uid)
    if job:
        rows.append([("⛔ Cancel my job", f"canceljob:{job.job_id}"), ("🧾 Queue", "queue")])
    else:
        resumable = [r for r in db.unfinished_jobs() if r["user_id"] == uid]
        if resumable:
            rows.append([("🔄 Resume", "resume"), ("🧾 Queue", "queue")])
        else:
            rows.append([("🧾 Queue", "queue")])
    rows.append([("📜 History", "history"), ("🔊 Preview voice", "preview")])
    rows.append([("🔄 Refresh", "status")] + CLOSE_ROW)
    return ikb(rows)


def history_text(uid: int) -> str:
    rows = db.user_history(uid, 12)
    if not rows:
        return "📜 <b>History</b>\n\nNo audiobooks yet."
    lines = ["📜 <b>Recent audiobooks</b>"]
    for r in rows:
        icon = {"done": "✅", "failed": "❌", "cancelled": "⛔", "running": "▶️", "paused": "⏸",
                "queued": "🕒", "interrupted": "💤"}.get(r["status"], "•")
        extra = f" · {fmt_duration(r['duration'])} · {r['parts']} part(s)" if r["status"] == "done" else f" · {r['status']}"
        lines.append(f"{icon} #{r['id']} {html.escape((r['title'] or '')[:32])} · {(r['chars'] or 0):,} ch{extra}"
                     f"\n      <i>{(r['created'] or '')[:16]}</i>")
    return "\n".join(lines)


def queue_text(uid: int) -> str:
    r_cnt, w_cnt = job_queue.counts()
    lines = [f"🧾 <b>Job queue</b> · {r_cnt}/{job_queue.parallel} running · {w_cnt} waiting"]
    if not job_queue.running and not job_queue.waiting:
        lines.append("\nThe queue is empty - your job would start immediately.")
    for j in job_queue.running:
        who = f"user {j.user_id}" if is_admin(uid) else ("you" if j.user_id == uid else "another user")
        lines.append(f"▶️ {who}: {html.escape(j.title[:30])} · {j.status_line or 'starting'}")
    for n, j in enumerate(job_queue.waiting, 1):
        who = f"user {j.user_id}" if is_admin(uid) else ("you" if j.user_id == uid else "another user")
        lines.append(f"#{n} {who}: {html.escape(j.title[:30])} · {j.chars:,} chars · ~{fmt_duration(job_queue.eta_seconds(j))}")
    return "\n".join(lines)


BACK_STATUS = [("🔙 Back", "status")] + CLOSE_ROW


# =============================================================================
# Basic commands
# =============================================================================
@bot.on_message(filters.command(["start", "menu"]) & filters.private)
async def cmd_start(client: Client, message: Message):
    u = message.from_user
    db.ensure_user(u.id, u.username, u.first_name)
    admin_states.pop(u.id, None)
    approved = db.is_approved(u.id)
    txt = (f"👋 Hello <b>{_user_line(u)}</b>!\n\n"
           "I turn books, stories and documents into <b>MP3 audiobooks</b> with natural neural voices "
           "(Hindi, English and other Indian languages).\n\n")
    if approved:
        txt += "📄 Send me a document or paste some text to begin."
        if is_admin(u.id) and not db.servers():
            txt += "\n\n⚠️ No TTS Worker registered yet - open <b>🛠 Admin → Workers → ➕ Add</b>."
    else:
        txt += f"Access is by approval - press <b>{BTN_REQUEST}</b> to ask the owner."
    await message.reply_text(txt, reply_markup=main_keyboard(u.id))


@bot.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, message: Message):
    await message.reply_text(HELP_TEXT.format(mb=MAX_FILE_MB), reply_markup=help_markup(message.from_user.id))


@bot.on_callback_query(filters.regex(r"^help$"))
async def cb_help(client: Client, cq: CallbackQuery):
    await safe_edit(cq.message, HELP_TEXT.format(mb=MAX_FILE_MB), help_markup(cq.from_user.id))
    await cq.answer()


@bot.on_message(filters.command(["status", "account"]) & filters.private)
async def cmd_status(client: Client, message: Message):
    u = message.from_user
    await message.reply_text(status_text(u.id, u.username, u.first_name), reply_markup=status_markup(u.id))


@bot.on_callback_query(filters.regex(r"^status$"))
async def cb_status(client: Client, cq: CallbackQuery):
    u = cq.from_user
    await safe_edit(cq.message, status_text(u.id, u.username, u.first_name), status_markup(u.id))
    await cq.answer()


@bot.on_message(filters.command("queue") & filters.private)
async def cmd_queue(client: Client, message: Message):
    await message.reply_text(queue_text(message.from_user.id), reply_markup=ikb([BACK_STATUS]))


@bot.on_callback_query(filters.regex(r"^queue$"))
async def cb_queue(client: Client, cq: CallbackQuery):
    await safe_edit(cq.message, queue_text(cq.from_user.id), ikb([[("🔄 Refresh", "queue")], BACK_STATUS]))
    await cq.answer()


@bot.on_message(filters.command("history") & filters.private)
async def cmd_history(client: Client, message: Message):
    await message.reply_text(history_text(message.from_user.id), reply_markup=ikb([BACK_STATUS]))


@bot.on_callback_query(filters.regex(r"^history$"))
async def cb_history(client: Client, cq: CallbackQuery):
    await safe_edit(cq.message, history_text(cq.from_user.id), ikb([BACK_STATUS]))
    await cq.answer()


async def request_access(client: Client, u, reply) -> None:
    db.ensure_user(u.id, u.username, u.first_name)
    if db.is_approved(u.id):
        await reply("✅ You are already approved.", reply_markup=main_keyboard(u.id))
        return
    row = db.user(u.id)
    if row and row["banned"]:
        await reply("🚫 Your access has been blocked.")
        return
    if db.one("SELECT 1 FROM requests WHERE user_id=?", (u.id,)):
        await reply("⏳ Your request is already pending. Please wait for the owner.")
        return
    db.x("INSERT OR REPLACE INTO requests(user_id, requested) VALUES (?,?)", (u.id, now_str()))
    await reply("📨 Request sent. You will be notified when approved.")
    if OWNER_ID:
        await safe_send(client, OWNER_ID,
                        f"🔑 <b>Access request</b>\n{_user_line(u)} (@{u.username or '-'}) · <code>{u.id}</code>",
                        reply_markup=approval_markup(u.id))


@bot.on_callback_query(filters.regex(r"^request$"))
async def cb_request(client: Client, cq: CallbackQuery):
    await cq.answer()

    async def reply(text: str, **kw):
        await safe_send(client, cq.message.chat.id, text, **kw)

    await request_access(client, cq.from_user, reply)


async def grant_access(client: Client, target: int, days: int) -> str:
    db.ensure_user(target, None, None)
    db.approve(target, days)
    until = "lifetime" if days == 0 else f"{days} days"
    await safe_send(client, target, f"✅ Your access has been approved ({until}). Send /start to begin.",
                    reply_markup=main_keyboard(target))
    return f"✅ User <code>{target}</code> approved ({until})."


async def ban_user(client: Client, target: int) -> str:
    db.ensure_user(target, None, None)
    db.revoke(target, ban=True)
    db.x("DELETE FROM requests WHERE user_id=?", (target,))
    j = active_jobs.get(target)
    if j:
        j.cancel.set()
    await safe_send(client, target, "🚫 Your access request was declined.")
    return f"🚫 User <code>{target}</code> banned."


@bot.on_callback_query(filters.regex(r"^(approve|ban):"))
async def cb_approval(client: Client, cq: CallbackQuery):
    """Buttons under the 'access request' notification the owner receives."""
    if not is_admin(cq.from_user.id):
        await cq.answer("Owner only", show_alert=True)
        return
    parts = cq.data.split(":")
    target = int(parts[1])
    msg = await (ban_user(client, target) if parts[0] == "ban" else grant_access(client, target, int(parts[2])))
    await safe_edit(cq.message, msg)
    await cq.answer("Done")


# =============================================================================
# Settings
# =============================================================================
@bot.on_message(filters.command("settings") & filters.private)
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
    admin_states.pop(cq.from_user.id, None)
    with contextlib.suppress(Exception):
        await cq.message.delete()
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^noop$"))
async def cb_noop(client: Client, cq: CallbackQuery):
    await cq.answer()


@bot.on_callback_query(filters.regex(r"^set:"))
async def cb_set(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    what = cq.data.split(":", 1)[1]
    s = db.settings(uid)
    if what == "voice":
        await safe_edit(cq.message, f"🗣 <b>Choose a voice</b>\nCurrent: {html.escape(voice_label(s['voice']))}",
                        voice_groups_markup())
    elif what == "rate":
        await safe_edit(cq.message, "⏩ <b>Speaking rate</b>", options_markup("rate", RATE_OPTIONS, s["rate"]))
    elif what == "pitch":
        await safe_edit(cq.message, "🎚 <b>Pitch</b>", options_markup("pitch", PITCH_OPTIONS, s["pitch"]))
    elif what == "volume":
        await safe_edit(cq.message, "🔉 <b>Volume</b>", options_markup("volume", VOLUME_OPTIONS, s["volume"], cols=4))
    elif what == "hours":
        await safe_edit(cq.message, "⏱ <b>Maximum hours per MP3 part</b>\nLong books are split into parts of this length.",
                        options_markup("hours", HOURS_OPTIONS, str(s["hours"])))
    elif what == "split":
        await safe_edit(cq.message, "✂️ <b>How to split long books</b>\n"
                                    "<b>By duration</b> - parts of at most N hours.\n"
                                    "<b>By chapter</b> - one part per detected chapter heading.",
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
                    "Full list: the Worker's <code>/voices</code> endpoint.",
                    ikb([[("🔙 Cancel", "back_settings")]]))
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
    await cq.answer("Saved ✓")


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


async def send_voice_preview(client: Client, chat_id: int, uid: int) -> None:
    s = db.settings(uid)
    text = PREVIEW_TEXTS.get(voice_language(s["voice"]), PREVIEW_TEXTS["en"])
    status = await safe_send(client, chat_id, f"🔊 <b>Voice preview</b>\n🗣 {html.escape(voice_label(s['voice']))}\n\n"
                                              f"🔍 Checking Workers…")
    if http_session is None:
        await safe_edit(status, "❌ Bot is still starting, try again in a moment.")
        return
    try:
        if not pool.states():
            pool.sync_from_db()
        if not pool.usable():
            await pool.refresh(http_session)
        if not pool.states():
            raise RuntimeError("No TTS Worker registered yet - the admin has to add one (🛠 Admin → Workers).")
        await safe_edit(status, f"🔊 <b>Voice preview</b>\n🗣 {html.escape(voice_label(s['voice']))}\n\n"
                                f"🎙 Synthesising sample…")
        data, dur, _ = await fetch_chunk(http_session, pool, text, 0, s, asyncio.Event())
        await safe_edit(status, f"🔊 <b>Voice preview</b>\n🗣 {html.escape(voice_label(s['voice']))}\n\n📤 Sending…")
        tmp = os.path.join(tempfile.gettempdir(), f"preview_{uid}_{uuid.uuid4().hex[:6]}.mp3")
        with open(tmp, "wb") as fh:
            fh.write(data)
        try:
            await client.send_audio(chat_id, tmp, caption=f"🔊 {html.escape(voice_label(s['voice']))} · rate {s['rate']}",
                                    title="Voice preview", performer="AudioBook Pro", duration=int(dur / 1000),
                                    reply_markup=ikb([[("🗣 Change voice", "set:voice"), ("🎧 Create audio", "new")]]))
        finally:
            with contextlib.suppress(OSError):
                os.remove(tmp)
        with contextlib.suppress(Exception):
            await status.delete()
    except Exception as e:  # noqa: BLE001
        await safe_edit(status, f"❌ <b>Preview failed</b>\n{html.escape(str(e)[:200])}",
                        ikb([[("🔁 Retry", "preview")] + CLOSE_ROW]))


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


@bot.on_message(filters.command("preview") & filters.private)
async def cmd_preview(client: Client, message: Message):
    if not db.is_approved(message.from_user.id):
        await deny(message)
        return
    await start_preview(client, message.chat.id, message.from_user.id)


# =============================================================================
# Create / cancel
# =============================================================================
CREATE_PROMPT = (f"📄 <b>Send me the text to narrate</b>\n\n"
                 f"• a document: <code>.txt .md .docx .html .epub .pdf</code> (up to {MAX_FILE_MB} MB)\n"
                 "• or simply paste / forward the text\n\n"
                 "You will see every step live: download → extract → split → voice → send.")


@bot.on_callback_query(filters.regex(r"^new$"))
async def cb_new(client: Client, cq: CallbackQuery):
    if not db.is_approved(cq.from_user.id):
        await cq.answer("Not approved", show_alert=True)
        return
    await cq.answer()
    await safe_send(client, cq.message.chat.id, CREATE_PROMPT)


async def busy_reply(client: Client, chat_id: int, job: Job) -> None:
    await safe_send(client, chat_id,
                    f"⏳ <b>You already have a job</b>\n{job_summary_line(job)}\n\n"
                    "Wait for it to finish or cancel it first.",
                    reply_markup=ikb([[("⛔ Cancel it", f"canceljob:{job.job_id}"), ("📊 Status", "status")]]))


@bot.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(client: Client, message: Message):
    uid = message.from_user.id
    if admin_states.pop(uid, None):
        await message.reply_text("Input cancelled.", reply_markup=main_keyboard(uid))
        return
    if pending_text.pop(uid, None):
        await message.reply_text("Pending text discarded.")
        return
    job = current_job(uid)
    if not job:
        await message.reply_text("Nothing to cancel.")
        return
    job.cancel.set()
    await job_queue.kick()
    await message.reply_text("⛔ Cancelling your job…")


@bot.on_callback_query(filters.regex(r"^canceljob:"))
async def cb_cancel_job(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    job = current_job(uid)
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
# Text & document intake (every step visible in one status message)
# =============================================================================
def _title_from_text(text: str) -> str:
    first = text.strip().split("\n", 1)[0].strip()
    first = re.sub(r"\s+", " ", first)
    return (first[:50] or "Text").strip()


def intake_card(title: str, meta: str, step: str) -> str:
    return f"📄 <b>{html.escape(title[:60])}</b>\n{meta}\n\n{step}"


async def start_job_from_file(client: Client, chat_id: int, uid: int, text_path: str, title: str,
                              chars: int, status: Optional[Message] = None) -> None:
    job = reserve_job(uid, title, chars)
    if job is None:
        with contextlib.suppress(OSError):
            os.remove(text_path)
        with contextlib.suppress(Exception):
            if status:
                await status.delete()
        cur = current_job(uid)
        if cur:
            await busy_reply(client, chat_id, cur)
        else:
            await safe_send(client, chat_id, "🚦 The queue is full right now, please try again later.")
        return
    settings = db.settings(uid)
    settings["voice_requested"] = settings["voice"]
    est = fmt_duration(estimate_seconds(chars, settings["rate"]))
    text = (f"🎧 <b>{html.escape(title[:60])}</b>\n{stage_line('queued')}\n\n"
            f"📝 {chars:,} characters · ≈ {est} of audio\n"
            f"🗣 {html.escape(voice_label(settings['voice']))} · ⏱ ≤ {settings['hours']} h per part\n\n"
            "🚀 Starting…")
    if status is not None:
        await safe_edit(status, text, cancel_markup(0))
    else:
        status = await safe_send(client, chat_id, text, reply_markup=cancel_markup(0))
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
    if is_admin(uid) and admin_states.get(uid) == "add_server" and ext in (".txt", ".csv", ".list", ""):
        # Fleet import: a text file with one Worker URL per line.
        admin_states.pop(uid, None)
        if (doc.file_size or 0) > 2 * 1024 * 1024:
            await message.reply_text("❌ URL list is too large (max 2 MB).")
            return
        status = await message.reply_text("⬇️ Reading the URL list…")
        try:
            tmp = os.path.join(JOBS_DIR, f"urls_{uid}_{uuid.uuid4().hex[:6]}.txt")
            os.makedirs(JOBS_DIR, exist_ok=True)
            await message.download(file_name=tmp)
            with open(tmp, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
            with contextlib.suppress(OSError):
                os.remove(tmp)
        except Exception as e:  # noqa: BLE001
            await safe_edit(status, f"❌ Could not read the file: {html.escape(str(e)[:100])}")
            return
        await add_servers(message, content, status=status)
        return
    if ext not in SUPPORTED_EXT:
        await message.reply_text(f"❌ Unsupported file type <b>{html.escape(ext or '?')}</b>.\n"
                                 f"Supported: <code>{' '.join(sorted(SUPPORTED_EXT))}</code>")
        return
    size = doc.file_size or 0
    if size > MAX_FILE_MB * 1024 * 1024:
        await message.reply_text(f"❌ File is larger than {MAX_FILE_MB} MB.")
        return
    cur = current_job(uid)
    if cur:
        await busy_reply(client, message.chat.id, cur)
        return
    title = os.path.splitext(name)[0][:60] or "Document"
    meta = f"{html.escape(ext)} · {fmt_size(size)}"
    status = await message.reply_text(intake_card(title, meta, f"⬇️ Downloading… {progress_bar(0)} 0%"))
    os.makedirs(JOBS_DIR, exist_ok=True)
    src = os.path.join(JOBS_DIR, f"src_{uid}_{uuid.uuid4().hex[:8]}{ext}")
    txt_path = src + ".txt"
    last = [0.0]

    async def dl_progress(cur_b: int, tot_b: int) -> None:
        now = time.time()
        if now - last[0] < 2.0 and cur_b < tot_b:
            return
        last[0] = now
        pct = int(100 * cur_b / max(1, tot_b))
        await safe_edit(status, intake_card(title, meta, f"⬇️ Downloading… {progress_bar(pct)} {pct}%  "
                                                         f"({fmt_size(cur_b)} / {fmt_size(tot_b)})"))

    try:
        await message.download(file_name=src, progress=dl_progress)
        await safe_edit(status, intake_card(title, meta, f"✅ Downloaded\n📖 Extracting text from {html.escape(ext)}…"))
        chars = await asyncio.to_thread(extract_text_to_file, src, ext, txt_path)
        if chars < 5:
            raise ValueError("No readable text found in this file (scanned PDF?)")
    except Exception as e:  # noqa: BLE001
        log.warning("extract failed for %s: %s", name, e)
        for p in (src, txt_path):
            with contextlib.suppress(OSError):
                os.remove(p)
        await safe_edit(status, intake_card(title, meta, f"❌ <b>Could not read the file</b>\n{html.escape(str(e)[:200])}"))
        return
    finally:
        with contextlib.suppress(OSError):
            os.remove(src)
    await safe_edit(status, intake_card(title, meta, f"✅ Downloaded\n✅ Extracted {chars:,} characters\n🚀 Starting job…"))
    await start_job_from_file(client, message.chat.id, uid, txt_path, title, chars, status=status)


_button_filter = filters.create(lambda _, __, m: bool(m.text) and m.text in ALL_BUTTONS)


@bot.on_message(filters.text & filters.private & _button_filter)
async def reply_button_dispatch(client: Client, message: Message):
    """Reply-keyboard buttons (current and legacy ones)."""
    u = message.from_user
    uid = u.id
    db.ensure_user(uid, u.username, u.first_name)
    admin_states.pop(uid, None)
    action = MAIN_BUTTONS.get(message.text) or LEGACY_BUTTONS.get(message.text, "home")
    if action == "create":
        if not db.is_approved(uid):
            await deny(message)
            return
        await message.reply_text(CREATE_PROMPT, reply_markup=main_keyboard(uid))
    elif action == "settings":
        await open_settings(client, message)
    elif action == "status":
        await cmd_status(client, message)
    elif action == "help":
        await cmd_help(client, message)
    elif action == "request":
        await request_access(client, u, message.reply_text)
    elif action == "preview":
        await cmd_preview(client, message)
    elif action == "history":
        await cmd_history(client, message)
    elif action.startswith("adm") or action == "admin":
        if not is_admin(uid):
            await message.reply_text("Main menu", reply_markup=main_keyboard(uid))
            return
        page = {"adm_add": "wadd", "adm_workers": "workers", "adm_users": "users", "adm_broadcast": "bcast",
                "adm_stats": "stats", "adm_jobs": "jobs"}.get(action, "home")
        await admin_open(client, message, page)
    else:  # home / legacy back
        await message.reply_text("🏠 Main menu", reply_markup=main_keyboard(uid))


@bot.on_message(filters.text & filters.private & ~_button_filter & ~filters.command(USER_COMMANDS))
async def text_handler(client: Client, message: Message):
    uid = message.from_user.id
    text = message.text or ""
    db.ensure_user(uid, message.from_user.username, message.from_user.first_name)
    state = admin_states.get(uid)
    if state == "custom_voice":
        admin_states.pop(uid, None)
        vid = text.strip()
        if not VOICE_RE.match(vid):
            await message.reply_text("❌ That does not look like a voice id (e.g. <code>en-US-AndrewNeural</code>).",
                                     reply_markup=ikb([[("✏️ Try again", "voice_custom"), ("🔙 Settings", "back_settings")]]))
            return
        db.set_setting(uid, "voice", vid)
        await message.reply_text(f"✅ Voice set to <code>{html.escape(vid)}</code>.",
                                 reply_markup=ikb([[("🔊 Preview", "preview"), ("⚙️ Settings", "back_settings")]]))
        return
    if is_admin(uid) and state and await _handle_admin_state(client, message, state, text):
        return
    if not db.is_approved(uid):
        await deny(message)
        return
    clean = clean_text(text)
    if len(clean) < 5 or not is_speakable(clean):
        await message.reply_text("Send a longer text or a document to narrate.")
        return
    cur = current_job(uid)
    if cur:
        await busy_reply(client, message.chat.id, cur)
        return
    token = uuid.uuid4().hex[:8]
    pending_text[uid] = (token, clean, time.time() + 600)
    s = db.settings(uid)
    await message.reply_text(
        f"📝 <b>{html.escape(_title_from_text(clean))}</b>\n"
        f"{len(clean):,} characters · ≈ {fmt_duration(estimate_seconds(len(clean), s['rate']))} of audio\n"
        f"🗣 {html.escape(voice_label(s['voice']))}\n\nCreate the audiobook?",
        reply_markup=ikb([[("🎧 Create Audio", f"mk:{token}")],
                          [("⚙️ Settings", "back_settings"), ("✖️ Discard", f"mkno:{token}")]]))


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
    await start_job_from_file(client, cq.message.chat.id, uid, txt_path, _title_from_text(text), len(text),
                              status=cq.message)


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
    text = (f"🔄 <b>Resuming</b> {html.escape((row['title'] or '')[:60])} (job #{row['id']})\n"
            f"{stage_line('queued')}\n\nContinuing from chunk {row['cursor'] or 0}/{row['chunks'] or '?'}…")
    if status is None:
        status = await safe_send(client, chat_id, text, reply_markup=cancel_markup(row["id"]))
    else:
        await safe_edit(status, text, cancel_markup(row["id"]))
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


async def _do_resume(client: Client, uid: int, chat_id: int, status: Optional[Message]) -> None:
    async def say(text: str, markup=None):
        if status is not None:
            await safe_edit(status, text, markup)
        else:
            await safe_send(client, chat_id, text, reply_markup=markup)

    if not db.is_approved(uid):
        await say("🔒 Not approved.")
        return
    cur = current_job(uid)
    if cur:
        await say(f"⏳ You already have a job:\n{job_summary_line(cur)}",
                  ikb([[("⛔ Cancel it", f"canceljob:{cur.job_id}"), ("📊 Status", "status")]]))
        return
    rows = [r for r in db.unfinished_jobs() if r["user_id"] == uid]
    if not rows:
        await say("Nothing to resume.", ikb([BACK_STATUS]))
        return
    if not await resume_job(client, rows[0], status=status):
        await say("❌ Could not resume - the source text is no longer available.", ikb([BACK_STATUS]))


@bot.on_message(filters.command("resume") & filters.private)
async def cmd_resume(client: Client, message: Message):
    await _do_resume(client, message.from_user.id, message.chat.id, None)


@bot.on_callback_query(filters.regex(r"^resume$"))
async def cb_resume(client: Client, cq: CallbackQuery):
    await cq.answer()
    await _do_resume(client, cq.from_user.id, cq.message.chat.id, cq.message)


# =============================================================================
# Admin panel - one inline message that edits itself (adm:<page>[:<arg>])
# =============================================================================
def _key_line() -> str:
    return ("🔑 API key: <b>set</b>" if TTS_API_KEY
            else "🔑 API key: not set <i>(optional - only if the Worker has an API_KEY secret)</i>")


def admin_home() -> Tuple[str, InlineKeyboardMarkup]:
    c = db.counts()
    r_cnt, w_cnt = job_queue.counts()
    txt = (f"🛠 <b>Admin panel</b> · bot v{VERSION}\n\n"
           f"🖥 Workers: <b>{len(pool.usable())}/{len(pool.states())}</b> usable\n"
           f"👥 Users: <b>{c['users']}</b> · approved {c['approved']} · banned {c['banned']}"
           + (f" · <b>⏳ {c['pending']} pending</b>" if c['pending'] else "") + "\n"
           f"🧾 Jobs: {r_cnt} running · {w_cnt} waiting\n"
           f"{_key_line()}")
    if not pool.states():
        txt += "\n\n⚠️ <b>No Worker registered</b> - nothing can be voiced yet. Add one below."
    rows = [[("🖥 Workers", "adm:workers"), ("👥 Users", "adm:users")],
            [(f"🔑 Requests ({c['pending']})" if c['pending'] else "🔑 Requests", "adm:req"), ("🧾 Jobs", "adm:jobs")],
            [("📈 Stats", "adm:stats"), ("📢 Broadcast", "adm:bcast")],
            [("🔄 Refresh", "adm:home")] + CLOSE_ROW]
    return txt, ikb(rows)


WORKERS_PER_PAGE = 12


def admin_workers(tested: bool = False, page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    """Fleet overview - stays readable with 100+ Workers (summary + paged list)."""
    sts = pool.states()
    c = pool.counts()
    lines = [f"🖥 <b>Workers</b> ({c['total']})" + (" · just re-tested" if tested else "")]
    if not sts:
        lines.append("\nNone registered. Press ➕ Add and paste Worker URLs\n"
                     "(<code>https://&lt;name&gt;.&lt;account&gt;.workers.dev</code>, any number, one per line) "
                     "or send a <code>.txt</code> file with one URL per line.")
    else:
        done_chunks = sum(x.chunks_done for x in sts)
        failed_chunks = sum(x.chunks_failed for x in sts)
        lines.append(f"✅ {c['ok']} healthy · ⛔ {c['benched']} benched · ❌ {c['down']} down\n"
                     f"⚡ {c['active']}/{c['capacity']} requests in flight · "
                     f"chunks this session {done_chunks} ok / {failed_chunks} failed")
        # troubled Workers first so they are visible on page 1
        order = sorted(sts, key=lambda x: (0 if x.benched else (1 if not x.ok else 2), x.url))
        pages = max(1, (len(order) + WORKERS_PER_PAGE - 1) // WORKERS_PER_PAGE)
        page = max(0, min(page, pages - 1))
        lines.append(f"\n<i>page {page + 1}/{pages}</i>")
        for x in order[page * WORKERS_PER_PAGE:(page + 1) * WORKERS_PER_PAGE]:
            name = html.escape(short_server(x.url))
            if x.benched:
                lines.append(f"⛔ <b>{name}</b> · benched {fmt_duration(x.bench_left)} (#{x.q_count})")
            elif not x.ok:
                lines.append(f"❌ <b>{name}</b> · {html.escape(x.error[:60] or 'unreachable')}")
            else:
                lines.append(f"✅ <b>{name}</b> · v{x.version or '?'} · {x.latency:.1f}s · "
                             f"{x.limiter.active}/{x.limiter.limit} · ok {x.chunks_done} / fail {x.chunks_failed}")
    lines.append(f"\n{_key_line()}")
    rows: List[List[Tuple[str, str]]] = []
    if sts:
        pages = max(1, (len(sts) + WORKERS_PER_PAGE - 1) // WORKERS_PER_PAGE)
        if pages > 1:
            nav: List[Tuple[str, str]] = []
            if page > 0:
                nav.append(("◀️", f"adm:workers:{page - 1}"))
            nav.append((f"{page + 1}/{pages}", f"adm:workers:{page}"))
            if page < pages - 1:
                nav.append(("▶️", f"adm:workers:{page + 1}"))
            rows.append(nav)
    rows.append([("➕ Add", "adm:wadd"), ("🔄 Test all", "adm:wtest")])
    if sts:
        rows.append([("🗑 Remove", "adm:wdel:0")] + ([("🧹 Remove all failing", "adm:wpurge")] if c["down"] else []))
    rows.append([("🔙 Back", "adm:home")] + CLOSE_ROW)
    return "\n".join(lines)[:4000], ikb(rows)


def admin_remove_workers(page: int) -> Tuple[str, InlineKeyboardMarkup]:
    urls = db.servers(enabled_only=False)
    if not urls:
        return "No Workers registered.", ikb([[("🔙 Back", "adm:workers")]])
    per = 10
    pages = max(1, (len(urls) + per - 1) // per)
    page = max(0, min(page, pages - 1))
    rows: List[List[Tuple[str, str]]] = []
    for i in range(page * per, min(len(urls), (page + 1) * per)):
        st = pool.servers.get(urls[i])
        mark = "⛔" if st and st.benched else ("❌" if st and not st.ok else "🗑")
        rows.append([(f"{mark} {short_server(urls[i])}", f"adm:wrm:{i}")])
    nav: List[Tuple[str, str]] = []
    if page > 0:
        nav.append(("◀️", f"adm:wdel:{page - 1}"))
    if pages > 1:
        nav.append((f"{page + 1}/{pages}", f"adm:wdel:{page}"))
    if page < pages - 1:
        nav.append(("▶️", f"adm:wdel:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([("🔙 Back", "adm:workers")])
    return ("🗑 <b>Remove Worker</b>\n\nTap a Worker to remove it (takes effect immediately). "
            "❌ = unreachable, ⛔ = benched right now."), ikb(rows)


ADD_SERVER_PROMPT = (
    "➕ <b>Add Workers</b>\n\nSend Worker URLs, e.g.\n<code>https://edge-tts-worker.your-account.workers.dev</code>\n"
    "Any number at once - one per line, comma or space separated - or upload a <code>.txt</code> file "
    "with one URL per line (handy for a fleet of 100+).\n\n"
    "Each URL gets a <code>/health</code> check and a real <code>/tts</code> synthesis "
    f"({HEALTH_PARALLELISM} tested in parallel) before it is registered.\n"
    "Tip: <code>/addserver &lt;url&gt; [&lt;url&gt; ...]</code> works from anywhere.")


def admin_users() -> Tuple[str, InlineKeyboardMarkup]:
    c = db.counts()
    txt = (f"👥 <b>Users</b>\n\nTotal <b>{c['users']}</b> · approved {c['approved']} · banned {c['banned']} · "
           f"pending {c['pending']}")
    rows = [[("📋 List", "adm:ulist:0"), (f"🔑 Requests ({c['pending']})", "adm:req")],
            [("✅ Approve by ID", "adm:uapprove"), ("🚫 Revoke / Ban", "adm:urevoke")],
            [("🔙 Back", "adm:home")] + CLOSE_ROW]
    return txt, ikb(rows)


USERS_PER_PAGE = 15


def admin_user_list(page: int) -> Tuple[str, InlineKeyboardMarkup]:
    total = db.counts()["users"]
    pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(0, min(page, pages - 1))
    rows_db = db.q("SELECT * FROM users ORDER BY joined DESC LIMIT ? OFFSET ?", (USERS_PER_PAGE, page * USERS_PER_PAGE))
    lines = [f"📋 <b>Users</b> · page {page + 1}/{pages}"]
    for r in rows_db:
        st = "🚫" if r["banned"] else ("✅" if r["approved"] else "⏳")
        lines.append(f"{st} <code>{r['user_id']}</code> {html.escape(r['first_name'] or '')} "
                     f"@{r['username'] or '-'} · {r['total_jobs'] or 0} jobs"
                     + (f" · until {r['expiry'][:10]}" if r["expiry"] else ""))
    nav: List[Tuple[str, str]] = []
    if page > 0:
        nav.append(("◀️", f"adm:ulist:{page - 1}"))
    if page < pages - 1:
        nav.append(("▶️", f"adm:ulist:{page + 1}"))
    rows = ([nav] if nav else []) + [[("🔙 Back", "adm:users")] + CLOSE_ROW]
    return "\n".join(lines)[:4000], ikb(rows)


def admin_requests() -> Tuple[str, InlineKeyboardMarkup]:
    pend = db.q("SELECT r.user_id, r.requested, u.first_name, u.username FROM requests r "
                "LEFT JOIN users u ON u.user_id=r.user_id ORDER BY r.requested")
    if not pend:
        return "🔑 <b>Access requests</b>\n\nNo pending requests.", ikb([[("🔙 Back", "adm:home")] + CLOSE_ROW])
    lines = [f"🔑 <b>Access requests</b> ({len(pend)})\nPick a duration under each user:"]
    rows: List[List[Tuple[str, str]]] = []
    for r in pend[:5]:
        who = f"{r['first_name'] or ''} @{r['username'] or '-'}".strip()
        lines.append(f"• <code>{r['user_id']}</code> {html.escape(who)} · {(r['requested'] or '')[:16]}")
        rows.append([(f"👤 {who[:22] or r['user_id']}", "noop")])
        t = r["user_id"]
        rows.append([("7d", f"adm:ok:{t}:7"), ("30d", f"adm:ok:{t}:30"), ("90d", f"adm:ok:{t}:90"),
                     ("♾", f"adm:ok:{t}:0"), ("🚫", f"adm:ban:{t}")])
    if len(pend) > 5:
        lines.append(f"…and {len(pend) - 5} more.")
    rows.append([("🔙 Back", "adm:home")] + CLOSE_ROW)
    return "\n".join(lines), ikb(rows)


def admin_jobs() -> Tuple[str, InlineKeyboardMarkup]:
    lines = ["🧾 <b>Jobs</b>"]
    rows: List[List[Tuple[str, str]]] = []
    live = job_queue.running + job_queue.waiting
    if not live:
        lines.append("\nNothing running or queued.")
    for j in job_queue.running:
        lines.append(f"▶️ #{j.job_id} user <code>{j.user_id}</code> · {html.escape(j.title[:30])} · "
                     f"{j.status_line or 'starting'} · {dict(JOB_STAGES).get(j.stage, j.stage)}")
    for n, j in enumerate(job_queue.waiting, 1):
        lines.append(f"🕒 #{n} user <code>{j.user_id}</code> · {html.escape(j.title[:30])} · {j.chars:,} chars")
    kill = [(f"⛔ #{j.job_id or '?'}", f"adm:kill:{j.user_id}") for j in live[:6]]
    if kill:
        rows.append(kill[:3])
        if kill[3:]:
            rows.append(kill[3:])
    paused = db.q("SELECT * FROM jobs WHERE status IN ('paused','interrupted') ORDER BY id DESC LIMIT 8")
    if paused:
        lines.append("")
        for r in paused:
            lines.append(f"⏸ #{r['id']} user <code>{r['user_id']}</code> · {html.escape((r['title'] or '')[:30])} · "
                         f"{r['cursor'] or 0}/{r['chunks'] or '?'} · {html.escape((r['last_error'] or '')[:50])}")
    recent = db.q("SELECT * FROM jobs WHERE status IN ('done','failed','cancelled') ORDER BY id DESC LIMIT 6")
    if recent:
        lines.append("")
        for r in recent:
            icon = {"done": "✅", "failed": "❌", "cancelled": "⛔"}.get(r["status"], "•")
            lines.append(f"{icon} #{r['id']} {html.escape((r['title'] or '')[:30])} · {(r['created'] or '')[:16]}")
    rows.append([("🔄 Refresh", "adm:jobs"), ("🔙 Back", "adm:home")] + CLOSE_ROW)
    return "\n".join(lines)[:4000], ikb(rows)


def admin_stats() -> Tuple[str, InlineKeyboardMarkup]:
    users = db.q("SELECT COUNT(*) AS n, SUM(approved) AS a, SUM(banned) AS b FROM users")[0]
    jobs = db.q("SELECT COUNT(*) AS n, SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS d, "
                "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS f, "
                "SUM(chars) AS c, SUM(duration) AS s FROM jobs")[0]
    today = db.q("SELECT COUNT(*) AS n, SUM(duration) AS s FROM jobs WHERE status='done' AND finished >= ?",
                 (datetime.now().strftime("%Y-%m-%d 00:00:00"),))[0]
    r_cnt, w_cnt = job_queue.counts()
    done_chunks = sum(s.chunks_done for s in pool.states())
    failed_chunks = sum(s.chunks_failed for s in pool.states())
    txt = (f"📈 <b>Stats</b>\n\n"
           f"👥 Users {users['n']} · approved {users['a'] or 0} · banned {users['b'] or 0}\n"
           f"🧾 Jobs {jobs['n']} · done {jobs['d'] or 0} · failed {jobs['f'] or 0}\n"
           f"🎵 {fmt_duration(jobs['s'] or 0)} of audio from {(jobs['c'] or 0):,} chars\n"
           f"📅 Today: {today['n'] or 0} done · {fmt_duration(today['s'] or 0)}\n"
           f"⚙️ Now: {r_cnt} running · {w_cnt} waiting · chunks this session {done_chunks} ok / {failed_chunks} failed\n"
           f"🖥 Workers {len(pool.usable())}/{len(pool.states())} usable · capacity {pool.capacity()} parallel · "
           f"💾 free disk {free_disk_mb(BASE_DIR):,.0f} MB · "
           f"ffmpeg {'yes' if FFMPEG else 'no'}")
    return txt, ikb([[("🔄 Refresh", "adm:stats"), ("🔙 Back", "adm:home")] + CLOSE_ROW])


ADMIN_PAGES = {"home": admin_home, "workers": admin_workers, "users": admin_users, "req": admin_requests,
               "jobs": admin_jobs, "stats": admin_stats}


async def admin_open(client: Client, message: Message, page: str = "home") -> None:
    """Send the admin panel as a new message (from a command / reply button)."""
    if page in ("wadd", "uapprove", "urevoke", "bcast"):
        msg = await message.reply_text("🛠 Admin panel", reply_markup=main_keyboard(message.from_user.id))
        await admin_render(msg, page, "", message.from_user.id)
        return
    txt, markup = ADMIN_PAGES.get(page, admin_home)()
    await message.reply_text(txt, reply_markup=markup)


async def admin_render(msg: Message, page: str, arg: str, uid: int) -> None:
    """Edit ``msg`` in place to show ``page``."""
    prompts = {
        "wadd": ("add_server", ADD_SERVER_PROMPT, "adm:workers"),
        "uapprove": ("approve", "✅ <b>Approve by ID</b>\n\nSend <code>user_id</code> or <code>user_id days</code> "
                                "(days 0 = lifetime).", "adm:users"),
        "urevoke": ("revoke", "🚫 <b>Revoke / Ban</b>\n\nSend <code>user_id</code> to revoke, "
                              "<code>user_id ban</code> to ban, <code>user_id unban</code> to lift a ban.", "adm:users"),
        "bcast": ("broadcast", "📢 <b>Broadcast</b>\n\nSend the message to deliver to all approved users.", "adm:home"),
    }
    if page in prompts:
        state, text, back = prompts[page]
        admin_states[uid] = state
        await safe_edit(msg, text, ikb([[("🔙 Cancel", back)]]))
        return
    admin_states.pop(uid, None)
    if page == "ulist":
        txt, markup = admin_user_list(int(arg or 0))
    elif page == "workers":
        txt, markup = admin_workers(page=int(arg or 0))
    elif page == "wdel":
        txt, markup = admin_remove_workers(int(arg or 0))
    else:
        txt, markup = ADMIN_PAGES.get(page, admin_home)()
    await safe_edit(msg, txt, markup)


@bot.on_message(filters.command("admin") & filters.private)
async def cmd_admin(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    await admin_open(client, message, "home")


@bot.on_message(filters.command("servers") & filters.private)
async def cmd_servers(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        return
    if http_session is None:
        await message.reply_text("Starting up, try again in a moment.")
        return
    msg = await message.reply_text("🔍 Testing Workers (health + real synthesis)…")
    await pool.refresh(http_session, deep=True)
    txt, markup = admin_workers(tested=True)
    await safe_edit(msg, txt, markup)


@bot.on_message(filters.command("users") & filters.private)
async def cmd_users(client: Client, message: Message):
    if is_admin(message.from_user.id):
        await admin_open(client, message, "users")


@bot.on_message(filters.command("stats") & filters.private)
async def cmd_stats(client: Client, message: Message):
    if is_admin(message.from_user.id):
        await admin_open(client, message, "stats")


@bot.on_message(filters.command("jobs") & filters.private)
async def cmd_jobs(client: Client, message: Message):
    if is_admin(message.from_user.id):
        await admin_open(client, message, "jobs")


@bot.on_message(filters.command(["addserver", "add_server"]) & filters.private)
async def cmd_add_server(client: Client, message: Message):
    """/addserver <url> [<url> ...] - register Worker(s) directly (owner only)."""
    if not is_admin(message.from_user.id):
        return
    arg = (message.text or "").split(None, 1)
    if len(arg) < 2 or not arg[1].strip():
        await admin_open(client, message, "wadd")
        return
    admin_states.pop(message.from_user.id, None)
    await add_servers(message, arg[1])


@bot.on_callback_query(filters.regex(r"^adm:"))
async def cb_admin(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    if not is_admin(uid):
        await cq.answer("Owner only", show_alert=True)
        return
    parts = cq.data.split(":")
    page = parts[1] if len(parts) > 1 else "home"
    arg = parts[2] if len(parts) > 2 else ""
    if page == "wtest":
        if http_session is None:
            await cq.answer("Starting up…", show_alert=True)
            return
        await cq.answer("Testing every Worker (health + real synthesis)…")
        await safe_edit(cq.message, "🔍 Testing Workers (health + real synthesis)…")
        await pool.refresh(http_session, deep=True)
        txt, markup = admin_workers(tested=True)
        await safe_edit(cq.message, txt, markup)
        return
    if page == "wrm":
        urls = db.servers(enabled_only=False)
        idx = int(arg or -1)
        if 0 <= idx < len(urls):
            db.remove_server(urls[idx])
            pool.sync_from_db()
            await cq.answer(f"Removed {short_server(urls[idx])}")
        else:
            await cq.answer("Already gone")
        await admin_render(cq.message, "wdel" if len(urls) > 1 else "workers", "0", uid)
        return
    if page == "wpurge":
        # drop every Worker that is unreachable / misconfigured (NOT the benched ones -
        # those are healthy Workers Microsoft is rate-limiting for a moment)
        bad = [s.url for s in pool.states() if not s.ok and not s.benched]
        for u in bad:
            db.remove_server(u)
        pool.sync_from_db()
        await cq.answer(f"Removed {len(bad)} failing Worker(s)")
        await admin_render(cq.message, "workers", "0", uid)
        return
    if page == "ok":
        note = await grant_access(client, int(arg), int(parts[3]) if len(parts) > 3 else 0)
        await cq.answer(re.sub(r"<[^>]+>", "", note))
        await admin_render(cq.message, "req", "", uid)
        return
    if page == "ban":
        note = await ban_user(client, int(arg))
        await cq.answer(re.sub(r"<[^>]+>", "", note))
        await admin_render(cq.message, "req", "", uid)
        return
    if page == "kill":
        j = active_jobs.get(int(arg or 0))
        if j:
            j.cancel.set()
            await job_queue.kick()
            await cq.answer("Cancelling…")
            await asyncio.sleep(1.0)
        else:
            await cq.answer("Job already finished")
        await admin_render(cq.message, "jobs", "", uid)
        return
    await admin_render(cq.message, page, arg, uid)
    await cq.answer()


def parse_server_urls(text: str) -> List[str]:
    urls: List[str] = []
    for raw in re.split(r"[\s,;]+", text or ""):
        nu = normalize_server_url(raw)
        if nu and nu not in urls:
            urls.append(nu)
    return urls


MAX_ADD_AT_ONCE = 500


async def add_servers(message: Message, text: str, status: Optional[Message] = None) -> None:
    """Probe every URL in ``text`` (health + real /tts, in parallel) and register the good ones.

    Built for fleets: 100+ URLs in one message or .txt file are tested
    HEALTH_PARALLELISM at a time with a live counter, and the result is a
    compact summary (the failures listed individually).
    """
    if http_session is None:
        await message.reply_text("Starting up, try again in a moment.")
        return
    urls = parse_server_urls(text)[:MAX_ADD_AT_ONCE]
    if not urls:
        await message.reply_text("❌ No valid URL found. Example: <code>https://name.account.workers.dev</code>",
                                 reply_markup=ikb([[("🔁 Try again", "adm:wadd"), ("🔙 Workers", "adm:workers")]]))
        return
    already = set(db.servers(enabled_only=False))
    fresh = [u for u in urls if u not in already]
    dupes = len(urls) - len(fresh)
    if status is None:
        status = await message.reply_text(f"🔍 Testing {len(fresh)} Worker(s)…\n1️⃣ /health · 2️⃣ real /tts synthesis")
    else:
        await safe_edit(status, f"🔍 Testing {len(fresh)} Worker(s)…\n1️⃣ /health · 2️⃣ real /tts synthesis")

    done = 0
    ok_list: List[Tuple[str, Dict[str, Any]]] = []
    failed: List[Tuple[str, str]] = []
    last_edit = [0.0]
    sem = asyncio.Semaphore(HEALTH_PARALLELISM)

    async def probe(u: str) -> None:
        nonlocal done
        async with sem:
            try:
                info = await check_server(http_session, u, deep=True)
            except Exception as e:  # noqa: BLE001
                info = {"ok": False, "error": f"{type(e).__name__}"}
            if info.get("ok"):
                db.add_server(u)
                ok_list.append((u, info))
            else:
                failed.append((u, info.get("error") or "unknown error"))
            done += 1
            now = time.time()
            if now - last_edit[0] >= 2.5:
                last_edit[0] = now
                await safe_edit(status, f"🔍 Testing Workers… {done}/{len(fresh)}\n"
                                        f"✅ {len(ok_list)} passed · ❌ {len(failed)} failed\n"
                                        f"{progress_bar(int(100 * done / max(1, len(fresh))))}")

    await asyncio.gather(*(probe(u) for u in fresh))
    pool.sync_from_db()
    if pool.states() and http_session is not None:
        with contextlib.suppress(Exception):
            await pool.refresh(http_session, only=[pool.servers[u] for u, _ in ok_list if u in pool.servers])

    out: List[str] = [f"➕ <b>Add Workers</b> · {len(urls)} URL(s)"]
    out.append(f"✅ <b>{len(ok_list)}</b> registered · ❌ {len(failed)} failed"
               + (f" · {dupes} already registered" if dupes else ""))
    if ok_list and len(ok_list) <= 15:
        for u, info in ok_list:
            out.append(f"✅ {html.escape(short_server(u))} · v{info.get('version') or '?'} · {info.get('latency')}s")
    elif ok_list:
        vers = sorted({str(i.get("version") or "?") for _, i in ok_list})
        lat = sorted(float(i.get("latency") or 0) for _, i in ok_list)
        out.append(f"   versions {', '.join(vers[:4])} · latency {lat[0]:.1f}s – {lat[-1]:.1f}s")
    if failed:
        out.append("")
        for u, err in failed[:20]:
            out.append(f"❌ <b>{html.escape(short_server(u))}</b>: {html.escape(err[:70])}")
        if len(failed) > 20:
            out.append(f"… and {len(failed) - 20} more")
    rows: List[List[Tuple[str, str]]] = []
    if failed:
        token = uuid.uuid4().hex[:8]
        pending_servers[token] = ([u for u, _ in failed], time.time() + 900)
        out.append("\nFailed Workers were not added - check URL / API key / deployment, or add them anyway "
                   "(the health loop re-tests them every few minutes).")
        rows.append([("➕ Add failed anyway", f"addsrv:{token}"), ("🔁 Add more", "adm:wadd")])
    else:
        rows.append([("➕ Add more", "adm:wadd")])
    out.append(f"\n🖥 Registered Workers: <b>{len(db.servers())}</b> · capacity ~{pool.capacity()} parallel requests")
    rows.append([("🖥 Workers", "adm:workers"), ("🛠 Admin panel", "adm:home")])
    await safe_edit(status, "\n".join(out)[:4000], ikb(rows))


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
    lines.append(f"🖥 Registered Workers: <b>{len(db.servers())}</b> · use 🔄 Test all to re-test.")
    await safe_edit(cq.message, "\n".join(lines), ikb([[("🖥 Workers", "adm:workers"), ("🛠 Admin panel", "adm:home")]]))
    await cq.answer("Added")


async def _handle_admin_state(client: Client, message: Message, state: str, text: str) -> bool:
    uid = message.from_user.id
    back = ikb([[("🛠 Admin panel", "adm:home")]])
    if state == "add_server":
        admin_states.pop(uid, None)
        await add_servers(message, text)
        return True
    if state == "approve":
        m = re.match(r"^\s*(\d{5,})\s*(\d+)?\s*$", text)
        if not m:
            await message.reply_text("Send <code>user_id</code> or <code>user_id days</code> (or press Cancel).")
            return True
        admin_states.pop(uid, None)
        msg = await grant_access(client, int(m.group(1)), int(m.group(2) or 0))
        await message.reply_text(msg, reply_markup=back)
        return True
    if state == "revoke":
        m = re.match(r"^\s*(\d{5,})\s*(ban|unban)?\s*$", text, re.I)
        if not m:
            await message.reply_text("Send <code>user_id</code>, <code>user_id ban</code> or <code>user_id unban</code>.")
            return True
        admin_states.pop(uid, None)
        target = int(m.group(1))
        mode = (m.group(2) or "").lower()
        db.ensure_user(target, None, None)
        if mode == "unban":
            db.x("UPDATE users SET banned=0 WHERE user_id=?", (target,))
            await message.reply_text(f"✅ User <code>{target}</code> unbanned (not approved yet).", reply_markup=back)
        else:
            db.revoke(target, ban=(mode == "ban"))
            j = active_jobs.get(target)
            if j:
                j.cancel.set()
            await safe_send(client, target, "🚫 Your access has been revoked.")
            await message.reply_text(f"🚫 User <code>{target}</code> {'banned' if mode == 'ban' else 'revoked'}.",
                                     reply_markup=back)
        return True
    if state == "broadcast":
        admin_states.pop(uid, None)
        rows = db.q("SELECT user_id FROM users WHERE approved=1 AND banned=0")
        status = await message.reply_text(f"📢 Sending to {len(rows)} users… 0%")
        sent = 0
        last = 0.0
        for n, r in enumerate(rows, 1):
            if await safe_send(client, int(r["user_id"]), f"📢 {text}"):
                sent += 1
            if time.time() - last > 2:
                last = time.time()
                await safe_edit(status, f"📢 Sending… {progress_bar(int(100 * n / len(rows)))} {n}/{len(rows)}")
            await asyncio.sleep(0.05)
        await safe_edit(status, f"📢 Broadcast delivered to <b>{sent}/{len(rows)}</b> users.", back)
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


async def register_commands() -> None:
    """Populate Telegram's '/' menu so users do not need a wall of keyboard buttons."""
    from pyrogram.types import BotCommand, BotCommandScopeChat
    user_cmds = [BotCommand("start", "Main menu"), BotCommand("settings", "Voice, rate, pitch, split"),
                 BotCommand("preview", "Hear the selected voice"), BotCommand("status", "My job, account, history"),
                 BotCommand("cancel", "Stop my current job"), BotCommand("resume", "Continue an interrupted job"),
                 BotCommand("queue", "Job queue"), BotCommand("help", "How to use")]
    with contextlib.suppress(Exception):
        await bot.set_bot_commands(user_cmds)
    if OWNER_ID:
        with contextlib.suppress(Exception):
            await bot.set_bot_commands(user_cmds + [BotCommand("admin", "Admin panel"),
                                                   BotCommand("servers", "Test all Workers"),
                                                   BotCommand("addserver", "Add a Worker URL"),
                                                   BotCommand("jobs", "All jobs"), BotCommand("stats", "Statistics")],
                                       scope=BotCommandScopeChat(chat_id=OWNER_ID))


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
    log.info("logged in as @%s (TTS_API_KEY %s)", me.username, "set" if TTS_API_KEY else "not set - optional")
    await register_commands()
    if pool.states():
        with contextlib.suppress(Exception):
            await pool.refresh(http_session)
            log.info("Workers: %s", pool.summary())
    else:
        log.warning("No TTS Worker registered yet - add one with /admin -> Workers -> Add (or TTS_SERVERS)")
    ka = asyncio.create_task(keep_alive_loop())
    if RESUME_ON_START:
        await resume_interrupted_jobs(bot)
    if OWNER_ID:
        n_ok, n_all = len(pool.usable()), len(pool.states())
        txt = (f"🤖 <b>Bot v{VERSION} started</b>\n🖥 Workers: {n_ok}/{n_all} usable\n{_key_line()}")
        if not n_all:
            txt += "\n\n⚠️ No Worker registered - open 🛠 Admin → Workers → ➕ Add."
        await safe_send(bot, OWNER_ID, txt, reply_markup=main_keyboard(OWNER_ID))
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
