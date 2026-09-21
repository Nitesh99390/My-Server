# AudioBook Pro — Telegram TTS Bot + Edge-TTS Render Server

Convert novels, stories and documents (`.txt .md .docx .html .epub .pdf` or plain
messages) into MP3 audiobooks directly from Telegram, using **free Microsoft Edge
neural voices** (`edge-tts`) — no API key, no paid TTS service.

| File | Role |
|------|------|
| `bot.py` | Telegram bot (Pyrogram/MTProto, uploads up to 2 GB). **Built-in free Edge-TTS engine**, chunking, chapter splitting, optional multi-server load balancing, progress bar, user approval system, admin panel. |
| `app.py` | *Optional* Flask micro-service wrapping `edge-tts` for extra render capacity on other IPs. `/tts`, `/tts/stream`, `/tts/subtitles`, `/voices`, `/health`, `/stats`. |

Version: **bot 4.3.0 / server 4.1.0**

---

## 🚀 v4.3: run every engine at 90 % of its ceiling from chunk 1

| Problem in v4.2 | v4.3 behaviour |
|-----------------|----------------|
| Engines started at 4 streams and grew **+1 per 8 clean chunks** - a 1 000-chunk job spent minutes ramping up; 4 engines showed *3/4 in flight* | **Warm start**: at job start (and after every pause) every healthy engine jumps to `TARGET_UTILISATION_PCT=90` of its ceiling. Defaults raised: built-in 6→8 streams, render servers 6→8 (`MAX_CONCURRENCY=6` on `app.py`). Growth every 3 clean chunks (2 for servers). |
| **Any** failure (timeout, dropped socket, empty audio) was treated as Microsoft throttling: capacity **halved**, 2-30 s cool-down | Failures are classified: **hard** (HTTP 403/429 / handshake refused) still halves; **soft** (timeout, `NoAudioReceived`, dropped socket) only steps down by one with a ≤6 s cool-down and retries almost immediately. |
| Two flaky chunks put a healthy server in a 20-180 s cooldown | Per-server cooldown `3 s × streak`, capped at 60 s. |
| `LOCAL_TTS_MIN_GAP_MS=150` / `MIN_START_GAP_MS=200` serialised connection starts | 40 ms / 50 ms - bursts are still prevented, slots fill in a fraction of the time. |
| Chunk checkpoint + MP3 append ran on the event loop | Disk I/O runs in a worker thread; the scheduler keeps feeding engines. |
| `app.py` queued requests for 45 s before 503 | `QUEUE_TIMEOUT=12`, `MAX_QUEUE=8`, `gunicorn --threads 16`: a full server hands the chunk back to the bot fast so it lands on a free engine. |
| Progress line showed chunk counts only | Professional dashboard: `%`, chunks & chars, audio ready, ETA + elapsed, `chars/s` · `× realtime`, in-flight / capacity %, **per-engine `active/limit`** with ●/⏸ state and throttle count. |

Typical effect with 4 engines: 3-4 requests in flight → **24-30**, ~285 chars/s → **1 200-1 800 chars/s** (5-6×), while the adaptive limiters still step down the moment Microsoft pushes back - jobs never fail, they pause and continue.

---

## ⚡ v4.2: multi-server throughput - every engine busy, all the time

| Problem in v4.1 | v4.2 behaviour |
|-----------------|----------------|
| Finishing a part (ffmpeg remux + Telegram upload of hundreds of MB) ran **inline** and froze all engines for minutes | Parts are handed to a **background uploader** (single worker → parts still arrive in order). Synthesis never stops; progress shows *"Uploading part N in background"*. |
| Round-robin scheduling sent the next chunk to a busy/slow server while a fast one sat idle | **Least-loaded scheduling**: the engine with the most free parallel slots gets the chunk, ties broken by latency. |
| Remote limiter grew slowly (`LOCAL_TTS_GROW_AFTER=8`) and capped at 2× start | Per-server limiter grows every `REMOTE_GROW_AFTER=3` clean chunks up to `PER_SERVER_MAX_CONCURRENCY=8`, **clamped to the `max_concurrency` the server advertises on `/health`** so requests are never queued on a full server. |
| A `503 slots in use` (server full) was treated like Microsoft throttling → long cooldown on a healthy server | Recognised as *"we sent one too many"*: trim the limit by one, fail over **immediately** to a free engine, 1 s cooldown only. Sleeping inside a slot only happens when *every* engine is cooling down. |
| Progress/ETA counted chunks (very different sizes) | Progress/ETA/speed computed on **characters** (`chars/s`, `x realtime`) plus live `active/limit requests in flight`. |
| Chunk plan ignored byte limits when render servers were used | `plan_limits()` sizes chunks by the strictest engine (`max_text_length` of every server, `CHUNK_SIZE`, built-in engine) **and** Edge's ~4 KB/message budget → exactly one Edge connection per chunk on every engine. |

---

## ⭐ v4: any file size, any number of servers, never gives up

| Problem in v3 | v4 behaviour |
|---------------|--------------|
| One chunk failing on every engine → whole job dies with *"0 complete part(s) delivered"* | Job **pauses** (5 s → 10 s → … → 5 min back-off), re-probes the engines and continues from the exact chunk. Only `/cancel` (or `JOB_MAX_STALL_MINUTES`) stops it. |
| Bot restart / Render sleep → job lost | Every chunk is **checkpointed** to `JOBS_DIR/<job>/chunks/`, the cursor is stored in SQLite. Jobs auto-resume on start (`RESUME_ON_START`) or with `/resume`. Delivered parts are never re-sent. |
| `MAX_EXTRACTED_CHARS=2,000,000` rejected big Hindi EPUBs | Streaming extractor (EPUB spine item-by-item, PDF page-by-page) writes straight to disk; default cap 50 M chars, files up to `MAX_FILE_MB=200`. |
| Render server: retry budget shared with queue wait → `502 NoAudioReceived` under load while `/servers` showed 4/4 green | `app.py` holds a slot for **one** connection only; waiting for a slot returns `503 + Retry-After` so the bot instantly fails over. `/health` exposes `throttled`, `queued`. |
| Only the built-in engine had an adaptive limiter; remote servers used a fixed semaphore | **One adaptive limiter per server** (starts at `PER_SERVER_CONCURRENCY`, grows to `PER_SERVER_MAX_CONCURRENCY` while healthy, halves on 429/502/504/timeout). Throughput scales linearly with servers; `MAX_TOTAL_CONCURRENCY=64` is the only global cap. |
| `/servers` probe said "OK" with a 2-letter text | Probe is a real ~120-char Hindi synthesis on every engine and shows each server's live limit / throttle count. |

New commands: `/resume` (user) · `/jobs` (admin: running / paused / interrupted jobs with progress and last stall reason).

> Put `JOBS_DIR` (and `DB_PATH`) on a persistent disk when hosting on Render so resume survives redeploys.

---

## 0. How the free Edge-TTS engine stays fast *and* error-free

Microsoft's free endpoint throttles an IP that opens too many streams at once
(HTTP 403 / dropped websockets). The bot therefore uses an **adaptive limiter**:

| Setting | Default | Meaning |
|---------|---------|---------|
| `LOCAL_TTS_ENABLED` | `true` | Synthesise directly inside the bot (no render server needed). |
| `LOCAL_TTS_CONCURRENCY` | `6` | Parallel Edge-TTS streams to start with (warm-started to `TARGET_UTILISATION_PCT` of the ceiling at job start). |
| `LOCAL_TTS_MAX_CONCURRENCY` | `8` | Ceiling. Grows +1 after `LOCAL_TTS_GROW_AFTER` clean chunks; -1 on a soft failure, halves on a hard 403/429. |
| `TARGET_UTILISATION_PCT` | `90` | Every engine is warmed straight to this % of its ceiling from chunk 1. |
| `LOCAL_TTS_CHUNK_SIZE` | `3000` | Characters per request. |
| `LOCAL_TTS_MAX_BYTES` | `3900` | Wire bytes per request. Edge accepts ~4096 bytes per websocket message; Hindi is 3 bytes/char, so chunks are sized by bytes so that **one chunk = one connection** (otherwise edge-tts splits it into several *sequential* connections). |
| `LOCAL_TTS_RETRIES` | `4` | Attempts per chunk, exponential back-off + jitter. |
| `LOCAL_TTS_MIN_GAP_MS` | `40` | Minimum spacing between new connections (no bursts). |
| `LOCAL_TTS_GROW_AFTER` | `3` | Consecutive clean chunks before adding one more stream. |
| `PROGRESS_EDIT_INTERVAL` | `5` | Seconds between Telegram progress edits (avoids FloodWait stalls). |

Chunks flow through a **sliding-window pipeline**: the moment one chunk finishes
the next is submitted, so a single slow request never idles the other slots (older
versions waited for the slowest chunk of every fixed batch of 8). Measured on a
Render-class IP: ~25× realtime for Hindi with zero 403s, and the limiter still halves
itself immediately if Microsoft ever pushes back.

Works on Render even though the outbound IP changes — the limiter is per-process,
so a new IP simply starts fresh.

If you also add external render servers (`app.py`), they are used **first** (more
IPs = more total throughput) and the built-in engine acts as an always-available
fallback.

## 1. (Optional) Deploy render servers (`app.py`)

Deploy one or more copies (Render free tier works). The bot load-balances across all of them.

**Render:** push this repo → *New Web Service* → Build `pip install -r requirements-server.txt`
→ Start `gunicorn app:app --workers 1 --threads 16 --timeout 300`.
Or use the included `render.yaml` blueprint.

**Docker:**
```bash
docker build -f Dockerfile.server -t tts-server .
docker run -p 10000:10000 -e API_KEY=mysecret tts-server
```

Key env vars: `PORT`, `API_KEY`, `MAX_TEXT_LENGTH=6000`, `MAX_CONCURRENCY=6` (max 8 per IP),
`MIN_START_GAP_MS=50`, `QUEUE_TIMEOUT=12`, `RATE_LIMIT=240`, `CACHE_MAX_MB=64`, `TRUST_PROXY_HOPS=1` (behind Render/Cloudflare).

Quick test:
```bash
curl https://your-server/health
curl -X POST https://your-server/tts -H 'X-API-Key: mysecret' \
     -H 'Content-Type: application/json' \
     -d '{"text":"नमस्ते दुनिया","voice":"hi-IN-MadhurNeural"}' -o out.mp3
```

## 2. Run the bot (`bot.py`)

1. Create a bot with [@BotFather](https://t.me/BotFather) → `BOT_TOKEN`.
2. Get `API_ID` / `API_HASH` from <https://my.telegram.org>.
3. Your Telegram numeric user id → `OWNER_ID`.

```bash
cp .env.example .env         # fill in values
pip install -r requirements-bot.txt
sudo apt install ffmpeg      # optional, better MP3 headers
export $(grep -v '^#' .env | xargs)
python bot.py
```

Docker Compose (server + bot together):
```bash
docker compose up -d --build
```

The bot works immediately with the built-in free engine. To add extra capacity,
in Telegram: **Admin Panel → ➕ Add Server** and paste each render-server URL
(`TTS_API_KEY` in the bot must equal `API_KEY` on the servers). `/servers` shows
the built-in engine's current parallel limit and throttle count.

### Troubleshooting: "Chunk 1 failed on all attempted servers"

The bot now tells you *why* (e.g. `HTTP 401: API key rejected - set TTS_API_KEY ...`).
Most common causes:

| Reason shown | Fix |
|--------------|-----|
| `HTTP 401 ... TTS_API_KEY` | The render server was deployed with `API_KEY=...` but the bot's `TTS_API_KEY` is empty or different. Set the same value on both (Render → Environment) and restart the bot. |
| `HTTP 404: /tts endpoint not found` | The URL is not an `app.py` render server. Remove it. |
| `HTTP 429 rate limited` | Too many parallel requests for that server; lower `PER_SERVER_CONCURRENCY`. |
| `timed out` | Free Render instance was asleep or overloaded; the bot retries and falls back to the built-in engine. |
| `built-in Edge-TTS -> ... 403` | Microsoft throttled the bot host IP; wait a few minutes or add a render server on another IP. |
| `built-in engine disabled` | `pip install edge-tts` on the bot host, or set `LOCAL_TTS_ENABLED=true`. |

**Add Server** and **/servers** now run a real authenticated test synthesis, so a
misconfigured server is rejected (with the reason) *before* it can break a job. At
job start, unusable servers are skipped automatically and the job continues on the
remaining engines.

## 3. Usage

- `/start` – menu, `/settings` – voice / rate / pitch / volume / split mode
- Send a document or paste text → **🎧 Create Audio**
- `/preview` – 1-line voice sample, `/cancel` – stop current job
- `/history`, `/account`, `/status`
- Admin: `/admin`, `/servers`, `/users`, `/stats`, approve/revoke, broadcast

Users must be approved by the owner (**🔑 Request Access** → approve for N days).

## 4. Tests

```bash
pip install pytest
python -m pytest tests -q
```

## 5. Layout

```
app.py                 Edge-TTS render server (optional)
bot.py                 Telegram bot with built-in free Edge-TTS engine
requirements*.txt      full / server-only / bot-only deps
.env.example           all environment variables
Dockerfile.server / Dockerfile.bot / docker-compose.yml
Procfile / render.yaml deploy configs
tests/                 offline unit tests for bot helpers
```
