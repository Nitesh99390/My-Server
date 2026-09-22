# AudioBook Pro — Telegram TTS Bot + Edge-TTS Render Server

Convert novels, stories and documents (`.txt .md .docx .html .epub .pdf` or plain
messages) into MP3 audiobooks directly from Telegram, using **free Microsoft Edge
neural voices** (`edge-tts`) — no API key, no paid TTS service.

| File | Role |
|------|------|
| `bot.py` | Telegram bot (Pyrogram/MTProto, uploads up to 2 GB). **Built-in free Edge-TTS engine**, chunking, chapter splitting, optional multi-server load balancing, progress bar, user approval system, admin panel. |
| `app.py` | *Optional* Flask micro-service wrapping `edge-tts` for extra render capacity on other IPs. `/tts`, `/tts/stream`, `/tts/subtitles`, `/voices`, `/health`, `/stats`. |

Version: **bot 4.1.1 / server 4.0.0**

---

## ⭐ v4: any file size, any number of servers, never gives up

| Problem in v3 | v4 behaviour |
|---------------|--------------|
| One chunk failing on every engine → whole job dies with *"0 complete part(s) delivered"* | Job **pauses** (5 s → 10 s → … → 5 min back-off), re-probes the engines and continues from the exact chunk. Only `/cancel` (or `JOB_MAX_STALL_MINUTES`) stops it. |
| Bot restart / Render sleep → job lost | Every chunk is **checkpointed** to `JOBS_DIR/<job>/chunks/`, the cursor is stored in SQLite. Jobs auto-resume on start (`RESUME_ON_START`) or with `/resume`. Delivered parts are never re-sent. |
| `MAX_EXTRACTED_CHARS=2,000,000` rejected big Hindi EPUBs | Streaming extractor (EPUB spine item-by-item, PDF page-by-page) writes straight to disk; default cap 50 M chars, files up to `MAX_FILE_MB=200`. |
| Render server: retry budget shared with queue wait → `502 NoAudioReceived` under load while `/servers` showed 4/4 green | `app.py` holds a slot for **one** connection only; waiting for a slot returns `503 + Retry-After` so the bot instantly fails over. `/health` exposes `throttled`, `queued`. |
| Only the built-in engine had an adaptive limiter; remote servers used a fixed semaphore | **One adaptive limiter per server** (starts at `PER_SERVER_CONCURRENCY`, grows to 2× while healthy, halves on 429/502/503/timeout). Throughput scales linearly with servers; `MAX_TOTAL_CONCURRENCY=64` is the only global cap. |
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
| `LOCAL_TTS_CONCURRENCY` | `4` | Parallel Edge-TTS streams to start with. |
| `LOCAL_TTS_MAX_CONCURRENCY` | `6` | Ceiling. Grows +1 after `LOCAL_TTS_GROW_AFTER` clean chunks, halves on any throttle signal. |
| `LOCAL_TTS_CHUNK_SIZE` | `3000` | Characters per request. |
| `LOCAL_TTS_MAX_BYTES` | `3900` | Wire bytes per request. Edge accepts ~4096 bytes per websocket message; Hindi is 3 bytes/char, so chunks are sized by bytes so that **one chunk = one connection** (otherwise edge-tts splits it into several *sequential* connections). |
| `LOCAL_TTS_RETRIES` | `4` | Attempts per chunk, exponential back-off + jitter. |
| `LOCAL_TTS_MIN_GAP_MS` | `150` | Minimum spacing between new connections (no bursts). |
| `LOCAL_TTS_GROW_AFTER` | `8` | Consecutive clean chunks before adding one more stream. |
| `PROGRESS_EDIT_INTERVAL` | `4` | Seconds between Telegram progress edits (avoids FloodWait stalls). |

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
→ Start `gunicorn app:app --workers 1 --threads 8 --timeout 300`.
Or use the included `render.yaml` blueprint.

**Docker:**
```bash
docker build -f Dockerfile.server -t tts-server .
docker run -p 10000:10000 -e API_KEY=mysecret tts-server
```

Key env vars: `PORT`, `API_KEY`, `MAX_TEXT_LENGTH=6000`, `MAX_CONCURRENCY=4` (keep ≤ 6 per IP),
`MIN_START_GAP_MS=200`, `RATE_LIMIT=120`, `CACHE_MAX_MB=64`, `TRUST_PROXY_HOPS=1` (behind Render/Cloudflare).

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
