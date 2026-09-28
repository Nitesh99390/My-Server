# AudioBook Pro - Telegram bot (VPS) + Edge-TTS Worker (Cloudflare)

Convert novels, stories and documents (`.txt .md .docx .html .epub .pdf` or plain
messages) into MP3 audiobooks from Telegram using Microsoft Edge neural voices.

```
Telegram user  ->  bot.py  (master, your Oracle VPS)  --HTTP-->  worker/  (Cloudflare Worker)
                                                      <--MP3---   Edge-TTS engine
```

| Part | Where it runs | File(s) |
| --- | --- | --- |
| **Master bot** - Telegram UI, text extraction, chunk planning, multi-Worker load balancing, part splitting, uploads, users/admin | your VPS (Oracle free tier is enough) | `bot.py` (single file), `requirements-bot.txt`, `.env.example`, `deploy/` |
| **TTS Worker** - turns text into MP3 (48 kbit/s CBR) with Edge neural voices | Cloudflare Workers | `worker/` |

The bot never talks to Microsoft itself, so the VPS IP can never be throttled.

## Fleet mode - why 100 Workers beat 1 Worker

Microsoft's free Edge endpoint rate-limits **per egress** (= per Cloudflare
account / Worker).  Squeezing one Worker with 8 parallel requests gets it a
`403` within minutes and the job pauses.  The design since v5.2 is the
opposite: **many Workers, each with a tiny, steady load**.

| | 1 Worker | 100 Workers (free accounts) |
| --- | --- | --- |
| parallel chunks | 4-8 (throttled fast) | 200 (2 per Worker) - nothing is ever "hot" |
| 20 MB EPUB (~7 M chars) | many hours, keeps pausing | **~15-25 min** |
| one Worker throttled | the whole job pauses | that Worker sits out 30 s, the other 99 continue |

What the code does to make a fleet safe:

- **Worker** answers `503 kind=throttle` **only** for a real `403/429` from
  Microsoft and `502 kind=transient` for timeouts / resets / empty audio.  A
  throttled request is not retried on the same Worker more than once.
- **Bot** benches a Worker only on `kind=throttle` (30 s -> 1 m -> 2 m -> 4 m,
  the level decays with clean chunks *and* with time).  Transient errors just
  move the chunk to another Worker; a Worker is benched for those only after
  `SOFT_FAIL_LIMIT` of them inside `SOFT_FAIL_WINDOW`.
- **Least-loaded + least-recently-used** scheduling rotates evenly through the
  whole fleet instead of hammering the fastest few Workers.
- Defaults: 2 requests per Worker (max 3), 250 ms gap, 2 000-char chunks,
  160 total in flight (`MAX_TOTAL_CONCURRENCY` - raise it with the fleet size).

### Deploying the fleet

```bash
cd worker && npm install
# accounts.txt - one Cloudflare account per line:  <API_TOKEN> <ACCOUNT_ID> [<worker-name>]
#   token: My Profile -> API Tokens -> Create -> "Edit Cloudflare Workers"
#   account id: Workers & Pages -> Overview (right sidebar)
TTS_API_KEY=optional-shared-key PARALLEL=8 ./deploy-fleet.sh accounts.txt
# -> fleet-urls.txt  (one URL per line)   fleet-failed.txt (re-run with it)
```

Then in Telegram: **🛠 Admin -> 🖥 Workers -> ➕ Add** and *upload
`fleet-urls.txt`* (or paste any number of URLs).  The bot probes them
`HEALTH_PARALLELISM` at a time (`/health` + a real `/tts`), registers the good
ones and lists the failures with an **Add anyway** button.  The Workers page
shows a fleet summary (healthy / benched / down, requests in flight) with a
paged list, **🗑 Remove** (paged) and **🧹 Remove all failing**.

`accounts.txt` and `fleet-*.txt` are git-ignored (they contain tokens).

## 1. Bot on the Oracle VPS (`bot.py`)

```bash
sudo apt update && sudo apt install -y python3-venv ffmpeg
mkdir -p ~/audiobook && cd ~/audiobook
# copy bot.py, requirements-bot.txt and .env.example here (git clone / scp / curl raw)
python3 -m venv venv && . venv/bin/activate
pip install -r requirements-bot.txt
cp .env.example .env && nano .env      # API_ID, API_HASH, BOT_TOKEN, OWNER_ID, TTS_SERVERS, TTS_API_KEY
python bot.py
```

Run it as a service (auto-restart, survives reboots):

```bash
sudo cp deploy/audiobook-bot.service /etc/systemd/system/   # edit User/paths if needed
sudo systemctl daemon-reload && sudo systemctl enable --now audiobook-bot
journalctl -u audiobook-bot -f
```

`deploy/install-oracle.sh` does the apt/venv/pip steps for you.

### What the user sees

The reply keyboard is just two rows (`🎧 Create Audio` `⚙️ Settings` /
`📊 My Status` `🛠 Admin` `❓ Help`); everything else is an inline menu that
edits itself in place, and the `/` command menu is registered with Telegram.

Every job is **one status card** that is edited live through all stages:

```
🎧 My Novel
🗣 Madhur (Male) [Hindi]
✅ Queue › ✅ Prepare › 🔵 Voice › ⚪ Send › ⚪ Done

██████░░░░░░ 52%
📝 312,400 / 600,000 chars · chunk 105/201
🎵 5h 46m of audio ready
⚡ 1,380 chars/s (~92× realtime) · 4/4 requests in flight
⏱ 3m 46s elapsed · ETA 3m 28s

📤 Sending part 1 · 63%
🖥 ✅ edge-tts-worker 4/4
```

Before that the same card shows *Downloading 37%* -> *Extracting text* ->
*Splitting into chunks* -> *Checking Workers*; at the end a summary with
duration, parts, speed and a **New audiobook** button.  A paused job (every
Worker throttled) shows why, the last error and the retry countdown.

`📊 My Status` = account, current job (with Cancel), queue, history, resume.
`🛠 Admin` = Workers (add / test all / remove), Users (list, requests with
one-tap approve, approve/revoke by id), Jobs (with kill buttons), Stats,
Broadcast (with progress).

Every chunk is checkpointed under `JOBS_DIR`, so a restart resumes the job;
delivered parts are never sent twice.  A Worker that Microsoft really
rate-limits is benched alone (30 s -> 1 -> 2 -> 4 min) while the job continues
on the others; transient hiccups never bench anything.  The job only pauses
(never dies) when every Worker is out, and while paused it re-probes just the
troubled Workers.

### Adding Workers from Telegram

1. `🛠 Admin` -> **🖥 Workers** -> **➕ Add** -> paste Worker URLs
   (`https://<name>.<account>.workers.dev`, any number, one per line) **or
   upload a `.txt` file** with one URL per line (`fleet-urls.txt`), or
2. `/addserver <url> [<url> ...]` from anywhere.

The bot calls `/health` and does a real `/tts` synthesis on every URL (in
parallel, with a live counter) before registering it.  Failures are listed
with the reason (wrong URL, API key missing, throttled...) and an **➕ Add
anyway** button; the health loop re-tests them every few minutes.
**🔄 Test all** / `/servers` re-probes the whole fleet.

### API key is optional

`TTS_API_KEY` may stay empty.  Only if you ran `wrangler secret put API_KEY` on
the Worker do you need the same value on the bot; the Worker test tells you
exactly that if they do not match.  You can add it to `.env` later and restart.

### Upgrading from bot.py 5.0 / 5.1 to 5.2

Replace `bot.py`, review the new fleet defaults in `.env.example`
(`PER_SERVER_CONCURRENCY=2`, `MAX_TOTAL_CONCURRENCY=160`, shorter
`QUARANTINE_STEPS`...) and redeploy the Worker (`5.2.0-cf`) - old Workers keep
working, but only the new one reports `kind=throttle|transient`, which is what
stops healthy Workers from being benched after a single timeout.

### Upgrading from bot.py 4.x

Just replace `bot.py` and restart.  On first start the old database
(`users.expiry_date`, `render_servers`, `user_settings`) is upgraded in place:
approved users, bans, per-user voice settings and Worker URLs are kept.
(Running a 5.0.x bot on a 4.x database crashed every handler with
`IndexError: No item with that key` - fixed in 5.1.0.)

Troubleshooting: if the bot logs `logged in as @...` but never answers any
button or command, you are running an old `bot.py` that started Pyrogram on a
different event loop than the one its handlers were bound to (fixed in 5.0.1 -
`git pull` and restart the service).

Offline self-test (no credentials needed): `python tests_bot/test_offline.py`
and `python tests_bot/test_routing.py`.

## 2. TTS Worker on Cloudflare (`worker/`)

## Quick start

```bash
cd worker
npm install
npx wrangler login
npx wrangler secret put API_KEY        # optional - same value the bot sends as X-API-Key
npx wrangler deploy
```

Then put `https://<name>.<account>.workers.dev` in the bot's `TTS_SERVERS`
(or Admin Panel -> Add Server) and the same `API_KEY` value in `TTS_API_KEY`.

## CI / auto-deploy (GitHub Actions)

Two ready-made workflows live in [`ci/github-workflows/`](ci/github-workflows/).
GitHub only runs them from `.github/workflows/`, and the bot that opens PRs is
not allowed to write there, so enable them once by hand (any of these):

- **Locally:** `sh ci/install-workflows.sh && git push`
- **GitHub web UI:** *Add file -> Create new file*, name it
  `.github/workflows/ci.yml`, paste the contents of `ci/github-workflows/ci.yml`,
  commit; repeat for `deploy.yml`.

| Workflow | Trigger | What it does |
| --- | --- | --- |
| `ci.yml` | every push / PR | `npm test` (offline) on Node 20 + 22, then `wrangler deploy --dry-run` |
| `deploy.yml` | push to `main` touching `worker/**`, or manual run | tests -> `wrangler deploy` -> optional `API_KEY` secret -> `/health` smoke test |

Auto-deploy is **off until you add secrets** (the job just prints a notice):

1. Cloudflare dashboard -> *My Profile -> API Tokens -> Create Token* ->
   template **Edit Cloudflare Workers**. Copy the token.
2. *Workers & Pages -> Overview* -> copy the **Account ID** (right sidebar).
3. GitHub repo -> *Settings -> Secrets and variables -> Actions*:
   - secret `CLOUDFLARE_API_TOKEN`
   - secret `CLOUDFLARE_ACCOUNT_ID`
   - secret `TTS_API_KEY` *(optional - becomes the Worker's `API_KEY`)*
   - variable `WORKER_URL` *(optional - e.g. `https://edge-tts-worker.<account>.workers.dev`, enables the post-deploy health check)*

After that every merge to `main` ships the Worker automatically; you can also
run it by hand from the *Actions* tab (**Deploy -> Run workflow**).

Local development:

```bash
cd worker
cp .dev.vars.example .dev.vars        # optional API_KEY for local runs
npm run dev                            # wrangler dev on http://127.0.0.1:8787
npm test                               # set EDGE_TTS_LIVE=0 to skip the network test
```

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| GET | `/` | plain `OK` |
| GET | `/health` | status, version, limits, piece settings, throttle info |
| GET | `/stats` | per-isolate runtime statistics |
| GET | `/voices` | voice catalogue (`?locale=` `?lang=` `?gender=` `?q=`) |
| GET | `/voices/locales` | locales with voice counts |
| POST/GET | `/tts` | `{text, voice, rate, pitch, volume}` -> `audio/mpeg` |
| POST/GET | `/tts/stream` | same, streamed while synthesising |
| POST/GET | `/tts/subtitles` | `{duration_ms, srt, vtt, words[], audio_base64?}` |

`POST /tts` accepts JSON, form-encoded or `text/plain` bodies. Response headers
include `X-Duration-Ms`, `X-Char-Count`, `X-Voice`, `X-Cache`, `X-Request-ID`
and `X-Server-Version`.

Authentication (when `API_KEY` is set): `X-API-Key: <key>`,
`Authorization: Bearer <key>` or `?api_key=<key>`.

Example:

```bash
curl -s -X POST https://<worker>/tts \
  -H 'Content-Type: application/json' \
  -d '{"text":"नमस्ते दुनिया","voice":"hi-IN-MadhurNeural","rate":"+0%"}' \
  -o speech.mp3
```

## How a request is synthesised

1. Text is validated and normalised (`validate.js`). If the requested voice
   cannot read the detected script the Worker returns a `400` the bot knows how
   to handle (it switches voice instead of retrying forever).
2. The text is split into pieces of at most `PIECE_BYTES` (default 3800 - Edge
   accepts ~4096 escaped bytes per websocket message), each piece being exactly
   one websocket connection.
3. Up to `PIECE_PARALLELISM` pieces (default 2 - gentle per Worker, the fleet
   provides the parallelism) are synthesised concurrently; each piece has its
   own retries (`TTS_RETRIES`, default 3) and per-attempt timeout
   (`ATTEMPT_TIMEOUT`). A timed-out attempt closes its websocket immediately.
   Only a real `403/429` from Microsoft counts as throttling; it is retried at
   most once on this Worker and then reported as `503 kind=throttle` so the bot
   moves the chunk elsewhere.  Every other failure is `502 kind=transient`.
4. MP3 frames are concatenated in order. Because the stream is CBR 48 kbit/s the
   duration is derived from the byte size (6 bytes per millisecond).
5. Results are cached in Cloudflare's edge cache for `CACHE_TTL` seconds keyed
   on `voice|rate|pitch|volume|text`.

Requests to `/tts/subtitles` run pieces sequentially so word offsets stay
monotonic.

There is **no global cooldown**: a Worker has no fixed egress IP, so a `403`
from Microsoft on one request must not slow down the next. Throttle counters
on `/health` (`throttled`, `throttle_events`, `last_throttle_ago_s`) are
informational only.

## Configuration (`wrangler.toml` `[vars]`)

| Var | Default | Meaning |
| --- | --- | --- |
| `MAX_TEXT_LENGTH` | `6000` | max characters per request |
| `MAX_CONCURRENCY` | `4` | slots advertised to the bot on `/health` (bot-side ceiling) |
| `DEFAULT_VOICE` | `hi-IN-MadhurNeural` | used when `voice` is omitted |
| `TTS_RETRIES` | `3` | attempts per piece (a real 403 stops after the 2nd) |
| `ATTEMPT_TIMEOUT` | `40` | seconds per attempt |
| `SYNTH_TIMEOUT` | `110` | whole-request budget in seconds (keep below the bot's 150 s chunk timeout) |
| `PIECE_BYTES` | `3800` | max bytes per piece (clamped to 500..4000) |
| `PIECE_PARALLELISM` | `2` | pieces synthesised concurrently (1..8) |
| `ENABLE_CORS` | `true` | add CORS headers |
| `CACHE_TTL` | `3600` | edge cache TTL in seconds (`0` disables) |
| `API_KEY` | - | **secret** (`wrangler secret put API_KEY`), never in `[vars]` |

## Error responses

All errors are JSON: `{"error": "...", "status": <code>, "request_id": "...", "kind"?: "throttle"|"transient"}`
(`kind` is also sent as the `X-Error-Kind` header).

| Status | kind | When |
| --- | --- | --- |
| 400 | | invalid/missing field, voice cannot read the text's script |
| 401 | | bad or missing API key |
| 413 | | body larger than 2 MB |
| 502 | `transient` | Edge failed after all retries for a non-rate-limit reason (timeout, reset, no audio) - retry on another Worker |
| 503 | `throttle` | Microsoft answered `403/429` - bench this Worker for `Retry-After` seconds |
| 504 | | synthesis exceeded `SYNTH_TIMEOUT` |

## Version

Worker `5.2.0-cf` - see `X-Server-Version` or `GET /health`.
