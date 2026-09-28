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
Add more Workers (Admin Panel -> Add Server) for more throughput.

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

Bot features: approval-based users, `/settings` (voice, rate, pitch, volume, hours
per part, chapter split), `/preview`, `/cancel`, `/queue`, `/resume`, `/history`,
admin panel (`/admin`: add/remove Workers with a live synthesis test, `/servers`,
approve/revoke/ban, `/users`, broadcast, `/stats`, `/jobs`).  Every chunk is
checkpointed under `JOBS_DIR`, so a restart resumes the job; delivered parts are
never sent twice.  A Worker that Microsoft throttles is benched alone
(1 -> 2 -> 5 -> 10 min) while the job continues on the others; the job only
pauses (never dies) when every Worker is out.

### Adding a Worker from Telegram

1. `/admin` -> **➕ Add Server** -> paste the Worker URL
   (`https://<name>.<account>.workers.dev`, several per message allowed), or
2. `/addserver https://<name>.<account>.workers.dev` from anywhere.

The bot calls `/health` and does a real `/tts` synthesis before registering.
If the test fails you get the reason (wrong URL, API key missing, throttled...)
and an **➕ Add anyway** button; the health loop re-tests it every few minutes.
**🖥 Server Status** / `/servers` re-probes all registered Workers.

Troubleshooting: if the bot logs `logged in as @...` but never answers any
button or command, you are running an old `bot.py` that started Pyrogram on a
different event loop than the one its handlers were bound to (fixed in 5.0.1 -
`git pull` and restart the service).

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
3. Up to `PIECE_PARALLELISM` pieces (default 4) are synthesised concurrently;
   each piece has its own retries (`TTS_RETRIES`) and per-attempt timeout
   (`ATTEMPT_TIMEOUT`). A timed-out attempt closes its websocket immediately.
4. MP3 frames are concatenated in order. Because the stream is CBR 48 kbit/s the
   duration is derived from the byte size (6 bytes per millisecond).
5. Results are cached in Cloudflare's edge cache for `CACHE_TTL` seconds keyed
   on `voice|rate|pitch|volume|text`.

Requests to `/tts/subtitles` run pieces sequentially so word offsets stay
monotonic.

There is **no global cooldown**: a Worker has no fixed egress IP, so a `403`
from Microsoft on one request must not slow down the next. Throttle counters
on `/health` are informational only.

## Configuration (`wrangler.toml` `[vars]`)

| Var | Default | Meaning |
| --- | --- | --- |
| `MAX_TEXT_LENGTH` | `6000` | max characters per request |
| `MAX_CONCURRENCY` | `6` | slots advertised to the bot on `/health` (bot-side ceiling) |
| `DEFAULT_VOICE` | `hi-IN-MadhurNeural` | used when `voice` is omitted |
| `TTS_RETRIES` | `2` | attempts per piece |
| `ATTEMPT_TIMEOUT` | `45` | seconds per attempt |
| `SYNTH_TIMEOUT` | `110` | whole-request budget in seconds (keep below the bot's 150 s chunk timeout) |
| `PIECE_BYTES` | `3800` | max bytes per piece (clamped to 500..4000) |
| `PIECE_PARALLELISM` | `4` | pieces synthesised concurrently (1..8) |
| `ENABLE_CORS` | `true` | add CORS headers |
| `CACHE_TTL` | `3600` | edge cache TTL in seconds (`0` disables) |
| `API_KEY` | - | **secret** (`wrangler secret put API_KEY`), never in `[vars]` |

## Error responses

All errors are JSON: `{"error": "...", "status": <code>, "request_id": "..."}`.

| Status | When |
| --- | --- |
| 400 | invalid/missing field, voice cannot read the text's script |
| 401 | bad or missing API key |
| 413 | body larger than 2 MB |
| 502 | Edge failed after all retries |
| 503 | Microsoft throttled the Worker (`Retry-After` set) |
| 504 | synthesis exceeded `SYNTH_TIMEOUT` |

## Version

Worker `5.0.0-cf` - see `X-Server-Version` or `GET /health`.
