# Edge-TTS Render Server (Cloudflare Worker)

The text-to-speech engine behind **AudioBook Pro**. It runs entirely on
Cloudflare Workers - no Python, no VMs, no cold starts - and turns text into a
single MP3 (48 kbit/s CBR, 24 kHz mono) using Microsoft Edge's neural voices.

Everything lives in [`worker/`](worker/).

```
worker/
├── src/index.js      HTTP routes, parallel piece synthesis, caching, auth
├── src/edge_tts.js   JS port of edge-tts 7.2.8 (DRM token, websocket protocol)
├── src/validate.js   request validation, text normalisation, script detection
├── test/unit.test.js node --test suite (incl. one live Edge-TTS call)
└── wrangler.toml     deployment config + tunables
```

## Quick start

```bash
cd worker
npm install
npx wrangler login
npx wrangler secret put API_KEY        # optional - same value the bot sends as X-API-Key
npx wrangler deploy
```

Then register `https://<name>.<account>.workers.dev` in the bot
(Admin Panel -> Add Server).

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
