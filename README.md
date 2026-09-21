# AudioBook Pro — Telegram TTS Bot + Edge-TTS Render Server

Convert novels, stories and documents (`.txt .md .docx .html .epub .pdf` or plain
messages) into MP3 audiobooks directly from Telegram, using a pool of
Microsoft Edge neural-voice render servers.

| File | Role |
|------|------|
| `bot.py` | Telegram bot (Pyrogram/MTProto, uploads up to 2 GB). Chunking, chapter splitting, multi-server load balancing, progress bar, user approval system, admin panel. |
| `app.py` | Flask micro-service wrapping `edge-tts`. `/tts`, `/tts/stream`, `/tts/subtitles`, `/voices`, `/health`, `/stats`. API-key auth, rate-limit, in-memory cache. |

Version: **3.1.0**

---

## 1. Deploy render servers (`app.py`)

Deploy one or more copies (Render free tier works). The bot load-balances across all of them.

**Render:** push this repo → *New Web Service* → Build `pip install -r requirements-server.txt`
→ Start `gunicorn app:app --workers 1 --threads 8 --timeout 300`.
Or use the included `render.yaml` blueprint.

**Docker:**
```bash
docker build -f Dockerfile.server -t tts-server .
docker run -p 10000:10000 -e API_KEY=mysecret tts-server
```

Key env vars: `PORT`, `API_KEY`, `MAX_TEXT_LENGTH=6000`, `MAX_CONCURRENCY=6`,
`RATE_LIMIT=120`, `CACHE_MAX_MB=64`, `TRUST_PROXY_HOPS=1` (behind Render/Cloudflare).

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

Then in Telegram: **Admin Panel → ➕ Add Server** and paste each render-server URL.
`TTS_API_KEY` in the bot must equal `API_KEY` on the servers.

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
app.py                 Edge-TTS render server
bot.py                 Telegram bot
requirements*.txt      full / server-only / bot-only deps
.env.example           all environment variables
Dockerfile.server / Dockerfile.bot / docker-compose.yml
Procfile / render.yaml deploy configs
tests/                 offline unit tests for bot helpers
```
