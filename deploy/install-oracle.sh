#!/usr/bin/env bash
# One-shot installer for the bot on a fresh Oracle Cloud Ubuntu VM.
#   curl -fsSL <raw url>/deploy/install-oracle.sh | bash      (or run it from the repo)
set -euo pipefail
APP_DIR="${APP_DIR:-$HOME/audiobook}"
REPO_RAW="${REPO_RAW:-}"   # e.g. https://raw.githubusercontent.com/<user>/<repo>/main
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip ffmpeg
mkdir -p "$APP_DIR" && cd "$APP_DIR"
if [ -n "$REPO_RAW" ]; then
  curl -fsSL "$REPO_RAW/bot.py" -o bot.py
  curl -fsSL "$REPO_RAW/requirements-bot.txt" -o requirements-bot.txt
  [ -f .env ] || curl -fsSL "$REPO_RAW/.env.example" -o .env
else
  SRC="$(cd "$(dirname "$0")/.." && pwd)"
  cp "$SRC/bot.py" "$SRC/requirements-bot.txt" .
  [ -f .env ] || cp "$SRC/.env.example" .env
fi
python3 -m venv venv
./venv/bin/pip install -U pip wheel
./venv/bin/pip install -r requirements-bot.txt
echo
echo "==> Edit $APP_DIR/.env (API_ID, API_HASH, BOT_TOKEN, OWNER_ID, TTS_SERVERS, TTS_API_KEY)"
echo "==> Then:  sudo tee /etc/systemd/system/audiobook-bot.service < deploy/audiobook-bot.service"
echo "           sudo systemctl daemon-reload && sudo systemctl enable --now audiobook-bot"
