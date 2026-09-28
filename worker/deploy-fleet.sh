#!/usr/bin/env bash
# =============================================================================
#  deploy-fleet.sh - deploy the Edge-TTS Worker to MANY Cloudflare accounts
# =============================================================================
#
#  One free Cloudflare account = one Worker = one egress towards Microsoft.
#  Microsoft rate-limits per egress, so a fleet of 100 Workers x 2 gentle
#  requests each is both much faster and much safer than 1 Worker x 8.
#
#  Usage
#  -----
#    1. Create a file  accounts.txt  (one account per line):
#
#         <API_TOKEN> <ACCOUNT_ID> [<worker-name>]
#
#       - API token:  dash.cloudflare.com -> My Profile -> API Tokens ->
#                     Create Token -> template "Edit Cloudflare Workers"
#       - Account ID: Workers & Pages -> Overview -> right sidebar
#       - worker-name is optional (default: edge-tts-<first 6 chars of account id>)
#       Lines starting with # are ignored.
#
#    2. (optional) export TTS_API_KEY=...   -> set as the Worker's API_KEY secret
#
#    3. cd worker && ./deploy-fleet.sh accounts.txt
#
#  Output
#  ------
#    fleet-urls.txt  - one https://<name>.<subdomain>.workers.dev per line.
#                      Send this file to the bot: 🛠 Admin -> Workers -> ➕ Add
#                      (upload the .txt) - it probes & registers all of them.
#    fleet-failed.txt - accounts whose deploy failed (re-run with just this file).
#
#  Re-running is idempotent: `wrangler deploy` updates an existing Worker.
#  PARALLEL=8 ./deploy-fleet.sh accounts.txt   deploys 8 accounts at once.
# =============================================================================
set -u

ACCOUNTS="${1:-accounts.txt}"
OUT="${OUT:-fleet-urls.txt}"
FAILED="${FAILED:-fleet-failed.txt}"
PARALLEL="${PARALLEL:-4}"
TTS_API_KEY="${TTS_API_KEY:-}"

if [ ! -f "$ACCOUNTS" ]; then
  echo "accounts file not found: $ACCOUNTS" >&2
  echo "format: <API_TOKEN> <ACCOUNT_ID> [<worker-name>]  (one per line)" >&2
  exit 2
fi
command -v npx >/dev/null || { echo "npx (Node.js) is required" >&2; exit 2; }
[ -d node_modules ] || npm install --silent

: > "$OUT.tmp"
: > "$FAILED.tmp"

deploy_one() {
  local token="$1" account="$2" name="$3"
  local log; log="$(mktemp)"
  local out
  # wrangler reads the credentials from the environment; --name overrides wrangler.toml
  if out="$(CLOUDFLARE_API_TOKEN="$token" CLOUDFLARE_ACCOUNT_ID="$account" \
            npx wrangler deploy --name "$name" 2>&1)"; then
    # wrangler prints the public URL on success
    local url
    url="$(printf '%s\n' "$out" | grep -Eo 'https://[A-Za-z0-9.-]+\.workers\.dev' | head -n1)"
    if [ -z "$url" ]; then
      # fall back to asking wrangler for the subdomain
      local sub
      sub="$(CLOUDFLARE_API_TOKEN="$token" CLOUDFLARE_ACCOUNT_ID="$account" npx wrangler whoami 2>/dev/null | grep -Eo '[a-z0-9-]+\.workers\.dev' | head -n1)"
      [ -n "$sub" ] && url="https://$name.$sub"
    fi
    if [ -n "$TTS_API_KEY" ]; then
      printf '%s' "$TTS_API_KEY" | CLOUDFLARE_API_TOKEN="$token" CLOUDFLARE_ACCOUNT_ID="$account" \
        npx wrangler secret put API_KEY --name "$name" >/dev/null 2>&1 || echo "  ! secret failed for $name" >&2
    fi
    if [ -n "$url" ]; then
      echo "$url" >> "$OUT.tmp"
      echo "✅ $name -> $url"
    else
      echo "$token $account $name" >> "$FAILED.tmp"
      echo "⚠️  $name deployed but URL not found - check the dashboard" >&2
    fi
  else
    echo "$token $account $name" >> "$FAILED.tmp"
    echo "❌ $name ($account): $(printf '%s\n' "$out" | tail -n 3 | tr '\n' ' ')" >&2
  fi
  rm -f "$log"
}

export -f deploy_one
export OUT FAILED TTS_API_KEY

n=0
grep -Ev '^\s*(#|$)' "$ACCOUNTS" | while read -r token account name _; do
  [ -z "${token:-}" ] || [ -z "${account:-}" ] && continue
  name="${name:-edge-tts-${account:0:6}}"
  n=$((n + 1))
  # simple parallelism with a job pool
  while [ "$(jobs -rp | wc -l)" -ge "$PARALLEL" ]; do sleep 0.5; done
  deploy_one "$token" "$account" "$name" &
done
wait

sort -u "$OUT.tmp" > "$OUT"; rm -f "$OUT.tmp"
if [ -s "$FAILED.tmp" ]; then mv "$FAILED.tmp" "$FAILED"; else rm -f "$FAILED.tmp" "$FAILED"; fi

echo
echo "Deployed $(wc -l < "$OUT") Worker(s) -> $OUT"
[ -f "$FAILED" ] && echo "Failed: $(wc -l < "$FAILED") -> $FAILED (fix and re-run: ./deploy-fleet.sh $FAILED)"
echo "Next: send $OUT to the bot (🛠 Admin -> Workers -> ➕ Add -> upload the file)"
