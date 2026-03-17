#!/bin/sh
set -e

PGN_FILE="/pgn/omotb.pgn"
ENDPOINT="${EXPLORER_ENDPOINT:-http://explorer:9002}"
BATCH_SIZE="${BATCH_SIZE:-200}"

# ── Validate required env vars ───────────────────────────────────────────────
if [ -z "$PGN_URL" ]; then
  echo "ERROR: PGN_URL is not set."
  echo "Usage: PGN_URL=\"https://m224.sync.com/...\" PGN_COOKIE=\"sync_auth=...\" docker compose --profile import up importer"
  exit 1
fi

if [ -z "$PGN_COOKIE" ]; then
  echo "ERROR: PGN_COOKIE is not set."
  exit 1
fi

# ── Download (skip if file already exists and looks valid) ───────────────────
if [ -f "$PGN_FILE" ]; then
  SIZE=$(wc -c < "$PGN_FILE")
  if [ "$SIZE" -gt 102400 ]; then
    echo "PGN file already exists ($(( SIZE / 1048576 )) MB) — skipping download."
  else
    echo "Existing file is too small ($SIZE bytes) — likely a failed previous download. Re-downloading..."
    rm -f "$PGN_FILE"
  fi
fi

if [ ! -f "$PGN_FILE" ]; then
  echo "Downloading OMOTB PGN to $PGN_FILE ..."
  curl --fail --location \
    --header "Cookie: ${PGN_COOKIE}" \
    --header "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36" \
    --output "$PGN_FILE" \
    --progress-bar \
    "${PGN_URL}"

  SIZE=$(wc -c < "$PGN_FILE")
  if [ "$SIZE" -lt 102400 ]; then
    echo "ERROR: Downloaded file is only $SIZE bytes — likely an HTML error page."
    echo "The Sync.com URL may have expired (valid ~1 hour). Capture a fresh HAR and re-run."
    rm -f "$PGN_FILE"
    exit 1
  fi
  echo "Download complete: $(( SIZE / 1048576 )) MB saved to $PGN_FILE"
fi

# ── Wait for explorer to accept connections ──────────────────────────────────
echo "Waiting for explorer at $ENDPOINT ..."
until curl --silent --fail "$ENDPOINT/monitor" > /dev/null 2>&1; do
  echo "  ...not ready yet, retrying in 5s"
  sleep 5
done
echo "Explorer is up."

# ── Run the importer ─────────────────────────────────────────────────────────
echo "Starting import: batch-size=$BATCH_SIZE endpoint=$ENDPOINT"
exec import-caissify \
  --endpoint "$ENDPOINT" \
  --batch-size "$BATCH_SIZE" \
  "$PGN_FILE"
