# PGN Import — Sync.com (Cookie-Authenticated Downloads)

This document explains end-to-end how a large PGN database (e.g. the OMOTB database stored on **sync.com**) gets from the source into CaissifyDB's PostgreSQL database.

---

## Why Cookies Are Needed

Sync.com is a personal cloud storage service. Its download links are:

1. **Time-limited** — the URL contains `cachekey`, `datakey`, and `access_token` query parameters that expire after ~1 hour.
2. **Session-authenticated** — the HTTP request must carry a `Cookie` header containing `sync_auth=...` and `signature=...` tokens, which are bound to your logged-in browser session.

Without these cookies the server returns a `302` redirect to an error page, or a tiny `776-byte` HTML response instead of the gigabyte-scale PGN file.

---

## Full Flow Diagram

```
Browser (you)
    │
    │  1. Open sync.com, click download
    │  2. Copy URL + Cookie header from DevTools (HAR capture)
    ▼
API call  POST /api/v1/admin/import-pgn
  { url: "https://m224.sync.com/...", cookies: "sync_auth=...", source: "omotb" }
    │
    ▼
admin.py  →  download_and_import_pgn_task.delay(...)
                 (Celery task queued in Redis)
    │
    ▼
Celery Worker (import_tasks.py)
    │
    ├── 1. DOWNLOAD phase
    │      httpx streaming GET with Cookie header
    │      → saves to /app/data/omotb.pgn  (persistent volume)
    │      → progress reported via PROGRESS state every 50 MB
    │
    ├── 2. IMPORT phase
    │      PGNImportService.import_pgn_file(...)
    │      → reads PGN game-by-game with python-chess
    │      → generates SHA-256 dedupe hash per game
    │      → batch-inserts into PostgreSQL (games table)
    │      → skips duplicates on conflict (dedupe_hash)
    │      → writes checkpoint every 50 000 games (resumable)
    │      → hooks into ExplorerIndexerService for position indexing
    │
    └── 3. COMPLETE
           Returns { total, inserted, skipped, errors }
           SyncJob record updated to status=completed
```

---

## Step-by-Step Instructions

### 1. Capture the Authenticated URL from Your Browser

1. Log into [sync.com](https://www.sync.com) in Chrome / Firefox.
2. Navigate to your shared PGN file.
3. Open **DevTools → Network tab** (`F12`).
4. Click the **Download** button on the file.
5. Find the request to `m224.sync.com/u/...` in the network list.
6. Extract two things:
   - **URL** — the full `https://m224.sync.com/u/FILE.pgn?cachekey=...&datakey=...&access_token=...`
   - **Cookie header value** — something like `sync_auth=abc123...; signature=xyz456...`

> **Tip:** You can also **Save all as HAR** from the Network tab, then use `scripts/test_har_parser.py` (powered by `app/utils/har_parser.py`) to extract both automatically.

#### Using the HAR parser utility

```bash
# Inside the container or local venv
python scripts/test_har_parser.py /path/to/sync_download.har
```

The `HARParser` class (`app/utils/har_parser.py`) looks for entries whose URL contains `sync.com/u/` and has both `datakey=` and `cachekey=` parameters. It returns the URL, all relevant headers (including `Cookie`), and can generate a ready-to-run `curl` command.

---

### 2. Trigger the Import via the API

```bash
curl -X POST http://localhost:8002/api/v1/admin/import-pgn \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://m224.sync.com/u/FILE.pgn?cachekey=...&datakey=...&access_token=...",
    "source": "omotb",
    "cookies": "sync_auth=...; signature=..."
  }'
```

**Response:**
```json
{
  "job_id": "084b4eb6-9bd5-42c4-8546-94d6c557b841",
  "message": "Import task queued for omotb",
  "estimated_duration": "12-24 hours for large files (8GB+)"
}
```

The job is immediately pushed onto the Redis Celery queue and returns a `job_id` you can poll.

---

### 3. What Happens Inside the Celery Worker

**File:** [app/tasks/import_tasks.py](../app/tasks/import_tasks.py)

#### Phase 1 — Download

The `download_and_import_pgn_task` task builds a browser-like header set and injects the `Cookie` value:

```python
download_headers = {
    "User-Agent": "Mozilla/5.0 ...",
    "Accept": "text/html,application/xhtml+xml,...",
    ...
}
if cookies:
    download_headers["Cookie"] = cookies   # ← the sync_auth token
```

It then streams the response in 8 MB chunks via `httpx.AsyncClient`, writing directly to `/app/data/<source>.pgn` on the persistent volume.

- Progress is reported to Celery every 50 MB (`state="PROGRESS"`).
- The filename is deterministic (`omotb.pgn`) so the task can resume safely if the container restarts — if the file already exists it is reused without re-downloading.
- A size check (`< 100 KB`) catches cases where sync.com returned an HTML error page instead of the real file (expired URL/cookie).

#### Phase 2 — Parse & Import

Once the file is on disk, control passes to `PGNImportService.import_pgn_file()`:

**File:** [app/services/pgn_import_service.py](../app/services/pgn_import_service.py)

| Step | Detail |
|------|--------|
| **Read** | `chess.pgn.read_game()` iterates the file one game at a time |
| **Parse** | Headers (White, Black, Date, Result, ECO, …) + moves extracted |
| **Hash** | SHA-256 of `white+black+date+result+first_20_move_tokens` → `dedupe_hash` |
| **Batch** | Games accumulated in memory until `batch_size` (default 1 000) is reached |
| **Insert** | `INSERT … ON CONFLICT (dedupe_hash) DO NOTHING` — safe to run multiple times |
| **Checkpoint** | JSON file written every 50 000 games so an interrupted import resumes from where it left off |
| **Explorer hook** | After each batch, qualifying games (≥2300 Elo, year ≥2000) are indexed into `explorer_positions` and `game_position_index` |

---

### 4. Monitor Progress

```bash
# Poll job status
curl http://localhost:8002/api/v1/jobs/<job_id>

# Watch worker logs
docker-compose logs -f worker

# Celery inspect
docker-compose exec worker celery -A app.core.celery_app inspect active
```

During the download phase the job meta looks like:
```json
{
  "status": "downloading",
  "phase": "downloading_pgn",
  "downloaded_gb": 2.4,
  "total_gb": 8.9,
  "remaining_gb": 6.5,
  "progress_percent": 26.9
}
```

During import it switches to:
```json
{
  "status": "importing",
  "total": 450000,
  "inserted": 312000,
  "skipped": 138000,
  "errors": 0
}
```

---

## Alternative: CLI Script (Direct, No Celery)

For local use or one-off imports, run directly:

```bash
# Inside the container
docker-compose exec api python scripts/import_pgn.py \
  --url "https://m224.sync.com/u/FILE.pgn?cachekey=...&datakey=..." \
  --cookies "sync_auth=...; signature=..." \
  --source omotb \
  --batch-size 1000
```

**File:** [scripts/import_pgn.py](../scripts/import_pgn.py)

This runs synchronously in the foreground — useful for debugging but will fail if your SSH/terminal disconnects. For production use the API endpoint (Celery task) instead.

---

## Key Files

| File | Role |
|------|------|
| [app/api/v1/endpoints/admin.py](../app/api/v1/endpoints/admin.py) | `POST /api/v1/admin/import-pgn` endpoint — validates request, queues Celery task |
| [app/tasks/import_tasks.py](../app/tasks/import_tasks.py) | `download_and_import_pgn_task` — streaming download with cookie injection, then calls import service |
| [app/services/pgn_import_service.py](../app/services/pgn_import_service.py) | Core parser — reads PGN, hashes, batch-inserts, checkpoints, hooks explorer |
| [app/utils/har_parser.py](../app/utils/har_parser.py) | `HARParser` — extracts URL + Cookie from a browser HAR capture |
| [docs/SYNCCOM_HAR_DOWNLOADS.md](SYNCCOM_HAR_DOWNLOADS.md) | Short quick-start for HAR extraction |
| [scripts/import_pgn.py](../scripts/import_pgn.py) | CLI alternative — direct download + import without Celery |

---

## Important Caveats

| Issue | Detail |
|-------|--------|
| **URL expiry** | Sync.com URLs expire in ~1 hour. Start the import within 60 minutes of capturing the HAR. |
| **Cookie expiry** | Session cookies last ~30 days. If you get `403` or an 800-byte response, regenerate the HAR. |
| **File reuse** | If `/app/data/omotb.pgn` already exists the download is skipped. Delete it manually or pass `force_reimport: true` to start fresh. |
| **Resumable** | If the Celery worker dies mid-import, restart it — the checkpoint file (`.omotb.pgn.checkpoint.json`) lets the import continue from the last saved position. |
| **Security** | Never commit or share HAR files or cookie strings — they grant full access to your sync.com account. |
