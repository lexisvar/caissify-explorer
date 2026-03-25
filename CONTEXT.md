# CONTEXT.md — caissify-explorer

Comprehensive developer reference for the **caissify-explorer** service.
Based on the upstream [lila-openingexplorer](https://github.com/lichess-org/lila-openingexplorer) project, adapted for the Caissify platform.

---

> ## ⚠️ IMPORTANT — Build Environment
>
> **Rust / `cargo` is NOT installed on the host machine.**
> All `cargo` commands (build, check, test, clippy, fmt, …) **must** be run
> inside the `dev` Docker container, which has the full Rust toolchain.
>
> ```bash
> # Any cargo command — always prefix with this:
> docker compose --profile dev run --rm dev <cargo command>
>
> # Examples:
> docker compose --profile dev run --rm dev cargo check
> docker compose --profile dev run --rm dev cargo build --release
> docker compose --profile dev run --rm dev cargo test
> docker compose --profile dev run --rm dev cargo clippy
> docker compose --profile dev run --rm dev cargo fmt
> ```
>
> After a release build, restart the running explorer to load the new binary:
>
> ```bash
> docker compose restart explorer
> ```
>
> **Never run `cargo` directly in a host terminal — it will not be found.**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack](#2-tech-stack)
3. [Repository Structure](#3-repository-structure)
4. [Environment Setup](#4-environment-setup)
5. [Running with Docker](#5-running-with-docker)
6. [Building & Running Locally](#6-building--running-locally)
7. [HTTP API Reference](#7-http-api-reference)
8. [Importing Games](#8-importing-games)
9. [Database Architecture](#9-database-architecture)
10. [Configuration Reference (CLI)](#10-configuration-reference-cli)
11. [Monitoring & Metrics](#11-monitoring--metrics)
12. [Railway Deployment](#12-railway-deployment)
13. [Development Workflows](#13-development-workflows)
14. [Architecture Deep-Dive](#14-architecture-deep-dive)

---

## 1. Project Overview

`caissify-explorer` is a high-performance chess **opening explorer** HTTP server. It stores and queries billions of chess positions from three separate data sources:

| Source | Description |
|---|---|
| **Masters** | Top-level OTB games (avg rating ≥ 2200) |
| **Lichess** | Full Lichess.org rated game database |
| **Player** | Per-player opening statistics indexed on-demand |
| **Caissify** | Custom OTB game database (e.g. OMOTB) — no rating floor, all games accepted |

Key capabilities:
- Handles **trillions of positions** using [RocksDB](https://rocksdb.org/) as the embedded storage engine.
- Streams player game history from Lichess in real time via the Lichess API.
- Serves opening names from the [chess-openings](https://github.com/lichess-org/chess-openings) dataset.
- Supports all major Lichess variants (Chess, Crazyhouse, ThreeCheck, KingOfTheHill, etc.).

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Language | Rust (edition 2024) |
| HTTP framework | [Axum 0.8](https://docs.rs/axum) |
| Async runtime | Tokio 1 (full) |
| Database | [RocksDB 0.24](https://rocksdb.org/) (embedded) |
| Chess logic | [shakmaty 0.30](https://docs.rs/shakmaty/) |
| In-memory cache | [moka 0.12](https://docs.rs/moka/) (async LRU/TinyLFU) |
| Serialization | serde + serde_json + serde_with |
| HTTP client | reqwest 0.12 (streaming) |
| Memory allocator | tikv-jemallocator (matches RocksDB's jemalloc) |
| CLI parsing | clap 4 (derive + env) |
| Containerization | Docker (multi-stage build) |
| Deployment | Railway |

---

## 3. Repository Structure

```
caissify-explorer/
├── src/
│   ├── main.rs             # Server entry point; all HTTP route handlers; AppState
│   ├── lib.rs              # Module declarations
│   ├── db.rs               # RocksDB wrapper (Database, column families, merge ops)
│   ├── lila.rs             # Lichess API HTTP client (game streaming, mod blacklist)
│   ├── metrics.rs          # AtomicU64 metrics in InfluxDB line-protocol format
│   ├── openapi.rs          # OpenAPI 3.1.0 spec (served at /api-docs/openapi.json)
│   ├── opening.rs          # Downloads and classifies opening names from TSV files
│   ├── util.rs             # ByColorDef, sort_by_key_and_truncate, DedupStreamExt
│   ├── zobrist.rs          # StableZobrist128 — stable Zobrist hashing across versions
│   ├── api/
│   │   ├── mod.rs          # Re-exports
│   │   ├── error.rs        # Error enum with axum IntoResponse (400/500/503)
│   │   ├── nd_json.rs      # NDJSON streaming response with keepalive
│   │   ├── query.rs        # Query parameter structs (MastersQuery, LichessQuery, etc.)
│   │   └── response.rs     # Response structs (ExplorerResponse, ExplorerMove, etc.)
│   ├── model/
│   │   ├── mod.rs          # Re-exports
│   │   ├── caissify_meta.rs# CaissifyGameMeta (15 bytes), CaissifyByDateKey, GameResult
│   │   ├── date.rs         # LaxDate, Month (u16 = year*12+month-1), Year
│   │   ├── fide.rs         # FidePlayer, FideRatingSnapshot, FideRatingKey, FideFlag
│   │   ├── game_id.rs      # GameId (base-62, 8 chars, stored as 6-byte LE u64)
│   │   ├── history.rs      # History, HistoryBuilder, HistorySegment
│   │   ├── key.rs          # KeyBuilder, KeyPrefix (12 bytes), Key (14 bytes)
│   │   ├── lichess.rs      # LichessEntry, RatingGroup, PreparedResponse
│   │   ├── lichess_game.rs # LichessGame binary format, GamePlayer
│   │   ├── masters.rs      # MastersEntry, MastersGame, PGN generation
│   │   ├── mode.rs         # Mode (Rated/Casual), ByMode<T>
│   │   ├── player.rs       # PlayerEntry, PlayerStatus, IndexRun
│   │   ├── speed.rs        # Speed enum (UltraBullet..Correspondence), BySpeed<T>
│   │   ├── stats.rs        # Stats (white/draws/black/rating_sum), FIDE performance
│   │   ├── uci.rs          # RawUciMove (packed u16 UCI move)
│   │   ├── uint.rs         # Variable-length LEB128 integer encoding
│   │   └── user.rs         # UserName (original case), UserId (lowercase)
│   └── indexer/
│       ├── mod.rs          # Re-exports
│       ├── caissify.rs     # CaissifyImporter (PUT /import/caissify) — no rating floor
│       ├── lichess.rs      # LichessGameImport, LichessImporter (PUT /import/lichess)
│       ├── masters.rs      # MastersImporter (PUT /import/masters)
│       ├── player.rs       # PlayerIndexerActor, PlayerIndexerStub
│       └── player_queue.rs # Queue<T>, Ticket, QueueFull
├── import-pgn/
│   ├── Cargo.toml          # Separate workspace crate for bulk PGN/FIDE import
│   └── src/bin/
│       ├── import-caissify.rs  # Reads .pgn / .pgn.bz2 / .pgn.zst, POSTs batches to /import/caissify
│       ├── import-fide.rs      # Downloads FIDE XML zip, parses, POSTs batches to /import/fide
│       └── import-lichess.rs   # Reads .pgn / .pgn.bz2 / .pgn.zst, POSTs batches to /import/lichess
├── benches/                # iai benchmarks
├── tests/                  # Integration tests
├── doc/
│   └── ROADMAP.md          # Feature roadmap with implementation status
├── Dockerfile              # Multi-stage build (builder → debian-slim)
├── docker-compose.yml      # explorer (dev server) + dev (cargo runner) + importer services
├── railway.toml            # Railway deployment configuration
├── .env                    # Build-time jemalloc tuning (must be sourced before build)
├── rustfmt.toml            # Rust formatting settings
└── Cargo.toml              # Workspace root + main crate
```

---

## 4. Environment Setup

### Required build-time variables

The `.env` file tunes jemalloc — **must be sourced before building**:

```bash
set -a && source .env && set +a
```

Contents of `.env`:

```env
JEMALLOC_SYS_WITH_MALLOC_CONF=abort_conf:true,background_thread:true,metadata_thp:auto,dirty_decay_ms:30000,muzzy_decay_ms:30000
```

### Optional runtime variables

| Variable | Description |
|---|---|
| `EXPLORER_LOG` | Log filter (e.g. `info`, `caissify_explorer=debug`) |
| `EXPLORER_LOG_STYLE` | Log style (`always` / `auto` / `never`) |
| `EXPLORER_BEARER` | Lichess API token (required for player indexer) |

### Prerequisites

- Rust stable (≥ 1.85) — install via [rustup](https://rustup.rs/)
- `libclang-dev` (for RocksDB's bindgen)
- `liburing-dev` (for io-uring on Linux)

```bash
# Ubuntu / Debian
sudo apt-get install clang libclang-dev liburing-dev

# macOS (io-uring is Linux-only; build without it)
# Edit Cargo.toml: remove "io-uring" from rocksdb features
```

---

## 5. Running with Docker

### Start the server

```bash
docker compose up -d
```

This builds the image if needed and starts the explorer on port `9002`.

### Build the image manually

```bash
docker build -t caissify-explorer .
```

### Run without compose

```bash
docker run -d \
  -p 9002:9002 \
  -v explorer-db:/data \
  -e EXPLORER_LOG=info \
  -e EXPLORER_BEARER=<your-lichess-token> \
  caissify-explorer \
  --bind 0.0.0.0:9002 \
  --db /data \
  --cors \
  --db-cache 4294967296
```

### Adjust DB cache for local dev

Edit `docker-compose.yml` and change `--db-cache` to a smaller value (e.g. `536870912` = 512 MB). Default is 4 GB.

### Increase file descriptor limit

RocksDB opens many SST files. On Linux hosts:

```bash
ulimit -n 131072
```

---

## 6. Building & Running Locally

> **The host has no Rust toolchain.** All `cargo` commands run inside the
> `dev` Docker container. See the ⚠️ note at the top of this file.

```bash
# 1. Compile a release binary (runs inside the dev container):
docker compose --profile dev run --rm dev cargo build --release

# 2. Restart the explorer container to load the new binary:
docker compose restart explorer

# Quick dev check (no binary produced — just verifies the code compiles):
docker compose --profile dev run --rm dev cargo check

# View all CLI options:
docker compose --profile dev run --rm dev cargo run --release -- --help
```

---

## 7. HTTP API Reference

Base URL: `http://localhost:9002`

> ⚠️ **Security**: In production, expose only `/masters`, `/lichess`, `/caissify`, `/player`, and `/api-docs` via your reverse proxy. Lock down all `/import/*`, `/compact`, and `/monitor` endpoints.

---

### `GET /api-docs`

Serves an interactive **Scalar API Reference** UI for exploring all endpoints.

```bash
open http://localhost:9002/api-docs
```

---

### `GET /api-docs/openapi.json`

Returns the full **OpenAPI 3.1.0** specification as JSON. Generated from `src/openapi.rs` — the spec covers all Explorer, PGN, Import, and Admin endpoints with request/response schemas.

```bash
curl http://localhost:9002/api-docs/openapi.json
```

The UI is powered by [Scalar](https://scalar.com/) loaded from CDN (`@scalar/api-reference`).

---

### `GET /masters`

Query master games (OTB games with average rating ≥ 2200).

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `fen` | string | start pos | FEN of the position |
| `play` | string | — | Comma-separated UCI moves to play from FEN |
| `variant` | string | `chess` | Variant name |
| `since` | year int | 1500 | Filter games since this year |
| `until` | year int | 3000 | Filter games until this year |
| `moves` | int | 12 | Max number of moves to return |
| `topGames` | int | 15 | Max top games to return |

**Response:**

```json
{
  "white": 120, "draws": 80, "black": 60,
  "moves": [
    {
      "uci": "e2e4", "san": "e4",
      "white": 100, "draws": 70, "black": 50,
      "averageRating": 2650,
      "game": { "id": "AbCd1234", "winner": "white", "speed": "classical", ... }
    }
  ],
  "topGames": [ ... ],
  "opening": { "eco": "B20", "name": "Sicilian Defense" }
}
```

---

### `GET /lichess`

Query Lichess games database.

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `fen` | string | start pos | FEN of the position |
| `play` | string | — | UCI moves |
| `variant` | string | `chess` | Variant |
| `speeds` | csv | all | Filter by speed(s): `ultraBullet,bullet,blitz,rapid,classical,correspondence` |
| `ratings` | csv | all | Filter by rating group(s): `1000,1200,1400,1600,1800,2000,2200,2500` |
| `since` | `YYYY-MM` | — | Filter since this month |
| `until` | `YYYY-MM` | — | Filter until this month |
| `moves` | int | 12 | Max moves |
| `topGames` | int | 4 | Max top games |
| `recentGames` | int | 4 | Max recent games |
| `history` | bool | false | Include per-month history |

---

### `GET /lichess/history`

Returns per-month game history for a position (Lichess games only).

Same query parameters as `/lichess` without `moves`/`topGames`/`recentGames`.

---

### `GET /player`

Per-player opening statistics. Streams NDJSON — first line is the result, subsequent lines are keepalive newlines or queue position updates.

**Query Parameters:**

| Param | Type | Required | Description |
|---|---|---|---|
| `player` | string | ✅ | Lichess username |
| `color` | `white`/`black` | ✅ | Player's color |
| `fen` | string | — | FEN |
| `play` | string | — | UCI moves |
| `variant` | string | — | Variant |
| `speeds` | csv | — | Speed filter |
| `modes` | csv | — | `rated`, `casual` |
| `moves` | int | — | Max moves (default 12) |
| `recentGames` | int | — | Max recent games |

**Streaming Response:** NDJSON — each line is a JSON object. The explorer streams keepalive `\n` bytes every 8 seconds while indexing is in progress, then sends the result.

---

### `GET /masters/pgn/:id`

Returns the full PGN of a master game by its 8-character base-62 ID.

```bash
curl http://localhost:9002/masters/pgn/AbCd1234
```

---

### `PUT /import/masters`

Import a single master game as PGN (multipart/form-data with field `pgn`).

```bash
curl -X PUT http://localhost:9002/import/masters \
  -F pgn=@game.pgn
```

Validates: avg rating ≥ 2200, no future dates, no duplicate games.

---

### `GET /caissify`

Query the Caissify custom game database (e.g. OMOTB). Accepts all games regardless of rating.

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `fen` | string | start pos | FEN of the position |
| `play` | string | — | Comma-separated UCI moves to play from FEN |
| `variant` | string | `chess` | Variant name |
| `since` | year int | 1500 | Filter games since this year |
| `until` | year int | 3000 | Filter games until this year |
| `moves` | int | 12 | Max number of moves to return |
| `topGames` | int | 15 | Max top games to return |

Response format is identical to `/masters`.

---

### `GET /caissify/pgn/:id`

Returns the full PGN of a Caissify game by its 8-character base-62 ID.

```bash
curl http://localhost:9002/caissify/pgn/AbCd1234
```

---

### `GET /caissify/games`

Paginated list of games with compact metadata. Cursor-based pagination via `page_token`. The token embeds a 1-byte sort tag — passing a token from a `sort_by=date` response to a `sort_by=rating` request returns HTTP 400.

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | integer | `50` | Results per page (max 200) |
| `since` | u16 (year) | `0` | Earliest year (inclusive) |
| `until` | u16 (year) | `65535` | Latest year (inclusive) |
| `page_token` | string | — | Cursor from previous response |
| `reverse` | bool | `true` | Newest / highest-rated first if true |
| `sort_by` | `"date"` \| `"rating"` | `"date"` | Primary sort dimension |
| `result` | `"white"` \| `"draw"` \| `"black"` | — | Filter by result |
| `min_rating` | u16 | — | Min max(white_rating, black_rating) |
| `max_rating` | u16 | — | Max max(white_rating, black_rating) |
| `min_moves` | u8 | — | Min half-move count (games with `move_count=0` always pass — v1 records lack this field) |
| `max_moves` | u8 | — | Max half-move count (same caveat) |
| `fide_id` | u32 | — | Filter to games by this FIDE player (`caissify_game_by_fide` CF — O(limit)) |
| `color` | `"white"` \| `"black"` | — | Combined with `fide_id` or `player`: restrict to games as that colour |
| `player` | string | — | Filter by normalised player name (FNV-1a name-hash lookup via `caissify_game_by_player` CF — 100% coverage) |
| `fen` | string | — | Position filter: only games reaching this FEN |
| `play` | string | — | UCI moves to play from `fen` before computing position hash |
| `include_total` | bool | — | When `true`, include `total` count in response. Requires `fide_id` or `fen`; returns HTTP 429 otherwise |

**Path routing (in priority order):**

| Conditions | Primary CF | Notes |
|---|---|---|
| `fen` + `fide_id` | `caissify_game_by_position` | budget scan 5000; FIDE match via meta point-get |
| `fen` + `sort_by=rating` | `caissify_game_by_rating` | budget scan 5000; position check via `game_at_position` |
| `fen` + `player` | `caissify_game_by_player` | player index primary; position membership check |
| `fen` (default) | `caissify_game_by_position` | pure date-ordered scan |
| `player` | `caissify_game_by_player` | direct player prefix scan |
| `fide_id` | `caissify_game_by_fide` | direct FIDE prefix scan |
| `sort_by=rating` | `caissify_game_by_rating` | direct rating range scan |
| (none) | `caissify_game_by_date` | full date-ordered fallback |

**Response:**

```json
{
  "games": [
    {
      "id": "AbCd1234",
      "white": "Carlsen, Magnus",
      "white_rating": 2882,
      "black": "Caruana, Fabiano",
      "black_rating": 2818,
      "event": "Norway Chess 2024",
      "site": "Stavanger NOR",
      "date": "2024.06.11",
      "round": "5",
      "result": "white",
      "white_fide_id": 1503014,
      "black_fide_id": 2020009,
      "move_count": 54
    }
  ],
  "next_page_token": "020a3f...",
  "total": 1847,
  "scan_exhausted": true
}
```

- `white_fide_id` / `black_fide_id`: omitted when 0 (not yet linked).
- `next_page_token`: absent on the last page.
- `total`: only present when `include_total=true`.
- `scan_exhausted`: only present (and `true`) when a position-intersect scan hit the 5000-entry server budget before filling a full page. The client should surface this to the user and offer a narrower filter.
- `move_count`: `0` means unknown (v1 meta record imported before Phase 8).

**Error responses:**

| Condition | Status |
|---|---|
| `sort_by` not `date` or `rating` | 400 |
| `page_token` sort tag mismatches `sort_by` | 400 |
| `include_total=true` without `fide_id` or `fen` | 429 |

---

### `GET /caissify/games/:id`

Returns compact metadata for a single Caissify game (fast point-get on `caissify_game_meta` CF).

```bash
curl http://localhost:9002/caissify/games/AbCd1234
```

**Response:** `CaissifyGameMeta` JSON object with `year`, `white_rating`, `black_rating`, `result`, `white_fide_id`, `black_fide_id`.

---

### `GET /fide/search`

Search FIDE players by name using the in-memory `FideNameIndex` (prefix match on normalised tokens).

**Query Parameters:**

| Param | Type | Description |
|---|---|---|
| `name` | string | Name query (exact or prefix; normalised as sorted lowercase tokens) |
| `limit` | integer | Results to return (default 10, max 50) |

```bash
curl "http://localhost:9002/fide/search?name=Carlsen"
```

**Response:** JSON array of `FidePlayer` objects (with latest rating snapshot added).

---

### `POST /import/caissify/fide-link`

Background re-linking pass: iterates `caissify_game_meta` records with unresolved FIDE IDs and attempts name-based resolution from `FideNameIndex`. Idempotent and cursor-resumable.

```bash
curl -X POST http://localhost:9002/import/caissify/fide-link \
  -H 'Content-Type: application/json' \
  -d '{"batch": 5000}'
# Resume from a previous run:
curl -X POST http://localhost:9002/import/caissify/fide-link \
  -d '{"batch": 5000, "cursor": "AbCd1234"}'
```

**Response:** `{ "linked": N, "skipped": M, "next_cursor": "..." }` — call again with `next_cursor` until it is `null`.

---

### `GET /fide/player/:fide_id`

Returns FIDE player profile (name, country, title, birth year).

```bash
curl http://localhost:9002/fide/player/1503014
```

---

### `GET /fide/player/:fide_id/ratings`

Returns monthly rating history for a FIDE player.

**Query Parameters:**

| Param | Type | Description |
|---|---|---|
| `since` | `YYYY-MM` | Start month (inclusive) |
| `until` | `YYYY-MM` | End month (inclusive) |

```bash
curl "http://localhost:9002/fide/player/1503014/ratings?since=2020-01"
```

**Response:** JSON array of `{ month, standard, rapid, blitz }`

---

### `PUT /import/fide`

Import a batch of FIDE player records and rating snapshots.

```bash
curl -X PUT http://localhost:9002/import/fide \
  -H "Content-Type: application/json" \
  -d '{ "month": "2026-03", "players": [{"fide_id": 1503014, ...}] }'
```

---

### `POST /import/caissify/reindex`

Backfill `caissify_game_meta` and `caissify_game_by_date` for all games imported before Phase 0. Idempotent. Slow on large databases — run during maintenance.

```bash
curl -X POST http://localhost:9002/import/caissify/reindex
```

---

### `POST /import/caissify/reindex-meta`

Cursor-resumable backfill that populates `caissify_game_by_rating` and `move_count` (meta v2) for all existing games. Run this once after upgrading to Phase 8, then after each page until `done` is `true`.

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `chunk` | u64 | `10000` | Games to process per call |
| `cursor` | string | — | `next_cursor` from the previous response |

```bash
# First call
curl -X POST "http://localhost:9002/import/caissify/reindex-meta?chunk=5000"

# Subsequent calls — pass the cursor until done=true
curl -X POST "http://localhost:9002/import/caissify/reindex-meta?chunk=5000&cursor=<next_cursor>"
```

**Response:** `{ "processed": N, "done": true|false, "next_cursor": "..." }`

---

### `POST /import/caissify/reindex-position`

Full backfill of the `caissify_game_by_position` CF. Replays every game's moves and writes one entry per unique position per game (~40 entries/game). Idempotent. Run once after Phase 6 upgrade or when the position CF is suspected to be incomplete.

```bash
curl -X POST http://localhost:9002/import/caissify/reindex-position
```

**Response:** `{ "processed": N, "entries_written": M }`

---

### `PUT /import/caissify`

Import a single game into the Caissify database as PGN (multipart/form-data with field `pgn`).

```bash
curl -X PUT http://localhost:9002/import/caissify \
  -F pgn=@game.pgn
```

Validates: no future dates, no duplicate games. **No rating floor** — all games are accepted.

---

### `PUT /import/lichess`

Import a batch of Lichess games as JSON array. Used by `import-pgn`.

```bash
curl -X PUT http://localhost:9002/import/lichess \
  -H "Content-Type: application/json" \
  -d '[{"id":"AbCd1234","variant":"chess",...}]'
```

---

### `POST /import/openings`

Re-download and refresh opening names from the chess-openings dataset.

```bash
curl -X POST http://localhost:9002/import/openings
```

---

### `GET /monitor`

Returns metrics in **InfluxDB line protocol** format.

```bash
curl http://localhost:9002/monitor
```

Example output:
```
opening_explorer block_index_miss=2271815u,block_index_hit=44204637u,...,lichess=121970833029u
```

---

### `GET /monitor/db/:prop`

Query a RocksDB DB-level property.

```bash
curl http://localhost:9002/monitor/db/rocksdb.estimate-num-keys
```

---

### `GET /monitor/cf/:cf/:prop`

Query a RocksDB column-family property.

```bash
curl http://localhost:9002/monitor/cf/lichess/rocksdb.stats
curl http://localhost:9002/monitor/cf/masters/rocksdb.estimate-live-data-size
```

Column families: `masters`, `masters_game`, `lichess`, `lichess_game`, `player`, `player_status`, `caissify`, `caissify_game`, `caissify_game_meta`, `caissify_game_by_date`, `caissify_game_by_fide`, `caissify_game_by_player`, `caissify_game_by_position`, `caissify_game_by_rating`, `caissify_game_by_moves`, `fide_player`, `fide_rating_history`

---

### `POST /compact`

Trigger a manual RocksDB compaction (slow — only run during maintenance windows).

```bash
curl -X POST http://localhost:9002/compact
```

---

## 8. Importing Games

### Bulk import of Lichess database dumps

1. Download PGN dumps from https://database.lichess.org/
   - Files come as `.pgn.zst` (Zstandard) or `.pgn.bz2`

2. Run the importer (from the `import-pgn` workspace crate):

```bash
cd import-pgn

# Build the importer
cargo build --release

# Import directly from compressed files
cargo run --release --bin import-lichess -- \
  --endpoint http://localhost:9002 \
  --batch-size 200 \
  *.pgn.zst

# Avoid peak hours (UTC hour numbers)
cargo run --release --bin import-lichess -- \
  --avoid-utc-hour 18 --avoid-utc-hour 19 --avoid-utc-hour 20 \
  *.pgn.zst
```

The importer:
- Reads `.pgn`, `.pgn.bz2`, or `.pgn.zst` files
- Batches games and POSTs them to `PUT /import/lichess`
- Shows a progress bar with per-file ETA and speed

### Import a single master game

```bash
curl -X PUT http://localhost:9002/import/masters \
  -F pgn=@grandmaster_game.pgn
```

### Bulk import of Caissify (e.g. OMOTB) games via Docker

The recommended way is to use the `importer` Docker Compose service, which handles the download and import in one step without needing a local Rust toolchain.

1. Capture the Sync.com URL and Cookie from your browser HAR (see `PGN_IMPORT_SYNCCOM.md`).

2. Make sure the explorer is running:

```bash
docker compose up -d
```

3. Start the importer service, passing the URL and cookie as env vars:

```bash
PGN_URL="https://m224.sync.com/u/OMOTB202602PGN.pgn?cachekey=...&datakey=...&access_token=..." \
PGN_COOKIE="sync_auth=...; signature=..." \
docker compose --profile import up importer
```

The `import-entrypoint.sh` script will:
- Download the PGN to the persistent `pgn-data` Docker volume (skipped if already present)
- Validate the file is not an HTML error page (expired URL/cookie)
- Wait for the explorer service to be ready
- Stream all games to `PUT /import/caissify` via `import-caissify`

If the download is interrupted, re-run the same command — the file will resume from scratch (Sync.com does not support range resumption for all URLs). If the import itself is interrupted, re-run it — the server deduplicates by GameId so no games will be double-counted.

> ⚠️ Sync.com URLs expire in ~1 hour. The 9.64 GB file downloads fast on a good connection but capture a fresh HAR if you get a `< 100 KB` file error.

### Import a single Caissify game

```bash
curl -X PUT http://localhost:9002/import/caissify \
  -F pgn=@game.pgn
```

---

## 9. Database Architecture

### Storage Engine

[RocksDB](https://rocksdb.org/) in **column family** mode with merge operators for lock-free concurrent writes.

### Column Families

| CF | Key | Value | Purpose |
|---|---|---|---|
| `masters` | `KeyPrefix(12) + Month(2)` | `MastersEntry` (merged) | Per-position master game stats + top games |
| `masters_game` | `GameId(6)` | `MastersGame` | Full game record for PGN generation |
| `lichess` | `KeyPrefix(12) + Month(2)` | `LichessEntry` (merged) | Per-position Lichess stats broken down by speed + rating group |
| `lichess_game` | `GameId(6)` | `LichessGame` (merged) | Game metadata (outcome, speed, players, indexed flags) |
| `player` | `KeyPrefix(12) + Month(2)` | `PlayerEntry` (merged) | Per-player-per-position stats |
| `player_status` | `UserId(bytes)` | `PlayerStatus` | Indexing progress and cooldown per player |
| `caissify` | `KeyPrefix(12) + Month(2)` | `MastersEntry` (merged) | Per-position Caissify game stats + top games (no rating floor) |
| `caissify_game` | `GameId(6)` | `MastersGame` | Full game record for PGN generation |
| `caissify_game_meta` | `GameId(6)` | `CaissifyGameMeta` (16 bytes v2 / 15 bytes v1) | Compact metadata: year, ratings, result, FIDE IDs, move_count |
| `caissify_game_by_date` | `Year(2) + GameId(6)` | `[]` (empty) | Secondary date index for paginated game listing |
| `caissify_game_by_fide` | `FideId(4) + Year(2) + GameId(6)` | `color(1)` | Secondary FIDE-player index; prefix=4 bytes; value 0=White 1=Black |
| `caissify_game_by_player` | `NameHash(8) + Year(2) + GameId(6)` | `color(1)` | Secondary player-name index (FNV-1a hash); prefix=8 bytes; 100% coverage |
| `caissify_game_by_position` | `KeyPrefix(12) + Year(2) + GameId(6)` | `[]` (empty) | Secondary position index; prefix=12 bytes; ~40 entries/game |
| `caissify_game_by_rating` | `MaxRating(2 BE) + Year(2) + GameId(6)` | `[]` (empty) | Secondary rating index (big-endian for natural high-first sort); no prefix |
| `caissify_game_by_moves` | `SHA-1(UCI moves)(20)` | `GameId(6)` | Content-based dedup; maps move-sequence fingerprint → first-seen game |
| `fide_player` | `FideId(4)` | `FidePlayer` (variable) | FIDE player profile: name, country, title, birth year |
| `fide_rating_history` | `FideId(4) + Month(2)` | `FideRatingSnapshot` (9 bytes) | Monthly standard/rapid/blitz rating snapshot per player |

### Key Encoding

Keys are 14 bytes: `[12-byte KeyPrefix][2-byte Month LE]`

- **`masters` / `lichess` / `caissify`**: `KeyPrefix = XOR(Zobrist128[..12], variant_mask[..12])`
- **`player`**: `KeyPrefix = XOR(SHA1(color+username)[..12], Zobrist128[..12], variant_mask[..12])`
- **`Month`**: `year * 12 + (month - 1)` encoded as 2-byte LE

Column families `masters`, `lichess`, `caissify`, and `player` use **RocksDB prefix iteration** with prefix length 12 to efficiently scan all months for a position.

### Merge Operators

All stats are accumulated using RocksDB's **merge operator** — individual game writes are small deltas that get merged into the base value lazily during compaction. This avoids read-modify-write cycles on hot paths.

### RocksDB Configuration Highlights

| Setting | Value | Reason |
|---|---|---|
| Block size | 64 KB | Large sequential reads |
| Compression | LZ4 (L0-L6), ZSTD (bottommost) | Space + CPU balance |
| Direct I/O | Enabled | Bypass OS page cache (jemalloc manages its own) |
| Filter | Ribbon (hybrid) | Bloom alternative, less memory |
| Cache | 4 GB default | Index + filter + data blocks |
| Rate limit | 10 MB/s | Throttle compaction I/O |

### Zobrist Hashing

Uses a **custom `StableZobrist128`** (not shakmaty's built-in) to guarantee the same hash across library updates. The hash is XORed with a per-variant constant so positions from different variants never collide.

---

## 10. Configuration Reference (CLI)

Run `caissify-explorer --help` for the full list.

| Flag | Default | Description |
|---|---|---|
| `--bind` | `127.0.0.1:9002` | TCP bind address |
| `--cors` | off | Enable CORS (adds `Access-Control-Allow-Origin: *`) |
| `--masters-cache` | 40000 | LRU cache size for masters queries |
| `--lichess-cache` | 40000 | LRU cache size for Lichess queries |
| `--caissify-cache` | 40000 | LRU cache size for Caissify queries |
| `--db` | `_db` | RocksDB data directory path |
| `--db-compaction-readahead` | off | Enable readahead during compaction (HDD benefit) |
| `--db-cache` | 4 GB | RocksDB block cache size in bytes |
| `--db-rate-limit` | 10 MB/s | Compaction I/O rate limit in bytes/sec |
| `--lila` | `https://lichess.org` | Lichess base URL (for player indexer) |
| `--bearer` | — | Lichess API token (env: `EXPLORER_BEARER`) |
| `--indexers` | 8 | Number of parallel player indexer tasks |

---

## 11. Monitoring & Metrics

### Prometheus / InfluxDB metrics

`GET /monitor` returns **InfluxDB line protocol**. Key fields:

| Metric | Description |
|---|---|
| `block_index_miss/hit` | RocksDB block cache index misses/hits |
| `block_filter_miss/hit` | Bloom/Ribbon filter misses/hits |
| `block_data_miss/hit` | Data block cache misses/hits |
| `indexing` | Number of players currently being indexed |
| `lichess_cache` / `masters_cache` / `caissify_cache` | In-memory LRU cache occupancy |
| `lichess_miss` / `masters_miss` / `caissify_miss` | Cache misses per source |
| `masters` / `lichess` / `player` / `caissify` | Number of keys in each CF |
| `player_status` | Number of indexed players |
| `caissify_game` | Number of games in the Caissify CF |

### RocksDB introspection

```bash
# Compaction stats for lichess column family
curl http://localhost:9002/monitor/cf/lichess/rocksdb.stats

# Estimated DB size
curl http://localhost:9002/monitor/db/rocksdb.estimate-live-data-size

# Pending compaction bytes
curl http://localhost:9002/monitor/cf/lichess/rocksdb.estimate-pending-compaction-bytes
```

---

## 12. Railway Deployment

### Setup

1. Push this repo to GitHub.

2. Create a new Railway project and connect the GitHub repo.

3. Set environment variables in Railway:

   ```
   EXPLORER_BEARER=<your-lichess-api-token>
   EXPLORER_LOG=info
   JEMALLOC_SYS_WITH_MALLOC_CONF=abort_conf:true,background_thread:true,metadata_thp:auto,dirty_decay_ms:30000,muzzy_decay_ms:30000
   ```

4. Add a **Railway Volume** mounted at `/data` for persistent RocksDB storage.

5. Railway will use `railway.toml` and the `Dockerfile` automatically.

6. The start command from `railway.toml` uses `$PORT` (Railway-injected):
   ```
   caissify-explorer --bind 0.0.0.0:$PORT --db /data --cors
   ```

### Important Railway notes

- The **db-cache** defaults to 4 GB — reduce it to fit your Railway plan's RAM:
  Add `--db-cache 536870912` (512 MB) to the start command in `railway.toml`.
- The Dockerfile build may take 5–15 minutes on first deploy due to RocksDB compilation.
- Use Railway's **health check** at `/monitor` (configured in `railway.toml`).
- Administrative endpoints (`/import/*`, `/compact`, `/monitor`) are accessible from within Railway's private network; use Railway's [Private Networking](https://docs.railway.app/reference/private-networking) to isolate them.

### Scaling considerations

| Resource | Recommendation |
|---|---|
| RAM | ≥ 8 GB for `--db-cache 4294967296` |
| Disk | SSD strongly preferred; RAID10 for production |
| CPU | Multi-core benefits compaction and concurrent queries |
| File descriptors | Set `ulimit -n 131072` (handled automatically in Docker) |

---

## 13. Development Workflows

> **All `cargo` commands must run inside the `dev` container — the host has no Rust toolchain.**

### Reflect code changes in the running explorer

```bash
# Step 1 — compile
docker compose --profile dev run --rm dev cargo build --release

# Step 2 — restart the server to load the new binary
docker compose restart explorer
```

### Check compilation (fast — no binary)

```bash
docker compose --profile dev run --rm dev cargo check
```

### Run tests

```bash
docker compose --profile dev run --rm dev cargo test
```

### Run benchmarks (iai)

```bash
docker compose --profile dev run --rm dev cargo bench
```

### Format code

```bash
docker compose --profile dev run --rm dev cargo fmt
```

### Lint

```bash
docker compose --profile dev run --rm dev cargo clippy
```

### Test a specific endpoint locally

```bash
# Check the start position
curl "http://localhost:9002/masters?moves=10"
curl "http://localhost:9002/lichess?speeds=blitz,rapid&ratings=2000,2200"
curl "http://localhost:9002/caissify?moves=10"
curl "http://localhost:9002/player?player=DrNykterstein&color=white"

# Check metrics
curl http://localhost:9002/monitor

# Trigger opening names refresh
curl -X POST http://localhost:9002/import/openings
```

### Debug logging

```bash
# Set EXPLORER_LOG in docker-compose.yml or pass via environment:
EXPLORER_LOG=caissify_explorer=debug docker compose up explorer
```

---

## 14. Architecture Deep-Dive

### Request lifecycle (e.g. `GET /lichess`)

1. Axum routes to the handler in `main.rs`
2. Query params parsed into `LichessQuery` (includes FEN + UCI play + filters)
3. Positions are replayed from FEN using shakmaty
4. Final position Zobrist hash computed via `StableZobrist128`
5. `KeyBuilder::lichess().with_zobrist(variant, zobrist)` produces the 12-byte prefix
6. RocksDB prefix scan collects all monthly entries for that position
7. Entries filtered by speeds/ratings/date range
8. Result sorted; top games and recent games selected via `partial_sort`
9. Opening name looked up from in-memory `Openings` map (keyed by `Zobrist64`)
10. JSON response serialized via serde and returned

### Cache strategy

Queries first check an in-memory **moka** LRU/TinyLFU cache (default 40k entries each for masters and lichess). The cache key is the full query (position + filters). Player queries are **not cached** due to their mutating nature.

A **cache hint** is applied for deep positions (ply ≥ 25): probabilistic bypass fills the cache at 1–5% rate, preventing cache pollution from rarely-repeated deep positions.

### Player indexer

The player indexer runs as a **background actor pool** (`--indexers 8` by default):

1. `POST /player?player=alice&color=white` arrives
2. `PlayerIndexerStub::index_player()` checks cooldown (2 min between runs, 24h revisit)
3. A `Ticket` is issued via `Queue<UserId>` (capacity 2000)
4. One of the 8 `PlayerIndexerActor` tasks picks up the job
5. Streams games from Lichess API (`GET /api/games/user/{player}`) via NDJSON
6. Each game is processed up to ply 50, deduplicating positions via `IntMap<Zobrist128, UciMove>`
7. A single atomic RocksDB batch per game commits the position stats + game metadata
8. Progress saved to `player_status` CF every 1024 games
9. The HTTP response streams keepalive `\n` bytes until indexing completes

### Binary encoding

All data is custom binary-encoded (no protobuf/msgpack) for maximum compactness:

- **Integers**: LEB128 variable-length (`read_uint`/`write_uint` in `model/uint.rs`)
- **GameId**: 6-byte LE u64 (base-62 display, decoded from 8-char string)
- **RawUciMove**: 16-bit packed (6 bits from-square, 6 bits to-square, 4 bits promotion/role)
- **Stats single-game optimization**: compact 2-byte header vs. full varint encoding
- **LichessGame**: flat byte sequence (outcome + speed + mode + flags + month + 2× player)

### Background tasks

Two periodic goroutines run in the background alongside the main server:

| Task | Interval | Action |
|---|---|---|
| `periodic_openings_import` | ~167 minutes | Re-fetches TSV files from chess-openings repo |
| `periodic_blacklist_update` | ~173 minutes | Fetches mod-marked accounts from Lichess API; skips their games |
| `periodic_fide_ratings_update` | ~32 days | Downloads FIDE monthly XML, upserts `fide_player` + `fide_rating_history` CFs |
