# Caissify Explorer — Feature Roadmap

Comprehensive technical analysis and implementation roadmap for extending `caissify-explorer` with:

1. Paginated game list API
2. Historical FIDE rating storage
3. FIDE player ↔ Caissify game linking
4. Performance guarantees
5. Service architecture (worker vs. API separation)

---

## Table of Contents

1. [Why This Architecture Is Fast — Technical Deep Dive](#1-why-this-architecture-is-fast--technical-deep-dive)
2. [Feasibility Assessment](#2-feasibility-assessment)
3. [Feature 1 — Paginated Game List API](#3-feature-1--paginated-game-list-api)
4. [Feature 2 — Historical FIDE Player Ratings](#4-feature-2--historical-fide-player-ratings)
5. [Feature 3 — FIDE Player ↔ Caissify Game Linking](#5-feature-3--fide-player--caissify-game-linking)
6. [Performance Impact Analysis](#6-performance-impact-analysis)
7. [Service Architecture — API vs. Worker Split](#7-service-architecture--api-vs-worker-split)
8. [Implementation Roadmap](#8-implementation-roadmap)

---

## 1. Why This Architecture Is Fast — Technical Deep Dive

The performance of `caissify-explorer` is not incidental — it is the result of structural decisions at every layer of the stack.

### 1.1 Execution Model

| Dimension | caissify-explorer (Axum/Tokio) |
|---|---|
| Concurrency model | True async I/O on a Tokio thread pool; zero-cost task overhead |
| CPU utilisation | No global interpreter lock; all cores fully utilized |
| Memory per connection | A few KB per async Tokio task |
| Startup overhead | Single statically-linked binary; no runtime to boot |
| Request parsing | Zero-copy via `axum`/`hyper`; HTTP headers are slices into the TCP buffer |

Tokio multiplexes thousands of concurrent requests across a fixed thread pool. There is no per-request process or thread spawn cost.

### 1.2 Memory Allocator

This service uses **jemalloc** (`tikv-jemallocator`) tuned to match RocksDB's internal allocator. Benefits:
- Thread-local memory arenas eliminate lock contention on allocation
- Background decay thread returns memory to OS without stalling requests
- Transparent huge pages for metadata (`metadata_thp:auto`)

Every allocation in a request handler happens through this tuned path, keeping per-request heap overhead in the low-kilobyte range.

### 1.3 Database Access — The Embedded Advantage

| Dimension | External DB (any HTTP service + SQL) | caissify-explorer + RocksDB |
|---|---|---|
| Query path | TCP round-trip + wire protocol + row deserialization | Direct function call into the same process |
| Network overhead | 0.5–2 ms minimum per query, even on localhost | Zero — no network involved |
| Connection management | Pool with lock contention under high concurrency | No pool needed |
| Pagination internals | `OFFSET N` scans and discards N rows | Cursor seek is O(log N); reading N records is always O(N) |

The absence of a network hop is the single biggest latency win. RocksDB returns data through a function call in **microseconds**; any networked database adds milliseconds before a single byte of data is read.

### 1.4 Serialization

`caissify-explorer` uses `serde_json`, which:
1. Reads from a pre-decoded native Rust struct already in memory
2. Writes directly to a `Vec<u8>` byte buffer in one pass
3. Has zero intermediate heap allocations per field

This single-pass approach keeps serialization of a 50-game response in the low-microsecond range.

### 1.5 Caching

The moka LRU cache holds `Arc<Result<Json<ExplorerResponse>>>` — a reference-counted pointer to an **already-serialized byte buffer**. A cache hit costs one atomic reference-count increment and an `Arc::clone()`. The bytes are never re-serialized; they are handed directly to the Axum response layer.

### 1.6 Expected Latency Profile

| Scenario | Expected p50 | Expected p99 |
|---|---|---|
| Explorer query (cache hit) | < 1 ms | 2–5 ms |
| Explorer query (cache miss, RocksDB) | 1–5 ms | 10–30 ms |
| Paginated game list (cursor, first page) | < 2 ms | 5–10 ms |
| FIDE rating history (60 months) | < 2 ms | 5 ms |

---

## 2. Feasibility Assessment

| Feature | Feasibility | RocksDB fit | Performance risk | Verdict |
|---|---|---|---|---|
| Paginated game list | High | Good | Low | ✅ Build in Rust |
| Historical FIDE ratings | High | Excellent | Zero | ✅ Build in Rust (new CF) |
| FIDE ↔ game linking | Medium | Moderate | Low-Medium | ✅ Build in Rust, with caveats |
| Player profile search (by name) | Low for Rust alone | Poor | High if naïve | ⚠️ Hybrid: Rust serves; separate search index for name lookup |
| Background job management | N/A | N/A | N/A | ✅ Use Tokio JoinSet (already present) |

The project already has everything needed for features 1–3. The only structurally new thing is a **secondary index** design for features that require non-key lookups (player name search, game filtering by player).

---

## 3. Feature 1 — Paginated Game List API

### 3.1 Problem with the current design

`caissify_game` column family is a pure key-value store: `GameId(6 bytes) → MastersGame`. There is no secondary index, so iterating games requires a full table scan. For the explorer use case (querying by position Zobrist hash), this is fine because the `caissify` CF stores the position → top-game-IDs index.

For a paginated list API that filters by date, player name, event, result, or rating range, a full scan on every request would scan tens of millions of rows. This is not acceptable.

### 3.2 Solution — Sorted secondary index column families

RocksDB's sorted key ordering can be exploited to implement secondary indexes without an external database. The strategy is the same one already used for the position index: design a key that encodes the sort/filter dimensions so range scans become fast prefix scans.

#### Index 1 — `caissify_game_by_date`

```
Key:  [2-byte Year LE][4-byte reverse-sequential counter LE][6-byte GameId]
Value: [] (empty — GameId is in the key)
```

This allows:
- Full scan in reverse chronological order (newest first)
- Date-range filter via start/end key on year bytes
- Cursor-based pagination using the last-seen full key

The counter within a year is a monotonic u32 (you already have GameId counters from the 6-byte base-62 storage). Since GameId encodes insertion order (it is stored as a LE u64), the existing GameId already serves as a tiebreaker.

#### Index 2 — `caissify_game_by_player`

```
Key:  [hash(normalized_player_name, 8 bytes)][2-byte Year LE][6-byte GameId]
Value: [1-byte color flag (white=0, black=1)]
```

This allows:
- All games by a player (scan by 8-byte name hash prefix)
- Filtered by year range within that player
- Cursor pagination

The player-name hash uses a truncated FNV-64 or xxHash-64 so it fits in 8 bytes. Collisions are possible but extremely rare across ~100k players; a disambiguation check against the actual game record catches them.

#### Index 3 — `caissify_game_meta`

Rather than storing duplicated metadata in multiple indexes, store a compact summary per GameId:

```
Key:  GameId (6 bytes)
Value: [2-byte Year][2-byte white_rating][2-byte black_rating][1-byte result][1-byte fide_white_flag][1-byte fide_black_flag][4-byte white_fide_id][4-byte black_fide_id]
```

This 18-byte value allows the paginated endpoint to filter and return metadata without decoding the full `MastersGame` binary blob for every record. Full PGN is only fetched for games the client actually wants.

### 3.3 API Design

```
GET /caissify/games
  ?page_token=<cursor>     # opaque base64 of (Year, GameId) — for pagination
  &limit=50                # max 200
  &since=YYYY              # year range
  &until=YYYY
  &result=white|black|draw
  &min_rating=INT          # filter avg_rating >= N
  &fide_id=INT             # all games by a FIDE player (white or black)
  &color=white|black       # combined with fide_id: games as that color
  &event=STRING            # substring match (requires offline index or linear scan within player prefix)
```

Response:

```json
{
  "games": [ { "id": "AbCd1234", "white": {...}, "black": {...}, "date": "2024.01.15", "result": "1-0", ... } ],
  "next_page_token": "AAAIAAAABhQ",
  "total_in_filter": null
}
```

`total_in_filter` is intentionally `null` by default. Computing an exact count requires scanning all matching records, which defeats pagination performance. It can be computed as an opt-in `include_total=true` parameter.

### 3.4 Pagination Strategy — Cursor vs. Offset

**Never use OFFSET-style pagination with RocksDB.** There is no `SKIP N ROWS` operation. The equivalent would mean iterating and discarding N records on every request.

**Cursor-based pagination** is the correct design:
1. Response includes a `next_page_token` which is the encoded last key seen.
2. Next request opens an iterator `seek(next_page_token)` and reads the next N records.
3. Cost is O(page_size) reads regardless of how deep in the dataset you are.

This is the same pattern RocksDB is designed for and what the Lichess explorer already uses internally.

### 3.5 Performance Characteristics (expected)

| Operation | Complexity | Expected latency |
|---|---|---|
| List games (no filter, first page) | O(limit) | < 2 ms |
| List games (year filter, first page) | O(limit) | < 2 ms |
| List games (year filter, page 100) | O(limit) | < 2 ms (cursor seek is O(log N)) |
| List games by FIDE player | O(limit) | < 5 ms |
| List games by player + year range | O(limit) | < 5 ms |

These are achievable because every query maps to a RocksDB range scan with a precise start key. There is no full-table scan.

---

## 4. Feature 2 — Historical FIDE Player Ratings

### 4.1 Data Source

FIDE publishes monthly rating lists at:
- `https://ratings.fide.com/download_lists.phtml`
- Files are ZIP archives containing one XML file each: `standard_rating_list_xml.zip`, `rapid_rating_list_xml.zip`, `blitz_rating_list_xml.zip`
- XML schema: `<playerslist>` with `<player>` records containing `<fideid>`, `<name>`, `<country>`, `<sex>`, `<title>`, `<rating>`, `<games>`, `<k>`, `<birthday>`, `<flag>` fields
- Published on the first day of each month; ~1 million records per file; ~50–100 MB per zip

For the historical chart view (like 2700chess.com), you need to store one rating snapshot per player per month.

### 4.2 Storage Design in RocksDB

#### Column family: `fide_player`

```
Key:   [4-byte FIDE ID LE]
Value: binary-encoded FidePlayer { name, country, sex, birth_year, titles... }
```

This is a pure key-value lookup. ~1 million records × ~80 bytes = ~80 MB on disk. Tiny.

#### Column family: `fide_rating_history`

```
Key:   [4-byte FIDE ID LE][2-byte Month LE]
Value: binary-encoded FideRatingSnapshot { standard: u16, rapid: u16, blitz: u16, games_standard: u16, k_standard: u8, ... }
```

This mirrors the exact design of the `lichess` column family:
- 12-byte prefix = FIDE ID (padded to 4 bytes) + variant bits (constants)
- 2-byte suffix = Month (`year * 12 + month - 1`, same as the rest of the codebase)
- Prefix extractor set to 4 bytes → efficient iteration of all months for one player

Per player per month: ~12 bytes key + ~10 bytes value = 22 bytes. For 1 million players × 12 months × 20 years of history = ~24 billion bytes ≈ **240 MB** total with LZ4 compression. This is extremely compact.

### 4.3 Import Strategy

#### One-time historical backfill

FIDE provides historical archives going back to approximately 2001 at:
`https://ratings.fide.com/download_lists.phtml`

The import binary (`import-pgn`-style worker) should:
1. Download each monthly zip in parallel (rate-limited to respect FIDE servers)
2. Parse XML using `quick-xml` (the fastest Rust XML parser)
3. Write a `FideRatingSnapshot` per player to RocksDB via batch write
4. Deduplicate: if a snapshot for (fide_id, month) already exists, skip

A machine with SSD can import ~10 years of history (120 monthly files × 1M players) in roughly 20–40 minutes.

#### Monthly refresh

Add a background task to `caissify-explorer` (alongside the existing `periodic_openings_import` and `periodic_blacklist_update`):

```
periodic_fide_ratings_update: interval ~32 days
  - Download current month's rating lists (standard + rapid + blitz)
  - Upsert all player records in fide_player CF
  - Write new rating snapshots to fide_rating_history CF
  - Log count of updated/new players
```

This runs entirely in the background and has **zero impact on request-serving performance**.

### 4.4 API Design

```
GET /fide/player/{fide_id}
  → FidePlayer { fide_id, name, country, title, rating_standard, rating_rapid, rating_blitz, ... }

GET /fide/player/{fide_id}/ratings
  ?since=YYYY-MM              # default: last 5 years
  &until=YYYY-MM
  &type=standard|rapid|blitz  # default: standard
  → [ { month: "2024-01", rating: 2812 }, ... ]  # sorted chronologically

GET /fide/search?name=Magnus%20Carlsen&limit=10
  → [ { fide_id, name, country, title, rating_standard } ]  # fuzzy name match
```

The name search endpoint is the only one that is problematic for RocksDB. See the performance discussion in §6.

### 4.5 Performance Characteristics

| Endpoint | RocksDB operation | Expected latency |
|---|---|---|
| Get player by FIDE ID | Single point get on `fide_player` | < 1 ms |
| Get rating history (5 years) | 60-record prefix scan on `fide_rating_history` | < 2 ms |
| Get rating history (20 years) | 240-record prefix scan | < 5 ms |
| Search by name | NOT efficient in RocksDB — see §6 | 50–500 ms naïve |

---

## 5. Feature 3 — FIDE Player ↔ Caissify Game Linking

### 5.1 The Problem

The caissify database stores player names as raw strings from the PGN headers (e.g. `"Carlsen, Magnus"`, `"Magnus Carlsen"`, `"M. Carlsen"`). FIDE stores the canonical name (e.g. `"Carlsen Magnus"`). These do not match directly.

This is the same problem your Django API solved with a `link_players_to_games` admin task. It is a data-quality problem, not primarily a performance problem.

### 5.2 Matching Strategies (in order of precision)

1. **FIDE ID in PGN header** (highest precision): Some PGN sources include `[WhiteFideId "1503014"]` and `[BlackFideId "4618]"` tags. The Caissify importer can read these during import and immediately write the FIDE ID to `caissify_game_meta`.

2. **Exact canonical name match**: Normalize both names (lowercase, strip punctuation, sort tokens: `"Carlsen Magnus"` → `carlsen magnus`) and do a map lookup. Works for ~80% of games from quality sources.

3. **Fuzzy name match** (offline): For the rest, a one-time offline pass using Levenshtein distance or token-sort ratio. This is CPU-intensive and should run as a Tokio `spawn_blocking` task, never on the hot path.

4. **Manual override table**: A small `caissify_player_alias` column family mapping normalized name strings → FIDE IDs. Populated by admins for high-frequency mismatches.

### 5.3 Storage Design for Linking

The linking information lives in two places:

**In `caissify_game_meta`** (value field — add 8 bytes):
- `white_fide_id: u32` (0 = unlinked)
- `black_fide_id: u32` (0 = unlinked)

**Secondary index `caissify_game_by_fide`** (new CF):
```
Key:   [4-byte FIDE ID LE][2-byte Year LE][6-byte GameId]
Value: [1-byte color (0=white, 1=black)]
```

This is identical in design to `caissify_game_by_player` from Feature 1, but keyed by FIDE ID instead of name hash. Once a game is linked, it is added to this index. Querying all games by a FIDE player becomes a O(limit) prefix scan.

### 5.4 Linking Workflow

**At import time** (zero latency cost):
- The caissify importer already parses the full `MastersGame` struct.
- After writing the game, attempt an in-memory name lookup against a pre-loaded `FideNameIndex` (a `HashMap<NormalizedName, FideId>` loaded from the `fide_player` CF at startup, ~30 MB in RAM).
- If a match is found, write `caissify_game_meta` with the FIDE IDs and add to `caissify_game_by_fide`.

**Background re-linking task** (does not affect serving latency):
- Triggered via `POST /import/fide-link` (admin-only).
- Iterates all `caissify_game_meta` records with `fide_id = 0`.
- Applies fuzzy matching using a sorted token ratio.
- Commits the links in batch writes.
- Can be paused/resumed using the last-seen GameId as cursor.

---

## 6. Performance Impact Analysis

### 6.1 What is completely safe (zero impact on existing requests)

- Adding new column families: RocksDB opens all CFs from the same `DB` instance; adding new CFs does not slow reads on existing CFs.
- Background periodic tasks: already the pattern used for openings and blacklist. Rate-limited by `db_rate_limit`.
- New HTTP routes for new features: Axum routes are resolved via a radix trie; adding 10 new routes costs ~0 µs of extra overhead per existing request.
- The `fide_player` and `fide_rating_history` CFs: read-only for request serving; writes only during monthly background refresh.

### 6.2 What requires careful design to avoid impact

| Feature | Risk | Mitigation |
|---|---|---|
| `caissify_game_by_date` full scan | If implemented naïve (no cursor), a page-100 request scans 5000 records | Cursor pagination eliminates this; enforce max `limit=200` |
| FIDE name search | Scanning all 1M `fide_player` keys to match a name string is ~500 ms | Preload a `HashMap<normalized_name, fide_id>` at startup (~30 MB RAM); O(1) exact match. Fuzzy search → separate endpoint with rate-limiting or a background worker |
| Secondary index writes during import | Writing to 3 CFs per imported game instead of 2 | Import is already serialized by a Mutex; batch write cost is still dominated by fsync. No change to query latency |
| `FideNameIndex` in-memory map | 1M entries × ~30 bytes avg = ~30 MB additional RSS | Well within budget on any deployment with ≥ 1 GB RAM |

### 6.3 What is genuinely hard in RocksDB

Arbitrary string search (e.g. event name contains "Wijk aan Zee") is not what RocksDB is designed for. Options:

1. **Accept the limitation**: only support exact or prefix searches on player name, event, and site. This covers ≥ 95% of real-world query patterns.
2. **Tantivy sidecar index**: [Tantivy](https://github.com/quickwit-oss/tantivy) is a Rust full-text search library. Add a small Tantivy index alongside RocksDB for text search on event/site/player name. The Tantivy index stores only the GameId; the actual data is still read from RocksDB. ~5–50 MB of additional disk, fully embedded, no external service.
3. **Delegate to the Django API**: For complex filtering queries that your Django API handles well (PostgreSQL's `ILIKE`, `gin` trie indexes on text columns), keep using Django. Rust serves positions, pagination by date/rating/player. This is the hybrid approach and is honest about each system's strengths.

---

## 7. Service Architecture — API vs. Worker Split

The current service runs everything in one process. This is correct and efficient for the current workload. As the new features are added, some tasks become heavyweight enough to warrant isolation.

### 7.1 Recommended Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Load Balancer / Reverse Proxy     │
│           (Nginx / Railway ingress / Cloudflare)    │
└────────────────┬──────────────────┬─────────────────┘
                 │                  │
        Public routes          Admin routes
        /caissify               /import/*
        /masters                /compact
        /lichess                /monitor
        /player                 /fide/import
        /fide/player            /caissify/games (write)
        /api-docs
                 │                  │
┌────────────────▼──────────────────▼─────────────────┐
│              caissify-explorer  (existing)           │
│         Axum server  +  RocksDB (embedded)          │
│   Tokio JoinSet background tasks:                   │
│   - periodic_openings_import (~167 min)             │
│   - periodic_blacklist_update (~173 min)            │
│   - NEW: periodic_fide_ratings_update (~32 days)    │
│   - NEW: FideNameIndex in-memory map (loaded once)  │
└─────────────────────────────────────────────────────┘
                 │
                 │  RocksDB data volume (shared)
                 │  (if worker split desired)
┌────────────────▼────────────────────────────────────┐
│         caissify-worker  (optional separate binary) │
│  Reads from the same RocksDB volume (read-only DB)  │
│  or communicates via HTTP to main instance          │
│  Tasks:                                             │
│  - FIDE historical import (one-time)               │
│  - FIDE monthly refresh                            │
│  - Background FIDE ↔ game re-linking               │
│  - Secondary index backfill for existing games      │
└─────────────────────────────────────────────────────┘
```

### 7.2 RocksDB Multi-Instance Safety

RocksDB does not support multiple writers on the same DB path. Options:

**Option A — Single process, background Tokio tasks** (simplest, recommended for start):
All background work runs as `tokio::spawn_blocking` tasks inside the existing server. The server already does this for the player indexer. This is sufficient for monthly FIDE updates and incremental linking. Zero operational complexity.

**Option B — Secondary instance with read-only DB** (for heavy backfill jobs):
RocksDB supports opening the same DB in read-only mode from a second process via `DB::open_for_read_only()`. A worker binary can read all game data and compute links offline, then POST the results to the main server via HTTP (`PUT /import/fide-link`). The main server applies the write batch. This completely isolates backfill I/O from the serving path.

**Option C — Worker writes to a separate CF column family file** (advanced):
Two processes each own disjoint column families and use `DB::open_cf_as_secondary()` for cross-reading. Complex to coordinate; not recommended unless Option A provides insufficient isolation.

**Recommendation**: Start with Option A. If the monthly FIDE import (120 MB of XML parsing + 1M RocksDB writes) causes measurable latency spikes, move FIDE imports to Option B.

### 7.3 Railway Deployment for Worker Split

Railway supports multiple services in one project, each with its own start command and volume mounts. The recommended setup if you split:

- **Service `explorer`**: existing `--bind 0.0.0.0:$PORT --db /data --cors`
- **Service `worker`**: `caissify-worker --db /data` (read-only, or writes via HTTP)
- **Shared volume**: `/data` mounted to both (Railway supports this for the same project)

Both services are built from the same Dockerfile; the entrypoint binary differs.

---

## 8. Implementation Roadmap

### Phase 0 — Foundation (prerequisite, ~1–2 days)
These are non-breaking internal changes that all subsequent features depend on.

- [x] **Add `caissify_game_meta` CF**: compact 15-byte value per GameId (year, ratings, result, FIDE ID slots). Written during import alongside the existing `caissify_game` write. ✅ 2026-03-19
- [x] **Add `caissify_game_by_date` CF**: secondary index `[2-byte Year LE][6-byte GameId]` → `[]`. Written during import. ✅ 2026-03-19
- [x] **Backfill task**: `POST /import/caissify/reindex` iterates all existing `caissify_game` records and populates the two new CFs for historical games. ✅ 2026-03-19

### Phase 1 — Paginated Game List (~2–3 days)

- [x] `GET /caissify/games` endpoint with cursor pagination, date/result/rating filters. ✅ 2026-03-19
- [x] `GET /caissify/games/{id}` single game metadata (fast point-get on `caissify_game_meta`). ✅ 2026-03-19
- [x] Update OpenAPI spec in `src/openapi.rs` to document the new endpoints. ✅ 2026-03-19
- [ ] Add `caissify_games` counter to `/monitor` metrics.

### Phase 2 — FIDE Player Database (~3–4 days)

- [x] Add `fide_player` and `fide_rating_history` CFs to `src/db.rs`. ✅ 2026-03-19
- [x] Implement `FidePlayer` and `FideRatingSnapshot` binary models in `src/model/fide.rs`. ✅ 2026-03-19
- [x] Write `import-fide` binary in `import-pgn/src/bin/import-fide.rs`: downloads and parses FIDE XML (standard_rating_list_xml.zip), ships to `PUT /import/fide` in batches. ✅ 2026-03-19
- [x] `GET /fide/player/{fide_id}` endpoint. ✅ 2026-03-19
- [x] `GET /fide/player/{fide_id}/ratings` endpoint with month range filter. ✅ 2026-03-19
- [x] `PUT /import/fide` admin endpoint (accepts JSON batch `{ month, players[] }`). ✅ 2026-03-19
- [ ] Add `periodic_fide_ratings_update` background task (~32 day interval).
- [x] Update OpenAPI spec. ✅ 2026-03-19

### Phase 3 — FIDE Player ↔ Game Linking (~3–4 days)

- [ ] Load `FideNameIndex` (`HashMap<NormalizedName, u32>`) into `AppState` at startup from the `fide_player` CF.
- [ ] Extend caissify importer: after writing the game, attempt name match and write FIDE IDs to `caissify_game_meta` + `caissify_game_by_fide` CF.
- [ ] Implement `caissify_game_by_fide` CF.
- [ ] `GET /caissify/games?fide_id=INT` on the existing paginated endpoint (uses the new CF).
- [ ] `POST /import/caissify/fide-link` background re-linking pass for existing games.
- [ ] Add FIDE-linked player counts to `/monitor` metrics.

### Phase 4 — FIDE Player Search (~1–2 days)

- [ ] `GET /fide/search?name=STRING` using `FideNameIndex` in-memory exact/prefix match.
- [ ] Decision: accept limitation of exact/prefix only, or integrate Tantivy for fuzzy.
- [ ] If Tantivy: add `tantivy` dependency, build index alongside `fide_player` CF, update in background on FIDE refresh.

### Phase 5 — Hardening and Observability (~1–2 days)

- [ ] Per-CF RocksDB metrics for all new column families in `/monitor`.
- [ ] Add in-memory caches (moka) for `/fide/player/{id}` and `/fide/player/{id}/ratings` — these are read-heavy but update only monthly.
- [ ] Stress test the paginated endpoint with cursor pagination depth > 10,000 records.
- [ ] Load test FIDE name index lookup under concurrent import pressure.

### Total Estimated Effort

| Phase | Effort | Risk |
|---|---|---|
| Phase 0 — Foundation | 1–2 days | Low |
| Phase 1 — Paginated games | 2–3 days | Low |
| Phase 2 — FIDE ratings | 3–4 days | Low |
| Phase 3 — Game linking | 3–4 days | Medium (name matching quality) |
| Phase 4 — Player search | 1–2 days | Low (if prefix only), Medium (if Tantivy) |
| Phase 5 — Hardening | 1–2 days | Low |
| **Total** | **11–17 days** | |


