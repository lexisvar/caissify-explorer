use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use sha1::{Digest, Sha1};
use shakmaty::{Chess, EnPassantMode, Position as _, variant::Variant};
use tokio::sync::Semaphore;

use crate::{
    api::{
        CaissifyQuery, Error, ExplorerGame, ExplorerGameWithUciMove, ExplorerMove, ExplorerResponse,
        Play, PlayPosition, WithSource,
    },
    db::{CacheHint, Database},
    indexer::{BroadcastAllImporter, BroadcastAllRequest, BroadcastAllStatus, BroadcastImporter, CaissifyImporter, ImportStatus, PgnUrlImporter},
    metrics::Metrics,
    model::{
        CaissifyByDateKey, CaissifyByFideKey, CaissifyByPlayerKey, CaissifyByPositionKey,
        CaissifyByRatingKey, CaissifyGameMeta, GameId, GameResult, KeyBuilder, MastersGame,
        MastersGameWithId, player_name_hash,
    },
    opening::Openings,
    state::ExplorerCache,
    util::{ply, spawn_blocking},
};

use super::GameIdPath;

// ─── Basic caissify explorer ──────────────────────────────────────────────────

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_import(
    State(importer): State<CaissifyImporter>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<MastersGameWithId>,
) -> Result<(), Error> {
    spawn_blocking(semaphore, move || importer.import(body)).await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_pgn(
    Path(GameIdPath(id)): Path<GameIdPath>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<MastersGame, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db.caissify().game(id).expect("get caissify game") {
            Some(game) => Ok(game),
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify(
    State(openings): State<&'static RwLock<Openings>>,
    State(db): State<Arc<Database>>,
    State(caissify_cache): State<ExplorerCache<CaissifyQuery>>,
    State(metrics): State<&'static Metrics>,
    State(semaphore): State<&'static Semaphore>,
    Query(WithSource { query, source }): Query<WithSource<CaissifyQuery>>,
) -> Result<Json<ExplorerResponse>, Error> {
    caissify_cache
        .get_with(query.clone(), async move {
            spawn_blocking(semaphore, move || {
                let started_at = Instant::now();
                let openings = openings.read().expect("read openings");
                let PlayPosition { pos, opening } = query.play.position(&openings)?;

                let key = KeyBuilder::caissify()
                    .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
                let cache_hint = CacheHint::from_ply(ply(&pos));
                let caissify_db = db.caissify();
                let entry = caissify_db
                    .read(key, query.since, query.until, cache_hint)
                    .expect("get caissify")
                    .prepare(&query.limits);

                let response = Ok(Json(ExplorerResponse {
                    total: entry.total,
                    moves: entry
                        .moves
                        .into_iter()
                        .map(|p| {
                            let mut pos_after = pos.clone();
                            let san = p.uci.to_move(&pos).map_or(
                                shakmaty::san::SanPlus {
                                    san: shakmaty::san::San::Null,
                                    suffix: None,
                                },
                                |m| {
                                    shakmaty::san::SanPlus::from_move_and_play_unchecked(
                                        &mut pos_after,
                                        m,
                                    )
                                },
                            );
                            ExplorerMove {
                                san,
                                uci: p.uci,
                                average_rating: p.average_rating,
                                average_opponent_rating: p.average_opponent_rating,
                                performance: p.performance,
                                stats: p.stats,
                                game: p.game.and_then(|id| {
                                    caissify_db
                                        .game(id)
                                        .expect("get caissify game")
                                        .map(|info| ExplorerGame::from_masters(id, info))
                                }),
                                opening: openings.classify_exact(&pos_after).cloned(),
                            }
                        })
                        .collect(),
                    top_games: Some(
                        caissify_db
                            .games(entry.top_games.iter().map(|(_, id)| *id))
                            .expect("get caissify games")
                            .into_iter()
                            .zip(entry.top_games.into_iter())
                            .filter_map(|(info, (uci, id))| {
                                info.map(|info| ExplorerGameWithUciMove {
                                    uci,
                                    row: ExplorerGame::from_masters(id, info),
                                })
                            })
                            .collect(),
                    ),
                    opening,
                    recent_games: None,
                    queue_position: None,
                    history: None,
                }));

                metrics.inc_masters(started_at.elapsed(), source, ply(&pos));
                response
            })
            .await
        })
        .await
}

// ─── PGN-URL import ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct PgnUrlImportRequest {
    url: String,
    cookie: String,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_pgn_url_import(
    State(importer): State<PgnUrlImporter>,
    Json(body): Json<PgnUrlImportRequest>,
) -> impl axum::response::IntoResponse {
    if importer.start(body.url, body.cookie) {
        (
            axum::http::StatusCode::ACCEPTED,
            axum::Json(serde_json::json!({
                "message": "PGN import job started in the background",
            })),
        )
    } else {
        (
            axum::http::StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "message": "An import is already running — check /import/caissify/pgn-url/status",
            })),
        )
    }
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_pgn_url_status(
    State(importer): State<PgnUrlImporter>,
) -> axum::Json<ImportStatus> {
    axum::Json(importer.status())
}

// ─── Broadcast import ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct BroadcastImportRequest {
    year: i32,
    month: u8,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_broadcast_import(
    State(importer): State<BroadcastImporter>,
    Json(body): Json<BroadcastImportRequest>,
) -> impl axum::response::IntoResponse {
    if importer.start(body.year, body.month) {
        (
            axum::http::StatusCode::ACCEPTED,
            axum::Json(serde_json::json!({
                "message": format!(
                    "Broadcast import for {}-{:02} started in the background",
                    body.year, body.month
                ),
            })),
        )
    } else {
        (
            axum::http::StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "message": "An import is already running — check /import/caissify/broadcast/status",
            })),
        )
    }
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_broadcast_status(
    State(importer): State<BroadcastImporter>,
) -> axum::Json<ImportStatus> {
    axum::Json(importer.status())
}

// ─── Broadcast all-archives import ───────────────────────────────────────────

#[derive(Deserialize)]
#[serde(default)]
pub struct BroadcastAllImportRequest {
    /// Earliest archive to include, inclusive (e.g. `"2024-01"`). Omit to include all.
    since: Option<String>,
    /// Latest archive to include, inclusive (e.g. `"2026-03"`). Omit to include all.
    until: Option<String>,
    /// Override the list URL (default: `https://database.lichess.org/broadcast/list.txt`).
    list_url: Option<String>,
}

impl Default for BroadcastAllImportRequest {
    fn default() -> Self {
        BroadcastAllImportRequest {
            since: None,
            until: None,
            list_url: None,
        }
    }
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_broadcast_all_import(
    State(importer): State<BroadcastAllImporter>,
    Json(body): Json<BroadcastAllImportRequest>,
) -> impl axum::response::IntoResponse {
    if importer.start(BroadcastAllRequest {
        since: body.since,
        until: body.until,
        list_url: body.list_url,
    }) {
        (
            axum::http::StatusCode::ACCEPTED,
            axum::Json(serde_json::json!({
                "message": "Broadcast all-archives import started in the background",
            })),
        )
    } else {
        (
            axum::http::StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "message": "An import is already running — check /import/caissify/broadcast/all/status",
            })),
        )
    }
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_broadcast_all_status(
    State(importer): State<BroadcastAllImporter>,
) -> axum::Json<BroadcastAllStatus> {
    axum::Json(importer.status())
}

// ─── Reindex endpoints ────────────────────────────────────────────────────────

/// Backfill `caissify_game_meta` and `caissify_game_by_date` for historical
/// games imported before Phase 0. Idempotent — already-indexed games are
/// skipped. Can be slow on large databases; run during a maintenance window.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_reindex(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<String, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db.caissify().reindex_meta() {
            Ok(count) => Ok(format!("reindexed {count} games")),
            Err(err) => {
                log::error!("caissify reindex failed: {err}");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    })
    .await
}

/// Query params for `POST /import/caissify/reindex-meta`.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
pub struct ReindexMetaQuery {
    /// Opaque cursor returned by a prior call; omit to start from the beginning.
    cursor: Option<String>,
    /// Number of game records to process per call (default 2000, max 10000).
    chunk: Option<usize>,
}

/// Response from `POST /import/caissify/reindex-meta`.
#[derive(serde::Serialize)]
pub struct ReindexMetaResponse {
    /// Games whose meta was written or upgraded in this chunk.
    updated: u64,
    /// Opaque cursor to pass to the next call.  `null` when the table is exhausted.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    /// Whether all games have been processed (no more work remaining).
    done: bool,
}

/// Cursor-resumable meta backfill.
///
/// Call repeatedly (passing `cursor` from each response) until `done: true`.
/// Each call processes `chunk` game records (default 2000); larger values
/// reduce round-trips at the cost of longer blocking time under the semaphore.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_reindex_meta(
    Query(q): Query<ReindexMetaQuery>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<ReindexMetaResponse>, Error> {
    // Decode optional cursor (a hex-encoded GameId).
    let cursor: Option<GameId> = match q.cursor.as_deref() {
        None | Some("") => None,
        Some(s) => {
            let mut bytes = [0u8; GameId::SIZE];
            if s.len() != GameId::SIZE * 2 {
                return Err(Error::BadQuery("invalid reindex-meta cursor".into()));
            }
            for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
                let hex = std::str::from_utf8(chunk)
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| Error::BadQuery("invalid reindex-meta cursor".into()))?;
                bytes[i] = hex;
            }
            Some(GameId::read(&mut bytes.as_slice()))
        }
    };
    let chunk_size = q.chunk.unwrap_or(2_000).min(10_000);

    spawn_blocking(semaphore, move || {
        match db.caissify().reindex_meta_from(cursor, chunk_size) {
            Ok((updated, next_id)) => {
                let next_cursor = next_id.map(|id| {
                    id.to_bytes()
                        .iter()
                        .fold(String::new(), |mut s, b| {
                            use std::fmt::Write as _;
                            let _ = write!(s, "{b:02x}");
                            s
                        })
                });
                let done = next_cursor.is_none();
                Ok(Json(ReindexMetaResponse {
                    updated,
                    next_cursor,
                    done,
                }))
            }
            Err(err) => {
                log::error!("reindex-meta failed: {err}");
                Err(Error::Internal)
            }
        }
    })
    .await
}

/// Response from `POST /import/caissify/reindex-position`.
#[derive(serde::Serialize)]
pub struct ReindexPositionResponse {
    /// Total games processed.
    processed: u64,
    /// Total position index entries written.
    entries_written: u64,
}

/// Full backfill pass for the `caissify_game_by_position` CF.
///
/// Iterates every game in `caissify_game`, replays its moves to collect all
/// unique Zobrist positions, and writes one `CaissifyByPositionKey` entry per
/// unique position per game.  Commits every `CHUNK` games to bound memory use.
///
/// Safe to call multiple times — puts are idempotent. Runs entirely inside a
/// single HTTP request; no pagination required.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_reindex_position(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<ReindexPositionResponse>, StatusCode> {
    spawn_blocking(semaphore, move || {
        const CHUNK: usize = 2_000; // games per RocksDB write batch

        let caissify_db = db.caissify();
        let mut cursor: Option<GameId> = None;
        let mut total_processed = 0u64;
        let mut total_entries = 0u64;

        loop {
            let games = caissify_db
                .iter_games_from(cursor, CHUNK)
                .expect("iter games for position reindex");

            if games.is_empty() {
                break;
            }

            cursor = games.last().map(|(id, _)| *id);
            let chunk_len = games.len();

            let mut batch = caissify_db.batch();
            let mut chunk_entries = 0u64;

            for (id, game) in &games {
                let year = u16::from(game.date.year());
                let mut pos = Chess::default();
                let mut seen: std::collections::HashSet<[u8; 12]> =
                    std::collections::HashSet::with_capacity(64);

                for uci in &game.moves {
                    let zobrist = pos.zobrist_hash(EnPassantMode::Legal);
                    let prefix_bytes = KeyBuilder::caissify()
                        .with_zobrist(Variant::Chess, zobrist)
                        .key_bytes();

                    if seen.insert(prefix_bytes) {
                        batch.put_by_position(CaissifyByPositionKey {
                            prefix: prefix_bytes,
                            year,
                            id: *id,
                        });
                        chunk_entries += 1;
                    }

                    match uci.to_move(&pos) {
                        Ok(m) => pos.play_unchecked(m),
                        Err(_) => break,
                    }
                }
            }

            batch.commit().expect("commit position reindex batch");
            total_processed += chunk_len as u64;
            total_entries += chunk_entries;

            log::info!(
                "position reindex: {total_processed} games processed, \
                 {total_entries} entries written"
            );

            if chunk_len < CHUNK {
                break; // last chunk — we're done
            }
        }

        Ok(Json(ReindexPositionResponse {
            processed: total_processed,
            entries_written: total_entries,
        }))
    })
    .await
}

/// Response from `POST /import/caissify/reindex-moves`.
#[derive(serde::Serialize)]
pub struct ReindexMovesResponse {
    /// Total games processed.
    processed: u64,
    /// Total move-fingerprint entries written to `caissify_game_by_moves`.
    entries_written: u64,
}

/// Backfill (or repair) the `caissify_game_by_moves` column family.
///
/// Iterates every game in `caissify_game` and writes one SHA-1 entry per game
/// into `caissify_game_by_moves`. Existing entries are overwritten with the
/// same value (idempotent — SHA-1 is deterministic). Games with no moves are
/// skipped.
///
/// This is the server-side half of Phase 7 deduplication: after running this
/// once, any future import of a duplicate game (even with a different player
/// name) will be rejected via the `has_by_moves` check in `CaissifyImporter`.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_reindex_moves(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<ReindexMovesResponse>, StatusCode> {
    spawn_blocking(semaphore, move || {
        const CHUNK: usize = 2_000;

        let caissify_db = db.caissify();
        let mut cursor: Option<GameId> = None;
        let mut total_processed = 0u64;
        let mut total_written = 0u64;

        loop {
            let games = caissify_db
                .iter_games_from(cursor, CHUNK)
                .expect("iter games for moves reindex");

            if games.is_empty() {
                break;
            }

            cursor = games.last().map(|(id, _)| *id);
            let chunk_len = games.len();

            let mut batch = caissify_db.batch();
            let mut chunk_written = 0u64;

            for (id, game) in &games {
                if game.moves.is_empty() {
                    continue;
                }
                let moves_str = game
                    .moves
                    .iter()
                    .map(|m| m.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                let fingerprint: [u8; 20] = Sha1::digest(moves_str.as_bytes()).into();
                batch.put_by_moves(fingerprint, *id);
                chunk_written += 1;
            }

            batch.commit().expect("commit moves reindex batch");
            total_processed += chunk_len as u64;
            total_written += chunk_written;

            log::info!(
                "moves reindex: {total_processed} games processed, \
                 {total_written} fingerprints written"
            );

            if chunk_len < CHUNK {
                break;
            }
        }

        Ok(Json(ReindexMovesResponse {
            processed: total_processed,
            entries_written: total_written,
        }))
    })
    .await
}

/// Response from `POST /import/caissify/reindex-player`.
#[derive(serde::Serialize)]
pub struct ReindexPlayerResponse {
    processed: u64,
    entries_written: u64,
}

/// Full backfill pass for the `caissify_game_by_player` CF.
///
/// Iterates every game in `caissify_game`, computes `player_name_hash` for the
/// White and Black player names, and writes one entry per player per game.
/// Safe to call multiple times — puts are idempotent.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_reindex_player(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<ReindexPlayerResponse>, StatusCode> {
    spawn_blocking(semaphore, move || {
        const CHUNK: usize = 2_000;

        let caissify_db = db.caissify();
        let mut cursor: Option<GameId> = None;
        let mut total_processed = 0u64;
        let mut total_written = 0u64;

        loop {
            let games = caissify_db
                .iter_games_from(cursor, CHUNK)
                .expect("iter games for player reindex");

            if games.is_empty() {
                break;
            }

            cursor = games.last().map(|(id, _)| *id);
            let chunk_len = games.len();
            let mut batch = caissify_db.batch();
            let mut chunk_written = 0u64;

            for (id, game) in &games {
                let year = u16::from(game.date.year());

                batch.put_by_player(
                    CaissifyByPlayerKey {
                        hash: player_name_hash(&game.players.white.name),
                        year,
                        id: *id,
                    },
                    false,
                );
                batch.put_by_player(
                    CaissifyByPlayerKey {
                        hash: player_name_hash(&game.players.black.name),
                        year,
                        id: *id,
                    },
                    true,
                );
                chunk_written += 2;
            }

            batch.commit().expect("commit player reindex batch");
            total_processed += chunk_len as u64;
            total_written += chunk_written;

            log::info!(
                "player reindex: {total_processed} games processed, \
                 {total_written} index entries written"
            );

            if chunk_len < CHUNK {
                break;
            }
        }

        Ok(Json(ReindexPlayerResponse {
            processed: total_processed,
            entries_written: total_written,
        }))
    })
    .await
}

// ─── Paginated game list ──────────────────────────────────────────────────────

/// Encode a 20-byte CaissifyByPositionKey as a 42-char lowercase hex string
/// (1-byte sort tag `03` + 20 data bytes).
fn encode_position_page_token(key: CaissifyByPositionKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::from("03"), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a 42-char position page token.  Validates the `03` sort tag and
/// that the embedded prefix matches `expected_prefix`.
fn decode_position_page_token(
    s: &str,
    expected_prefix: &[u8; 12],
) -> Option<CaissifyByPositionKey> {
    if s.len() != 42 || !s.starts_with("03") {
        return None;
    }
    let s = &s[2..];
    let mut bytes = [0u8; CaissifyByPositionKey::SIZE];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    let key = CaissifyByPositionKey::read(&mut bytes.as_slice());
    (&key.prefix == expected_prefix).then_some(key)
}

/// Encode a 16-byte CaissifyByPlayerKey as a 34-char lowercase hex string
/// (1-byte sort tag `05` + 16 data bytes).
fn encode_player_page_token(key: CaissifyByPlayerKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::from("05"), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a 34-char player page token.  Validates the `05` sort tag and
/// the embedded hash to prevent cross-player token reuse.
fn decode_player_page_token(s: &str, expected_hash: u64) -> Option<CaissifyByPlayerKey> {
    if s.len() != 34 || !s.starts_with("05") {
        return None;
    }
    let s = &s[2..];
    let mut bytes = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    let key = CaissifyByPlayerKey::read(&mut bytes.as_slice());
    (key.hash == expected_hash).then_some(key)
}

/// Encode a 9-byte CaissifyByDateKey as a 20-char lowercase hex string
/// (sort tag `01` + 9 key bytes = 2 + 18 hex chars).
fn encode_page_token(key: CaissifyByDateKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::from("01"), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a 20-char hex date page token.  Validates the `01` sort tag.
fn decode_page_token(s: &str) -> Option<CaissifyByDateKey> {
    if s.len() != 20 || !s.starts_with("01") {
        return None;
    }
    let s = &s[2..];
    let mut bytes = [0u8; 9];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    Some(CaissifyByDateKey::read(&mut bytes.as_slice()))
}

/// Encode a 12-byte CaissifyByFideKey as a 26-char lowercase hex string
/// (1-byte sort tag `04` + 12 data bytes).
fn encode_fide_page_token(key: CaissifyByFideKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::from("04"), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a 26-char FIDE page token.  Validates the `04` sort tag and the
/// embedded FIDE ID to prevent cross-player token reuse.
fn decode_fide_page_token(s: &str, expected_fide_id: u32) -> Option<CaissifyByFideKey> {
    if s.len() != 26 || !s.starts_with("04") {
        return None;
    }
    let s = &s[2..];
    let mut bytes = [0u8; 12];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    let key = CaissifyByFideKey::read(&mut bytes.as_slice());
    (key.fide_id == expected_fide_id).then_some(key)
}

/// Encode a 10-byte `CaissifyByRatingKey` as a 22-char lowercase hex string
/// (1-byte sort tag `02` + 10 data bytes).
fn encode_rating_page_token(key: CaissifyByRatingKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::from("02"), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a 22-char rating page token back to a `CaissifyByRatingKey`.
/// Validates the `02` sort tag.
fn decode_rating_page_token(s: &str) -> Option<CaissifyByRatingKey> {
    if s.len() != 22 || !s.starts_with("02") {
        return None;
    }
    let s = &s[2..];
    let mut bytes = [0u8; CaissifyByRatingKey::SIZE];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    Some(CaissifyByRatingKey::read(&mut bytes.as_slice()))
}

/// Unwrap an optional page token string, returning HTTP 400 when the token is
/// present but fails to decode (wrong sort tag, corrupted hex, etc.).
/// A missing or empty token yields `Ok(None)`.
fn parse_page_token<K>(
    raw: Option<&str>,
    decode: impl Fn(&str) -> Option<K>,
) -> Result<Option<K>, Error> {
    match raw {
        None | Some("") => Ok(None),
        Some(s) => decode(s)
            .map(Some)
            .ok_or_else(|| Error::BadQuery("invalid or stale page_token".into())),
    }
}

/// Serialised entry in the paginated game list.
#[derive(serde::Serialize)]
pub struct CaissifyGameListEntry {
    id: String,
    white: String,
    white_rating: u16,
    black: String,
    black_rating: u16,
    event: String,
    site: String,
    date: String,
    round: String,
    result: GameResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    white_fide_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    black_fide_id: Option<u32>,
    /// Half-move count. 0 = unknown (game imported before Phase 8; run reindex).
    move_count: u8,
}

#[derive(serde::Serialize)]
pub struct CaissifyGameListResponse {
    games: Vec<CaissifyGameListEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
    /// Set when the player-in-position scan hit the server-side budget before
    /// filling a full page. The client should treat this as a partial result.
    #[serde(skip_serializing_if = "Option::is_none")]
    scan_exhausted: Option<bool>,
    /// Total matching entries, only present when `include_total=true` is sent
    /// and a scoping filter (`fide_id` or `fen`) is provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
pub struct CaissifyGamesQuery {
    /// Number of results to return. Default 50, max 200.
    limit: Option<usize>,
    /// Earliest year to include (inclusive).
    since: Option<u16>,
    /// Latest year to include (inclusive).
    until: Option<u16>,
    /// Opaque cursor from a previous response.
    page_token: Option<String>,
    /// If true (default) return newest / highest-rated games first.
    reverse: Option<bool>,
    /// Filter by game result: "white", "draw", or "black".
    result: Option<GameResult>,
    /// Minimum rating of either player.
    min_rating: Option<u16>,
    /// Maximum rating of either player.
    max_rating: Option<u16>,
    /// Minimum half-move count (requires meta v2; games with move_count=0 are
    /// always passed through because they lack the field).
    min_moves: Option<u8>,
    /// Maximum half-move count (same caveat as min_moves).
    max_moves: Option<u8>,
    /// Sort dimension. `"date"` (default) or `"rating"`.
    /// On a FEN+sort_by=rating path the position and rating indices are
    /// intersected; game_at_position() is the tiebreaker.
    sort_by: Option<String>,
    /// Filter to games by a specific FIDE player (FIDE ID integer).
    /// When set the `caissify_game_by_fide` secondary index is used — O(limit)
    /// regardless of how many games exist in the database.
    fide_id: Option<u32>,
    /// Filter to games by player name (any formatting variant is accepted —
    /// the name is normalised and hashed before lookup).  Uses the
    /// `caissify_game_by_player` index, which has 100 % coverage.
    player: Option<String>,
    /// Combined with `fide_id` or `player`: restrict to games where the player
    /// was `"white"` or `"black"`.  Omit for both colours.
    color: Option<String>,
    /// Optional position filter: FEN + UCI moves to play. When provided the
    /// response uses the `caissify_game_by_position` CF for full paginated
    /// iteration over all games through that position (with `next_page_token`).
    #[serde(flatten)]
    pub position_filter: Play,
    /// When `true`, include a `total` count in the response.
    /// Allowed only when `fide_id` or a position (`fen`/`play`) is provided;
    /// returns HTTP 429 otherwise to prevent unbounded full-table scans.
    include_total: Option<bool>,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_games(
    Query(q): Query<CaissifyGamesQuery>,
    State(openings): State<&'static RwLock<Openings>>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<CaissifyGameListResponse>, Error> {
    // ── Input validation (before blocking thread) ────────────────────────────
    let has_fen = q.position_filter.fen.is_some() || !q.position_filter.play.is_empty();
    let has_fide = q.fide_id.is_some();
    let has_player = q.player.is_some();

    if has_fide && has_player {
        return Err(Error::BadQuery(
            "fide_id and player are mutually exclusive".into(),
        ));
    }
    if q.color.is_some() && !has_fide && !has_player {
        return Err(Error::BadQuery("color requires fide_id or player".into()));
    }
    if q.include_total == Some(true) && !has_fide && !has_fen {
        return Err(Error::TooManyRequests);
    }
    let sort_by_rating = match q.sort_by.as_deref() {
        None | Some("date") => false,
        Some("rating") => true,
        Some(_) => {
            return Err(Error::BadQuery(
                "sort_by must be 'date' or 'rating'".into(),
            ))
        }
    };

    spawn_blocking(semaphore, move || {
        let limit = q.limit.unwrap_or(50).min(200);
        let since = q.since.unwrap_or(0);
        let until = q.until.unwrap_or(u16::MAX);
        let reverse = q.reverse.unwrap_or(true);
        let caissify_db = db.caissify();

        // Pre-compute optional player hash and color filter once.
        let player_hash: Option<u64> = q.player.as_deref().map(player_name_hash);
        let color_filter: Option<bool> = match q.color.as_deref() {
            Some("white") => Some(false), // is_black = false
            Some("black") => Some(true),  // is_black = true
            _ => None,
        };

        // Shared meta filter — applied on every path.
        let meta_passes = |meta: &CaissifyGameMeta| -> bool {
            if let Some(rf) = q.result {
                if meta.result != rf {
                    return false;
                }
            }
            let max_r = meta.white_rating.max(meta.black_rating);
            if q.min_rating.is_some_and(|m| max_r < m) {
                return false;
            }
            if q.max_rating.is_some_and(|m| max_r > m) {
                return false;
            }
            // move_count == 0 means unknown (v1 record) — always pass through.
            if meta.move_count != 0 {
                if q.min_moves.is_some_and(|m| meta.move_count < m) {
                    return false;
                }
                if q.max_moves.is_some_and(|m| meta.move_count > m) {
                    return false;
                }
            }
            true
        };

        // Build a CaissifyGameListEntry from its parts.
        let make_entry = |id: GameId, meta: &CaissifyGameMeta, game: &MastersGame| {
            CaissifyGameListEntry {
                id: id.to_string(),
                white: game.players.white.name.clone(),
                white_rating: meta.white_rating,
                black: game.players.black.name.clone(),
                black_rating: meta.black_rating,
                event: game.event.clone(),
                site: game.site.clone(),
                date: game.date.to_string(),
                round: game.round.clone(),
                result: meta.result,
                white_fide_id: (meta.white_fide_id != 0).then_some(meta.white_fide_id),
                black_fide_id: (meta.black_fide_id != 0).then_some(meta.black_fide_id),
                move_count: meta.move_count,
            }
        };

        // ── Path 1: FEN + fide_id — use case B (player in position) ─────────
        // Streaming position scan with a server-side scan budget. The `fide_id`
        // check is a point-get on meta per entry, not a secondary index scan —
        // this is efficient because the meta record is 16 bytes.
        if has_fen && has_fide {
            const SCAN_CAP: usize = 5000;
            let fide_id = q.fide_id.unwrap();

            let openings_guard = openings.read().expect("read openings");
            let PlayPosition { pos, .. } = q.position_filter.position(&openings_guard)?;
            drop(openings_guard);

            let key_prefix_obj = KeyBuilder::caissify()
                .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
            let prefix_bytes = key_prefix_obj.key_bytes();

            let initial_cursor = parse_page_token(
                q.page_token.as_deref(),
                |s| decode_position_page_token(s, &prefix_bytes),
            )?;

            let mut scan_count = 0usize;
            let mut result_cursor: Option<CaissifyByPositionKey> = initial_cursor;
            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            let mut next_page_token: Option<String> = None;
            let scan_exhausted;

            'outer: loop {
                let remaining = limit.saturating_sub(games.len());
                let budget_left = SCAN_CAP.saturating_sub(scan_count);
                if remaining == 0 || budget_left == 0 {
                    scan_exhausted = budget_left == 0 && games.len() < limit;
                    break;
                }
                let chunk_size = remaining.min(budget_left).min(64);
                let chunk = caissify_db
                    .iter_by_position(
                        prefix_bytes,
                        since,
                        until,
                        result_cursor,
                        chunk_size,
                        reverse,
                    )
                    .expect("iter caissify by position for fide");

                let at_end = chunk.len() < chunk_size;

                for key in &chunk {
                    scan_count += 1;
                    result_cursor = Some(*key);
                    let Some(meta) = caissify_db
                        .game_meta(key.id)
                        .expect("get meta for fide+position")
                    else {
                        continue;
                    };
                    let fide_matches = match color_filter {
                        Some(false) => meta.white_fide_id == fide_id,
                        Some(true) => meta.black_fide_id == fide_id,
                        None => {
                            meta.white_fide_id == fide_id || meta.black_fide_id == fide_id
                        }
                    };
                    if !fide_matches || !meta_passes(&meta) {
                        continue;
                    }
                    let Some(game) = caissify_db
                        .game(key.id)
                        .expect("get game for fide+position")
                    else {
                        continue;
                    };
                    games.push(make_entry(key.id, &meta, &game));
                    if games.len() >= limit {
                        next_page_token = Some(encode_position_page_token(*key));
                        scan_exhausted = false;
                        break 'outer;
                    }
                }

                if at_end {
                    scan_exhausted = false;
                    break;
                }
                if scan_count >= SCAN_CAP {
                    scan_exhausted = true;
                    break;
                }
            }

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: scan_exhausted.then_some(true),
                total: q.include_total.filter(|&b| b).map(|_| {
                    caissify_db
                        .count_by_fide(fide_id, since, until, color_filter)
                        .expect("count_by_fide for include_total")
                }),
            }));
        }

        // ── Path 2: FEN + sort_by=rating — intersect position + rating CFs ──
        if has_fen && sort_by_rating {
            const SCAN_CAP: usize = 5_000;

            let openings_guard = openings.read().expect("read openings");
            let PlayPosition { pos, .. } = q.position_filter.position(&openings_guard)?;
            drop(openings_guard);

            let key_prefix_obj = KeyBuilder::caissify()
                .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
            let prefix_bytes = key_prefix_obj.key_bytes();

            let rating_floor = q.min_rating.unwrap_or(0);
            let rating_ceiling = q.max_rating.unwrap_or(u16::MAX);
            let mut cursor =
                parse_page_token(q.page_token.as_deref(), decode_rating_page_token)?;

            let mut scan_count = 0usize;
            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            let mut next_page_token: Option<String> = None;
            let scan_exhausted;

            'outer: loop {
                let remaining = limit.saturating_sub(games.len());
                let budget_left = SCAN_CAP.saturating_sub(scan_count);
                if remaining == 0 || budget_left == 0 {
                    scan_exhausted = budget_left == 0 && games.len() < limit;
                    break;
                }
                let chunk_size = remaining.min(budget_left).min(64);
                let chunk = caissify_db
                    .iter_by_rating(
                        rating_floor,
                        rating_ceiling,
                        since,
                        until,
                        cursor,
                        chunk_size,
                        reverse,
                    )
                    .expect("iter caissify by rating for fen");

                let at_end = chunk.len() < chunk_size;

                for key in &chunk {
                    scan_count += 1;
                    cursor = Some(*key);
                    if !caissify_db.game_at_position(prefix_bytes, key.year, key.id) {
                        continue;
                    }
                    let Some(meta) = caissify_db
                        .game_meta(key.id)
                        .expect("get meta for rating+fen")
                    else {
                        continue;
                    };
                    if !meta_passes(&meta) {
                        continue;
                    }
                    let Some(game) = caissify_db
                        .game(key.id)
                        .expect("get game for rating+fen")
                    else {
                        continue;
                    };
                    games.push(make_entry(key.id, &meta, &game));
                    if games.len() >= limit {
                        next_page_token = Some(encode_rating_page_token(*key));
                        scan_exhausted = false;
                        break 'outer;
                    }
                }

                if at_end {
                    scan_exhausted = false;
                    break;
                }
                if scan_count >= SCAN_CAP {
                    scan_exhausted = true;
                    break;
                }
            }

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: scan_exhausted.then_some(true),
                total: None,
            }));
        }

        // ── Path 3: FEN + player — player index primary, position membership check ─
        // Player filter is also active: use the player index as the primary scan.
        // Each result gets an O(1) position membership check via game_at_position().
        // This beats iterating the position CF when the player has far fewer games
        // than the position has total games.
        if has_fen && player_hash.is_some() {
            let hash = player_hash.unwrap();

            let openings_guard = openings.read().expect("read openings");
            let PlayPosition { pos, .. } = q.position_filter.position(&openings_guard)?;
            drop(openings_guard);
            let kp = KeyBuilder::caissify()
                .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
            let position_prefix = kp.key_bytes();

            let cursor = parse_page_token(
                q.page_token.as_deref(),
                |s| decode_player_page_token(s, hash),
            )?;

            let scan_size = limit + 1;
            let player_keys = caissify_db
                .iter_by_player(hash, since, until, color_filter, cursor, scan_size, reverse)
                .expect("iter caissify by player+fen");

            let has_more = player_keys.len() > limit;
            let page_pairs = &player_keys[..player_keys.len().min(limit)];

            let full_games = caissify_db
                .games(page_pairs.iter().map(|(k, _)| k.id))
                .expect("batch fetch games for player+fen");

            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            for ((key, _), maybe_game) in page_pairs.iter().zip(full_games.iter()) {
                if !caissify_db.game_at_position(position_prefix, key.year, key.id) {
                    continue;
                }
                let Some(meta) = caissify_db
                    .game_meta(key.id)
                    .expect("get meta for player+fen")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };
                if !meta_passes(&meta) {
                    continue;
                }
                games.push(make_entry(key.id, &meta, game));
            }

            let next_page_token = if has_more {
                page_pairs.last().map(|(k, _)| encode_player_page_token(*k))
            } else {
                None
            };

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: None,
                total: None,
            }));
        }

        // ── Path 4: FEN only, sort_by=date (default) ─────────────────────────
        if has_fen {
            let openings_guard = openings.read().expect("read openings");
            let PlayPosition { pos, .. } = q.position_filter.position(&openings_guard)?;
            drop(openings_guard);

            let key_prefix_obj = KeyBuilder::caissify()
                .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
            let prefix_bytes = key_prefix_obj.key_bytes();

            let cursor = parse_page_token(
                q.page_token.as_deref(),
                |s| decode_position_page_token(s, &prefix_bytes),
            )?;

            let scan_size = limit + 1;
            let pos_keys = caissify_db
                .iter_by_position(prefix_bytes, since, until, cursor, scan_size, reverse)
                .expect("iter caissify by position");

            let has_more = pos_keys.len() > limit;
            let page_keys = &pos_keys[..pos_keys.len().min(limit)];

            let full_games = caissify_db
                .games(page_keys.iter().map(|k| k.id))
                .expect("batch fetch games for position");

            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            for (key, maybe_game) in page_keys.iter().zip(full_games.iter()) {
                let Some(meta) = caissify_db
                    .game_meta(key.id)
                    .expect("get meta for position")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };
                if !meta_passes(&meta) {
                    continue;
                }
                games.push(make_entry(key.id, &meta, game));
            }

            let next_page_token = if has_more {
                page_keys.last().map(|k| encode_position_page_token(*k))
            } else {
                None
            };

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: None,
                total: q.include_total.filter(|&b| b).map(|_| {
                    caissify_db
                        .count_by_position(prefix_bytes, since, until)
                        .expect("count_by_position for include_total")
                }),
            }));
        }

        // ── Path 5: player-name filter (no FEN) ──────────────────────────────
        if let Some(hash) = player_hash {
            let cursor = parse_page_token(
                q.page_token.as_deref(),
                |s| decode_player_page_token(s, hash),
            )?;

            let scan_size = limit + 1;
            let player_keys = caissify_db
                .iter_by_player(hash, since, until, color_filter, cursor, scan_size, reverse)
                .expect("iter caissify by player");

            let has_more = player_keys.len() > limit;
            let page_pairs = &player_keys[..player_keys.len().min(limit)];

            let full_games = caissify_db
                .games(page_pairs.iter().map(|(k, _)| k.id))
                .expect("batch fetch games for player");

            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            for ((key, _), maybe_game) in page_pairs.iter().zip(full_games.iter()) {
                let Some(meta) = caissify_db
                    .game_meta(key.id)
                    .expect("get meta for player")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };
                if !meta_passes(&meta) {
                    continue;
                }
                games.push(make_entry(key.id, &meta, game));
            }

            let next_page_token = if has_more {
                page_pairs.last().map(|(k, _)| encode_player_page_token(*k))
            } else {
                None
            };

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: None,
                total: None,
            }));
        }

        // ── Path 6: FIDE player (no FEN) ─────────────────────────────────────
        if let Some(fide_id) = q.fide_id {
            let cursor = parse_page_token(
                q.page_token.as_deref(),
                |s| decode_fide_page_token(s, fide_id),
            )?;

            let scan_size = limit + 1;
            let fide_keys = caissify_db
                .iter_by_fide(fide_id, since, until, color_filter, cursor, scan_size, reverse)
                .expect("iter caissify by fide");

            let has_more = fide_keys.len() > limit;
            let page_pairs = &fide_keys[..fide_keys.len().min(limit)];

            let full_games = caissify_db
                .games(page_pairs.iter().map(|(k, _)| k.id))
                .expect("batch fetch games for fide");

            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            for ((key, _is_black), maybe_game) in page_pairs.iter().zip(full_games.iter()) {
                let Some(meta) = caissify_db
                    .game_meta(key.id)
                    .expect("get meta for fide player")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };
                if !meta_passes(&meta) {
                    continue;
                }
                games.push(make_entry(key.id, &meta, game));
            }

            let next_page_token = if has_more {
                page_pairs.last().map(|(k, _)| encode_fide_page_token(*k))
            } else {
                None
            };

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: None,
                total: q.include_total.filter(|&b| b).map(|_| {
                    caissify_db
                        .count_by_fide(fide_id, since, until, color_filter)
                        .expect("count_by_fide for include_total path6")
                }),
            }));
        }

        // ── Path 7: sort_by=rating (no FEN, no player filter) ────────────────
        if sort_by_rating {
            let rating_floor = q.min_rating.unwrap_or(0);
            let rating_ceiling = q.max_rating.unwrap_or(u16::MAX);
            let cursor = parse_page_token(q.page_token.as_deref(), decode_rating_page_token)?;

            let scan_size = limit + 1;
            let rating_keys = caissify_db
                .iter_by_rating(
                    rating_floor,
                    rating_ceiling,
                    since,
                    until,
                    cursor,
                    scan_size,
                    reverse,
                )
                .expect("iter caissify by rating");

            let has_more = rating_keys.len() > limit;
            let page_keys = &rating_keys[..rating_keys.len().min(limit)];

            let full_games = caissify_db
                .games(page_keys.iter().map(|k| k.id))
                .expect("batch fetch games for rating");

            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            for (key, maybe_game) in page_keys.iter().zip(full_games.iter()) {
                let Some(meta) = caissify_db
                    .game_meta(key.id)
                    .expect("get meta for rating")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };
                if !meta_passes(&meta) {
                    continue;
                }
                games.push(make_entry(key.id, &meta, game));
            }

            let next_page_token = if has_more {
                page_keys.last().map(|k| encode_rating_page_token(*k))
            } else {
                None
            };

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
                scan_exhausted: None,
                total: None,
            }));
        }

        // ── Path 8: date-index (default fallback) ────────────────────────────
        let initial_cursor = parse_page_token(q.page_token.as_deref(), decode_page_token)?;
        let scan_size = limit + 1;
        let keys = caissify_db
            .iter_by_date(since, until, initial_cursor, scan_size, reverse)
            .expect("iter caissify by date");

        let has_more = keys.len() > limit;
        let page_keys = &keys[..keys.len().min(limit)];

        let full_games = caissify_db
            .games(page_keys.iter().map(|k| k.id))
            .expect("batch fetch caissify games");

        let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(page_keys.len());
        for (key, maybe_game) in page_keys.iter().zip(full_games.iter()) {
            let Some(meta) = caissify_db
                .game_meta(key.id)
                .expect("get caissify game meta")
            else {
                continue;
            };
            let Some(game) = maybe_game.as_ref() else {
                continue;
            };
            if !meta_passes(&meta) {
                continue;
            }
            games.push(make_entry(key.id, &meta, game));
        }

        let next_page_token = if has_more {
            page_keys.last().map(|k| encode_page_token(*k))
        } else {
            None
        };

        Ok(Json(CaissifyGameListResponse {
            games,
            next_page_token,
            scan_exhausted: None,
            total: None,
        }))
    })
    .await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_game_meta_endpoint(
    Path(GameIdPath(id)): Path<GameIdPath>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<CaissifyGameMeta>, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db
            .caissify()
            .game_meta(id)
            .expect("get caissify game meta")
        {
            Some(meta) => Ok(Json(meta)),
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}
