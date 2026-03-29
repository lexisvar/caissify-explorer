use std::{convert::Infallible, sync::Arc};

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Response as AxumResponse,
};
use bytes::Bytes;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    api::Error,
    db::Database,
    model::{
        CaissifyByFideKey, CaissifyGameMeta, FideFlag, FideNameIndex, FidePlayer, FideRatingKey,
        FideRatingSnapshot, GameId, Month,
    },
    tasks::fide_ratings_import_once,
    util::spawn_blocking,
};
// ─── GET /fide/player/:fide_id/pgn ───────────────────────────────────────────

#[derive(Deserialize)]
pub struct FidePlayerPgnQuery {
    /// Earliest year to include (inclusive, default: all time).
    pub since: Option<u16>,
    /// Latest year to include (inclusive, default: all time).
    pub until: Option<u16>,
    /// Restrict to games played as this colour (`white` or `black`).
    pub color: Option<String>,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn fide_player_pgn(
    Path(fide_id): Path<u32>,
    Query(params): Query<FidePlayerPgnQuery>,
    State(db): State<Arc<Database>>,
) -> AxumResponse {
    let since = params.since.unwrap_or(0);
    let until = params.until.unwrap_or(u16::MAX);
    let color_filter: Option<bool> = match params.color.as_deref() {
        Some("white") => Some(false), // value=0 in the index means White
        Some("black") => Some(true),  // value=1 means Black
        _ => None,
    };

    // Channel capacity: 32 PGN chunks buffered ahead of the network write.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(32);

    tokio::task::spawn_blocking(move || {
        const PAGE: usize = 200;
        let mut cursor: Option<CaissifyByFideKey> = None;

        loop {
            // Fetch one page of index keys.
            let keys = db
                .caissify()
                .iter_by_fide(fide_id, since, until, color_filter, cursor, PAGE, false)
                .expect("iter_by_fide");

            let has_more = keys.len() > PAGE;
            let page = &keys[..keys.len().min(PAGE)];

            if page.is_empty() {
                break;
            }

            // Batch-fetch the full game records.
            let games = db
                .caissify()
                .games(page.iter().map(|(k, _)| k.id))
                .expect("batch fetch games");

            for game in games.into_iter().flatten() {
                let mut pgn = game.to_pgn_bytes();
                pgn.push(b'\n'); // blank line between games (PGN standard)
                if tx.blocking_send(Ok(Bytes::from(pgn))).is_err() {
                    return; // client disconnected — stop early
                }
            }

            if !has_more {
                break;
            }
            cursor = page.last().map(|(k, _)| *k);
        }
    });

    AxumResponse::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/x-chess-pgn")
        .header("Content-Disposition", format!("attachment; filename=\"fide_{fide_id}.pgn\""))
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(ReceiverStream::new(rx)))
        .unwrap()
}
// ─── GET /fide/player/:fide_id ────────────────────────────────────────────────

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn fide_player(
    Path(fide_id): Path<u32>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    spawn_blocking(semaphore, move || {
        let fide_db = db.fide();
        match fide_db.get_player(fide_id).expect("get fide player") {
            Some(player) => {
                let mut json =
                    serde_json::to_value(&player).expect("serialize fide player");
                // Attach the latest rating snapshot so callers don't need a
                // second round-trip to /fide/player/{id}/ratings.
                if let Some((_month, snap)) = fide_db
                    .get_latest_rating_snapshot(fide_id)
                    .expect("get latest fide rating")
                {
                    let obj = json.as_object_mut().unwrap();
                    macro_rules! ins_rating {
                        ($key:literal, $val:expr) => {
                            obj.insert(
                                $key.into(),
                                if $val > 0 {
                                    $val.into()
                                } else {
                                    serde_json::Value::Null
                                },
                            );
                        };
                    }
                    macro_rules! ins_u {
                        ($key:literal, $val:expr) => {
                            obj.insert($key.into(), ($val as u64).into());
                        };
                    }
                    ins_rating!("rating_standard", snap.standard);
                    ins_u!("games_standard", snap.games_standard);
                    ins_u!("k_standard", snap.k_standard);
                    ins_rating!("rating_rapid", snap.rapid);
                    ins_u!("games_rapid", snap.games_rapid);
                    ins_u!("k_rapid", snap.k_rapid);
                    ins_rating!("rating_blitz", snap.blitz);
                    ins_u!("games_blitz", snap.games_blitz);
                    ins_u!("k_blitz", snap.k_blitz);
                }
                Ok(Json(json))
            }
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

// ─── GET /fide/player/:fide_id/ratings ───────────────────────────────────────

#[serde_as]
#[derive(serde::Deserialize)]
pub struct FideRatingsQuery {
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    since: Option<Month>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    until: Option<Month>,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn fide_player_ratings(
    Path(fide_id): Path<u32>,
    Query(query): Query<FideRatingsQuery>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    spawn_blocking(semaphore, move || {
        let history = db
            .fide()
            .get_rating_history(fide_id, query.since, query.until)
            .expect("get fide rating history");

        if history.is_empty() {
            // Return 404 if no history at all (player not found or no ratings)
            return match db.fide().get_player(fide_id).expect("check fide player") {
                None => Err(StatusCode::NOT_FOUND),
                Some(_) => Ok(Json(serde_json::json!([]))),
            };
        }

        let entries: Vec<_> = history
            .into_iter()
            .map(|(month, snap)| {
                serde_json::json!({
                    "month": month.to_string(),
                    "standard": if snap.standard > 0 { Some(snap.standard) } else { None },
                    "rapid": if snap.rapid > 0 { Some(snap.rapid) } else { None },
                    "blitz": if snap.blitz > 0 { Some(snap.blitz) } else { None },
                })
            })
            .collect();

        Ok(Json(serde_json::json!(entries)))
    })
    .await
}

// ─── PUT /import/fide ─────────────────────────────────────────────────────────

/// One record in a FIDE import batch — all three time controls + profile.
#[derive(serde::Deserialize)]
pub struct FideImportRecord {
    fide_id: u32,
    name: String,
    country: String,
    #[serde(default)]
    sex: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    w_title: String,
    #[serde(default)]
    o_title: String,
    #[serde(default)]
    foa_title: String,
    #[serde(default)]
    birth_year: u16,
    /// "active" | "inactive" (anything else → Unknown)
    #[serde(default)]
    flag: String,
    #[serde(default)]
    standard: u16,
    #[serde(default)]
    rapid: u16,
    #[serde(default)]
    blitz: u16,
    #[serde(default)]
    games_standard: u16,
    #[serde(default)]
    games_rapid: u16,
    #[serde(default)]
    games_blitz: u16,
    #[serde(default)]
    k_standard: u8,
    #[serde(default)]
    k_rapid: u8,
    #[serde(default)]
    k_blitz: u8,
}

#[serde_as]
#[derive(serde::Deserialize)]
pub struct FideImportBatch {
    #[serde_as(as = "DisplayFromStr")]
    month: Month,
    players: Vec<FideImportRecord>,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn fide_import(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<FideImportBatch>,
) -> Result<String, StatusCode> {
    spawn_blocking(semaphore, move || {
        let fide_db = db.fide();
        let mut batch = fide_db.batch();
        let count = body.players.len();

        for rec in body.players {
            let player = FidePlayer {
                fide_id: rec.fide_id,
                name: rec.name,
                country: rec.country,
                sex: rec.sex,
                title: rec.title,
                w_title: rec.w_title,
                o_title: rec.o_title,
                foa_title: rec.foa_title,
                birth_year: rec.birth_year,
                flag: match rec.flag.as_str() {
                    "active" => FideFlag::Active,
                    "inactive" => FideFlag::Inactive,
                    _ => FideFlag::Unknown,
                },
            };
            batch.put_player(&player);

            let snap = FideRatingSnapshot {
                standard: rec.standard,
                rapid: rec.rapid,
                blitz: rec.blitz,
                games_standard: rec.games_standard,
                games_rapid: rec.games_rapid,
                games_blitz: rec.games_blitz,
                k_standard: rec.k_standard,
                k_rapid: rec.k_rapid,
                k_blitz: rec.k_blitz,
            };
            batch.put_rating_snapshot(
                FideRatingKey {
                    fide_id: rec.fide_id,
                    month: body.month,
                },
                &snap,
            );
        }

        batch.commit().map_err(|err| {
            log::error!("fide import commit failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(format!("imported {count} FIDE records for {}", body.month))
    })
    .await
}

// ─── POST /import/fide/refresh ────────────────────────────────────────────────

/// Admin endpoint — trigger a FIDE rating list download immediately.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn fide_refresh(
    State(db): State<Arc<Database>>,
) -> Result<String, StatusCode> {
    match fide_ratings_import_once(db).await {
        Ok(n) => Ok(format!("imported {n} FIDE players")),
        Err(e) => {
            log::error!("FIDE manual refresh failed: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ─── GET /fide/search ─────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct FideSearchQuery {
    /// Partial or full player name (case-insensitive prefix match).
    name: String,
    /// Maximum results to return. Default 10, max 50.
    #[serde(default)]
    limit: Option<usize>,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn fide_search(
    Query(q): Query<FideSearchQuery>,
    State(db): State<Arc<Database>>,
    State(fide_index): State<Arc<FideNameIndex>>,
    State(semaphore): State<&'static Semaphore>,
) -> Json<Vec<serde_json::Value>> {
    let limit = q.limit.unwrap_or(10).min(50);
    let name_query = q.name.clone();
    spawn_blocking(semaphore, move || {
        // First try exact normalized match (O(1)).
        let exact_matches: Vec<u32> = fide_index.search_prefix(&name_query, limit);

        let fide_db = db.fide();
        let entries: Vec<serde_json::Value> = exact_matches
            .into_iter()
            .filter_map(|fide_id| {
                fide_db
                    .get_player(fide_id)
                    .expect("get fide player for search")
                    .map(|p| {
                        serde_json::json!({
                            "fide_id": p.fide_id,
                            "name": p.name,
                            "country": p.country,
                            "title": p.title,
                        })
                    })
            })
            .collect();
        Ok::<_, Error>(Json(entries))
    })
    .await
    .unwrap_or_else(|_| Json(vec![]))
}

// ─── POST /import/caissify/fide-link ─────────────────────────────────────────

/// Request body for `POST /import/caissify/fide-link`.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
pub struct FideLinkRequest {
    /// Maximum games to process per call.  Defaults to 5000.
    batch: Option<usize>,
    /// Opaque cursor from a previous response (GameId hex string).
    cursor: Option<String>,
}

/// Response from the FIDE re-linking endpoint.
#[derive(serde::Serialize)]
pub struct FideLinkResponse {
    /// Total player-game links written in this batch (white + black combined).
    linked: u64,
    /// Games skipped (both sides already linked, or game body not found).
    skipped: u64,
    /// Games where at least one side was resolved via tier-1 (sorted-token normalisation).
    tier_sorted: u64,
    /// Games where at least one side was resolved via tier-2 (exact case-insensitive).
    tier_exact_lower: u64,
    /// Games where at least one side was resolved via tier-3 (last-name-only fallback).
    tier_last_name: u64,
    /// Games where at least one side was resolved via tier-4 (abbreviated compound surname).
    tier_abbreviated: u64,
    /// Cursor for the next batch, or null when done.
    next_cursor: Option<String>,
}

/// Background pass that iterates all `caissify_game_meta` records and:
/// 1. Skips games that are already fully linked (both FIDE IDs set).
/// 2. Attempts name-based resolution from the `FideNameIndex`.
/// 3. Writes `caissify_game_meta` with resolved IDs + `caissify_game_by_fide` entries.
///
/// The pass is **idempotent** and can be resumed via the returned `next_cursor`.
#[axum::debug_handler(state = crate::state::AppState)]
pub async fn caissify_fide_link(
    State(db): State<Arc<Database>>,
    State(fide_index): State<Arc<FideNameIndex>>,
    State(semaphore): State<&'static Semaphore>,
    body: Option<Json<FideLinkRequest>>,
) -> Json<FideLinkResponse> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let batch_size = req.batch.unwrap_or(5000).min(50_000);

    // Decode the resumption cursor (optional).
    let cursor: Option<GameId> = req.cursor.as_deref().and_then(|s| s.parse().ok());

    spawn_blocking(semaphore, move || {
        let caissify_db = db.caissify();
        let records = caissify_db
            .iter_meta_from(cursor, batch_size)
            .expect("iter meta for fide link");

        let last_id = records.last().map(|(id, _)| *id);
        let total_scanned = records.len();

        let mut linked = 0u64;
        let mut skipped = 0u64;
        let mut tier_sorted = 0u64;
        let mut tier_exact_lower = 0u64;
        let mut tier_last_name = 0u64;
        let mut tier_abbreviated = 0u64;

        for (id, meta) in &records {
            // Skip if both sides are already linked.
            if meta.white_fide_id != 0 && meta.black_fide_id != 0 {
                skipped += 1;
                continue;
            }

            // Fetch the full game to get player names for name-based resolution.
            let Some(game) = caissify_db
                .game(*id)
                .expect("fetch game for fide link")
            else {
                skipped += 1;
                continue;
            };

            let (white_tier, resolved_white) = if meta.white_fide_id != 0 {
                ("already", meta.white_fide_id)
            } else {
                let (tier, id) = fide_index.lookup_with_tier(&game.players.white.name);
                (tier, id)
            };
            let (black_tier, resolved_black) = if meta.black_fide_id != 0 {
                ("already", meta.black_fide_id)
            } else {
                let (tier, id) = fide_index.lookup_with_tier(&game.players.black.name);
                (tier, id)
            };

            let new_white = resolved_white;
            let new_black = resolved_black;

            // Nothing new resolved — skip.
            if new_white == meta.white_fide_id && new_black == meta.black_fide_id {
                skipped += 1;
                continue;
            }

            // Count which tiers were used for new resolutions.
            let mut game_used_tier = false;
            if new_white != 0 && meta.white_fide_id == 0 {
                match white_tier {
                    "sorted" => {
                        tier_sorted += 1;
                        game_used_tier = true;
                    }
                    "exact_lower" => {
                        tier_exact_lower += 1;
                        game_used_tier = true;
                    }
                    "last_name" => {
                        tier_last_name += 1;
                        game_used_tier = true;
                    }
                    "abbreviated" => {
                        tier_abbreviated += 1;
                        game_used_tier = true;
                    }
                    _ => {}
                }
            }
            if new_black != 0 && meta.black_fide_id == 0 {
                match black_tier {
                    "sorted" => {
                        tier_sorted += 1;
                        game_used_tier = true;
                    }
                    "exact_lower" => {
                        tier_exact_lower += 1;
                        game_used_tier = true;
                    }
                    "last_name" => {
                        tier_last_name += 1;
                        game_used_tier = true;
                    }
                    "abbreviated" => {
                        tier_abbreviated += 1;
                        game_used_tier = true;
                    }
                    _ => {}
                }
            }

            let updated_meta = CaissifyGameMeta {
                white_fide_id: new_white,
                black_fide_id: new_black,
                ..*meta
            };

            let mut batch = caissify_db.batch();
            batch.update_meta_fide_ids(*id, &updated_meta);

            if new_white != 0 && meta.white_fide_id == 0 {
                batch.put_by_fide(
                    CaissifyByFideKey {
                        fide_id: new_white,
                        year: meta.year,
                        id: *id,
                    },
                    false,
                );
            }
            if new_black != 0 && meta.black_fide_id == 0 {
                batch.put_by_fide(
                    CaissifyByFideKey {
                        fide_id: new_black,
                        year: meta.year,
                        id: *id,
                    },
                    true,
                );
            }
            batch.commit().expect("commit fide link batch");
            if game_used_tier {
                linked += 1;
            }
        }

        // Return a cursor only when there may be more records.
        let next_cursor = if total_scanned == batch_size {
            last_id.map(|id| id.to_string())
        } else {
            None
        };

        Ok::<_, Error>(Json(FideLinkResponse {
            linked,
            skipped,
            tier_sorted,
            tier_exact_lower,
            tier_last_name,
            tier_abbreviated,
            next_cursor,
        }))
    })
    .await
    .unwrap_or_else(|_| {
        Json(FideLinkResponse {
            linked: 0,
            skipped: 0,
            tier_sorted: 0,
            tier_exact_lower: 0,
            tier_last_name: 0,
            tier_abbreviated: 0,
            next_cursor: None,
        })
    })
}
