use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Response,
};
use shakmaty::{
    EnPassantMode, Position as _,
    san::{San, SanPlus},
};
use tokio::sync::Semaphore;

use crate::{
    api::{Error, ExplorerGame, ExplorerGameWithUciMove, ExplorerMove, ExplorerResponse, MastersQuery, PlayPosition, WithSource},
    db::{CacheHint, Database},
    indexer::MastersImporter,
    metrics::Metrics,
    model::{KeyBuilder, MastersGameWithId},
    opening::Openings,
    state::ExplorerCache,
    util::{ply, spawn_blocking},
};

use super::GameIdPath;

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn masters_import(
    State(importer): State<MastersImporter>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<MastersGameWithId>,
) -> Result<(), Error> {
    spawn_blocking(semaphore, move || importer.import(body)).await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn masters_pgn(
    Path(GameIdPath(id)): Path<GameIdPath>,
    State(db): State<Arc<Database>>,
    State(openings): State<&'static RwLock<Openings>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Response, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db.masters().game(id).expect("get masters game") {
            Some(game) => {
                let openings = openings.read().expect("read openings");
                let opening = openings.classify_game(&game.moves);
                let pgn = game.to_pgn_bytes(
                    opening.as_ref().map(|o| o.eco.as_str()),
                    opening.as_ref().map(|o| o.name.as_str()),
                );
                Ok(Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "application/x-chess-pgn")
                    .body(Body::from(pgn))
                    .unwrap())
            }
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn masters(
    State(openings): State<&'static RwLock<Openings>>,
    State(db): State<Arc<Database>>,
    State(masters_cache): State<ExplorerCache<MastersQuery>>,
    State(metrics): State<&'static Metrics>,
    State(semaphore): State<&'static Semaphore>,
    Query(WithSource { query, source }): Query<WithSource<MastersQuery>>,
) -> Result<Json<ExplorerResponse>, Error> {
    masters_cache
        .get_with(query.clone(), async move {
            spawn_blocking(semaphore, move || {
                let started_at = Instant::now();
                let openings = openings.read().expect("read openings");
                let PlayPosition { pos, opening } = query.play.position(&openings)?;

                let key = KeyBuilder::masters()
                    .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
                let cache_hint = CacheHint::from_ply(ply(&pos));
                let masters_db = db.masters();
                let entry = masters_db
                    .read(key, query.since, query.until, cache_hint)
                    .expect("get masters")
                    .prepare(&query.limits);

                let response = Ok(Json(ExplorerResponse {
                    total: entry.total,
                    moves: entry
                        .moves
                        .into_iter()
                        .map(|p| {
                            let mut pos_after = pos.clone();
                            let san = p.uci.to_move(&pos).map_or(
                                SanPlus {
                                    san: San::Null,
                                    suffix: None,
                                },
                                |m| SanPlus::from_move_and_play_unchecked(&mut pos_after, m),
                            );
                            ExplorerMove {
                                san,
                                uci: p.uci,
                                average_rating: p.average_rating,
                                average_opponent_rating: p.average_opponent_rating,
                                performance: p.performance,
                                stats: p.stats,
                                game: p.game.and_then(|id| {
                                    masters_db
                                        .game(id)
                                        .expect("get masters game")
                                        .map(|info| ExplorerGame::from_masters(id, info))
                                }),
                                opening: openings.classify_exact(&pos_after).cloned(),
                            }
                        })
                        .collect(),
                    top_games: Some(
                        masters_db
                            .games(entry.top_games.iter().map(|(_, id)| *id))
                            .expect("get masters games")
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
