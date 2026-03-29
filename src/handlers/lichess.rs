use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::Instant,
};

use axum::{
    Json,
    extract::{Query, State},
};
use shakmaty::{EnPassantMode, Position as _};
use tokio::sync::Semaphore;

use crate::{
    api::{Error, ExplorerResponse, HistoryWanted, LichessQuery, PlayPosition, WithSource},
    db::{CacheHint, Database},
    indexer::{LichessGameImport, LichessImporter},
    metrics::Metrics,
    model::{KeyBuilder, UserId},
    opening::Openings,
    state::ExplorerCache,
    util::{ply, spawn_blocking},
};

use super::{finalize_lichess_games, finalize_lichess_moves};

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn lichess_import(
    State(importer): State<LichessImporter>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<Vec<LichessGameImport>>,
) -> Result<(), Error> {
    spawn_blocking(semaphore, move || importer.import_many(body)).await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn lichess(
    State(openings): State<&'static RwLock<Openings>>,
    State(blacklist): State<&'static RwLock<HashSet<UserId>>>,
    State(db): State<Arc<Database>>,
    State(lichess_cache): State<ExplorerCache<LichessQuery>>,
    State(metrics): State<&'static Metrics>,
    State(semaphore): State<&'static Semaphore>,
    Query(WithSource { query, source }): Query<WithSource<LichessQuery>>,
) -> Result<Json<ExplorerResponse>, Error> {
    lichess_cache
        .get_with(query.clone(), async move {
            spawn_blocking(semaphore, move || {
                let started_at = Instant::now();

                let openings = openings.read().expect("read openings");
                let PlayPosition { pos, opening } = query.play.position(&openings)?;

                let key = KeyBuilder::lichess()
                    .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
                let cache_hint = CacheHint::from_ply(ply(&pos));
                let lichess_db = db.lichess();
                let (filtered, history) = lichess_db
                    .read_lichess(
                        &key,
                        &query.filter,
                        &query.limits,
                        query.history,
                        cache_hint,
                    )
                    .expect("get lichess");

                let blacklist = blacklist.read().expect("read blacklist");
                let response = Ok(Json(ExplorerResponse {
                    total: filtered.total,
                    moves: finalize_lichess_moves(filtered.moves, &pos, &lichess_db, &openings),
                    recent_games: Some(finalize_lichess_games(
                        filtered.recent_games,
                        &lichess_db,
                        &blacklist,
                    )),
                    top_games: Some(finalize_lichess_games(
                        filtered.top_games,
                        &lichess_db,
                        &blacklist,
                    )),
                    opening,
                    history,
                    queue_position: None,
                }));

                metrics.inc_lichess(started_at.elapsed(), source, ply(&pos));
                response
            })
            .await
        })
        .await
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn lichess_history(
    openings: State<&'static RwLock<Openings>>,
    blacklist: State<&'static RwLock<HashSet<UserId>>>,
    db: State<Arc<Database>>,
    lichess_cache: State<ExplorerCache<LichessQuery>>,
    metrics: State<&'static Metrics>,
    semaphore: State<&'static Semaphore>,
    Query(mut with_source): Query<WithSource<LichessQuery>>,
) -> Result<Json<ExplorerResponse>, Error> {
    with_source.query.history = HistoryWanted::Yes;
    with_source.query.limits.recent_games = 0;
    with_source.query.limits.top_games = 0;
    with_source.query.limits.moves = 0;
    lichess(
        openings,
        blacklist,
        db,
        lichess_cache,
        metrics,
        semaphore,
        Query(with_source),
    )
    .await
}
