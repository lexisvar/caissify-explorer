use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use axum::extract::{Query, State};
use futures_util::stream::Stream;
use shakmaty::{Color, EnPassantMode, Position as _};
use tokio::sync::Semaphore;

use crate::{
    api::{
        Error, ExplorerResponse, NdJson, PlayPosition, PlayerLimits, PlayerQuery,
        PlayerQueryFilter,
    },
    db::{CacheHint, Database},
    indexer::{PlayerIndexerStub, QueueFull, Ticket},
    metrics::Metrics,
    model::{KeyBuilder, KeyPrefix, UserId},
    opening::{Opening, Openings},
    util::{DedupStreamExt, ply, spawn_blocking},
};

use super::{finalize_lichess_games, finalize_lichess_moves};

struct PlayerStreamState {
    player_indexer: PlayerIndexerStub,
    ticket: Ticket,
    key: KeyPrefix,
    db: Arc<Database>,
    color: Color,
    filter: PlayerQueryFilter,
    limits: PlayerLimits,
    pos: shakmaty::variant::VariantPosition,
    opening: Option<Opening>,
    first_response: Option<ExplorerResponse>,
    done: bool,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn player(
    State(openings): State<&'static RwLock<Openings>>,
    State(db): State<Arc<Database>>,
    State(player_indexer): State<PlayerIndexerStub>,
    State(metrics): State<&'static Metrics>,
    State(semaphore): State<&'static Semaphore>,
    Query(query): Query<PlayerQuery>,
) -> Result<NdJson<impl Stream<Item = ExplorerResponse>>, Error> {
    let player = UserId::from(query.player);
    let key_builder = KeyBuilder::player(&player, query.color);
    let ticket = player_indexer
        .index_player(player, semaphore)
        .await
        .map_err(|QueueFull(player)| {
            log::error!(
                "not indexing {} because queue is full",
                player.as_lowercase_str()
            );
            Error::IndexerQueueFull
        })?;
    let PlayPosition { pos, opening } = query
        .play
        .position(&openings.read().expect("read openings"))?;
    let cache_hint = CacheHint::from_ply(ply(&pos));
    let key = key_builder.with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));

    let state = PlayerStreamState {
        player_indexer,
        color: query.color,
        filter: query.filter,
        limits: query.limits,
        db,
        ticket,
        opening,
        key,
        pos,
        first_response: None,
        done: false,
    };

    Ok(NdJson(
        futures_util::stream::unfold(state, move |mut state| async move {
            if state.done {
                return None;
            }

            let first = state.first_response.is_none();
            state.done = tokio::select! {
                biased;
                _ = state.ticket.completed() => true,
                _ = tokio::time::sleep(Duration::from_millis(if first { 0 } else { 1000 })) => false,
            };

            let preceding_tickets = state.player_indexer.preceding_tickets(&state.ticket);

            Some(match state.first_response {
                Some(ref first_response) if preceding_tickets > 0 => {
                    // While indexing has not even started, just repeat the
                    // first response with updated queue position.
                    let response = ExplorerResponse {
                        queue_position: Some(preceding_tickets),
                        ..first_response.clone()
                    };
                    (response, state)
                }
                _ => {
                    spawn_blocking(semaphore, move || {
                        let started_at = Instant::now();

                        let lichess_db = state.db.lichess();
                        let filtered = lichess_db
                            .read_player(
                                &state.key,
                                state.filter.since,
                                state.filter.until,
                                cache_hint,
                            )
                            .expect("read player")
                            .prepare(state.color, &state.filter, &state.limits);

                        let response = ExplorerResponse {
                            total: filtered.total,
                            moves: finalize_lichess_moves(
                                filtered.moves,
                                &state.pos,
                                &lichess_db,
                                &openings.read().expect("read openings"),
                            ),
                            recent_games: Some(finalize_lichess_games(
                                filtered.recent_games,
                                &lichess_db,
                                &HashSet::new(),
                            )),
                            top_games: None,
                            history: None,
                            opening: state.opening.clone(),
                            queue_position: Some(preceding_tickets),
                        };

                        if state.first_response.is_none() {
                            state.first_response = Some(response.clone());
                        }

                        metrics.inc_player(started_at.elapsed(), state.done, ply(&state.pos));
                        (response, state)
                    })
                    .await
                }
            })
        })
        .dedup_by_key(|res| (res.queue_position, res.total.total())),
    ))
}
