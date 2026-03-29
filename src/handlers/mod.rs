pub mod admin;
pub mod caissify;
pub mod fide;
pub mod lichess;
pub mod masters;
pub mod player;

use std::collections::HashSet;

use shakmaty::{
    san::{San, SanPlus},
    uci::UciMove,
    variant::VariantPosition,
};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};

use crate::{
    api::{ExplorerGame, ExplorerGameWithUciMove, ExplorerMove},
    db::LichessDatabase,
    model::{GameId, PreparedMove, UserId},
    opening::Openings,
};

// ─── Shared path-extraction type ─────────────────────────────────────────────

/// Path extractor that deserialises a base-62 [`GameId`] from a URL segment.
/// Used by both the masters and caissify PGN endpoints.
#[serde_as]
#[derive(Deserialize)]
pub struct GameIdPath(#[serde_as(as = "DisplayFromStr")] pub GameId);

// ─── Shared response-assembly helpers ────────────────────────────────────────

pub(crate) fn finalize_lichess_moves(
    moves: Vec<PreparedMove>,
    pos: &VariantPosition,
    lichess_db: &LichessDatabase,
    openings: &Openings,
) -> Vec<ExplorerMove> {
    moves
        .into_iter()
        .map(|p| {
            let mut pos_after = pos.clone();
            let san = p.uci.to_move(pos).map_or(
                SanPlus {
                    san: San::Null,
                    suffix: None,
                },
                |m| SanPlus::from_move_and_play_unchecked(&mut pos_after, m),
            );
            ExplorerMove {
                stats: p.stats,
                san,
                uci: p.uci,
                average_rating: p.average_rating,
                average_opponent_rating: p.average_opponent_rating,
                performance: p.performance,
                game: p.game.and_then(|id| {
                    lichess_db
                        .game(id)
                        .expect("get game")
                        .map(|info| ExplorerGame::from_lichess(id, info))
                }),
                opening: openings.classify_exact(&pos_after).cloned(),
            }
        })
        .collect()
}

pub(crate) fn finalize_lichess_games(
    games: Vec<(UciMove, GameId)>,
    lichess_db: &LichessDatabase,
    blacklist: &HashSet<UserId>,
) -> Vec<ExplorerGameWithUciMove> {
    lichess_db
        .games(games.iter().map(|(_, id)| *id))
        .expect("get games")
        .into_iter()
        .zip(games)
        .filter_map(|(info, (uci, id))| {
            info.filter(|info| {
                info.players
                    .iter()
                    .filter_map(|player| {
                        player
                            .name
                            .parse::<crate::model::UserName>()
                            .ok()
                            .map(UserId::from)
                    })
                    .all(|player_id| !blacklist.contains(&player_id))
            })
            .map(|info| ExplorerGameWithUciMove {
                uci,
                row: ExplorerGame::from_lichess(id, info),
            })
        })
        .collect()
}
