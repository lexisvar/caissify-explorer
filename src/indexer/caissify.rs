use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use nohash_hasher::IntMap;
use shakmaty::{Chess, Color, EnPassantMode, KnownOutcome, Position, uci::UciMove, variant::Variant};

use sha1::{Digest, Sha1};

use crate::{
    api::Error,
    db::Database,
    model::{
        CaissifyByDateKey, CaissifyByFideKey, CaissifyByPlayerKey, CaissifyByPositionKey,
        CaissifyByRatingKey, CaissifyGameMeta, FideNameIndex, GameId, GameResult, KeyBuilder,
        LaxDate, MastersEntry, MastersGameWithId, player_name_hash,
    },
    zobrist::StableZobrist128,
};

#[derive(Clone)]
pub struct CaissifyImporter {
    db: Arc<Database>,
    mutex: Arc<Mutex<()>>,
    fide_index: Arc<FideNameIndex>,
}

impl CaissifyImporter {
    pub fn new(db: Arc<Database>, fide_index: Arc<FideNameIndex>) -> CaissifyImporter {
        CaissifyImporter {
            db,
            mutex: Arc::new(Mutex::new(())),
            fide_index,
        }
    }

    pub fn import(&self, body: MastersGameWithId) -> Result<(), Error> {
        // Unlike masters, no minimum rating is enforced — accept any game.
        if body.game.date > LaxDate::tomorrow() {
            return Err(Error::RejectedDate {
                id: body.id,
                date: body.game.date,
            });
        }

        let _guard = self.mutex.lock().expect("lock caissify db");
        let caissify_db = self.db.caissify();

        // ── FIDE ID resolution (done early so we can use it on both paths) ───
        let white_fide_id = if body.white_fide_id != 0 {
            body.white_fide_id
        } else {
            self.fide_index
                .lookup(&body.game.players.white.name)
                .unwrap_or(0)
        };
        let black_fide_id = if body.black_fide_id != 0 {
            body.black_fide_id
        } else {
            self.fide_index
                .lookup(&body.game.players.black.name)
                .unwrap_or(0)
        };

        // ── Helper: upgrade an existing duplicate game with better data ─────
        //
        // Two kinds of upgrade are attempted:
        //
        //  1. FIDE ID upgrade — if the incoming game resolved FIDE IDs (either
        //     from PGN tags or name lookup) and the stored record has zeros,
        //     write new `caissify_game_meta` and `caissify_game_by_fide` entries.
        //
        //  2. Game-body upgrade — if the incoming game carried explicit
        //     `[WhiteFideId]` / `[BlackFideId]` PGN tags it came from an
        //     authoritative source (e.g. a Lichess broadcast) whose player names
        //     and event strings are likely more complete than those from a
        //     lower-quality source (chess-results, abbreviated PGN, …).  In that
        //     case overwrite the stored game body and add new by-player hash
        //     entries so look-ups by the full name also work.

        // True when the incoming PGN itself carried FIDE ID tags (not just name
        // lookup).  This is our signal that the source is authoritative.
        let has_pgn_fide = body.white_fide_id != 0 || body.black_fide_id != 0;

        let try_upgrade_fide = |existing_id: GameId| {
            // Quick exit: nothing we can possibly improve.
            if !has_pgn_fide && white_fide_id == 0 && black_fide_id == 0 {
                return;
            }
            let existing_meta = match caissify_db
                .game_meta(existing_id)
                .expect("read meta for fide upgrade")
            {
                Some(m) => m,
                None => return,
            };

            // — FIDE ID improvement check ————————————————————————
            let new_white = if existing_meta.white_fide_id == 0 { white_fide_id } else { existing_meta.white_fide_id };
            let new_black = if existing_meta.black_fide_id == 0 { black_fide_id } else { existing_meta.black_fide_id };
            let fide_changed = new_white != existing_meta.white_fide_id
                || new_black != existing_meta.black_fide_id;

            if !fide_changed && !has_pgn_fide {
                return; // nothing to do
            }

            let mut batch = caissify_db.batch();

            if fide_changed {
                let updated_meta = CaissifyGameMeta {
                    white_fide_id: new_white,
                    black_fide_id: new_black,
                    ..existing_meta
                };
                batch.update_meta_fide_ids(existing_id, &updated_meta);
                if new_white != 0 && existing_meta.white_fide_id == 0 {
                    batch.put_by_fide(
                        CaissifyByFideKey { fide_id: new_white, year: existing_meta.year, id: existing_id },
                        false,
                    );
                }
                if new_black != 0 && existing_meta.black_fide_id == 0 {
                    batch.put_by_fide(
                        CaissifyByFideKey { fide_id: new_black, year: existing_meta.year, id: existing_id },
                        true,
                    );
                }
                log::debug!(
                    "upgraded FIDE IDs for {existing_id}: white {} → {new_white}, black {} → {new_black}",
                    existing_meta.white_fide_id, existing_meta.black_fide_id,
                );
            }

            // — Game-body upgrade (names, event, site) ———————————
            if has_pgn_fide {
                batch.put_game(existing_id, &body.game);
                batch.put_by_player(
                    CaissifyByPlayerKey {
                        hash: player_name_hash(&body.game.players.white.name),
                        year: existing_meta.year,
                        id: existing_id,
                    },
                    false,
                );
                batch.put_by_player(
                    CaissifyByPlayerKey {
                        hash: player_name_hash(&body.game.players.black.name),
                        year: existing_meta.year,
                        id: existing_id,
                    },
                    true,
                );
                log::debug!(
                    "upgraded game body for {existing_id}: '{}' / '{}'",
                    body.game.players.white.name, body.game.players.black.name,
                );
            }

            batch.commit().expect("commit fide/body upgrade");
        };

        if caissify_db
            .has_game(body.id)
            .expect("check for caissify game")
        {
            try_upgrade_fide(body.id);
            return Err(Error::DuplicateGame { id: body.id });
        }

        // ── Move-sequence fingerprint (cross-source deduplication) ───────────
        // SHA-1 of the space-separated UCI move string is content-addressable:
        // two games with the same moves are the same game, regardless of how
        // the player names happen to be formatted in this import's source.
        let moves_fingerprint: [u8; 20] = {
            let moves_str = body
                .game
                .moves
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            Sha1::digest(moves_str.as_bytes()).into()
        };

        if !body.game.moves.is_empty() {
            if let Some(existing_id) = caissify_db
                .game_id_by_moves(&moves_fingerprint)
                .expect("check for caissify moves duplicate")
            {
                try_upgrade_fide(existing_id);
                return Err(Error::DuplicateGame { id: body.id });
            }
        }

        let mut without_loops: IntMap<StableZobrist128, (UciMove, Color)> =
            HashMap::with_capacity_and_hasher(body.game.moves.len(), Default::default());
        let mut pos = Chess::default();

        for uci in &body.game.moves {
            let key = pos.zobrist_hash(EnPassantMode::Legal);
            let m = uci.to_move(&pos)?;
            without_loops.insert(key, (UciMove::from_chess960(m), pos.turn()));
            pos.play_unchecked(m);
        }

        let mut batch = caissify_db.batch();
        batch.put_game(body.id, &body.game);

        let year = u16::from(body.game.date.year());
        let month = body.game.date.month_u8();
        let move_count = (body.game.moves.len() as u64).min(u8::MAX as u64) as u8;
        let meta = CaissifyGameMeta {
            year,
            month,
            white_rating: body.game.players.white.rating,
            black_rating: body.game.players.black.rating,
            result: GameResult::from_winner(body.game.winner),
            white_fide_id,
            black_fide_id,
            move_count,
            is_v3: true,
        };
        batch.put_game_meta(body.id, &meta);
        batch.put_by_date(CaissifyByDateKey { year, month, id: body.id });
        batch.put_by_rating(CaissifyByRatingKey {
            max_rating: meta.white_rating.max(meta.black_rating),
            year,
            id: body.id,
        });

        // Write the move-sequence fingerprint for future cross-source dedup.
        if !body.game.moves.is_empty() {
            batch.put_by_moves(moves_fingerprint, body.id);
        }

        // Write FIDE secondary index entries when IDs were resolved.
        if white_fide_id != 0 {
            batch.put_by_fide(
                CaissifyByFideKey {
                    fide_id: white_fide_id,
                    year,
                    id: body.id,
                },
                false, // is_black = false → White
            );
        }
        if black_fide_id != 0 {
            batch.put_by_fide(
                CaissifyByFideKey {
                    fide_id: black_fide_id,
                    year,
                    id: body.id,
                },
                true, // is_black = true → Black
            );
        }

        // Write the name-hash player index — 100 % coverage unconditionally.
        batch.put_by_player(
            CaissifyByPlayerKey {
                hash: player_name_hash(&body.game.players.white.name),
                year,
                id: body.id,
            },
            false, // White
        );
        batch.put_by_player(
            CaissifyByPlayerKey {
                hash: player_name_hash(&body.game.players.black.name),
                year,
                id: body.id,
            },
            true, // Black
        );

        for (key, (uci, turn)) in without_loops {
            let key_prefix = KeyBuilder::caissify().with_zobrist(Variant::Chess, key);

            // Opening-stats merge (pre-aggregated, top-N per position)
            batch.merge(
                key_prefix.with_year(body.game.date.year()),
                MastersEntry::new_single(
                    uci,
                    body.id,
                    KnownOutcome::from_winner(body.game.winner),
                    body.game.players.get(turn).rating,
                    body.game.players.get(!turn).rating,
                ),
            );

            // Full position index (one entry per unique position per game)
            batch.put_by_position(CaissifyByPositionKey {
                prefix: key_prefix.key_bytes(),
                year,
                id: body.id,
            });
        }

        batch.commit().expect("commit caissify game");
        Ok(())
    }
}
