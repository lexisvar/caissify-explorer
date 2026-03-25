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
        CaissifyByRatingKey, CaissifyGameMeta, FideNameIndex, GameResult, KeyBuilder, LaxDate,
        MastersEntry, MastersGameWithId, player_name_hash,
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

        if caissify_db
            .has_game(body.id)
            .expect("check for caissify game")
        {
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

        if !body.game.moves.is_empty()
            && caissify_db
                .has_by_moves(&moves_fingerprint)
                .expect("check for caissify moves duplicate")
        {
            return Err(Error::DuplicateGame { id: body.id });
        }

        let mut without_loops: IntMap<StableZobrist128, (UciMove, Color)> =
            HashMap::with_capacity_and_hasher(body.game.moves.len(), Default::default());
        let mut pos = Chess::default();
        let mut final_key = None;

        for uci in &body.game.moves {
            let key = pos.zobrist_hash(EnPassantMode::Legal);
            final_key = Some(key);
            let m = uci.to_move(&pos)?;
            without_loops.insert(key, (UciMove::from_chess960(m), pos.turn()));
            pos.play_unchecked(m);
        }

        if let Some(final_key) = final_key
            && caissify_db
                .has(
                    KeyBuilder::caissify()
                        .with_zobrist(Variant::Chess, final_key)
                        .with_year(body.game.date.year()),
                )
                .expect("check for caissify entry")
        {
            return Err(Error::DuplicateGame { id: body.id });
        }

        // ── FIDE ID resolution ───────────────────────────────────────────────
        // 1. Use the explicit FIDE IDs passed in the request body (from PGN
        //    header tags like WhiteFideId / BlackFideId) when non-zero.
        // 2. Fall back to an in-memory name-based lookup against FideNameIndex.
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
