use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use nohash_hasher::IntMap;
use shakmaty::{Chess, Color, EnPassantMode, KnownOutcome, Position, uci::UciMove, variant::Variant};

use crate::{
    api::Error,
    db::Database,
    model::{KeyBuilder, LaxDate, MastersEntry, MastersGameWithId},
    zobrist::StableZobrist128,
};

#[derive(Clone)]
pub struct CaissifyImporter {
    db: Arc<Database>,
    mutex: Arc<Mutex<()>>,
}

impl CaissifyImporter {
    pub fn new(db: Arc<Database>) -> CaissifyImporter {
        CaissifyImporter {
            db,
            mutex: Arc::new(Mutex::new(())),
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

        let mut batch = caissify_db.batch();
        batch.put_game(body.id, &body.game);
        for (key, (uci, turn)) in without_loops {
            batch.merge(
                KeyBuilder::caissify()
                    .with_zobrist(Variant::Chess, key)
                    .with_year(body.game.date.year()),
                MastersEntry::new_single(
                    uci,
                    body.id,
                    KnownOutcome::from_winner(body.game.winner),
                    body.game.players.get(turn).rating,
                    body.game.players.get(!turn).rating,
                ),
            );
        }

        batch.commit().expect("commit caissify game");
        Ok(())
    }
}
