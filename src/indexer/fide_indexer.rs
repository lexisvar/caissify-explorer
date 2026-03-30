//! Background actor pool that scrapes chess-results.com once per calendar
//! month per FIDE player and imports the retrieved games into the Caissify DB.
//!
//! ## Trigger
//! Handlers call [`FideIndexerStub::queue_fide_id`] with a FIDE ID whenever a
//! client requests a player's profile.  The stub enqueues the ID for
//! background processing and returns immediately.
//!
//! ## Cooldown
//! Before scraping, the actor reads `caissify_fide_status` from RocksDB.  If
//! the stored month equals the current UTC calendar month, the run is skipped.
//! This prevents hammering chess-results.com more than necessary.
//!
//! ## Deduplication
//! [`CaissifyImporter::import`] already rejects games whose ID or move-sequence
//! SHA-1 fingerprint is already present, so repeated runs are idempotent.

use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, task, task::JoinSet, time::sleep};

use crate::{
    db::Database,
    indexer::{CaissifyImporter, chess_results},
    model::Month,
};

const QUEUE_CAPACITY: usize = 500;
/// Pause between consecutive chess-results requests to be a polite scraper.
const RATE_LIMIT: Duration = Duration::from_secs(3);

// ─── Public stub ─────────────────────────────────────────────────────────────

/// Cheap, cloneable handle to the FIDE indexer actor pool.
///
/// Uses a bounded [`mpsc`] channel so that fire-and-forget submissions are
/// never silently discarded by ticket-based cancellation.
#[derive(Clone)]
pub struct FideIndexerStub {
    tx: mpsc::Sender<u32>,
}

impl FideIndexerStub {
    /// Spawn `num_workers` actor tasks and return a stub for submitting work.
    pub fn spawn(
        join_set: &mut JoinSet<()>,
        db: Arc<Database>,
        importer: CaissifyImporter,
        num_workers: usize,
    ) -> FideIndexerStub {
        let (tx, rx) = mpsc::channel::<u32>(QUEUE_CAPACITY);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let client = Arc::new(chess_results::build_client());

        for _ in 0..num_workers {
            join_set.spawn(
                FideIndexerActor {
                    rx: Arc::clone(&rx),
                    db: Arc::clone(&db),
                    importer: importer.clone(),
                    client: Arc::clone(&client),
                }
                .run(),
            );
        }

        FideIndexerStub { tx }
    }

    /// Enqueue a FIDE ID for background indexing (fire-and-forget).
    ///
    /// If the channel is full the request is silently dropped; the monthly
    /// DB cooldown means the player will be picked up on the next request.
    pub fn queue_fide_id(&self, fide_id: u32) {
        match self.tx.try_send(fide_id) {
            Ok(()) => {}
            Err(_) => {
                log::debug!("fide-indexer: channel full, dropping FIDE ID {fide_id}");
            }
        }
    }
}

// ─── Actor ───────────────────────────────────────────────────────────────────

struct FideIndexerActor {
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<u32>>>,
    db: Arc<Database>,
    importer: CaissifyImporter,
    client: Arc<reqwest::Client>,
}

impl FideIndexerActor {
    async fn run(self) {
        loop {
            let fide_id = {
                let mut rx = self.rx.lock().await;
                match rx.recv().await {
                    Some(id) => id,
                    None => break, // all senders dropped — shut down
                }
            };
            self.index_fide_id(fide_id).await;
        }
    }

    async fn index_fide_id(&self, fide_id: u32) {
        let current_month = current_month();

        // ── 1. Check monthly cooldown ────────────────────────────────────
        let cooldown_result = {
            let db = Arc::clone(&self.db);
            task::spawn_blocking(move || db.caissify().get_fide_index_status(fide_id))
                .await
                .expect("join get_fide_index_status")
        };

        match cooldown_result {
            Ok(Some(last)) if last >= current_month => {
                log::debug!(
                    "fide-indexer: FIDE {fide_id} already indexed for {current_month}, skipping"
                );
                return;
            }
            Err(e) => {
                log::warn!("fide-indexer: DB error reading status for FIDE {fide_id}: {e}");
                return;
            }
            _ => {}
        }

        // ── 2. Look up the player's canonical name ────────────────────────
        let fide_name = {
            let db = Arc::clone(&self.db);
            task::spawn_blocking(move || {
                db.fide()
                    .get_player(fide_id)
                    .map(|opt| opt.map(|p| p.name))
            })
            .await
            .expect("join get_player")
        };

        let fide_name = match fide_name {
            Ok(Some(n)) => n,
            Ok(None) => {
                // Player not in our FIDE DB yet, use empty name;
                // the importer will still try name-based matching.
                log::debug!("fide-indexer: FIDE {fide_id} not in FIDE DB, scraping anyway");
                String::new()
            }
            Err(e) => {
                log::warn!("fide-indexer: DB error getting name for FIDE {fide_id}: {e}");
                return;
            }
        };

        log::info!("fide-indexer: scraping chess-results for FIDE {fide_id} ({fide_name})");

        // ── 3. Rate-limit delay before HTTP request ───────────────────────
        sleep(RATE_LIMIT).await;

        // ── 4. Scrape chess-results.com ───────────────────────────────────
        let games = match chess_results::fetch_fide_games(&self.client, fide_id, &fide_name).await
        {
            Ok(g) => g,
            Err(e) => {
                log::warn!("fide-indexer: scrape failed for FIDE {fide_id}: {e}");
                return;
            }
        };

        let num_fetched = games.len();

        // ── 5. Import games (blocking) ────────────────────────────────────
        let (imported, skipped) = {
            let importer = self.importer.clone();
            task::spawn_blocking(move || {
                let mut ok = 0u32;
                let mut dup = 0u32;
                for game in games {
                    match importer.import(game) {
                        Ok(()) => ok += 1,
                        Err(_) => dup += 1, // duplicate or rejected
                    }
                }
                (ok, dup)
            })
            .await
            .expect("join import games")
        };

        log::info!(
            "fide-indexer: FIDE {fide_id} — {num_fetched} fetched, \
             {imported} imported, {skipped} skipped (duplicates/rejected)"
        );

        // ── 6. Record that we indexed this player for the current month ────
        let db = Arc::clone(&self.db);
        if let Err(e) = task::spawn_blocking(move || {
            db.caissify().put_fide_index_status(fide_id, current_month)
        })
        .await
        .expect("join put_fide_index_status")
        {
            log::warn!("fide-indexer: could not update status for FIDE {fide_id}: {e}");
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn current_month() -> Month {
    let now = time::OffsetDateTime::now_utc();
    format!("{}-{:02}", now.year(), u8::from(now.month()))
        .parse()
        .expect("current month is always valid")
}
