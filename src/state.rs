use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use axum::{Json, extract::FromRef};
use moka::future::Cache;
use tokio::sync::Semaphore;

use crate::{
    api::{CaissifyQuery, Error, ExplorerResponse, LichessQuery, MastersQuery},
    db::Database,
    indexer::{
        BroadcastImporter, CaissifyImporter, LichessImporter, MastersImporter, PlayerIndexerStub,
        PgnUrlImporter,
    },
    metrics::Metrics,
    model::{FideNameIndex, UserId},
    opening::Openings,
};

/// Shared cache type used for all three explorer endpoints.
pub type ExplorerCache<T> = Cache<T, Result<Json<ExplorerResponse>, Error>>;

/// Shared application state injected into every route handler via [`axum::extract::State`].
#[derive(FromRef, Clone)]
pub struct AppState {
    pub openings: &'static RwLock<Openings>,
    pub blacklist: &'static RwLock<HashSet<UserId>>,
    pub db: Arc<Database>,
    pub lichess_cache: ExplorerCache<LichessQuery>,
    pub masters_cache: ExplorerCache<MastersQuery>,
    pub caissify_cache: ExplorerCache<CaissifyQuery>,
    pub metrics: &'static Metrics,
    pub lichess_importer: LichessImporter,
    pub masters_importer: MastersImporter,
    pub caissify_importer: CaissifyImporter,
    pub pgn_url_importer: PgnUrlImporter,
    pub broadcast_importer: BroadcastImporter,
    pub player_indexer: PlayerIndexerStub,
    pub semaphore: &'static Semaphore,
    /// In-memory FIDE name → ID lookup index (built at startup, refreshed periodically).
    pub fide_index: Arc<FideNameIndex>,
}
