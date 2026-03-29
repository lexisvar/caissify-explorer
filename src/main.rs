#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod api;
pub mod db;
pub mod handlers;
pub mod indexer;
pub mod lila;
pub mod metrics;
pub mod model;
pub mod openapi;
pub mod opening;
pub mod state;
pub mod tasks;
pub mod util;
pub mod zobrist;

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{Router, routing::{get, post, put}};
use clap::Parser;
use moka::future::Cache;
use tikv_jemallocator::Jemalloc;
use tokio::{net::TcpListener, sync::Semaphore, task, task::JoinSet};

use crate::{
    api::{CaissifyQuery, LichessQuery, MastersQuery},
    db::{Database, DbOpt},
    indexer::{
        BroadcastImporter, CaissifyImporter, LichessImporter, MastersImporter, PlayerIndexerOpt,
        PlayerIndexerStub, PgnUrlImporter,
    },
    lila::LilaOpt,
    metrics::Metrics,
    model::{FideNameIndex, UserId},
    opening::Openings,
    state::{AppState, ExplorerCache},
};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
struct Opt {
    /// Binding address. Defaults to 127.0.0.1:9002, or 0.0.0.0:$PORT when
    /// the PORT environment variable is set (e.g. Railway / Heroku).
    #[arg(long)]
    bind: Option<SocketAddr>,
    /// Allow access from all origins.
    #[arg(long)]
    cors: bool,
    /// Maximum number of cached responses for /masters.
    #[arg(long, default_value = "40000")]
    masters_cache: u64,
    /// Maximum number of cached responses for /lichess.
    #[arg(long, default_value = "40000")]
    lichess_cache: u64,
    /// Maximum number of cached responses for /caissify.
    #[arg(long, default_value = "40000")]
    caissify_cache: u64,
    #[command(flatten)]
    db: DbOpt,
    #[command(flatten)]
    player_indexer: PlayerIndexerOpt,
    #[command(flatten)]
    lila: LilaOpt,
}

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::new()
            .filter("EXPLORER_LOG")
            .write_style("EXPLORER_LOG_STYLE"),
    )
    .format_timestamp(None)
    .format_module_path(false)
    .format_target(false)
    .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(128)
        .build()
        .expect("tokio runtime")
        .block_on(serve());
}

async fn serve() {
    let opt = Opt::parse();

    let mut join_set = JoinSet::new();

    let openings: &'static RwLock<Openings> = Box::leak(Box::default());
    join_set.spawn(tasks::periodic_openings_import(openings));

    let blacklist: &'static RwLock<HashSet<UserId>> = Box::leak(Box::default());
    join_set.spawn(tasks::periodic_blacklist_update(blacklist, opt.lila.clone()));

    let db = task::block_in_place(|| Arc::new(Database::open(opt.db).expect("db")));
    join_set.spawn(tasks::periodic_fide_ratings_update(Arc::clone(&db)));

    let player_indexer =
        PlayerIndexerStub::spawn(&mut join_set, Arc::clone(&db), opt.player_indexer, opt.lila);

    let fide_index: Arc<FideNameIndex> = Arc::new(task::block_in_place(|| {
        db.fide()
            .build_name_index()
            .expect("build fide name index")
    }));
    log::info!("fide name index ready: {} entries", fide_index.len());
    let fide_index_state = Arc::clone(&fide_index);

    let lichess_cache: ExplorerCache<LichessQuery> = Cache::builder()
        .max_capacity(opt.lichess_cache)
        .time_to_live(Duration::from_secs(60 * 60 * 2))
        .time_to_idle(Duration::from_secs(60 * 10))
        .build();
    let masters_cache: ExplorerCache<MastersQuery> = Cache::builder()
        .max_capacity(opt.masters_cache)
        .time_to_live(Duration::from_secs(60 * 60 * 4))
        .time_to_idle(Duration::from_secs(60 * 10))
        .build();
    let caissify_cache: ExplorerCache<CaissifyQuery> = Cache::builder()
        .max_capacity(opt.caissify_cache)
        .time_to_live(Duration::from_secs(60 * 60 * 4))
        .time_to_idle(Duration::from_secs(60 * 10))
        .build();

    let app = Router::new()
        // API docs
        .route("/api-docs/openapi.json", get(handlers::admin::openapi_json))
        .route("/api-docs", get(handlers::admin::openapi_ui))
        // Monitor / admin
        .route("/monitor/cf/{cf}/{prop}", get(handlers::admin::cf_prop))
        .route("/monitor/db/{prop}", get(handlers::admin::db_prop))
        .route("/monitor", get(handlers::admin::monitor))
        .route("/compact", post(handlers::admin::compact))
        // Import
        .route("/import/masters", put(handlers::masters::masters_import))
        .route("/import/lichess", put(handlers::lichess::lichess_import))
        .route("/import/caissify", put(handlers::caissify::caissify_import))
        .route("/import/caissify/pgn-url", post(handlers::caissify::caissify_pgn_url_import))
        .route("/import/caissify/pgn-url/status", get(handlers::caissify::caissify_pgn_url_status))
        .route("/import/caissify/broadcast", post(handlers::caissify::caissify_broadcast_import))
        .route("/import/caissify/broadcast/status", get(handlers::caissify::caissify_broadcast_status))
        .route("/import/caissify/reindex", post(handlers::caissify::caissify_reindex))
        .route("/import/caissify/reindex-meta", post(handlers::caissify::caissify_reindex_meta))
        .route("/import/caissify/reindex-position", post(handlers::caissify::caissify_reindex_position))
        .route("/import/caissify/reindex-moves", post(handlers::caissify::caissify_reindex_moves))
        .route("/import/caissify/reindex-player", post(handlers::caissify::caissify_reindex_player))
        .route("/import/caissify/fide-link", post(handlers::fide::caissify_fide_link))
        .route("/import/fide", put(handlers::fide::fide_import))
        .route("/import/fide/refresh", post(handlers::fide::fide_refresh))
        .route("/import/openings", post(handlers::admin::openings_import))
        // Masters
        .route("/masters/pgn/{id}", get(handlers::masters::masters_pgn))
        .route("/masters", get(handlers::masters::masters))
        // Caissify
        .route("/caissify/pgn/{id}", get(handlers::caissify::caissify_pgn))
        .route("/caissify", get(handlers::caissify::caissify))
        .route("/caissify/games", get(handlers::caissify::caissify_games))
        .route("/caissify/games/{id}", get(handlers::caissify::caissify_game_meta_endpoint))
        // FIDE
        .route("/fide/player/{fide_id}", get(handlers::fide::fide_player))
        .route("/fide/player/{fide_id}/ratings", get(handlers::fide::fide_player_ratings))
        .route("/fide/search", get(handlers::fide::fide_search))
        // Lichess
        .route("/lichess", get(handlers::lichess::lichess))
        .route("/lichess/history", get(handlers::lichess::lichess_history))
        // Player
        .route("/player", get(handlers::player::player))
        // Backwards-compat aliases
        .route("/master/pgn/{id}", get(handlers::masters::masters_pgn))
        .route("/master", get(handlers::masters::masters))
        .route("/personal", get(handlers::player::player))
        .with_state(AppState {
            openings,
            blacklist,
            lichess_cache,
            masters_cache,
            caissify_cache,
            metrics: Box::leak(Box::<Metrics>::default()),
            lichess_importer: LichessImporter::new(Arc::clone(&db)),
            masters_importer: MastersImporter::new(Arc::clone(&db)),
            caissify_importer: CaissifyImporter::new(
                Arc::clone(&db),
                Arc::clone(&fide_index_state),
            ),
            pgn_url_importer: PgnUrlImporter::new(CaissifyImporter::new(
                Arc::clone(&db),
                Arc::clone(&fide_index_state),
            )),
            broadcast_importer: BroadcastImporter::new(CaissifyImporter::new(
                Arc::clone(&db),
                Arc::clone(&fide_index_state),
            )),
            player_indexer,
            db,
            semaphore: Box::leak(Box::new(Semaphore::new(128))),
            fide_index: fide_index_state,
        });

    let app = if opt.cors {
        app.layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            axum::http::HeaderValue::from_static("*"),
        ))
    } else {
        app
    };

    let bind: SocketAddr = opt.bind.unwrap_or_else(|| {
        if let Ok(port_str) = std::env::var("PORT") {
            if let Ok(port) = port_str.parse::<u16>() {
                return SocketAddr::from(([0, 0, 0, 0], port));
            }
        }
        SocketAddr::from(([127, 0, 0, 1], 9002))
    });
    log::info!("Listening on {bind}");
    let listener = TcpListener::bind(&bind).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}
