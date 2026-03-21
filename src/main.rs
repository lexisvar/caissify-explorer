#![forbid(unsafe_code)]
#![recursion_limit = "512"]

pub mod api;
pub mod db;
pub mod indexer;
pub mod lila;
pub mod metrics;
pub mod model;
pub mod openapi;
pub mod opening;
pub mod util;
pub mod zobrist;

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime},
};

use axum::{
    Json, Router,
    extract::{FromRef, Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use clap::Parser;
use futures_util::{StreamExt, stream::Stream};
use moka::future::Cache;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use shakmaty::{
    Color, EnPassantMode, Position as _,
    san::{San, SanPlus},
    uci::UciMove,
    variant::VariantPosition,
};
use tikv_jemallocator::Jemalloc;
use tokio::{
    net::TcpListener,
    sync::Semaphore,
    task,
    task::JoinSet,
    time,
    time::{sleep, timeout},
};

use crate::{
    api::{
        CaissifyQuery, Error, ExplorerGame, ExplorerGameWithUciMove, ExplorerMove, ExplorerResponse,
        HistoryWanted, LichessQuery, MastersQuery, NdJson, Play, PlayPosition, PlayerLimits,
        PlayerQuery, PlayerQueryFilter, WithSource,
    },
    db::{CacheHint, Database, DbOpt, LichessDatabase},
    indexer::{
        CaissifyImporter, ImportStatus, LichessGameImport, LichessImporter, MastersImporter,
        PlayerIndexerOpt, PlayerIndexerStub, PgnUrlImporter, QueueFull, Ticket,
    },
    lila::{Lila, LilaOpt},
    metrics::Metrics,
    model::{
        CaissifyByDateKey, CaissifyByFideKey, CaissifyGameMeta, FideNameIndex, GameId, GameResult,
        KeyBuilder, KeyPrefix, MastersGame, MastersGameWithId, Month, PreparedMove, UserId,
        UserName, Year,
    },
    opening::{Opening, Openings},
    util::{DedupStreamExt as _, ply, spawn_blocking},
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

type ExplorerCache<T> = Cache<T, Result<Json<ExplorerResponse>, Error>>;

#[derive(FromRef, Clone)]
struct AppState {
    openings: &'static RwLock<Openings>,
    blacklist: &'static RwLock<HashSet<UserId>>,
    db: Arc<Database>,
    lichess_cache: ExplorerCache<LichessQuery>,
    masters_cache: ExplorerCache<MastersQuery>,
    caissify_cache: ExplorerCache<CaissifyQuery>,
    metrics: &'static Metrics,
    lichess_importer: LichessImporter,
    masters_importer: MastersImporter,
    caissify_importer: CaissifyImporter,
    pgn_url_importer: PgnUrlImporter,
    player_indexer: PlayerIndexerStub,
    semaphore: &'static Semaphore,
    /// In-memory FIDE name → ID lookup index (built at startup, refreshed periodically).
    fide_index: Arc<FideNameIndex>,
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
    join_set.spawn(periodic_openings_import(openings));

    let blacklist: &'static RwLock<HashSet<UserId>> = Box::leak(Box::default());
    join_set.spawn(periodic_blacklist_update(blacklist, opt.lila.clone()));

    let db = task::block_in_place(|| Arc::new(Database::open(opt.db).expect("db")));
    join_set.spawn(periodic_fide_ratings_update(Arc::clone(&db)));
    let player_indexer =
        PlayerIndexerStub::spawn(&mut join_set, Arc::clone(&db), opt.player_indexer, opt.lila);

    // Build the FIDE name index from the fide_player CF.  This is an O(N)
    // scan of ~1 M records taking ~200 ms on a warm SSD.  It runs inside
    // block_in_place so the executor is not stalled.
    let fide_index: Arc<FideNameIndex> = Arc::new(task::block_in_place(|| {
        db.fide()
            .build_name_index()
            .expect("build fide name index")
    }));
    log::info!(
        "fide name index ready: {} entries",
        fide_index.len()
    );
    // Keep a shared reference in a static so the periodic updater can swap it.
    let fide_index_state = Arc::clone(&fide_index);

    let app = Router::new()
        .route("/api-docs/openapi.json", get(openapi_json))
        .route("/api-docs", get(openapi_ui))
        .route("/monitor/cf/{cf}/{prop}", get(cf_prop))
        .route("/monitor/db/{prop}", get(db_prop))
        .route("/monitor", get(monitor))
        .route("/compact", post(compact))
        .route("/import/masters", put(masters_import))
        .route("/import/lichess", put(lichess_import))
        .route("/import/caissify", put(caissify_import))
        .route("/import/caissify/pgn-url", post(caissify_pgn_url_import))
        .route("/import/caissify/pgn-url/status", get(caissify_pgn_url_status))
        .route("/import/caissify/reindex", post(caissify_reindex))
        .route("/import/caissify/fide-link", post(caissify_fide_link))
        .route("/import/fide", put(fide_import))
        .route("/import/fide/refresh", post(fide_refresh))
        .route("/import/openings", post(openings_import))
        .route("/masters/pgn/{id}", get(masters_pgn))
        .route("/masters", get(masters))
        .route("/caissify/pgn/{id}", get(caissify_pgn))
        .route("/caissify", get(caissify))
        .route("/caissify/games", get(caissify_games))
        .route("/caissify/games/{id}", get(caissify_game_meta_endpoint))
        .route("/fide/player/{fide_id}", get(fide_player))
        .route("/fide/player/{fide_id}/ratings", get(fide_player_ratings))
        .route("/fide/search", get(fide_search))
        .route("/lichess", get(lichess))
        .route("/lichess/history", get(lichess_history)) // bc
        .route("/player", get(player))
        .route("/master/pgn/{id}", get(masters_pgn)) // bc
        .route("/master", get(masters)) // bc
        .route("/personal", get(player)) // bc
        .with_state(AppState {
            openings,
            blacklist,
            lichess_cache: Cache::builder()
                .max_capacity(opt.lichess_cache)
                .time_to_live(Duration::from_secs(60 * 60 * 2))
                .time_to_idle(Duration::from_secs(60 * 10))
                .build(),
            masters_cache: Cache::builder()
                .max_capacity(opt.masters_cache)
                .time_to_live(Duration::from_secs(60 * 60 * 4))
                .time_to_idle(Duration::from_secs(60 * 10))
                .build(),
            caissify_cache: Cache::builder()
                .max_capacity(opt.caissify_cache)
                .time_to_live(Duration::from_secs(60 * 60 * 4))
                .time_to_idle(Duration::from_secs(60 * 10))
                .build(),
            metrics: Box::leak(Box::default()),
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

// ─── FIDE periodic updater ───────────────────────────────────────────────────

/// Raw record parsed from FIDE XML before writing to RocksDB.
struct FideXmlRecord {
    fide_id: u32,
    name: String,
    country: String,
    sex: String,
    title: String,
    w_title: String,
    o_title: String,
    foa_title: String,
    birth_year: u16,
    flag: String,
    standard: u16,
    rapid: u16,
    blitz: u16,
    games_standard: u16,
    games_rapid: u16,
    games_blitz: u16,
    k_standard: u8,
    k_rapid: u8,
    k_blitz: u8,
}

/// Event-based SAX-style XML parser for the FIDE standard rating list.
fn fide_parse_xml(xml: &[u8]) -> Vec<FideXmlRecord> {
    use quick_xml::{Reader, events::Event};

    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut players: Vec<FideXmlRecord> = Vec::with_capacity(400_000);
    let mut current: Option<FideXmlRecord> = None;
    let mut current_tag = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_owned();
                if tag == "player" {
                    current = Some(FideXmlRecord {
                        fide_id: 0,
                        name: String::new(),
                        country: String::new(),
                        sex: String::new(),
                        title: String::new(),
                        w_title: String::new(),
                        o_title: String::new(),
                        foa_title: String::new(),
                        birth_year: 0,
                        flag: String::new(),
                        standard: 0,
                        rapid: 0,
                        blitz: 0,
                        games_standard: 0,
                        games_rapid: 0,
                        games_blitz: 0,
                        k_standard: 0,
                        k_rapid: 0,
                        k_blitz: 0,
                    });
                }
                current_tag = tag;
            }
            Ok(Event::Text(e)) => {
                if let Some(ref mut p) = current {
                    let text = e.unescape().unwrap_or_default();
                    let text = text.trim();
                    if text.is_empty() {
                        buf.clear();
                        continue;
                    }
                    match current_tag.as_str() {
                        "fideid"       => p.fide_id       = text.parse().unwrap_or(0),
                        "name"         => p.name          = text.to_owned(),
                        "country"      => p.country       = text.to_owned(),
                        "sex"          => p.sex           = text.to_owned(),
                        "title"        => p.title         = text.to_owned(),
                        "w_title"      => p.w_title       = text.to_owned(),
                        "o_title"      => p.o_title       = text.to_owned(),
                        "foa_title"    => p.foa_title     = text.to_owned(),
                        "birthday"     => p.birth_year    = text.parse().unwrap_or(0),
                        // FIDE flag: "wi" = inactive woman, "i" = inactive,
                        // "w" = woman (active), anything else = active.
                        // Absence of the tag entirely also means active.
                        "flag"         => p.flag = if text.to_ascii_lowercase().contains('i') {
                                              "inactive".to_owned()
                                          } else {
                                              "active".to_owned()
                                          },
                        "rating"       => p.standard      = text.parse().unwrap_or(0),
                        "games"        => p.games_standard = text.parse().unwrap_or(0),
                        "k"            => { p.k_standard = text.parse().unwrap_or(0); p.k_rapid = p.k_standard; p.k_blitz = p.k_standard; }
                        "rapid_rating" => p.rapid         = text.parse().unwrap_or(0),
                        "blitz_rating" => p.blitz         = text.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                if std::str::from_utf8(e.name().as_ref()).unwrap_or("") == "player" {
                    if let Some(p) = current.take() {
                        if p.fide_id > 0 {
                            players.push(p);
                        }
                    }
                }
                current_tag.clear();
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    players
}

async fn periodic_fide_ratings_update(db: Arc<Database>) {
    const INTERVAL: Duration = Duration::from_secs(60 * 60 * 24 * 32);

    loop {
        match fide_ratings_import_once(Arc::clone(&db)).await {
            Ok(n)  => log::info!("FIDE updater: imported {n} players"),
            Err(e) => log::error!("FIDE updater: {e} — retrying in 1h"),
        }
        time::sleep(INTERVAL).await;
    }
}

/// Admin endpoint — trigger a FIDE rating list download immediately.
#[axum::debug_handler(state = AppState)]
async fn fide_refresh(
    State(db): State<Arc<Database>>,
) -> Result<String, StatusCode> {
    match fide_ratings_import_once(db).await {
        Ok(n)  => Ok(format!("imported {n} FIDE players")),
        Err(e) => {
            log::error!("FIDE manual refresh failed: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Download + parse + write the current FIDE player list.
///
/// Uses `players_list_xml.zip` — the combined FOA master list with ALL ~1.8M
/// registered players including their standard, rapid, and blitz ratings.
/// Unrated players are stored with zero ratings.
async fn fide_ratings_import_once(db: Arc<Database>) -> Result<usize, String> {
    use crate::model::{FideFlag, FidePlayer, FideRatingKey, FideRatingSnapshot};
    use std::io::Read as _;
    use ::time::OffsetDateTime;

    const URL: &str = "https://ratings.fide.com/download/players_list_xml.zip";

    let client = reqwest::Client::builder()
        .user_agent("caissify-explorer/fide-updater")
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|e| e.to_string())?;

    let now = OffsetDateTime::now_utc();
    let month_str = format!("{}-{:02}", now.year(), u8::from(now.month()));
    let month: Month = month_str.parse().map_err(|e: crate::model::InvalidDate| e.to_string())?;

    log::info!("FIDE: downloading player list for {month}");

    let zip_bytes = client
        .get(URL)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;

    log::info!("FIDE: downloaded {} MB — parsing…", zip_bytes.len() / 1_048_576);

    task::spawn_blocking(move || -> Result<usize, String> {
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor).map_err(|e| e.to_string())?;
        let mut xml = Vec::new();
        archive
            .by_index(0)
            .map_err(|e| e.to_string())?
            .read_to_end(&mut xml)
            .map_err(|e| e.to_string())?;

        let records = fide_parse_xml(&xml);
        let total = records.len();
        log::info!("FIDE: parsed {total} records — writing…");

        const BATCH_SIZE: usize = 2000;
        // Log progress every 5% (at least every 10k records).
        let log_every = ((total / 20).max(10_000) / BATCH_SIZE).max(1);

        let fide_db = db.fide();
        let mut written = 0usize;
        for (batch_idx, chunk) in records.chunks(BATCH_SIZE).enumerate() {
            let mut batch = fide_db.batch();
            for rec in chunk {
                batch.put_player(&FidePlayer {
                    fide_id:    rec.fide_id,
                    name:       rec.name.clone(),
                    country:    rec.country.clone(),
                    sex:        rec.sex.clone(),
                    title:      rec.title.clone(),
                    w_title:    rec.w_title.clone(),
                    o_title:    rec.o_title.clone(),
                    foa_title:  rec.foa_title.clone(),
                    birth_year: rec.birth_year,
                    flag: match rec.flag.as_str() {
                        "inactive" => FideFlag::Inactive,
                        _          => FideFlag::Active,
                    },
                });
                batch.put_rating_snapshot(
                    FideRatingKey { fide_id: rec.fide_id, month },
                    &FideRatingSnapshot {
                        standard:      rec.standard,
                        rapid:         rec.rapid,
                        blitz:         rec.blitz,
                        games_standard: rec.games_standard,
                        games_rapid:   rec.games_rapid,
                        games_blitz:   rec.games_blitz,
                        k_standard:    rec.k_standard,
                        k_rapid:       rec.k_rapid,
                        k_blitz:       rec.k_blitz,
                    },
                );
            }
            batch.commit().map_err(|e| e.to_string())?;
            written += chunk.len();
            if (batch_idx + 1) % log_every == 0 {
                let pct = written * 100 / total;
                log::info!("FIDE: writing… {pct}% ({written}/{total})");
            }
        }

        Ok(total)
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn periodic_openings_import(openings: &'static RwLock<Openings>) {
    loop {
        match Openings::download().await {
            Ok(new_openings) => {
                log::info!("refreshed {} opening names", new_openings.len());
                *openings.write().expect("write openings") = new_openings;
            }
            Err(err) => {
                log::error!("failed to refresh opening names: {err}");
            }
        }
        time::sleep(Duration::from_secs(60 * 167)).await;
    }
}

async fn periodic_blacklist_update(blacklist: &'static RwLock<HashSet<UserId>>, opt: LilaOpt) {
    let lila = Lila::new(opt);

    let mut last_update = SystemTime::UNIX_EPOCH;
    loop {
        // Request
        let begin = SystemTime::now();
        let old_blacklist_size = blacklist.read().expect("read blacklist").len();
        let mut users = match timeout(
            Duration::from_secs(60),
            lila.mod_marked_since(
                last_update
                    .checked_sub(Duration::from_secs(60 * 10)) // Overlap
                    .unwrap_or(SystemTime::UNIX_EPOCH),
            ),
        )
        .await
        {
            Ok(Ok(users)) => users,
            Ok(Err(err)) => {
                log::warn!("blacklist request failed (no valid bearer token?): {err}");
                sleep(Duration::from_secs(60 * 173)).await;
                continue;
            }
            Err(timed_out) => {
                log::error!("blacklist request to lila: {timed_out}");
                continue;
            }
        };

        // Read stream
        loop {
            let user_id = match timeout(Duration::from_secs(60), users.next()).await {
                Ok(Some(Ok(user))) => user,
                Ok(Some(Err(err))) => {
                    log::error!("blacklist: {err}");
                    continue;
                }
                Ok(None) => break,
                Err(timed_out) => {
                    log::error!("blacklist stream from lila: {timed_out}");
                    break;
                }
            };

            blacklist.write().expect("write blacklist").insert(user_id);
        }

        // Done
        let new_blacklist_size = blacklist.read().expect("read blacklist").len();
        log::info!(
            "blacklist updated in {:.3?}: {} new users, {} users total",
            begin.elapsed().unwrap_or_default(),
            new_blacklist_size.saturating_sub(old_blacklist_size),
            new_blacklist_size,
        );
        last_update = begin;
        time::sleep(Duration::from_secs(60 * 173)).await;
    }
}

#[derive(Deserialize)]
struct ColumnFamilyProp {
    cf: String,
    prop: String,
}

#[axum::debug_handler(state = AppState)]
async fn cf_prop(
    Path(path): Path<ColumnFamilyProp>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<String, StatusCode> {
    spawn_blocking(semaphore, move || {
        db.inner
            .cf_handle(&path.cf)
            .and_then(|cf| {
                db.inner
                    .property_value_cf(cf, &path.prop)
                    .expect("property value")
            })
            .ok_or(StatusCode::NOT_FOUND)
    })
    .await
}

#[axum::debug_handler(state = AppState)]
async fn db_prop(
    Path(prop): Path<String>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<String, StatusCode> {
    spawn_blocking(semaphore, move || {
        db.inner
            .property_value(&prop)
            .expect("property value")
            .ok_or(StatusCode::NOT_FOUND)
    })
    .await
}

#[cfg(tokio_unstable)]
fn tokio_metrics_to_influx_string() -> String {
    let rt_metrics = tokio::runtime::Handle::current().metrics();

    [
        format!("tokio_num_workers={}u", rt_metrics.num_workers()),
        format!(
            "tokio_num_blocking_threads={}u",
            rt_metrics.num_blocking_threads()
        ),
        format!(
            "tokio_num_idle_blocking_threads={}u",
            rt_metrics.num_idle_blocking_threads()
        ),
        format!(
            "tokio_remote_schedule_count={}u",
            rt_metrics.remote_schedule_count()
        ),
        format!(
            "tokio_budget_forced_yield_count={}u",
            rt_metrics.budget_forced_yield_count()
        ),
        format!(
            "tokio_global_queue_depth={}u",
            rt_metrics.global_queue_depth()
        ),
        format!(
            "tokio_blocking_queue_depth={}u",
            rt_metrics.blocking_queue_depth()
        ),
        format!(
            "tokio_io_driver_fd_registered_count={}u",
            rt_metrics.io_driver_fd_registered_count()
        ),
        format!(
            "tokio_io_driver_fd_deregistered_count={}u",
            rt_metrics.io_driver_fd_deregistered_count()
        ),
        format!(
            "tokio_io_driver_ready_count={}u",
            rt_metrics.io_driver_ready_count()
        ),
    ]
    .join(",")
}

#[axum::debug_handler(state = AppState)]
async fn monitor(
    State(lichess_cache): State<ExplorerCache<LichessQuery>>,
    State(masters_cache): State<ExplorerCache<MastersQuery>>,
    State(caissify_cache): State<ExplorerCache<CaissifyQuery>>,
    State(metrics): State<&'static Metrics>,
    State(player_indexer): State<PlayerIndexerStub>,
    State(blacklist): State<&'static RwLock<HashSet<UserId>>>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> String {
    spawn_blocking(semaphore, move || {
        format!(
            "opening_explorer {}",
            [
                // Cache entries
                format!("lichess_cache={}u", lichess_cache.entry_count()),
                format!("masters_cache={}u", masters_cache.entry_count()),
                format!("caissify_cache={}u", caissify_cache.entry_count()),
                // Request metrics
                metrics.to_influx_string(),
                // Block cache
                db.metrics().expect("db metrics").to_influx_string(),
                // Indexer
                format!("indexing={}u", player_indexer.num_indexing()),
                // Blacklist
                format!(
                    "blacklist={}u",
                    blacklist.read().expect("read blacklist").len()
                ),
                // Column families
                db.masters()
                    .estimate_metrics()
                    .expect("masters metrics")
                    .to_influx_string(),
                db.caissify()
                    .estimate_metrics()
                    .expect("caissify metrics")
                    .to_influx_string(),
                db.lichess()
                    .estimate_metrics()
                    .expect("lichess metrics")
                    .to_influx_string(),
                // Tokio
                #[cfg(tokio_unstable)]
                tokio_metrics_to_influx_string(),
            ]
            .join(",")
        )
    })
    .await
}

#[axum::debug_handler(state = AppState)]
async fn compact(State(db): State<Arc<Database>>, State(semaphore): State<&'static Semaphore>) {
    spawn_blocking(semaphore, move || db.compact()).await
}

#[axum::debug_handler(state = AppState)]
async fn openings_import(
    State(openings): State<&'static RwLock<Openings>>,
    State(lichess_cache): State<ExplorerCache<LichessQuery>>,
    State(masters_cache): State<ExplorerCache<MastersQuery>>,
    State(caissify_cache): State<ExplorerCache<CaissifyQuery>>,
) -> Result<(), Error> {
    let new_openings = Openings::download().await?;
    log::info!("loaded {} opening names", new_openings.len());

    let mut write_lock = openings.write().expect("write openings");
    lichess_cache.invalidate_all();
    masters_cache.invalidate_all();
    caissify_cache.invalidate_all();
    *write_lock = new_openings;
    Ok(())
}

fn finalize_lichess_moves(
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

fn finalize_lichess_games(
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
                    .filter_map(|player| player.name.parse::<UserName>().ok().map(UserId::from))
                    .all(|player_id| !blacklist.contains(&player_id))
            })
            .map(|info| ExplorerGameWithUciMove {
                uci,
                row: ExplorerGame::from_lichess(id, info),
            })
        })
        .collect()
}

struct PlayerStreamState {
    player_indexer: PlayerIndexerStub,
    ticket: Ticket,
    key: KeyPrefix,
    db: Arc<Database>,
    color: Color,
    filter: PlayerQueryFilter,
    limits: PlayerLimits,
    pos: VariantPosition,
    opening: Option<Opening>,
    first_response: Option<ExplorerResponse>,
    done: bool,
}

#[axum::debug_handler(state = AppState)]
async fn player(
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

    Ok(NdJson(futures_util::stream::unfold(
        state,
        move |mut state| async move {
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
                },
                _ => {
                    spawn_blocking(semaphore, move || {
                        let started_at = Instant::now();

                        let lichess_db = state.db.lichess();
                        let filtered = lichess_db
                            .read_player(&state.key, state.filter.since, state.filter.until, cache_hint)
                            .expect("read player")
                            .prepare(state.color, &state.filter, &state.limits);

                        let response = ExplorerResponse {
                            total: filtered.total,
                            moves: finalize_lichess_moves(filtered.moves, &state.pos, &lichess_db, &openings.read().expect("read openings")),
                            recent_games: Some(finalize_lichess_games(filtered.recent_games, &lichess_db, &HashSet::new())),
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
                    }).await
                }
            })
        },
    ).dedup_by_key(|res| (res.queue_position, res.total.total()))))
}

#[axum::debug_handler(state = AppState)]
async fn masters_import(
    State(importer): State<MastersImporter>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<MastersGameWithId>,
) -> Result<(), Error> {
    spawn_blocking(semaphore, move || importer.import(body)).await
}

#[serde_as]
#[derive(Deserialize)]
struct MastersGameId(#[serde_as(as = "DisplayFromStr")] GameId);

#[axum::debug_handler(state = AppState)]
async fn masters_pgn(
    Path(MastersGameId(id)): Path<MastersGameId>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<MastersGame, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db.masters().game(id).expect("get masters game") {
            Some(game) => Ok(game),
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

#[axum::debug_handler(state = AppState)]
async fn masters(
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

#[axum::debug_handler(state = AppState)]
async fn caissify_import(
    State(importer): State<CaissifyImporter>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<MastersGameWithId>,
) -> Result<(), Error> {
    spawn_blocking(semaphore, move || importer.import(body)).await
}

#[derive(serde::Deserialize)]
struct PgnUrlImportRequest {
    url: String,
    cookie: String,
}

#[axum::debug_handler(state = AppState)]
async fn caissify_pgn_url_import(
    State(importer): State<PgnUrlImporter>,
    Json(body): Json<PgnUrlImportRequest>,
) -> impl axum::response::IntoResponse {
    if importer.start(body.url, body.cookie) {
        (
            axum::http::StatusCode::ACCEPTED,
            axum::Json(serde_json::json!({
                "message": "PGN import job started in the background",
            })),
        )
    } else {
        (
            axum::http::StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "message": "An import is already running — check /import/caissify/pgn-url/status",
            })),
        )
    }
}

#[axum::debug_handler(state = AppState)]
async fn caissify_pgn_url_status(
    State(importer): State<PgnUrlImporter>,
) -> axum::Json<ImportStatus> {
    axum::Json(importer.status())
}

/// Backfill `caissify_game_meta` and `caissify_game_by_date` for historical
/// games imported before Phase 0. Idempotent — already-indexed games are
/// skipped. Can be slow on large databases; run during a maintenance window.
#[axum::debug_handler(state = AppState)]
async fn caissify_reindex(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<String, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db.caissify().reindex_meta() {
            Ok(count) => Ok(format!("reindexed {count} games")),
            Err(err) => {
                log::error!("caissify reindex failed: {err}");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    })
    .await
}

// ─── FIDE endpoints ───────────────────────────────────────────────────────────

#[axum::debug_handler(state = AppState)]
async fn fide_player(
    Path(fide_id): Path<u32>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    spawn_blocking(semaphore, move || {
        let fide_db = db.fide();
        match fide_db.get_player(fide_id).expect("get fide player") {
            Some(player) => {
                let mut json =
                    serde_json::to_value(&player).expect("serialize fide player");
                // Attach the latest rating snapshot so callers don't need a
                // second round-trip to /fide/player/{id}/ratings.
                if let Some((_month, snap)) = fide_db
                    .get_latest_rating_snapshot(fide_id)
                    .expect("get latest fide rating")
                {
                    let obj = json.as_object_mut().unwrap();
                    macro_rules! ins_rating {
                        ($key:literal, $val:expr) => {
                            obj.insert($key.into(), if $val > 0 { $val.into() } else { serde_json::Value::Null });
                        };
                    }
                    macro_rules! ins_u {
                        ($key:literal, $val:expr) => {
                            obj.insert($key.into(), ($val as u64).into());
                        };
                    }
                    ins_rating!("rating_standard", snap.standard);
                    ins_u!("games_standard",  snap.games_standard);
                    ins_u!("k_standard",      snap.k_standard);
                    ins_rating!("rating_rapid", snap.rapid);
                    ins_u!("games_rapid",     snap.games_rapid);
                    ins_u!("k_rapid",         snap.k_rapid);
                    ins_rating!("rating_blitz", snap.blitz);
                    ins_u!("games_blitz",     snap.games_blitz);
                    ins_u!("k_blitz",         snap.k_blitz);
                }
                Ok(Json(json))
            }
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

#[serde_as]
#[derive(serde::Deserialize)]
struct FideRatingsQuery {
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    since: Option<Month>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    until: Option<Month>,
}

#[axum::debug_handler(state = AppState)]
async fn fide_player_ratings(
    Path(fide_id): Path<u32>,
    Query(query): Query<FideRatingsQuery>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    spawn_blocking(semaphore, move || {
        let history = db
            .fide()
            .get_rating_history(fide_id, query.since, query.until)
            .expect("get fide rating history");

        if history.is_empty() {
            // Return 404 if no history at all (player not found or no ratings)
            return match db.fide().get_player(fide_id).expect("check fide player") {
                None => Err(StatusCode::NOT_FOUND),
                Some(_) => Ok(Json(serde_json::json!([]))),
            };
        }

        let entries: Vec<_> = history
            .into_iter()
            .map(|(month, snap)| {
                serde_json::json!({
                    "month": month.to_string(),
                    "standard": if snap.standard > 0 { Some(snap.standard) } else { None },
                    "rapid": if snap.rapid > 0 { Some(snap.rapid) } else { None },
                    "blitz": if snap.blitz > 0 { Some(snap.blitz) } else { None },
                })
            })
            .collect();

        Ok(Json(serde_json::json!(entries)))
    })
    .await
}

// ─── FIDE import endpoint ─────────────────────────────────────────────────────

/// One record in a FIDE import batch — all three time controls + profile.
#[derive(serde::Deserialize)]
struct FideImportRecord {
    fide_id: u32,
    name: String,
    country: String,
    #[serde(default)]
    sex: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    w_title: String,
    #[serde(default)]
    o_title: String,
    #[serde(default)]
    foa_title: String,
    #[serde(default)]
    birth_year: u16,
    /// "active" | "inactive" (anything else → Unknown)
    #[serde(default)]
    flag: String,
    #[serde(default)]
    standard: u16,
    #[serde(default)]
    rapid: u16,
    #[serde(default)]
    blitz: u16,
    #[serde(default)]
    games_standard: u16,
    #[serde(default)]
    games_rapid: u16,
    #[serde(default)]
    games_blitz: u16,
    #[serde(default)]
    k_standard: u8,
    #[serde(default)]
    k_rapid: u8,
    #[serde(default)]
    k_blitz: u8,
}

#[serde_as]
#[derive(serde::Deserialize)]
struct FideImportBatch {
    #[serde_as(as = "DisplayFromStr")]
    month: Month,
    players: Vec<FideImportRecord>,
}

#[axum::debug_handler(state = AppState)]
async fn fide_import(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<FideImportBatch>,
) -> Result<String, StatusCode> {
    spawn_blocking(semaphore, move || {
        use crate::model::{FideFlag, FidePlayer, FideRatingKey, FideRatingSnapshot};

        let fide_db = db.fide();
        let mut batch = fide_db.batch();
        let count = body.players.len();

        for rec in body.players {
            let player = FidePlayer {
                fide_id: rec.fide_id,
                name: rec.name,
                country: rec.country,
                sex: rec.sex,
                title: rec.title,
                w_title: rec.w_title,
                o_title: rec.o_title,
                foa_title: rec.foa_title,
                birth_year: rec.birth_year,
                flag: match rec.flag.as_str() {
                    "active" => FideFlag::Active,
                    "inactive" => FideFlag::Inactive,
                    _ => FideFlag::Unknown,
                },
            };
            batch.put_player(&player);

            let snap = FideRatingSnapshot {
                standard:       rec.standard,
                rapid:          rec.rapid,
                blitz:          rec.blitz,
                games_standard: rec.games_standard,
                games_rapid:    rec.games_rapid,
                games_blitz:    rec.games_blitz,
                k_standard:     rec.k_standard,
                k_rapid:        rec.k_rapid,
                k_blitz:        rec.k_blitz,
            };
            batch.put_rating_snapshot(
                FideRatingKey {
                    fide_id: rec.fide_id,
                    month: body.month,
                },
                &snap,
            );
        }

        batch.commit().map_err(|err| {
            log::error!("fide import commit failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(format!("imported {count} FIDE records for {}", body.month))
    })
    .await
}

#[axum::debug_handler(state = AppState)]
async fn caissify_pgn(
    Path(MastersGameId(id)): Path<MastersGameId>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<MastersGame, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db.caissify().game(id).expect("get caissify game") {
            Some(game) => Ok(game),
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

// ─── Paginated game list ──────────────────────────────────────────────────────

/// Encode an 8-byte CaissifyByDateKey as a 16-char lowercase hex string.
fn encode_page_token(key: CaissifyByDateKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::with_capacity(16), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a hex page token back to a CaissifyByDateKey.
fn decode_page_token(s: &str) -> Option<CaissifyByDateKey> {
    if s.len() != 16 {
        return None;
    }
    let mut bytes = [0u8; 8];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    Some(CaissifyByDateKey::read(&mut bytes.as_slice()))
}

/// Encode a 12-byte CaissifyByFideKey as a 24-char lowercase hex string.
fn encode_fide_page_token(key: CaissifyByFideKey) -> String {
    let bytes = key.into_bytes();
    bytes.iter().fold(String::with_capacity(24), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decode a 24-char FIDE page token.  Validates that the embedded FIDE ID
/// matches `expected_fide_id` to prevent cross-player token reuse.
fn decode_fide_page_token(s: &str, expected_fide_id: u32) -> Option<CaissifyByFideKey> {
    if s.len() != 24 {
        return None;
    }
    let mut bytes = [0u8; 12];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    let key = CaissifyByFideKey::read(&mut bytes.as_slice());
    (key.fide_id == expected_fide_id).then_some(key)
}

/// Serialised entry in the paginated game list.
#[derive(serde::Serialize)]
struct CaissifyGameListEntry {
    id: String,
    white: String,
    white_rating: u16,
    black: String,
    black_rating: u16,
    event: String,
    site: String,
    date: String,
    round: String,
    result: GameResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    white_fide_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    black_fide_id: Option<u32>,
}

#[derive(serde::Serialize)]
struct CaissifyGameListResponse {
    games: Vec<CaissifyGameListEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct CaissifyGamesQuery {
    /// Number of results to return. Default 50, max 200.
    limit: Option<usize>,
    /// Earliest year to include (inclusive).
    since: Option<u16>,
    /// Latest year to include (inclusive).
    until: Option<u16>,
    /// Opaque cursor from a previous response.
    page_token: Option<String>,
    /// If true (default) return newest games first.
    reverse: Option<bool>,
    /// Filter by game result: "white", "draw", or "black".
    result: Option<GameResult>,
    /// Minimum rating of either player.
    min_rating: Option<u16>,
    /// Maximum rating of either player.
    max_rating: Option<u16>,
    /// Filter to games by a specific FIDE player (FIDE ID integer).
    /// When set the `caissify_game_by_fide` secondary index is used — O(limit)
    /// regardless of how many games exist in the database.
    fide_id: Option<u32>,
    /// Combined with `fide_id`: restrict to games where the player was
    /// `"white"` or `"black"`.  Omit for both colours.
    color: Option<String>,
    /// Optional position filter: FEN + UCI moves to play. When provided the
    /// response is drawn from the position index (up to 15 top games) instead
    /// of the date index, and `next_page_token` is never returned.
    #[serde(flatten)]
    pub position_filter: Play,
}

#[axum::debug_handler(state = AppState)]
async fn caissify_games(
    Query(q): Query<CaissifyGamesQuery>,
    State(openings): State<&'static RwLock<Openings>>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<CaissifyGameListResponse>, Error> {
    spawn_blocking(semaphore, move || {
        let limit = q.limit.unwrap_or(50).min(200);
        let since = q.since.unwrap_or(0);
        let until = q.until.unwrap_or(u16::MAX);
        let reverse = q.reverse.unwrap_or(true);
        let position_filter = q.position_filter;
        let caissify_db = db.caissify();

        // ── Position (FEN) filter path ───────────────────────────────────────
        // When a FEN or play sequence is provided, look up the position in the
        // caissify opening index and return the stored top games for it.
        if position_filter.fen.is_some() || !position_filter.play.is_empty() {
            let openings_guard = openings.read().expect("read openings");
            let PlayPosition { pos, .. } = position_filter.position(&openings_guard)?;
            drop(openings_guard);

            let key = KeyBuilder::caissify()
                .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));

            let ids = caissify_db
                .read(key, Year::min_value(), Year::max_value(), CacheHint::always())
                .expect("read caissify entry for position")
                .all_game_ids();

            let full_games = caissify_db
                .games(ids.iter().copied())
                .expect("batch fetch caissify games for position");

            let mut games: Vec<CaissifyGameListEntry> = Vec::new();
            for (id, maybe_game) in ids.iter().zip(full_games.iter()) {
                let Some(meta) = caissify_db
                    .game_meta(*id)
                    .expect("get caissify game meta")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };

                // Apply all filters post-hoc.
                if meta.year < since || meta.year > until {
                    continue;
                }
                if let Some(rf) = q.result {
                    if meta.result != rf {
                        continue;
                    }
                }
                let max_player_rating = meta.white_rating.max(meta.black_rating);
                if let Some(min_r) = q.min_rating {
                    if max_player_rating < min_r {
                        continue;
                    }
                }
                if let Some(max_r) = q.max_rating {
                    if max_player_rating > max_r {
                        continue;
                    }
                }
                games.push(CaissifyGameListEntry {
                    id: id.to_string(),
                    white: game.players.white.name.clone(),
                    white_rating: meta.white_rating,
                    black: game.players.black.name.clone(),
                    black_rating: meta.black_rating,
                    event: game.event.clone(),
                    site: game.site.clone(),
                    date: game.date.to_string(),
                    round: game.round.clone(),
                    result: meta.result,
                    white_fide_id: if meta.white_fide_id == 0 {
                        None
                    } else {
                        Some(meta.white_fide_id)
                    },
                    black_fide_id: if meta.black_fide_id == 0 {
                        None
                    } else {
                        Some(meta.black_fide_id)
                    },
                });
            }

            // Sort newest-first (or oldest-first if reverse=false).
            if reverse {
                games.sort_by(|a, b| b.date.cmp(&a.date));
            } else {
                games.sort_by(|a, b| a.date.cmp(&b.date));
            }
            games.truncate(limit);

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token: None,
            }));
        }

        // ── FIDE player filter path (cursor-paginated via caissify_game_by_fide) ──
        if let Some(fide_id) = q.fide_id {
            // Parse optional color restriction.
            let color_filter: Option<bool> = match q.color.as_deref() {
                Some("white") => Some(false), // is_black = false
                Some("black") => Some(true),  // is_black = true
                _ => None,
            };

            let cursor = q
                .page_token
                .as_deref()
                .and_then(|s| decode_fide_page_token(s, fide_id));

            let scan_size = limit + 1; // +1 for has-more detection

            let fide_keys = caissify_db
                .iter_by_fide(
                    fide_id,
                    since,
                    until,
                    color_filter,
                    cursor,
                    scan_size,
                    reverse,
                )
                .expect("iter caissify by fide");

            let has_more = fide_keys.len() > limit;
            let page_pairs = &fide_keys[..fide_keys.len().min(limit)];

            let full_games = caissify_db
                .games(page_pairs.iter().map(|(k, _)| k.id))
                .expect("batch fetch caissify games for fide player");

            let meta_passes = |meta: &CaissifyGameMeta| -> bool {
                if let Some(rf) = q.result {
                    if meta.result != rf {
                        return false;
                    }
                }
                let max_r = meta.white_rating.max(meta.black_rating);
                if q.min_rating.is_some_and(|m| max_r < m) {
                    return false;
                }
                if q.max_rating.is_some_and(|m| max_r > m) {
                    return false;
                }
                true
            };

            let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(limit);
            for ((key, _is_black), maybe_game) in page_pairs.iter().zip(full_games.iter()) {
                let Some(meta) = caissify_db
                    .game_meta(key.id)
                    .expect("get caissify game meta for fide player")
                else {
                    continue;
                };
                let Some(game) = maybe_game.as_ref() else {
                    continue;
                };
                if !meta_passes(&meta) {
                    continue;
                }
                games.push(CaissifyGameListEntry {
                    id: key.id.to_string(),
                    white: game.players.white.name.clone(),
                    white_rating: meta.white_rating,
                    black: game.players.black.name.clone(),
                    black_rating: meta.black_rating,
                    event: game.event.clone(),
                    site: game.site.clone(),
                    date: game.date.to_string(),
                    round: game.round.clone(),
                    result: meta.result,
                    white_fide_id: if meta.white_fide_id == 0 {
                        None
                    } else {
                        Some(meta.white_fide_id)
                    },
                    black_fide_id: if meta.black_fide_id == 0 {
                        None
                    } else {
                        Some(meta.black_fide_id)
                    },
                });
            }

            let next_page_token = if has_more {
                page_pairs
                    .last()
                    .map(|(k, _)| encode_fide_page_token(*k))
            } else {
                None
            };

            return Ok(Json(CaissifyGameListResponse {
                games,
                next_page_token,
            }));
        }

        // ── Date-index path (cursor-paginated) ──────────────────────────────
        let initial_cursor = q.page_token.as_deref().and_then(decode_page_token);

        // Helper closure: applies filters that only need `meta`.
        let meta_passes = |meta: &CaissifyGameMeta| -> bool {
            if let Some(rf) = q.result {
                if meta.result != rf {
                    return false;
                }
            }
            let max_r = meta.white_rating.max(meta.black_rating);
            if q.min_rating.is_some_and(|m| max_r < m) {
                return false;
            }
            if q.max_rating.is_some_and(|m| max_r > m) {
                return false;
            }
            true
        };

        let make_entry =
            |key: &CaissifyByDateKey, meta: &CaissifyGameMeta, game: &MastersGame| {
                CaissifyGameListEntry {
                    id: key.id.to_string(),
                    white: game.players.white.name.clone(),
                    white_rating: meta.white_rating,
                    black: game.players.black.name.clone(),
                    black_rating: meta.black_rating,
                    event: game.event.clone(),
                    site: game.site.clone(),
                    date: game.date.to_string(),
                    round: game.round.clone(),
                    result: meta.result,
                    white_fide_id: if meta.white_fide_id == 0 {
                        None
                    } else {
                        Some(meta.white_fide_id)
                    },
                    black_fide_id: if meta.black_fide_id == 0 {
                        None
                    } else {
                        Some(meta.black_fide_id)
                    },
                }
            };

        let scan_size = limit + 1;
        let keys = caissify_db
            .iter_by_date(since, until, initial_cursor, scan_size, reverse)
            .expect("iter caissify by date");

        let has_more = keys.len() > limit;
        let page_keys = &keys[..keys.len().min(limit)];

        let full_games = caissify_db
            .games(page_keys.iter().map(|k| k.id))
            .expect("batch fetch caissify games");

        let mut games: Vec<CaissifyGameListEntry> = Vec::with_capacity(page_keys.len());
        for (key, maybe_game) in page_keys.iter().zip(full_games.iter()) {
            let Some(meta) = caissify_db
                .game_meta(key.id)
                .expect("get caissify game meta")
            else {
                continue;
            };
            let Some(game) = maybe_game.as_ref() else {
                continue;
            };
            if !meta_passes(&meta) {
                continue;
            }
            games.push(make_entry(key, &meta, game));
        }

        let next_page_token = if has_more {
            page_keys.last().map(|k| encode_page_token(*k))
        } else {
            None
        };

        Ok(Json(CaissifyGameListResponse {
            games,
            next_page_token,
        }))
    })
    .await
}

#[axum::debug_handler(state = AppState)]
async fn caissify_game_meta_endpoint(
    Path(MastersGameId(id)): Path<MastersGameId>,
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) -> Result<Json<CaissifyGameMeta>, StatusCode> {
    spawn_blocking(semaphore, move || {
        match db
            .caissify()
            .game_meta(id)
            .expect("get caissify game meta")
        {
            Some(meta) => Ok(Json(meta)),
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

#[axum::debug_handler(state = AppState)]
async fn caissify(
    State(openings): State<&'static RwLock<Openings>>,
    State(db): State<Arc<Database>>,
    State(caissify_cache): State<ExplorerCache<CaissifyQuery>>,
    State(metrics): State<&'static Metrics>,
    State(semaphore): State<&'static Semaphore>,
    Query(WithSource { query, source }): Query<WithSource<CaissifyQuery>>,
) -> Result<Json<ExplorerResponse>, Error> {
    caissify_cache
        .get_with(query.clone(), async move {
            spawn_blocking(semaphore, move || {
                let started_at = Instant::now();
                let openings = openings.read().expect("read openings");
                let PlayPosition { pos, opening } = query.play.position(&openings)?;

                let key = KeyBuilder::caissify()
                    .with_zobrist(pos.variant(), pos.zobrist_hash(EnPassantMode::Legal));
                let cache_hint = CacheHint::from_ply(ply(&pos));
                let caissify_db = db.caissify();
                let entry = caissify_db
                    .read(key, query.since, query.until, cache_hint)
                    .expect("get caissify")
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
                                    caissify_db
                                        .game(id)
                                        .expect("get caissify game")
                                        .map(|info| ExplorerGame::from_masters(id, info))
                                }),
                                opening: openings.classify_exact(&pos_after).cloned(),
                            }
                        })
                        .collect(),
                    top_games: Some(
                        caissify_db
                            .games(entry.top_games.iter().map(|(_, id)| *id))
                            .expect("get caissify games")
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

#[axum::debug_handler(state = AppState)]
async fn lichess_import(
    State(importer): State<LichessImporter>,
    State(semaphore): State<&'static Semaphore>,
    Json(body): Json<Vec<LichessGameImport>>,
) -> Result<(), Error> {
    spawn_blocking(semaphore, move || importer.import_many(body)).await
}

#[axum::debug_handler(state = AppState)]
async fn lichess(
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

#[axum::debug_handler(state = AppState)]
async fn lichess_history(
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

// ─── FIDE player search ───────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct FideSearchQuery {
    /// Partial or full player name (case-insensitive prefix match).
    name: String,
    /// Maximum results to return. Default 10, max 50.
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(serde::Serialize)]
struct FideSearchEntry {
    fide_id: u32,
    name: String,
    country: String,
    title: String,
}

#[axum::debug_handler(state = AppState)]
async fn fide_search(
    Query(q): Query<FideSearchQuery>,
    State(db): State<Arc<Database>>,
    State(fide_index): State<Arc<FideNameIndex>>,
    State(semaphore): State<&'static Semaphore>,
) -> Json<Vec<FideSearchEntry>> {
    let limit = q.limit.unwrap_or(10).min(50);
    let name_query = q.name.clone();
    spawn_blocking(semaphore, move || {
        // First try exact normalized match (O(1)).
        let exact_matches: Vec<u32> = fide_index.search_prefix(&name_query, limit);

        let fide_db = db.fide();
        let entries = exact_matches
            .into_iter()
            .filter_map(|fide_id| {
                fide_db
                    .get_player(fide_id)
                    .expect("get fide player for search")
                    .map(|p| FideSearchEntry {
                        fide_id: p.fide_id,
                        name: p.name,
                        country: p.country,
                        title: p.title,
                    })
            })
            .collect();
        Ok::<_, Error>(Json(entries))
    })
    .await
    .unwrap_or_else(|_| Json(vec![]))
}

// ─── FIDE ↔ game background re-linking ───────────────────────────────────────

/// Request body for `POST /import/caissify/fide-link`.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct FideLinkRequest {
    /// Maximum games to process per call.  Defaults to 5000.
    batch: Option<usize>,
    /// Opaque cursor from a previous response (GameId hex string).
    cursor: Option<String>,
}

/// Response from the FIDE re-linking endpoint.
#[derive(serde::Serialize)]
struct FideLinkResponse {
    /// Total player-game links written in this batch (white + black combined).
    linked: u64,
    /// Games skipped (both sides already linked, or game body not found).
    skipped: u64,
    /// Games where at least one side was resolved via tier-1 (sorted-token normalisation).
    tier_sorted: u64,
    /// Games where at least one side was resolved via tier-2 (exact case-insensitive).
    tier_exact_lower: u64,
    /// Games where at least one side was resolved via tier-3 (last-name-only fallback).
    tier_last_name: u64,
    /// Cursor for the next batch, or null when done.
    next_cursor: Option<String>,
}

/// Background pass that iterates all `caissify_game_meta` records and:
/// 1. Skips games that are already fully linked (both FIDE IDs set).
/// 2. Attempts name-based resolution from the `FideNameIndex`.
/// 3. Writes `caissify_game_meta` with resolved IDs + `caissify_game_by_fide` entries.
///
/// The pass is **idempotent** and can be resumed via the returned `next_cursor`.
#[axum::debug_handler(state = AppState)]
async fn caissify_fide_link(
    State(db): State<Arc<Database>>,
    State(fide_index): State<Arc<FideNameIndex>>,
    State(semaphore): State<&'static Semaphore>,
    body: Option<Json<FideLinkRequest>>,
) -> Json<FideLinkResponse> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let batch_size = req.batch.unwrap_or(5000).min(50_000);

    // Decode the resumption cursor (optional).
    let cursor: Option<GameId> = req.cursor.as_deref().and_then(|s| s.parse().ok());

    spawn_blocking(semaphore, move || {
        let caissify_db = db.caissify();
        let records = caissify_db
            .iter_meta_from(cursor, batch_size)
            .expect("iter meta for fide link");

        let last_id = records.last().map(|(id, _)| *id);
        let total_scanned = records.len();

        let mut linked = 0u64;
        let mut skipped = 0u64;
        let mut tier_sorted = 0u64;
        let mut tier_exact_lower = 0u64;
        let mut tier_last_name = 0u64;

        for (id, meta) in &records {
            // Skip if both sides are already linked.
            if meta.white_fide_id != 0 && meta.black_fide_id != 0 {
                skipped += 1;
                continue;
            }

            // Fetch the full game to get player names for name-based resolution.
            let Some(game) = caissify_db
                .game(*id)
                .expect("fetch game for fide link")
            else {
                skipped += 1;
                continue;
            };

            let (white_tier, resolved_white) = if meta.white_fide_id != 0 {
                ("already", meta.white_fide_id)
            } else {
                let (tier, id) = fide_index.lookup_with_tier(&game.players.white.name);
                (tier, id)
            };
            let (black_tier, resolved_black) = if meta.black_fide_id != 0 {
                ("already", meta.black_fide_id)
            } else {
                let (tier, id) = fide_index.lookup_with_tier(&game.players.black.name);
                (tier, id)
            };

            let new_white = resolved_white;
            let new_black = resolved_black;

            // Nothing new resolved — skip.
            if new_white == meta.white_fide_id && new_black == meta.black_fide_id {
                skipped += 1;
                continue;
            }

            // Count which tiers were used for new resolutions.
            let mut game_used_tier = false;
            if new_white != 0 && meta.white_fide_id == 0 {
                match white_tier {
                    "sorted"      => { tier_sorted      += 1; game_used_tier = true; }
                    "exact_lower" => { tier_exact_lower += 1; game_used_tier = true; }
                    "last_name"   => { tier_last_name   += 1; game_used_tier = true; }
                    _ => {}
                }
            }
            if new_black != 0 && meta.black_fide_id == 0 {
                match black_tier {
                    "sorted"      => { tier_sorted      += 1; game_used_tier = true; }
                    "exact_lower" => { tier_exact_lower += 1; game_used_tier = true; }
                    "last_name"   => { tier_last_name   += 1; game_used_tier = true; }
                    _ => {}
                }
            }

            let updated_meta = CaissifyGameMeta {
                white_fide_id: new_white,
                black_fide_id: new_black,
                ..*meta
            };

            let mut batch = caissify_db.batch();
            batch.update_meta_fide_ids(*id, &updated_meta);

            if new_white != 0 && meta.white_fide_id == 0 {
                batch.put_by_fide(
                    CaissifyByFideKey {
                        fide_id: new_white,
                        year: meta.year,
                        id: *id,
                    },
                    false,
                );
            }
            if new_black != 0 && meta.black_fide_id == 0 {
                batch.put_by_fide(
                    CaissifyByFideKey {
                        fide_id: new_black,
                        year: meta.year,
                        id: *id,
                    },
                    true,
                );
            }
            batch.commit().expect("commit fide link batch");
            if game_used_tier {
                linked += 1;
            }
        }

        // Return a cursor only when there may be more records.
        let next_cursor = if total_scanned == batch_size {
            last_id.map(|id| id.to_string())
        } else {
            None
        };

        Ok::<_, Error>(Json(FideLinkResponse {
            linked,
            skipped,
            tier_sorted,
            tier_exact_lower,
            tier_last_name,
            next_cursor,
        }))
    })
    .await
    .unwrap_or_else(|_| {
        Json(FideLinkResponse {
            linked: 0,
            skipped: 0,
            tier_sorted: 0,
            tier_exact_lower: 0,
            tier_last_name: 0,
            next_cursor: None,
        })
    })
}

async fn openapi_json() -> impl axum::response::IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        crate::openapi::spec().to_string(),
    )
}

async fn openapi_ui() -> impl axum::response::IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        r#"<!doctype html>
<html>
<head>
  <title>Caissify Explorer — API Docs</title>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
</head>
<body>
  <script id="api-reference" data-url="/api-docs/openapi.json"></script>
  <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>"#,
    )
}
