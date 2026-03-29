use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::{
    api::{CaissifyQuery, Error, LichessQuery, MastersQuery},
    db::Database,
    indexer::PlayerIndexerStub,
    metrics::Metrics,
    model::UserId,
    opening::Openings,
    state::ExplorerCache,
    util::spawn_blocking,
};

// ─── DB / column-family property lookup ──────────────────────────────────────

#[derive(Deserialize)]
pub struct ColumnFamilyProp {
    pub cf: String,
    pub prop: String,
}

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn cf_prop(
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

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn db_prop(
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

// ─── Monitor ─────────────────────────────────────────────────────────────────

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

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn monitor(
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

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn compact(
    State(db): State<Arc<Database>>,
    State(semaphore): State<&'static Semaphore>,
) {
    spawn_blocking(semaphore, move || db.compact()).await
}

// ─── Openings admin ───────────────────────────────────────────────────────────

#[axum::debug_handler(state = crate::state::AppState)]
pub async fn openings_import(
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

// ─── OpenAPI docs ─────────────────────────────────────────────────────────────

pub async fn openapi_json() -> impl axum::response::IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        crate::openapi::spec().to_string(),
    )
}

pub async fn openapi_ui() -> impl axum::response::IntoResponse {
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
