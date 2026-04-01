use std::{
    collections::HashSet,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime},
};

use serde::Serialize;

use futures_util::StreamExt;
use tokio::{task, time, time::sleep, time::timeout};

use crate::{
    db::Database,
    lila::{Lila, LilaOpt},
    model::{FideFlag, FidePlayer, FideRatingKey, FideRatingSnapshot, Month, UserId},
    opening::Openings,
};

// ─── FIDE XML parsing ─────────────────────────────────────────────────────────

/// Raw record parsed from the FIDE XML rating list before writing to RocksDB.
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

/// Download + parse + write the current FIDE player list.
///
pub(crate) async fn fide_ratings_import_once(db: Arc<Database>) -> Result<usize, String> {
    use std::io::Read as _;

    const URL: &str = "https://ratings.fide.com/download/players_list_xml.zip";

    let client = reqwest::Client::builder()
        .user_agent("caissify-explorer/fide-updater")
        .timeout(Duration::from_secs(300))
        // Disable automatic content-encoding decompression: FIDE's server may
        // send Content-Encoding: gzip on the .zip binary, which would corrupt
        // the ZIP bytes before the zip crate ever sees them.
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()
        .map_err(|e| e.to_string())?;

    // HEAD first — cheap check to see if the file has changed since last import.
    let head = client
        .head(URL)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    // Prefer ETag; fall back to Last-Modified as the version fingerprint.
    let server_tag: Option<String> = head
        .headers()
        .get("etag")
        .or_else(|| head.headers().get("last-modified"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    // Derive the ratings month from Last-Modified rather than `now()` so that
    // a file published on March 31 is stored as "2026-03", not whatever the
    // local clock says.
    let month: Month = head
        .headers()
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_http_date_month)
        .unwrap_or_else(|| {
            let now = ::time::OffsetDateTime::now_utc();
            format!("{}-{:02}", now.year(), u8::from(now.month()))
                .parse()
                .expect("current month is always valid")
        });

    // Skip the download if the server's file hasn't changed.
    let stored_tag = task::block_in_place(|| {
        db.fide().get_last_import_etag().map_err(|e| e.to_string())
    })?;
    if let (Some(server), Some(stored)) = (&server_tag, &stored_tag) {
        if server == stored {
            log::info!("FIDE: file unchanged ({server}) — skipping download");
            return Ok(0);
        }
    }

    log::info!("FIDE: downloading player list for {month}");

    let zip_bytes = client
        .get(URL)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;

    log::info!(
        "FIDE: downloaded {} MB — parsing…",
        zip_bytes.len() / 1_048_576
    );

    let db_for_write = Arc::clone(&db);
    let result = task::spawn_blocking(move || -> Result<usize, String> {
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

        let fide_db = db_for_write.fide();
        let mut written = 0usize;
        for (batch_idx, chunk) in records.chunks(BATCH_SIZE).enumerate() {
            let mut batch = fide_db.batch();
            for rec in chunk {
                batch.put_player(&FidePlayer {
                    fide_id: rec.fide_id,
                    name: rec.name.clone(),
                    country: rec.country.clone(),
                    sex: rec.sex.clone(),
                    title: rec.title.clone(),
                    w_title: rec.w_title.clone(),
                    o_title: rec.o_title.clone(),
                    foa_title: rec.foa_title.clone(),
                    birth_year: rec.birth_year,
                    flag: match rec.flag.as_str() {
                        "inactive" => FideFlag::Inactive,
                        _ => FideFlag::Active,
                    },
                });
                batch.put_rating_snapshot(
                    FideRatingKey {
                        fide_id: rec.fide_id,
                        month,
                    },
                    &FideRatingSnapshot {
                        standard: rec.standard,
                        rapid: rec.rapid,
                        blitz: rec.blitz,
                        games_standard: rec.games_standard,
                        games_rapid: rec.games_rapid,
                        games_blitz: rec.games_blitz,
                        k_standard: rec.k_standard,
                        k_rapid: rec.k_rapid,
                        k_blitz: rec.k_blitz,
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
    .map_err(|e| e.to_string())??;

    // Persist the server's version tag so future runs can skip unchanged files.
    if let Some(ref tag) = server_tag {
        task::block_in_place(|| {
            db.fide()
                .put_last_import_etag(tag)
                .map_err(|e| e.to_string())
        })?;
    }

    Ok(result)
}

/// Parse an HTTP `Last-Modified` / `Date` header value (RFC 7231 format,
/// e.g. `"Tue, 31 Mar 2026 21:08:38 GMT"`) and return the `Month`.
fn parse_http_date_month(s: &str) -> Option<Month> {
    // Expected format: "Day, DD Mon YYYY HH:MM:SS GMT"
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 4 {
        return None;
    }
    let year: u16 = parts[3].parse().ok()?;
    let month_num: u16 = match parts[2] {
        "Jan" => 1, "Feb" => 2,  "Mar" => 3,
        "Apr" => 4, "May" => 5,  "Jun" => 6,
        "Jul" => 7, "Aug" => 8,  "Sep" => 9,
        "Oct" => 10, "Nov" => 11, "Dec" => 12,
        _ => return None,
    };
    format!("{year}-{month_num:02}").parse().ok()
}

// ─── Periodic background tasks ────────────────────────────────────────────────

/// Accepts the shared importer so the periodic run updates the same status that
/// the `/import/fide/refresh/status` endpoint exposes.
pub(crate) async fn periodic_fide_ratings_update(importer: FideRefreshImporter) {
    const INTERVAL: Duration = Duration::from_secs(60 * 60 * 24 * 32);

    loop {
        importer.run_now().await;
        time::sleep(INTERVAL).await;
    }
}

// ─── FideRefreshImporter ──────────────────────────────────────────────────────

/// Status of a manual FIDE rating-list refresh triggered via `POST /import/fide/refresh`.
#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum FideRefreshStatus {
    Idle,
    Running,
    Done {
        players_imported: usize,
        elapsed_secs: f64,
    },
    Failed {
        error: String,
    },
}

type RefreshState = Arc<Mutex<(FideRefreshStatus, Option<Instant>)>>;

/// Cloneable handle that manages a single background FIDE-refresh task.
/// Cheap to clone — all clones share the same internal state.
#[derive(Clone)]
pub struct FideRefreshImporter {
    db: Arc<Database>,
    state: RefreshState,
}

impl FideRefreshImporter {
    pub fn new(db: Arc<Database>) -> Self {
        FideRefreshImporter {
            db,
            state: Arc::new(Mutex::new((FideRefreshStatus::Idle, None))),
        }
    }

    pub fn status(&self) -> FideRefreshStatus {
        self.state.lock().expect("lock fide_refresh state").0.clone()
    }

    /// Run the import in the current task, updating shared status throughout.
    /// If already running (e.g. triggered by the endpoint while the periodic
    /// task is active, or vice-versa), returns immediately without starting a
    /// second concurrent download.
    pub async fn run_now(&self) {
        {
            let mut guard = self.state.lock().expect("lock fide_refresh state");
            if matches!(guard.0, FideRefreshStatus::Running) {
                log::info!("FIDE refresh already running — skipping duplicate run");
                return;
            }
            guard.0 = FideRefreshStatus::Running;
            guard.1 = Some(Instant::now());
        }

        let result = fide_ratings_import_once(Arc::clone(&self.db)).await;
        let mut guard = self.state.lock().expect("lock fide_refresh state");
        let elapsed = guard.1.map_or(0.0, |t| t.elapsed().as_secs_f64());
        guard.0 = match result {
            Ok(n) => {
                log::info!("FIDE refresh: imported {n} players in {elapsed:.1}s");
                FideRefreshStatus::Done {
                    players_imported: n,
                    elapsed_secs: elapsed,
                }
            }
            Err(e) => {
                log::error!("FIDE refresh failed: {e}");
                FideRefreshStatus::Failed { error: e }
            }
        };
    }

    /// Spawn `run_now()` as a background task.
    /// Returns `false` (and does nothing) if a run is already in progress.
    pub fn start(&self) -> bool {
        if matches!(
            self.state.lock().expect("lock fide_refresh state").0,
            FideRefreshStatus::Running
        ) {
            return false;
        }
        let me = self.clone();
        tokio::spawn(async move { me.run_now().await });
        true
    }
}

pub(crate) async fn periodic_openings_import(openings: &'static RwLock<Openings>) {
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

pub(crate) async fn periodic_blacklist_update(
    blacklist: &'static RwLock<HashSet<UserId>>,
    opt: LilaOpt,
) {
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
