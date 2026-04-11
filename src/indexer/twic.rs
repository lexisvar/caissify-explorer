/// TwicImporter — server-side TWIC (The Week In Chess) importer.
///
/// Downloads weekly ZIP archives from `https://theweekinchess.com/zips/twic{N}g.zip`,
/// extracts the PGN, corrects player names using the embedded FIDE DB, and
/// imports every game into the Caissify database via `CaissifyImporter`.
///
/// FIDE name correction is done with a direct DB lookup (`db.fide().get_player()`)
/// avoiding any HTTP round-trips.
use std::{
    collections::HashMap,
    ops::ControlFlow,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use pgn_reader::{KnownOutcome, RawTag, Reader, SanPlus, Visitor};
use serde::Serialize;
use sha1::{Digest, Sha1};
use shakmaty::{ByColor, CastlingMode, Chess, Color, Position, fen::Fen};

use crate::{
    api::Error as ApiError,
    db::Database,
    indexer::CaissifyImporter,
    model::{GameId, GamePlayer, LaxDate, MastersGame, MastersGameWithId},
};

// ── Status ────────────────────────────────────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum TwicStatus {
    Idle,
    Running {
        current_week: u32,
        week_index: usize,
        total_weeks: usize,
        games_imported: u64,
        games_skipped: u64,
    },
    Done {
        total_weeks: usize,
        games_imported: u64,
        games_skipped: u64,
        elapsed_secs: f64,
    },
    Failed {
        error: String,
        failed_week: u32,
        games_imported: u64,
        games_skipped: u64,
    },
}

// ── Request ───────────────────────────────────────────────────────────────────

pub struct TwicRequest {
    pub from_week: u32,
    pub to_week: u32,
}

// ── TwicImporter ──────────────────────────────────────────────────────────────

type State = Arc<Mutex<(TwicStatus, Option<Instant>)>>;

#[derive(Clone)]
pub struct TwicImporter {
    caissify: CaissifyImporter,
    db: Arc<Database>,
    state: State,
    /// Week number of the most-recently successfully imported issue.
    /// `0` means nothing has been imported in this server session yet.
    pub last_week: Arc<Mutex<u32>>,
}

impl TwicImporter {
    pub fn new(caissify: CaissifyImporter, db: Arc<Database>) -> Self {
        TwicImporter {
            caissify,
            db,
            state: Arc::new(Mutex::new((TwicStatus::Idle, None))),
            last_week: Arc::new(Mutex::new(0)),
        }
    }

    pub fn status(&self) -> TwicStatus {
        self.state.lock().expect("lock twic state").0.clone()
    }

    /// Start a background import for the given week range (inclusive).
    /// Returns `false` (and does nothing) if an import is already running.
    pub fn start(&self, req: TwicRequest) -> bool {
        {
            let mut guard = self.state.lock().expect("lock twic state");
            if matches!(guard.0, TwicStatus::Running { .. }) {
                return false;
            }
            guard.0 = TwicStatus::Running {
                current_week: req.from_week,
                week_index: 0,
                total_weeks: (req.to_week.saturating_sub(req.from_week) + 1) as usize,
                games_imported: 0,
                games_skipped: 0,
            };
            guard.1 = Some(Instant::now());
        }

        let me = self.clone();
        tokio::spawn(async move {
            let result = me.run(req).await;

            let mut guard = me.state.lock().expect("lock twic state");
            let elapsed = guard.1.map_or(0.0, |t| t.elapsed().as_secs_f64());
            let (total_weeks, games_imported, games_skipped) =
                if let TwicStatus::Running {
                    total_weeks,
                    games_imported,
                    games_skipped,
                    ..
                } = &guard.0
                {
                    (*total_weeks, *games_imported, *games_skipped)
                } else {
                    (0, 0, 0)
                };

            guard.0 = match result {
                Ok(()) => TwicStatus::Done {
                    total_weeks,
                    games_imported,
                    games_skipped,
                    elapsed_secs: elapsed,
                },
                Err((error, failed_week)) => TwicStatus::Failed {
                    error,
                    failed_week,
                    games_imported,
                    games_skipped,
                },
            };
        });

        true
    }

    async fn run(&self, req: TwicRequest) -> Result<(), (String, u32)> {
        let client = reqwest::Client::builder()
            .user_agent("caissify-explorer/twic-importer")
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| (e.to_string(), req.from_week))?;

        let weeks: Vec<u32> = (req.from_week..=req.to_week).collect();
        let total = weeks.len();

        for (idx, &week) in weeks.iter().enumerate() {
            {
                let mut guard = self.state.lock().expect("lock twic state");
                if let TwicStatus::Running {
                    current_week,
                    week_index,
                    ..
                } = &mut guard.0
                {
                    *current_week = week;
                    *week_index = idx;
                }
            }

            let url = format!("https://theweekinchess.com/zips/twic{week}g.zip");
            log::info!("[twic] [{}/{}] downloading {}", idx + 1, total, url);

            // Download ZIP with up to 3 attempts and exponential back-off.
            const MAX_ATTEMPTS: u32 = 3;
            let mut bytes_buf: Vec<u8> = Vec::new();
            let mut last_err = String::new();
            let mut succeeded = false;

            for attempt in 1..=MAX_ATTEMPTS {
                if attempt > 1 {
                    let delay = Duration::from_secs(5 * u64::from(attempt - 1));
                    log::warn!(
                        "[twic] attempt {attempt}/{MAX_ATTEMPTS} for TWIC #{week} in {}s — {last_err}",
                        delay.as_secs()
                    );
                    tokio::time::sleep(delay).await;
                }

                let response = match client.get(&url).send().await {
                    Ok(r) => r,
                    Err(e) => { last_err = e.to_string(); continue; }
                };

                if !response.status().is_success() {
                    last_err = format!("HTTP {}", response.status());
                    continue;
                }

                let mut buf: Vec<u8> = Vec::new();
                let mut stream = response.bytes_stream();
                let mut stream_ok = true;
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(c) => buf.extend_from_slice(&c),
                        Err(e) => { last_err = e.to_string(); stream_ok = false; break; }
                    }
                }
                if stream_ok {
                    bytes_buf = buf;
                    succeeded = true;
                    break;
                }
            }

            if !succeeded {
                return Err((
                    format!("failed to download TWIC #{week} after {MAX_ATTEMPTS} attempts: {last_err}"),
                    week,
                ));
            }

            log::info!(
                "[twic] [{}/{}] downloaded {} KB — parsing",
                idx + 1,
                total,
                bytes_buf.len() / 1024
            );

            // Parse + FIDE-correct + import on a blocking thread.
            let caissify = self.caissify.clone();
            let db = Arc::clone(&self.db);
            let state = Arc::clone(&self.state);

            let (imported, skipped) = tokio::task::spawn_blocking(move || {
                parse_and_import_zip(bytes_buf, week, caissify, db, state)
            })
            .await
            .map_err(|e| (e.to_string(), week))?
            .map_err(|e| (e, week))?;

            // Update last_week after each successful week.
            {
                let mut lw = self.last_week.lock().expect("lock last_week");
                if week > *lw {
                    *lw = week;
                }
            }

            log::info!(
                "[twic] [{}/{}] TWIC #{week}: {imported} imported, {skipped} skipped",
                idx + 1,
                total,
            );
        }

        Ok(())
    }
}

// ── Blocking parse + import ───────────────────────────────────────────────────

fn parse_and_import_zip(
    zip_bytes: Vec<u8>,
    _week: u32,
    caissify: CaissifyImporter,
    db: Arc<Database>,
    state: State,
) -> Result<(u64, u64), String> {
    use std::io::Read as _;

    // Extract first entry from ZIP.
    let cursor = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|e| e.to_string())?;
    let mut entry = archive
        .by_index(0)
        .map_err(|e| format!("zip entry: {e}"))?;
    let mut pgn_bytes: Vec<u8> = Vec::new();
    entry
        .read_to_end(&mut pgn_bytes)
        .map_err(|e| e.to_string())?;

    // Build a per-run FIDE name cache (keyed by FIDE ID → canonical name).
    let mut fide_cache: HashMap<u32, Option<String>> = HashMap::new();

    let mut visitor = TwicVisitor {
        caissify,
        db,
        fide_cache: &mut fide_cache,
        state: Arc::clone(&state),
        imported: 0,
        skipped: 0,
    };

    let mut reader = Reader::new(&pgn_bytes[..]);
    while let Some(()) = reader.read_game(&mut visitor).unwrap_or(None) {}

    let imported = visitor.imported;
    let skipped = visitor.skipped;

    // Push final counts into shared state.
    {
        let mut guard = state.lock().expect("lock twic state");
        if let TwicStatus::Running {
            games_imported,
            games_skipped,
            ..
        } = &mut guard.0
        {
            *games_imported += imported;
            *games_skipped += skipped;
        }
    }

    Ok((imported, skipped))
}

// ── FIDE name lookup (cached per run) ────────────────────────────────────────

fn fide_name(
    db: &Database,
    fide_id: u32,
    cache: &mut HashMap<u32, Option<String>>,
) -> Option<String> {
    if let Some(cached) = cache.get(&fide_id) {
        return cached.clone();
    }
    let result = db
        .fide()
        .get_player(fide_id)
        .ok()
        .flatten()
        .map(|p| p.name);
    cache.insert(fide_id, result.clone());
    result
}

// ── Stable 8-char base-62 game ID ────────────────────────────────────────────

fn twic_game_id(event: &str, white: &str, black: &str, date: &str, round: &str) -> Option<GameId> {
    let mut hasher = Sha1::new();
    hasher.update(event.as_bytes());
    hasher.update(b"\x00");
    hasher.update(white.as_bytes());
    hasher.update(b"\x00");
    hasher.update(black.as_bytes());
    hasher.update(b"\x00");
    hasher.update(date.as_bytes());
    hasher.update(b"\x00");
    hasher.update(round.as_bytes());
    let hash = hasher.finalize();

    let mut n: u64 = 0;
    for &byte in hash[..6].iter().rev() {
        n = (n << 8) | u64::from(byte);
    }
    n %= 62u64.pow(8);

    let chars: Vec<u8> = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        .to_vec();
    let mut out = String::with_capacity(8);
    for _ in 0..8 {
        out.push(chars[(n % 62) as usize] as char);
        n /= 62;
    }
    out.parse().ok()
}

// ── PGN visitor ───────────────────────────────────────────────────────────────

#[derive(Default)]
struct RawGame {
    event: Option<String>,
    site: Option<String>,
    date: Option<String>,
    round: Option<String>,
    white_name: String,
    white_rating: u16,
    white_fide_id: u32,
    black_name: String,
    black_rating: u16,
    black_fide_id: u32,
    winner: Option<Option<Color>>,
    fen: Option<String>,
    sans: Vec<SanPlus>,
}

struct TwicVisitor<'a> {
    caissify: CaissifyImporter,
    db: Arc<Database>,
    fide_cache: &'a mut HashMap<u32, Option<String>>,
    state: State,
    imported: u64,
    skipped: u64,
}

impl Visitor for TwicVisitor<'_> {
    type Tags = RawGame;
    type Movetext = RawGame;
    type Output = ();

    fn begin_tags(&mut self) -> ControlFlow<Self::Output, Self::Tags> {
        ControlFlow::Continue(RawGame::default())
    }

    fn tag(
        &mut self,
        g: &mut RawGame,
        name: &[u8],
        value: RawTag<'_>,
    ) -> ControlFlow<Self::Output> {
        match name {
            b"Event" => g.event = Some(value.decode_utf8().unwrap_or_default().into_owned()),
            b"Site" => g.site = Some(value.decode_utf8().unwrap_or_default().into_owned()),
            b"Date" | b"UTCDate" => {
                g.date = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"Round" => g.round = Some(value.decode_utf8().unwrap_or_default().into_owned()),
            b"White" => g.white_name = value.decode_utf8().unwrap_or_default().into_owned(),
            b"Black" => g.black_name = value.decode_utf8().unwrap_or_default().into_owned(),
            b"WhiteElo" => {
                if value.as_bytes() != b"?" {
                    g.white_rating = std::str::from_utf8(value.as_bytes())
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                }
            }
            b"BlackElo" => {
                if value.as_bytes() != b"?" {
                    g.black_rating = std::str::from_utf8(value.as_bytes())
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                }
            }
            b"WhiteFideId" => {
                g.white_fide_id = std::str::from_utf8(value.as_bytes())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            }
            b"BlackFideId" => {
                g.black_fide_id = std::str::from_utf8(value.as_bytes())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            }
            b"Result" => match KnownOutcome::from_ascii(value.as_bytes()) {
                Ok(outcome) => g.winner = Some(outcome.winner()),
                Err(_) => return ControlFlow::Break(()),
            },
            b"FEN" => {
                let s = value.decode_utf8().unwrap_or_default().into_owned();
                if s != "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1" {
                    g.fen = Some(s);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn begin_movetext(&mut self, g: RawGame) -> ControlFlow<Self::Output, Self::Movetext> {
        if g.winner.is_none() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(g)
        }
    }

    fn san(&mut self, g: &mut RawGame, san: SanPlus) -> ControlFlow<Self::Output> {
        g.sans.push(san);
        ControlFlow::Continue(())
    }

    fn end_game(&mut self, mut g: RawGame) -> Self::Output {
        // ── FIDE name correction ──────────────────────────────────────────────
        if g.white_fide_id != 0 {
            if let Some(canonical) = fide_name(&self.db, g.white_fide_id, self.fide_cache) {
                g.white_name = canonical;
            }
        }
        if g.black_fide_id != 0 {
            if let Some(canonical) = fide_name(&self.db, g.black_fide_id, self.fide_cache) {
                g.black_name = canonical;
            }
        }

        // ── Convert to MastersGameWithId ──────────────────────────────────────
        let game = match convert_raw_game(g) {
            Some(g) => g,
            None => {
                self.skipped += 1;
                return;
            }
        };

        match self.caissify.import(game) {
            Ok(()) => self.imported += 1,
            Err(ApiError::DuplicateGame { .. }) => self.skipped += 1,
            Err(e) => {
                log::debug!("[twic] skipped game: {e}");
                self.skipped += 1;
            }
        }

        if (self.imported + self.skipped) % 1_000 == 0 {
            let mut guard = self.state.lock().expect("lock twic state (progress)");
            if let TwicStatus::Running {
                games_imported,
                games_skipped,
                ..
            } = &mut guard.0
            {
                *games_imported += self.imported;
                *games_skipped += self.skipped;
                self.imported = 0;
                self.skipped = 0;
            }
        }
    }
}

// ── Convert raw PGN game → MastersGameWithId ─────────────────────────────────

fn convert_raw_game(g: RawGame) -> Option<MastersGameWithId> {
    let mut pos: Chess = match g.fen.as_deref() {
        Some(s) => match s.parse::<Fen>() {
            Ok(fen) => fen.into_position(CastlingMode::Standard).ok()?,
            Err(_) => return None,
        },
        None => Chess::default(),
    };

    let mut uci_moves: Vec<shakmaty::uci::UciMove> = Vec::with_capacity(g.sans.len());
    for san_plus in &g.sans {
        match san_plus.san.to_move(&pos) {
            Ok(m) => {
                uci_moves.push(shakmaty::uci::UciMove::from_move(m, CastlingMode::Standard));
                pos.play_unchecked(m);
            }
            Err(_) => return None,
        }
    }

    let event = g.event.as_deref().unwrap_or("");
    let date_str = g.date.as_deref().unwrap_or("????.??.??");
    let round = g.round.as_deref().unwrap_or("?");

    let id = twic_game_id(event, &g.white_name, &g.black_name, date_str, round)?;
    let date = date_str.parse::<LaxDate>().ok()?;

    Some(MastersGameWithId {
        id,
        white_fide_id: g.white_fide_id,
        black_fide_id: g.black_fide_id,
        game: MastersGame {
            event: g.event.unwrap_or_default(),
            site: g.site.unwrap_or_default(),
            date,
            round: round.to_string(),
            players: ByColor {
                white: GamePlayer {
                    name: g.white_name,
                    rating: g.white_rating,
                },
                black: GamePlayer {
                    name: g.black_name,
                    rating: g.black_rating,
                },
            },
            winner: g.winner.flatten(),
            moves: uci_moves,
        },
    })
}
