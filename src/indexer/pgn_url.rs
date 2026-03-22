use std::{
    fmt::Write as _,
    ops::ControlFlow,
    sync::{Arc, Mutex},
    time::Instant,
};

use futures_util::StreamExt;
use pgn_reader::{KnownOutcome, RawTag, Reader, SanPlus, Visitor};
use serde::Serialize;
use sha1::{Digest, Sha1};
use shakmaty::{ByColor, CastlingMode, Chess, Color, Position, fen::Fen};
use tokio::io::AsyncWriteExt as _;

use crate::{
    api::Error as ApiError,
    indexer::CaissifyImporter,
    model::{GameId, GamePlayer, LaxDate, MastersGame, MastersGameWithId},
};

// ── Import status ─────────────────────────────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ImportStatus {
    Idle,
    Running {
        games_imported: u64,
        games_skipped: u64,
        bytes_downloaded: u64,
    },
    Done {
        games_imported: u64,
        games_skipped: u64,
        elapsed_secs: f64,
    },
    Failed {
        error: String,
    },
}

// ── PgnUrlImporter ────────────────────────────────────────────────────────────

/// Shared state: (status, started_at)
type State = Arc<Mutex<(ImportStatus, Option<Instant>)>>;

#[derive(Clone)]
pub struct PgnUrlImporter {
    caissify: CaissifyImporter,
    state: State,
}

impl PgnUrlImporter {
    pub fn new(caissify: CaissifyImporter) -> Self {
        PgnUrlImporter {
            caissify,
            state: Arc::new(Mutex::new((ImportStatus::Idle, None))),
        }
    }

    pub fn status(&self) -> ImportStatus {
        self.state.lock().expect("lock pgn_url state").0.clone()
    }

    /// Starts a background import job.
    /// Returns `false` (and does nothing) if an import is already running.
    pub fn start(&self, url: String, cookie: String) -> bool {
        {
            let mut guard = self.state.lock().expect("lock pgn_url state");
            if matches!(guard.0, ImportStatus::Running { .. }) {
                return false;
            }
            guard.0 = ImportStatus::Running {
                games_imported: 0,
                games_skipped: 0,
                bytes_downloaded: 0,
            };
            guard.1 = Some(Instant::now());
        }

        let me = self.clone();
        tokio::spawn(async move {
            let result = me.run(url, cookie).await;
            let mut guard = me.state.lock().expect("lock pgn_url state");
            let elapsed = guard.1.map_or(0.0, |t| t.elapsed().as_secs_f64());
            guard.0 = match result {
                Ok((imported, skipped)) => ImportStatus::Done {
                    games_imported: imported,
                    games_skipped: skipped,
                    elapsed_secs: elapsed,
                },
                Err(e) => ImportStatus::Failed {
                    error: e.to_string(),
                },
            };
        });

        true
    }

    async fn run(
        &self,
        url: String,
        cookie: String,
    ) -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
        // ── Download to a temp file ──────────────────────────────────────────
        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .header("Cookie", &cookie)
            .header(
                "User-Agent",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            )
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("Download failed with HTTP {}", response.status()).into());
        }

        let tmp_path = std::env::temp_dir().join("caissify_pgn_import.pgn");
        {
            let mut file = tokio::fs::File::create(&tmp_path).await?;
            let mut bytes_downloaded: u64 = 0;
            let mut stream = response.bytes_stream();

            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                bytes_downloaded += chunk.len() as u64;
                file.write_all(&chunk).await?;

                let mut guard = self.state.lock().expect("lock pgn_url state");
                if let ImportStatus::Running {
                    bytes_downloaded: bd,
                    ..
                } = &mut guard.0
                {
                    *bd = bytes_downloaded;
                }
            }
            file.flush().await?;

            if bytes_downloaded < 100 * 1024 {
                tokio::fs::remove_file(&tmp_path).await.ok();
                return Err(
                    "Downloaded file is too small — likely an HTML error page. \
                     The Sync.com URL may have expired."
                        .into(),
                );
            }

            log::info!(
                "PGN download complete: {} MB, starting import",
                bytes_downloaded / 1_048_576
            );
        }

        // ── Parse + import on a blocking thread ──────────────────────────────
        let caissify = self.caissify.clone();
        let state = Arc::clone(&self.state);
        let path = tmp_path.clone();

        let (imported, skipped) = tokio::task::spawn_blocking(move || {
            parse_and_import(path, caissify, state)
        })
        .await??;

        tokio::fs::remove_file(&tmp_path).await.ok();
        Ok((imported, skipped))
    }
}

// ── BroadcastImporter ─────────────────────────────────────────────────────────

/// Background importer for monthly Lichess broadcast archives.
///
/// Archives are fetched from:
/// `https://database.lichess.org/broadcast/lichess_db_broadcast_{YYYY}-{MM:02}.pgn.zst`
#[derive(Clone)]
pub struct BroadcastImporter {
    caissify: CaissifyImporter,
    state: State,
}

impl BroadcastImporter {
    pub fn new(caissify: CaissifyImporter) -> Self {
        BroadcastImporter {
            caissify,
            state: Arc::new(Mutex::new((ImportStatus::Idle, None))),
        }
    }

    pub fn status(&self) -> ImportStatus {
        self.state.lock().expect("lock broadcast state").0.clone()
    }

    /// Starts a background import for the given year + month.
    /// Returns `false` (and does nothing) if an import is already running.
    pub fn start(&self, year: i32, month: u8) -> bool {
        {
            let mut guard = self.state.lock().expect("lock broadcast state");
            if matches!(guard.0, ImportStatus::Running { .. }) {
                return false;
            }
            guard.0 = ImportStatus::Running {
                games_imported: 0,
                games_skipped: 0,
                bytes_downloaded: 0,
            };
            guard.1 = Some(Instant::now());
        }

        let me = self.clone();
        tokio::spawn(async move {
            let url = format!(
                "https://database.lichess.org/broadcast/lichess_db_broadcast_{year}-{month:02}.pgn.zst"
            );
            let result = me.run(url).await;
            let mut guard = me.state.lock().expect("lock broadcast state");
            let elapsed = guard.1.map_or(0.0, |t| t.elapsed().as_secs_f64());
            guard.0 = match result {
                Ok((imported, skipped)) => ImportStatus::Done {
                    games_imported: imported,
                    games_skipped: skipped,
                    elapsed_secs: elapsed,
                },
                Err(e) => ImportStatus::Failed {
                    error: e.to_string(),
                },
            };
        });

        true
    }

    async fn run(
        &self,
        url: String,
    ) -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
        // ── Download compressed archive to a temp file ───────────────────────
        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .header("User-Agent", "caissify-explorer/1.0 (https://caissify.com)")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(
                format!("Broadcast download failed with HTTP {}", response.status()).into(),
            );
        }

        let tmp_path = std::env::temp_dir().join("caissify_broadcast_import.pgn.zst");
        {
            let mut file = tokio::fs::File::create(&tmp_path).await?;
            let mut bytes_downloaded: u64 = 0;
            let mut stream = response.bytes_stream();

            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                bytes_downloaded += chunk.len() as u64;
                file.write_all(&chunk).await?;

                let mut guard = self.state.lock().expect("lock broadcast state");
                if let ImportStatus::Running {
                    bytes_downloaded: bd,
                    ..
                } = &mut guard.0
                {
                    *bd = bytes_downloaded;
                }
            }
            file.flush().await?;

            log::info!(
                "Broadcast download complete: {} MB, decompressing and importing",
                bytes_downloaded / 1_048_576
            );
        }

        // ── Decompress + parse + import on a blocking thread ─────────────────
        let caissify = self.caissify.clone();
        let state = Arc::clone(&self.state);
        let path = tmp_path.clone();

        let (imported, skipped) = tokio::task::spawn_blocking(move || {
            parse_and_import_zstd(path, caissify, state)
        })
        .await??;

        tokio::fs::remove_file(&tmp_path).await.ok();
        Ok((imported, skipped))
    }
}

fn parse_and_import_zstd(
    path: std::path::PathBuf,
    caissify: CaissifyImporter,
    state: State,
) -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::open(&path)?;
    let decoder = zstd::Decoder::new(file)?;
    let mut visitor = PgnVisitor {
        caissify,
        state: Arc::clone(&state),
        imported: 0,
        skipped: 0,
    };

    Reader::new(decoder).visit_all_games(&mut visitor)?;

    let imported = visitor.imported;
    let skipped = visitor.skipped;

    {
        let mut guard = state.lock().expect("lock broadcast state");
        if let ImportStatus::Running {
            games_imported,
            games_skipped,
            ..
        } = &mut guard.0
        {
            *games_imported = imported;
            *games_skipped = skipped;
        }
    }

    log::info!(
        "Broadcast import finished: {} imported, {} skipped",
        imported,
        skipped
    );
    Ok((imported, skipped))
}

// ── PGN parsing + import ──────────────────────────────────────────────────────

fn parse_and_import(
    path: std::path::PathBuf,
    caissify: CaissifyImporter,
    state: State,
) -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::open(&path)?;
    let mut visitor = PgnVisitor {
        caissify,
        state: Arc::clone(&state),
        imported: 0,
        skipped: 0,
    };

    Reader::new(file).visit_all_games(&mut visitor)?;

    let imported = visitor.imported;
    let skipped = visitor.skipped;

    // Final status update
    {
        let mut guard = state.lock().expect("lock pgn_url state");
        if let ImportStatus::Running {
            games_imported,
            games_skipped,
            ..
        } = &mut guard.0
        {
            *games_imported = imported;
            *games_skipped = skipped;
        }
    }

    log::info!(
        "PGN import finished: {} imported, {} skipped",
        imported,
        skipped
    );
    Ok((imported, skipped))
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
    winner: Option<Option<Color>>, // None = result tag missing / invalid
    fen: Option<String>,
    sans: Vec<SanPlus>,
}

struct PgnVisitor {
    caissify: CaissifyImporter,
    state: State,
    imported: u64,
    skipped: u64,
}

impl Visitor for PgnVisitor {
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
            b"WhiteFideId" => {
                g.white_fide_id = std::str::from_utf8(value.as_bytes())
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            }
            b"BlackElo" => {
                if value.as_bytes() != b"?" {
                    g.black_rating = std::str::from_utf8(value.as_bytes())
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                }
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

    fn end_game(&mut self, g: RawGame) -> Self::Output {
        let game = convert_raw_game(g);

        match game {
            Some(game) => match self.caissify.import(game) {
                Ok(()) => self.imported += 1,
                Err(ApiError::DuplicateGame { .. }) => self.skipped += 1,
                Err(e) => {
                    log::debug!("skipped game during pgn-url import: {e}");
                    self.skipped += 1;
                }
            },
            None => self.skipped += 1,
        }

        if (self.imported + self.skipped) % 1_000 == 0 {
            let mut guard = self.state.lock().expect("lock pgn_url state");
            if let ImportStatus::Running {
                games_imported,
                games_skipped,
                ..
            } = &mut guard.0
            {
                *games_imported = self.imported;
                *games_skipped = self.skipped;
            }
        }
    }
}

fn convert_raw_game(g: RawGame) -> Option<MastersGameWithId> {
    // Replay moves from starting position (or custom FEN)
    let mut pos: Chess = match g.fen.as_deref() {
        Some(s) => match s.parse::<Fen>() {
            Ok(fen) => match fen.into_position(CastlingMode::Standard) {
                Ok(p) => p,
                Err(_) => return None,
            },
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

    let id_str = make_game_id(event, &g.white_name, &g.black_name, date_str, round);
    let id = id_str.parse::<GameId>().ok()?;
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

// ── Game ID (must match import-caissify.rs) ───────────────────────────────────

fn make_game_id(event: &str, white: &str, black: &str, date: &str, round: &str) -> String {
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

    let mut out = String::with_capacity(8);
    for _ in 0..8 {
        let rem = n % 62;
        out.write_char(char::from(if rem >= 10 + 26 {
            (rem - (10 + 26)) as u8 + b'a'
        } else if rem >= 10 {
            (rem - 10) as u8 + b'A'
        } else {
            rem as u8 + b'0'
        }))
        .unwrap();
        n /= 62;
    }
    out
}
