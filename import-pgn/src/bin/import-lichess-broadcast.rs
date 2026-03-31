/// Import all Lichess broadcast game archives into the Caissify database.
///
/// Fetches the list of available archives from
/// `https://database.lichess.org/broadcast/list.txt`, then downloads and
/// imports every archive that has not yet been recorded in
/// `<data-dir>/imported.txt`.
///
/// Games are sent to `/import/caissify` with `WhiteFideId`/`BlackFideId`
/// forwarded from PGN headers so FIDE player links are preserved.
///
/// Re-running is always safe:
///   - Completed archives listed in `imported.txt` are skipped entirely.
///   - The server deduplicates by game-id, so partial re-imports produce
///     harmless 409 responses.
use std::{
    collections::HashSet,
    fmt::Write as _,
    fs,
    io::{self, BufRead, Write as _},
    mem,
    ops::ControlFlow,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use pgn_reader::{KnownOutcome, RawTag, Reader, SanPlus, Visitor};
use serde::Serialize;
use sha1::{Digest, Sha1};
use shakmaty::{Chess, CastlingMode, Color, Position, fen::Fen};
use time::OffsetDateTime;

// --------------------------------------------------------------------------
// Serialisable structures — same JSON schema as `/import/caissify`
// --------------------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Player {
    name: String,
    rating: u16,
}

#[derive(Debug, Default, Serialize)]
struct GameRecord {
    id: String,
    event: Option<String>,
    site: Option<String>,
    date: Option<String>,
    round: Option<String>,
    white: Player,
    black: Player,
    #[serde(skip_serializing_if = "Option::is_none")]
    winner: Option<String>,
    /// Space-separated UCI moves
    moves: String,
    /// FIDE ID for White (0 = unknown)
    #[serde(rename = "whiteFideId", skip_serializing_if = "is_zero")]
    white_fide_id: u32,
    /// FIDE ID for Black (0 = unknown)
    #[serde(rename = "blackFideId", skip_serializing_if = "is_zero")]
    black_fide_id: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

// --------------------------------------------------------------------------
// Worker message
// --------------------------------------------------------------------------

enum WorkerMsg {
    Batch(Batch),
    /// After all batches for an archive have been consumed, the main thread
    /// sends this to synchronise and mark the archive as completed.
    Sync {
        url: String,
        ack: crossbeam::channel::Sender<()>,
    },
}

// --------------------------------------------------------------------------
// Stable 8-char base-62 id
// --------------------------------------------------------------------------

/// When a `GameURL` is available (Lichess broadcast games always have one),
/// hash only the URL — it is globally unique per game regardless of player
/// name encoding.  Fall back to the classic `event+players+date+round` hash
/// for games without a URL.
fn make_game_id(
    game_url: Option<&str>,
    event: &str,
    white: &str,
    black: &str,
    date: &str,
    round: &str,
) -> String {
    let mut hasher = Sha1::new();
    if let Some(url) = game_url {
        hasher.update(url.as_bytes());
    } else {
        hasher.update(event.as_bytes());
        hasher.update(b"\x00");
        hasher.update(white.as_bytes());
        hasher.update(b"\x00");
        hasher.update(black.as_bytes());
        hasher.update(b"\x00");
        hasher.update(date.as_bytes());
        hasher.update(b"\x00");
        hasher.update(round.as_bytes());
    }
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

// --------------------------------------------------------------------------
// PGN visitor state
// --------------------------------------------------------------------------

#[derive(Default)]
struct RawGame {
    event: Option<String>,
    site: Option<String>,
    date: Option<String>,
    round: Option<String>,
    white: Player,
    black: Player,
    winner: Option<Option<Color>>,
    fen: Option<String>,
    game_url: Option<String>,
    white_fide_id: u32,
    black_fide_id: u32,
    sans: Vec<SanPlus>,
}

// --------------------------------------------------------------------------
// Batch sender
// --------------------------------------------------------------------------

struct Batch {
    label: String,
    games: Vec<GameRecord>,
}

struct Importer<'a> {
    tx: crossbeam::channel::Sender<WorkerMsg>,
    label: String,
    batch_size: usize,
    progress: &'a ProgressBar,
    batch: Vec<GameRecord>,
    /// Year and month extracted from the archive filename, used as a fallback
    /// when a game carries `[Date "????.??.??"]`.  Without this the server
    /// rejects the game because `LaxDate` cannot parse an unknown year.
    fallback_ym: Option<(i32, u8)>,
}

impl<'a> Importer<'a> {
    fn new(
        tx: crossbeam::channel::Sender<WorkerMsg>,
        label: String,
        batch_size: usize,
        progress: &'a ProgressBar,
        fallback_ym: Option<(i32, u8)>,
    ) -> Self {
        Importer {
            tx,
            label,
            batch_size,
            progress,
            batch: Vec::with_capacity(batch_size),
            fallback_ym,
        }
    }

    fn flush(&mut self) {
        let last_event = self
            .batch
            .last()
            .and_then(|g| g.event.as_deref())
            .unwrap_or("")
            .to_string();
        let batch = Batch {
            label: self.label.clone(),
            games: mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size)),
        };
        self.progress.set_message(last_event);
        self.tx.send(WorkerMsg::Batch(batch)).expect("send batch");
    }
}

impl Visitor for Importer<'_> {
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
            b"White" => g.white.name = value.decode_utf8().unwrap_or_default().into_owned(),
            b"Black" => g.black.name = value.decode_utf8().unwrap_or_default().into_owned(),
            b"WhiteElo" => {
                if value.as_bytes() != b"?" {
                    g.white.rating = btoi::btoi(value.as_bytes()).unwrap_or(0);
                }
            }
            b"BlackElo" => {
                if value.as_bytes() != b"?" {
                    g.black.rating = btoi::btoi(value.as_bytes()).unwrap_or(0);
                }
            }
            b"Result" => match KnownOutcome::from_ascii(value.as_bytes()) {
                Ok(outcome) => g.winner = Some(outcome.winner()),
                Err(_) => return ControlFlow::Break(()),
            },
            b"FEN" => {
                let fen_str = value.decode_utf8().unwrap_or_default().into_owned();
                if fen_str != "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1" {
                    g.fen = Some(fen_str);
                }
            }
            // Lichess-specific broadcast tags
            b"GameURL" | b"LichessURL" => {
                g.game_url = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"WhiteFideId" => {
                g.white_fide_id = btoi::btoi(value.as_bytes()).unwrap_or(0);
            }
            b"BlackFideId" => {
                g.black_fide_id = btoi::btoi(value.as_bytes()).unwrap_or(0);
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
        let mut pos: Chess = match g.fen.as_deref() {
            Some(fen_str) => match fen_str.parse::<Fen>() {
                Ok(fen) => match fen.into_position(shakmaty::CastlingMode::Standard) {
                    Ok(p) => p,
                    Err(_) => return,
                },
                Err(_) => return,
            },
            None => Chess::default(),
        };

        let mut uci_moves: Vec<String> = Vec::with_capacity(g.sans.len());
        for san_plus in &g.sans {
            match san_plus.san.to_move(&pos) {
                Ok(m) => {
                    uci_moves.push(
                        shakmaty::uci::UciMove::from_move(m, CastlingMode::Standard).to_string(),
                    );
                    pos.play_unchecked(m);
                }
                Err(_) => return,
            }
        }

        let event = g.event.as_deref().unwrap_or("");
        let white = &g.white.name;
        let black = &g.black.name;
        let raw_date = g.date.as_deref().unwrap_or("????.??.??");
        // Apply archive year-month fallback when the PGN date has no concrete
        // year (e.g. `[Date "????.??.??"]`).  Without this the server rejects
        // the game because `LaxDate` cannot parse an unknown year.
        let date: String = if raw_date.starts_with('?') {
            match self.fallback_ym {
                Some((y, m)) => format!("{y}.{m:02}.??"),
                None => raw_date.to_string(),
            }
        } else {
            raw_date.to_string()
        };
        let round = g.round.as_deref().unwrap_or("?");

        let id = make_game_id(g.game_url.as_deref(), event, white, black, &date, round);

        let winner_str = g
            .winner
            .flatten()
            .map(|c| if c == Color::White { "white" } else { "black" }.to_string());

        let record = GameRecord {
            id,
            event: g.event,
            site: g.site,
            date: Some(date),
            round: g.round,
            white: g.white,
            black: g.black,
            winner: winner_str,
            moves: uci_moves.join(" "),
            white_fide_id: g.white_fide_id,
            black_fide_id: g.black_fide_id,
        };

        self.batch.push(record);
        if self.batch.len() >= self.batch_size {
            self.flush();
        }
    }
}

// --------------------------------------------------------------------------
// Archive list + state helpers
// --------------------------------------------------------------------------

/// Fetch the broadcast archive list from Lichess; returns one URL per line.
fn fetch_list(url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let text = reqwest::blocking::get(url)?
        .error_for_status()?
        .text()?;
    let urls = text
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && l.contains(".pgn"))
        .collect();
    Ok(urls)
}

/// Extract `(year, month)` from a broadcast archive URL or filename.
fn url_year_month(url: &str) -> Option<(i32, u8)> {
    let stem = url.split('/').next_back()?;
    let s = stem
        .strip_suffix(".pgn.zst")
        .or_else(|| stem.strip_suffix(".pgn"))?;
    if s.len() < 7 {
        return None;
    }
    parse_ym(&s[s.len() - 7..]).ok()
}

/// Human-readable label for an archive URL (e.g. `"2024-03"`).
fn archive_label(url: &str) -> String {
    url_year_month(url)
        .map(|(y, m)| format!("{y}-{m:02}"))
        .unwrap_or_else(|| url.split('/').next_back().unwrap_or(url).to_string())
}

/// Load the set of fully-imported archive URLs from `<data_dir>/imported.txt`.
fn load_completed(path: &Path) -> HashSet<String> {
    if !path.exists() {
        return HashSet::new();
    }
    let file = fs::File::open(path).expect("open imported.txt");
    io::BufReader::new(file)
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Append one archive URL to the completed-state file (atomic append).
fn mark_completed(path: &Path, url: &str) {
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open imported.txt for append");
    writeln!(f, "{url}").expect("write imported.txt");
}

// --------------------------------------------------------------------------
// Archive download helpers
// --------------------------------------------------------------------------

/// Extract the filename component from a URL.
fn archive_filename(url: &str) -> String {
    url.split('/').next_back().unwrap_or("archive.pgn.zst").to_string()
}

/// Download `url` to `dest` (atomic: writes to a `.tmp` file, then renames).
/// Skips the download if `dest` already exists.
fn download_archive(url: &str, dest: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if dest.exists() {
        eprintln!("  cached: {}", dest.display());
        return Ok(());
    }

    eprintln!("  downloading {url} …");
    let tmp = dest.with_extension("zst.tmp");

    let mut resp = reqwest::blocking::get(url)?.error_for_status()?;
    {
        let mut file = fs::File::create(&tmp)?;
        resp.copy_to(&mut file)?;
    }

    fs::rename(&tmp, dest)?;
    eprintln!("  saved: {}", dest.display());
    Ok(())
}

// --------------------------------------------------------------------------
// CLI
// --------------------------------------------------------------------------

/// Parse `"YYYY-MM"` into `(year, month)`.
fn parse_ym(s: &str) -> Result<(i32, u8), String> {
    let parts: Vec<&str> = s.splitn(2, '-').collect();
    if parts.len() != 2 {
        return Err(format!("expected YYYY-MM, got '{s}'"));
    }
    let year = parts[0]
        .parse::<i32>()
        .map_err(|_| format!("invalid year in '{s}'"))?;
    let month = parts[1]
        .parse::<u8>()
        .map_err(|_| format!("invalid month in '{s}'"))?;
    if !(1..=12).contains(&month) {
        return Err(format!("month out of range in '{s}'"));
    }
    Ok((year, month))
}

#[derive(Parser)]
#[command(
    name = "import-lichess-broadcast",
    about = "Download and import all Lichess broadcast PGN archives into Caissify"
)]
struct Args {
    /// Base URL of the caissify-explorer import API.
    #[arg(long, default_value = "http://localhost:9002")]
    endpoint: String,

    /// URL of the Lichess broadcast archive list.
    #[arg(long, default_value = "https://database.lichess.org/broadcast/list.txt")]
    list_url: String,

    /// Only import archives from this month onwards, inclusive (e.g. 2024-01).
    /// Omit to process all archives found in the list.
    #[arg(long)]
    since: Option<String>,

    /// Only import archives up to this month, inclusive (e.g. 2025-12).
    /// Defaults to the current month.
    #[arg(long)]
    until: Option<String>,

    /// Directory to cache downloaded .pgn.zst archives and track import state
    /// (`imported.txt` lives here).
    #[arg(long, default_value = "/tmp/lichess-broadcasts")]
    data_dir: PathBuf,

    /// Number of games per HTTP request.
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Pause importing during these UTC hours (e.g. peak traffic hours).
    #[arg(long)]
    avoid_utc_hour: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // ------------------------------------------------------------------
    // 1.  Fetch the broadcast archive list from Lichess
    // ------------------------------------------------------------------
    println!("Fetching archive list from {} …", args.list_url);
    let available: Vec<String> = fetch_list(&args.list_url)?;
    println!("  {} archive(s) found", available.len());

    // ------------------------------------------------------------------
    // 2.  Apply --since / --until filter
    // ------------------------------------------------------------------
    let since: Option<(i32, u8)> = args
        .since
        .as_deref()
        .map(parse_ym)
        .transpose()
        .map_err(|e| format!("--since: {e}"))?;

    let now = OffsetDateTime::now_utc();
    let until: (i32, u8) = match &args.until {
        Some(s) => parse_ym(s).map_err(|e| format!("--until: {e}"))?,
        None => (now.year(), now.month() as u8),
    };

    let filtered: Vec<String> = available
        .into_iter()
        .filter(|url| {
            if let Some((y, m)) = url_year_month(url) {
                if let Some((sy, sm)) = since {
                    if (y, m) < (sy, sm) {
                        return false;
                    }
                }
                let (uy, um) = until;
                (y, m) <= (uy, um)
            } else {
                true // can't parse month range — include
            }
        })
        .collect();

    // ------------------------------------------------------------------
    // 3.  Load completed-archive state from <data_dir>/imported.txt
    // ------------------------------------------------------------------
    fs::create_dir_all(&args.data_dir)?;
    let imported_path = args.data_dir.join("imported.txt");
    let completed: HashSet<String> = load_completed(&imported_path);

    let to_import: Vec<String> = filtered
        .into_iter()
        .filter(|url| !completed.contains(url))
        .collect();

    if to_import.is_empty() {
        println!("Nothing new to import — all archives up to date.");
        return Ok(());
    }

    println!(
        "Importing {} archive(s) ({} already completed).",
        to_import.len(),
        completed.len()
    );

    // ------------------------------------------------------------------
    // 4.  Worker thread: receive batches and POST to /import/caissify
    // ------------------------------------------------------------------
    let (tx, rx) = crossbeam::channel::bounded::<WorkerMsg>(50);

    let endpoint = args.endpoint.clone();
    let avoid_utc_hour = args.avoid_utc_hour.clone();
    let imported_path_bg = imported_path.clone();

    let bg = thread::spawn(move || {
        let client = reqwest::blocking::Client::builder()
            .timeout(None)
            .build()
            .expect("build http client");

        while let Ok(msg) = rx.recv() {
            match msg {
                WorkerMsg::Batch(batch) => {
                    while avoid_utc_hour.contains(&OffsetDateTime::now_utc().hour()) {
                        println!("paused during busy hour — sleeping 10 min…");
                        thread::sleep(Duration::from_secs(10 * 60));
                    }

                    for game in &batch.games {
                        let res = client
                            .put(format!("{}/import/caissify", endpoint))
                            .json(game)
                            .send()
                            .expect("send game");

                        if !res.status().is_success() {
                            let status = res.status();
                            let body = res.text().unwrap_or_default();
                            // 409 Conflict = duplicate — expected and harmless.
                            if status.as_u16() != 409 {
                                eprintln!(
                                    "[{}] {} ({status}) — {body}",
                                    batch.label, game.id
                                );
                            }
                        }
                    }
                }
                WorkerMsg::Sync { url, ack } => {
                    // All batches for this archive are consumed — record it as
                    // done and unblock the main thread.
                    mark_completed(&imported_path_bg, &url);
                    let _ = ack.send(());
                }
            }
        }
    });

    // ------------------------------------------------------------------
    // 5.  Main loop: download → parse → import each archive
    // ------------------------------------------------------------------
    for url in &to_import {
        let filename = archive_filename(url);
        let archive_path = args.data_dir.join(&filename);
        let label = archive_label(url);

        println!("\n[{label}] Downloading…");
        match download_archive(url, &archive_path) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("  skipping {label}: {e}");
                continue;
            }
        }

        let file = fs::File::open(&archive_path)?;
        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        let progress = ProgressBar::with_draw_target(
            Some(file_size),
            ProgressDrawTarget::stdout_with_hz(4),
        )
        .with_style(
            ProgressStyle::with_template(
                "{spinner} {prefix} {msg} {wide_bar} {bytes_per_sec:>14} {eta:>7}",
            )
            .unwrap(),
        )
        .with_prefix(label.clone());

        let file = progress.wrap_read(file);
        let decoder = zstd::Decoder::new(file)?;
        let mut reader = Reader::new(decoder);
        let fallback_ym = url_year_month(url);
        let mut importer = Importer::new(tx.clone(), label.clone(), args.batch_size, &progress, fallback_ym);
        reader.visit_all_games(&mut importer).map_err(|e| {
            format!("PGN parse error in {label}: {e}")
        })?;
        importer.flush();
        progress.finish();

        // Synchronise with the background worker: wait until it has consumed
        // every batch for this archive, then it writes the URL to imported.txt.
        let (ack_tx, ack_rx) = crossbeam::channel::bounded(1);
        tx.send(WorkerMsg::Sync {
            url: url.clone(),
            ack: ack_tx,
        })
        .expect("send sync");
        ack_rx.recv().expect("recv ack");

        println!("[{label}] ✓ done");
    }

    drop(tx);
    bg.join().expect("bg thread join");
    println!("\nAll archives imported.");
    Ok(())
}
