/// Import official Lichess broadcast games from the monthly PGN archives at
/// https://database.lichess.org/#broadcasts
///
/// Archives are zstd-compressed PGN files:
///   lichess_db_broadcast_YYYY-MM.pgn.zst
///
/// Key advantages over `import-caissify`:
///   - Uses `GameURL` as the primary hash seed for a stable `GameId` (no
///     player name in the hash → immune to name normalisation differences).
///   - Forwards `WhiteFideId`/`BlackFideId` from PGN headers directly,
///     bypassing the server-side name-matching fallback.
///   - The server also performs a SHA-1 move-sequence fingerprint check on
///     every import, so even if the same game arrives from a different source
///     with different player names it will be rejected as a duplicate.
use std::{
    fmt::Write as _,
    fs,
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
    tx: crossbeam::channel::Sender<Batch>,
    label: String,
    batch_size: usize,
    progress: &'a ProgressBar,
    batch: Vec<GameRecord>,
}

impl<'a> Importer<'a> {
    fn new(
        tx: crossbeam::channel::Sender<Batch>,
        label: String,
        batch_size: usize,
        progress: &'a ProgressBar,
    ) -> Self {
        Importer {
            tx,
            label,
            batch_size,
            progress,
            batch: Vec::with_capacity(batch_size),
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
        self.tx.send(batch).expect("send batch");
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
        let date = g.date.as_deref().unwrap_or("????.??.??");
        let round = g.round.as_deref().unwrap_or("?");

        let id = make_game_id(g.game_url.as_deref(), event, white, black, date, round);

        let winner_str = g
            .winner
            .flatten()
            .map(|c| if c == Color::White { "white" } else { "black" }.to_string());

        let record = GameRecord {
            id,
            event: g.event,
            site: g.site,
            date: g.date,
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
// Archive download helpers
// --------------------------------------------------------------------------

fn archive_url(year: i32, month: u8) -> String {
    format!(
        "https://database.lichess.org/broadcast/lichess_db_broadcast_{year}-{month:02}.pgn.zst"
    )
}

fn archive_filename(year: i32, month: u8) -> String {
    format!("lichess_db_broadcast_{year}-{month:02}.pgn.zst")
}

/// Download `url` to `dest` (atomic: writes to a temp file, then renames).
/// Skips the download if `dest` already exists.
fn download_archive(url: &str, dest: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if dest.exists() {
        eprintln!("  cached: {}", dest.display());
        return Ok(());
    }

    eprintln!("  downloading {url} …");
    let tmp = dest.with_extension("zst.tmp");

    let mut resp = reqwest::blocking::get(url)?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {} for {url}", resp.status()).into());
    }

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

/// Parse "YYYY-MM" into (year, month).
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
    about = "Download and import Lichess broadcast PGN archives"
)]
struct Args {
    /// Base URL of the caissify-explorer import API
    #[arg(long, default_value = "http://localhost:9002")]
    endpoint: String,

    /// First month to import, inclusive (e.g. 2024-01)
    #[arg(long, default_value = "2024-01")]
    since: String,

    /// Last month to import, inclusive (defaults to current month)
    #[arg(long)]
    until: Option<String>,

    /// Directory to cache downloaded .pgn.zst archives
    #[arg(long, default_value = "/tmp/lichess-broadcasts")]
    data_dir: PathBuf,

    /// Number of games per HTTP request
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Pause importing during these UTC hours (e.g. peak traffic hours)
    #[arg(long)]
    avoid_utc_hour: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (since_year, since_month) = parse_ym(&args.since)?;

    let now = OffsetDateTime::now_utc();
    let (until_year, until_month) = match &args.until {
        Some(s) => parse_ym(s)?,
        None => (now.year(), now.month() as u8),
    };

    fs::create_dir_all(&args.data_dir)?;

    // Collect all (year, month) pairs in range.
    let mut months: Vec<(i32, u8)> = Vec::new();
    let (mut y, mut m) = (since_year, since_month);
    loop {
        months.push((y, m));
        if (y, m) == (until_year, until_month) {
            break;
        }
        m += 1;
        if m > 12 {
            m = 1;
            y += 1;
        }
        if y > until_year || (y == until_year && m > until_month) {
            break;
        }
    }

    println!(
        "Importing {} month(s): {since_year}-{since_month:02} … {until_year}-{until_month:02}",
        months.len()
    );

    let (tx, rx) = crossbeam::channel::bounded::<Batch>(50);

    let endpoint = args.endpoint.clone();
    let avoid_utc_hour = args.avoid_utc_hour.clone();

    // Background HTTP poster thread (identical pattern to import-caissify)
    let bg = thread::spawn(move || {
        let client = reqwest::blocking::Client::builder()
            .timeout(None)
            .build()
            .expect("build http client");

        while let Ok(batch) = rx.recv() {
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
                            "[{}] {} ({status}) - {body}",
                            batch.label, game.id
                        );
                    }
                }
            }
        }
    });

    for (year, month) in &months {
        let year = *year;
        let month = *month;
        let label = format!("{year}-{month:02}");
        let filename = archive_filename(year, month);
        let archive_path = args.data_dir.join(&filename);
        let url = archive_url(year, month);

        println!("\n[{label}] Downloading archive…");
        match download_archive(&url, &archive_path) {
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
        let mut importer = Importer::new(tx.clone(), label.clone(), args.batch_size, &progress);
        reader.visit_all_games(&mut importer).map_err(|e| {
            format!("PGN parse error in {label}: {e}")
        })?;
        importer.flush();
        progress.finish();

        println!("[{label}] done");
    }

    drop(tx);
    bg.join().expect("bg thread join");
    println!("\nAll months imported.");
    Ok(())
}
