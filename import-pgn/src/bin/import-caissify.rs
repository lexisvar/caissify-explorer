use std::{
    ffi::OsStr,
    fmt::Write as _,
    fs::File,
    io,
    mem,
    ops::ControlFlow,
    path::PathBuf,
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
// Serialisable structures that match `MastersGameWithId` JSON representation
// --------------------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Player {
    name: Option<String>,
    rating: Option<u16>,
}

/// Matches the `MastersGameWithId` JSON schema understood by `/import/caissify`
#[derive(Debug, Default, Serialize)]
struct GameRecord {
    /// 8-char base-62 game id, generated from SHA-1 of key fields
    id: String,
    event: Option<String>,
    site: Option<String>,
    date: Option<String>,
    round: Option<String>,
    white: Player,
    black: Player,
    #[serde(skip_serializing_if = "Option::is_none")]
    winner: Option<String>,
    /// Space-separated UCI moves, e.g. "e2e4 e7e5 …"
    moves: String,
}

// --------------------------------------------------------------------------
// Stable 8-char base-62 id from SHA-1
// --------------------------------------------------------------------------

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

    // Take the first 6 bytes as a little-endian u64 and base62-encode it (8 chars)
    let mut n: u64 = 0;
    for &byte in hash[..6].iter().rev() {
        n = (n << 8) | u64::from(byte);
    }
    // Ensure the value fits in 62^8
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
    winner: Option<Option<Color>>, // None = result tag missing / invalid
    fen: Option<String>,
    sans: Vec<SanPlus>,
}

// --------------------------------------------------------------------------
// Visitor / batch sender
// --------------------------------------------------------------------------

struct Batch {
    filename: PathBuf,
    games: Vec<GameRecord>,
}

impl Batch {
    fn last_event(&self) -> &str {
        self.games
            .last()
            .and_then(|g| g.event.as_deref())
            .unwrap_or("")
    }
}

struct Importer<'a> {
    tx: crossbeam::channel::Sender<Batch>,
    filename: PathBuf,
    batch_size: usize,
    progress: &'a ProgressBar,
    batch: Vec<GameRecord>,
}

impl<'a> Importer<'a> {
    fn new(
        tx: crossbeam::channel::Sender<Batch>,
        filename: PathBuf,
        batch_size: usize,
        progress: &'a ProgressBar,
    ) -> Self {
        Importer {
            tx,
            filename,
            batch_size,
            progress,
            batch: Vec::with_capacity(batch_size),
        }
    }

    fn flush(&mut self) {
        let batch = Batch {
            filename: self.filename.clone(),
            games: mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size)),
        };
        self.progress.set_message(batch.last_event().to_string());
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
            b"White" => {
                g.white.name = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"Black" => {
                g.black.name = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"WhiteElo" => {
                if value.as_bytes() != b"?" {
                    g.white.rating = btoi::btoi(value.as_bytes()).ok();
                }
            }
            b"BlackElo" => {
                if value.as_bytes() != b"?" {
                    g.black.rating = btoi::btoi(value.as_bytes()).ok();
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
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn begin_movetext(&mut self, g: RawGame) -> ControlFlow<Self::Output, Self::Movetext> {
        // Drop games with no result
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
        // Convert SAN moves to UCI
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
                    uci_moves.push(shakmaty::uci::UciMove::from_move(m, CastlingMode::Standard).to_string());
                    pos.play_unchecked(m);
                }
                Err(_) => {
                    // Illegal move — skip this game
                    return;
                }
            }
        }

        let event = g.event.as_deref().unwrap_or("");
        let white = g.white.name.as_deref().unwrap_or("");
        let black = g.black.name.as_deref().unwrap_or("");
        let date = g.date.as_deref().unwrap_or("????.??.??");
        let round = g.round.as_deref().unwrap_or("?");

        let id = make_game_id(event, white, black, date, round);

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
        };

        self.batch.push(record);
        if self.batch.len() >= self.batch_size {
            self.flush();
        }
    }
}

// --------------------------------------------------------------------------
// CLI
// --------------------------------------------------------------------------

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:9002")]
    endpoint: String,
    /// Number of games per HTTP request
    #[arg(long, default_value = "100")]
    batch_size: usize,
    /// Pause importing during these UTC hours (e.g. peak traffic hours)
    #[arg(long)]
    avoid_utc_hour: Vec<u8>,
    pgns: Vec<PathBuf>,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let (tx, rx) = crossbeam::channel::bounded::<Batch>(50);

    let endpoint = args.endpoint.clone();
    let avoid_utc_hour = args.avoid_utc_hour.clone();

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
                    eprintln!(
                        "{:?}: {} ({}) - {}",
                        batch.filename,
                        game.id,
                        res.status(),
                        res.text().unwrap_or_default()
                    );
                }
            }
        }
    });

    for pgn_path in &args.pgns {
        let file = File::open(pgn_path)?;
        let progress = ProgressBar::with_draw_target(
            Some(file.metadata()?.len()),
            ProgressDrawTarget::stdout_with_hz(4),
        )
        .with_style(
            ProgressStyle::with_template(
                "{spinner} {prefix} {msg} {wide_bar} {bytes_per_sec:>14} {eta:>7}",
            )
            .unwrap(),
        )
        .with_prefix(format!("{pgn_path:?}"));
        let file = progress.wrap_read(file);

        let uncompressed: Box<dyn io::Read> =
            if pgn_path.extension() == Some(OsStr::new("bz2")) {
                Box::new(bzip2::read::MultiBzDecoder::new(file))
            } else if pgn_path.extension() == Some(OsStr::new("zst")) {
                Box::new(zstd::Decoder::new(file)?)
            } else {
                Box::new(file)
            };

        let mut reader = Reader::new(uncompressed);
        let mut importer = Importer::new(tx.clone(), pgn_path.clone(), args.batch_size, &progress);
        reader.visit_all_games(&mut importer)?;
        importer.flush();

        progress.finish();
    }

    drop(tx);
    bg.join().expect("bg thread join");
    Ok(())
}
