/// import-twic — Import TWIC (The Week In Chess) weekly game archives into
/// the Caissify database.
///
/// TWIC publishes weekly ZIP archives at:
///   https://theweekinchess.com/zips/twic{N}g.zip
///
/// For every player that carries a `WhiteFideId` / `BlackFideId` PGN tag the
/// importer queries `GET /fide/player/{id}` on the explorer to obtain the
/// official FIDE name, replacing the name that TWIC recorded in the PGN
/// (which is sometimes abbreviated or mis-spelled).
///
/// Usage — download and import a specific week:
///   import-twic --endpoint http://localhost:9002 --week 1639
///
/// Usage — download a range of weeks:
///   import-twic --endpoint http://localhost:9002 --from-week 1600 --to-week 1639
///
/// Usage — import local PGN or ZIP files:
///   import-twic --endpoint http://localhost:9002 file.pgn twic1638g.zip
///
/// When --no-fide-lookup is set the FIDE name correction step is skipped and
/// the raw TWIC names are used as-is (useful for offline testing).
use std::{
    collections::HashMap,
    fmt::Write as _,
    fs::File,
    io::Read,
    ops::ControlFlow,
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use pgn_reader::{KnownOutcome, RawTag, Reader, SanPlus, Visitor};
use reqwest::blocking::Client;
use serde::Serialize;
use sha1::{Digest, Sha1};
use shakmaty::{Chess, CastlingMode, Color, Position, fen::Fen};
use zip::ZipArchive;

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
    /// Space-separated UCI moves.
    moves: String,
    /// FIDE ID for White (0 = unknown); signals an authoritative name source.
    #[serde(rename = "whiteFideId", skip_serializing_if = "is_zero")]
    white_fide_id: u32,
    /// FIDE ID for Black (0 = unknown); signals an authoritative name source.
    #[serde(rename = "blackFideId", skip_serializing_if = "is_zero")]
    black_fide_id: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

// --------------------------------------------------------------------------
// Stable 8-char base-62 id from SHA-1 of key fields
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
    white_fide_id: u32,
    black_fide_id: u32,
    sans: Vec<SanPlus>,
}

// --------------------------------------------------------------------------
// PGN visitor
// --------------------------------------------------------------------------

struct TwicVisitor {
    games: Vec<GameRecord>,
}

impl TwicVisitor {
    fn new() -> Self {
        TwicVisitor { games: Vec::new() }
    }
}

impl Visitor for TwicVisitor {
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
                        shakmaty::uci::UciMove::from_move(m, CastlingMode::Standard)
                            .to_string(),
                    );
                    pos.play_unchecked(m);
                }
                Err(_) => return, // illegal move — skip game
            }
        }

        let event = g.event.as_deref().unwrap_or("");
        let white = &g.white.name;
        let black = &g.black.name;
        let date = g.date.as_deref().unwrap_or("????.??.??");
        let round = g.round.as_deref().unwrap_or("?");

        // Game ID is computed from the (possibly still uncorrected) TWIC names
        // here; it will be recomputed from the corrected names after the FIDE
        // lookup step so that the stable ID is based on the canonical name.
        let id = make_game_id(event, white, black, date, round);

        let winner_str = g
            .winner
            .flatten()
            .map(|c| if c == Color::White { "white" } else { "black" }.to_string());

        self.games.push(GameRecord {
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
        });
    }
}

// --------------------------------------------------------------------------
// FIDE name lookup  (cached — one API call per unique FIDE ID per run)
// --------------------------------------------------------------------------

/// Looks up the canonical FIDE player name for `fide_id`.
///
/// Returns `Some(name)` on success.  Returns `None` when the player is not
/// found in our FIDE database (either the FIDE import has not been run yet,
/// or the ID is invalid).
///
/// Results are cached in `cache` so each ID is fetched at most once per run.
fn lookup_fide_name(
    client: &Client,
    endpoint: &str,
    fide_id: u32,
    cache: &mut HashMap<u32, Option<String>>,
) -> Option<String> {
    if let Some(cached) = cache.get(&fide_id) {
        return cached.clone();
    }

    let url = format!("{endpoint}/fide/player/{fide_id}");
    let result = client
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .ok()
        .and_then(|resp| {
            if resp.status().is_success() {
                resp.json::<serde_json::Value>().ok().and_then(|json| {
                    json.get("name")
                        .and_then(|n| n.as_str())
                        .map(str::to_owned)
                })
            } else {
                None
            }
        });

    cache.insert(fide_id, result.clone());
    result
}

// --------------------------------------------------------------------------
// Apply FIDE name corrections to a batch of games
// --------------------------------------------------------------------------

/// For every game that has a FIDE ID on either side, queries the explorer for
/// the canonical name and replaces the TWIC name when they differ.
///
/// The game ID is recomputed after the name correction so that it is stable
/// across imports from different sources that use the same canonical name.
fn apply_fide_names(
    games: &mut Vec<GameRecord>,
    client: &Client,
    endpoint: &str,
    cache: &mut HashMap<u32, Option<String>>,
) {
    let mut corrected = 0usize;

    for game in games.iter_mut() {
        // White
        if game.white_fide_id != 0 {
            if let Some(canonical) =
                lookup_fide_name(client, endpoint, game.white_fide_id, cache)
            {
                if canonical != game.white.name {
                    eprintln!(
                        "  [name] White {}: {:?} → {:?}",
                        game.white_fide_id, game.white.name, canonical
                    );
                    game.white.name = canonical;
                    corrected += 1;
                }
            }
        }

        // Black
        if game.black_fide_id != 0 {
            if let Some(canonical) =
                lookup_fide_name(client, endpoint, game.black_fide_id, cache)
            {
                if canonical != game.black.name {
                    eprintln!(
                        "  [name] Black {}: {:?} → {:?}",
                        game.black_fide_id, game.black.name, canonical
                    );
                    game.black.name = canonical;
                    corrected += 1;
                }
            }
        }

        // Recompute game ID using the (potentially corrected) names so the ID
        // is stable regardless of which source provided the player name.
        game.id = make_game_id(
            game.event.as_deref().unwrap_or(""),
            &game.white.name,
            &game.black.name,
            game.date.as_deref().unwrap_or("????.??.??"),
            game.round.as_deref().unwrap_or("?"),
        );
    }

    if corrected > 0 {
        eprintln!("  → corrected {corrected} player name(s)");
    }
}

// --------------------------------------------------------------------------
// PGN bytes → Vec<GameRecord>
// --------------------------------------------------------------------------

fn parse_pgn(pgn: &[u8]) -> Vec<GameRecord> {
    let mut visitor = TwicVisitor::new();
    let mut reader = Reader::new(pgn);
    while let Some(()) = reader.read_game(&mut visitor).unwrap_or(None) {}
    visitor.games
}

// --------------------------------------------------------------------------
// Download TWIC ZIP → PGN bytes
// --------------------------------------------------------------------------

fn download_week(client: &Client, week: u32) -> Vec<u8> {
    let url = format!("https://theweekinchess.com/zips/twic{week}g.zip");
    eprintln!("Downloading {url} …");
    let bytes = client
        .get(&url)
        .timeout(Duration::from_secs(120))
        .send()
        .unwrap_or_else(|e| panic!("HTTP error downloading TWIC #{week}: {e}"))
        .error_for_status()
        .unwrap_or_else(|e| panic!("Server error downloading TWIC #{week}: {e}"))
        .bytes()
        .unwrap_or_else(|e| panic!("Read error for TWIC #{week}: {e}"));

    unzip_first_entry(&bytes)
}

fn unzip_first_entry(bytes: &[u8]) -> Vec<u8> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor).expect("open zip archive");
    let mut entry = archive.by_index(0).expect("zip has no entries");
    let mut buf = Vec::new();
    entry.read_to_end(&mut buf).expect("read zip entry");
    buf
}

fn read_file_or_zip(path: &PathBuf) -> Vec<u8> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    let mut file = File::open(path)
        .unwrap_or_else(|e| panic!("cannot open {}: {e}", path.display()));

    if ext.eq_ignore_ascii_case("zip") {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).expect("read zip file");
        unzip_first_entry(&bytes)
    } else {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).expect("read pgn file");
        buf
    }
}

// --------------------------------------------------------------------------
// Send games to the explorer
// --------------------------------------------------------------------------

fn send_games(
    client: &Client,
    endpoint: &str,
    games: &[GameRecord],
    pb: &ProgressBar,
) -> (usize, usize) {
    let url = format!("{endpoint}/import/caissify");
    let mut ok = 0usize;
    let mut errors = 0usize;

    for game in games {
        match client
            .put(&url)
            .json(game)
            .timeout(Duration::from_secs(30))
            .send()
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    ok += 1;
                } else if status.as_u16() == 409 {
                    // Duplicate — server may still have upgraded FIDE IDs/names.
                    ok += 1;
                } else {
                    errors += 1;
                    eprintln!(
                        "  [warn] {} — id={} — {}",
                        status,
                        game.id,
                        resp.text().unwrap_or_default()
                    );
                }
            }
            Err(e) => {
                errors += 1;
                eprintln!("  [error] id={} — {e}", game.id);
            }
        }
        pb.inc(1);
        pb.set_message(format!(
            "{}/{}",
            game.event.as_deref().unwrap_or(""),
            game.white.name
        ));
    }

    (ok, errors)
}

// --------------------------------------------------------------------------
// CLI
// --------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "import-twic",
    about = "Import TWIC weekly chess archives into the Caissify database.\n\
             Player names are corrected using the canonical FIDE name when a\n\
             WhiteFideId / BlackFideId is present in the PGN."
)]
struct Args {
    /// Explorer HTTP endpoint.
    #[arg(long, default_value = "http://localhost:9002")]
    endpoint: String,

    /// Download and import one specific TWIC week (e.g. --week 1639).
    /// Can be repeated.
    #[arg(long)]
    week: Vec<u32>,

    /// First week of a range to download (inclusive).  Requires --to-week.
    #[arg(long, requires = "to_week")]
    from_week: Option<u32>,

    /// Last week of a range to download (inclusive).  Requires --from-week.
    #[arg(long, requires = "from_week")]
    to_week: Option<u32>,

    /// Skip the FIDE name-correction step and use raw TWIC names as-is.
    #[arg(long)]
    no_fide_lookup: bool,

    /// Local PGN or ZIP files to import.
    files: Vec<PathBuf>,
}

// --------------------------------------------------------------------------
// Entry point
// --------------------------------------------------------------------------

fn main() {
    let args = Args::parse();

    let client = Client::builder()
        .user_agent("caissify-import-twic/1.0")
        .build()
        .expect("build HTTP client");

    // ── Collect all (label, pgn_bytes) sources ────────────────────────────

    let mut sources: Vec<(String, Vec<u8>)> = Vec::new();

    for path in &args.files {
        let label = path.display().to_string();
        eprintln!("Reading {label} …");
        let pgn = read_file_or_zip(path);
        sources.push((label, pgn));
    }

    for &week in &args.week {
        let pgn = download_week(&client, week);
        sources.push((format!("TWIC #{week}"), pgn));
    }

    if let (Some(from), Some(to)) = (args.from_week, args.to_week) {
        for week in from..=to {
            let pgn = download_week(&client, week);
            sources.push((format!("TWIC #{week}"), pgn));
        }
    }

    if sources.is_empty() {
        eprintln!(
            "No input sources.  Use --week N, --from-week M --to-week N, \
             or provide local PGN/ZIP file paths."
        );
        std::process::exit(1);
    }

    // ── FIDE name cache (shared across all sources in this run) ───────────

    let mut fide_cache: HashMap<u32, Option<String>> = HashMap::new();

    // ── Process each source ───────────────────────────────────────────────

    let mut total_ok = 0usize;
    let mut total_errors = 0usize;

    for (label, pgn) in sources {
        eprintln!("Parsing {label} …");
        let mut games = parse_pgn(&pgn);
        eprintln!("  {} games parsed", games.len());

        if !args.no_fide_lookup {
            apply_fide_names(&mut games, &client, &args.endpoint, &mut fide_cache);
        }

        let pb = ProgressBar::with_draw_target(
            Some(games.len() as u64),
            ProgressDrawTarget::stderr(),
        );
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] \
                     [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("=>-"),
        );

        let (ok, errors) = send_games(&client, &args.endpoint, &games, &pb);
        pb.finish_and_clear();

        eprintln!("  {label}: {ok} imported, {errors} errors");
        total_ok += ok;
        total_errors += errors;
    }

    eprintln!("─────────────────────────────────────────────");
    eprintln!("Total: {total_ok} imported, {total_errors} errors");
}
