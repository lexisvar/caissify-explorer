//! Scraper for chess-results.com — fetches a FIDE player's games as PGN and
//! converts them into [`MastersGameWithId`] records ready for insertion.
//!
//! Flow:
//!   1. GET `PartieSuche.aspx?lan=1&SNode=S0` — harvest ASP.NET ViewState.
//!   2. POST the form with the player's FIDE ID + PGN download button.
//!   3. Parse the PGN response with `pgn_reader`, converting SAN→UCI.
//!   4. Set the correct `white_fide_id` / `black_fide_id` by comparing the
//!      FIDE player's name (from our DB) against the PGN White/Black tags.
//!
//! Deduplication is handled downstream by [`CaissifyImporter::import`], which
//! rejects duplicates by both game-ID and move-sequence fingerprint.

use std::{fmt::Write as _, ops::ControlFlow, time::Duration};

use pgn_reader::{KnownOutcome, RawTag, Reader, SanPlus, Visitor};
use reqwest::Client;
use sha1::{Digest, Sha1};
use shakmaty::{ByColor, CastlingMode, Chess, Color, Position, fen::Fen, uci::UciMove};

use crate::model::{GameId, GamePlayer, LaxDate, MastersGame, MastersGameWithId};

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

const SEARCH_URL: &str = "https://s1.chess-results.com/PartieSuche.aspx";
const TIMEOUT: Duration = Duration::from_secs(60);

/// Build a `reqwest::Client` suitable for chess-results.com.
pub fn build_client() -> Client {
    Client::builder()
        .timeout(TIMEOUT)
        .user_agent("caissify-explorer/fide-indexer")
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .expect("build chess-results HTTP client")
}

/// Fetch all games for a FIDE player from chess-results.com.
///
/// Issues two HTTP requests (GET + POST).  Returns an empty `Vec` when the
/// player has no games listed or when chess-results returns a non-PGN body.
///
/// `fide_name` is the canonical name from the FIDE DB (used as a hint for
/// colour detection).  If it doesn't match any PGN tag exactly, the scraper
/// falls back to auto-detecting the player's name as the one that appears most
/// frequently across all returned games — since every game in the response is
/// by the queried player.
pub async fn fetch_fide_games(
    client: &Client,
    fide_id: u32,
    fide_name: &str,
) -> Result<Vec<MastersGameWithId>, String> {
    // ── Step 1: GET the search form ────────────────────────────────────────
    let html = client
        .get(format!("{SEARCH_URL}?lan=1&SNode=S0"))
        .send()
        .await
        .map_err(|e| format!("chess-results GET: {e}"))?
        .text()
        .await
        .map_err(|e| format!("chess-results GET body: {e}"))?;

    let viewstate = extract_hidden_field(&html, "__VIEWSTATE");
    let viewstate_gen = extract_hidden_field(&html, "__VIEWSTATEGENERATOR");
    let ev_validation = extract_hidden_field(&html, "__EVENTVALIDATION");

    // ── Step 2: POST to download PGN ──────────────────────────────────────
    let fide_id_str = fide_id.to_string();
    let form: Vec<(&str, &str)> = vec![
        ("__VIEWSTATE", &viewstate),
        ("__VIEWSTATEGENERATOR", &viewstate_gen),
        ("__EVENTVALIDATION", &ev_validation),
        ("ctl00$P1$Txt_FideID", &fide_id_str),
        ("ctl00$P1$cb_DownLoadPGN", "Download as PGN-File"),
    ];

    let body = client
        .post(format!("{SEARCH_URL}?lan=1&SNode=S0"))
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("chess-results POST: {e}"))?
        .text()
        .await
        .map_err(|e| format!("chess-results POST body: {e}"))?;

    // A non-PGN response (HTML error page, empty body) means no games.
    let trimmed = body.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('<') {
        log::debug!(
            "chess-results: no PGN for FIDE {fide_id} (got {} bytes, non-PGN)",
            body.len()
        );
        return Ok(Vec::new());
    }

    // ── Step 3: parse PGN ─────────────────────────────────────────────────
    let games = parse_pgn(body.as_bytes(), fide_id, fide_name);
    log::info!(
        "chess-results: {} games fetched for FIDE {fide_id} ({})",
        games.len(),
        fide_name
    );
    Ok(games)
}

// ─── ViewState extraction ─────────────────────────────────────────────────────

/// Extract the `value` attribute of the `<input>` element whose `name`
/// attribute equals `field_name`.  Handles the standard ASP.NET layout where
/// the element is `<input type="hidden" name="..." id="..." value="..." />`.
fn extract_hidden_field(html: &str, field_name: &str) -> String {
    let needle = format!("name=\"{field_name}\"");
    let Some(name_pos) = html.find(&needle) else {
        log::warn!("chess-results: hidden field '{field_name}' not found in form");
        return String::new();
    };

    // Scan backwards to the opening `<` of this tag.
    let tag_start = html[..name_pos].rfind('<').unwrap_or(0);
    // Scan forwards to the closing `>`.
    let after = &html[tag_start..];
    let tag_end = tag_start + after.find('>').unwrap_or(after.len());
    let tag_slice = &html[tag_start..=tag_end];

    if let Some(v_pos) = tag_slice.find("value=\"") {
        let rest = &tag_slice[v_pos + 7..]; // skip `value="`
        if let Some(end) = rest.find('"') {
            return rest[..end].to_string();
        }
    }
    String::new()
}

// ─── PGN parsing ─────────────────────────────────────────────────────────────

/// Parse a PGN byte slice into `MastersGameWithId` records.
///
/// Strategy for colour detection:
/// 1. Parse all games, collecting every White and Black name.
/// 2. Find the name with the highest total frequency — that is the searched
///    player (chess-results returns only their games).
/// 3. Re-scan to assign `white_fide_id` / `black_fide_id`.
///
/// If `hint_name` (from FIDE DB) already appears in the name frequency table,
/// it is used directly without needing the majority vote.
fn parse_pgn(pgn: &[u8], fide_id: u32, hint_name: &str) -> Vec<MastersGameWithId> {
    // ── First pass: parse all games without FIDE IDs ─────────────────────
    let mut collector = GameCollector::default();
    let _ = Reader::new(pgn).visit_all_games(&mut collector);
    let mut games = collector.games;

    if games.is_empty() {
        return games;
    }

    // ── Find the player's name ────────────────────────────────────────────
    // Count occurrences of each normalised name across White + Black tags.
    let mut freq: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for g in &games {
        *freq
            .entry(normalise_name(&g.game.players.white.name))
            .or_default() += 1;
        *freq
            .entry(normalise_name(&g.game.players.black.name))
            .or_default() += 1;
    }

    // Check if the FIDE DB hint name is already in the table (exact or close match).
    let hint_norm = normalise_name(hint_name);
    let player_norm = if !hint_norm.is_empty() && freq.contains_key(&hint_norm) {
        hint_norm
    } else {
        // Auto-detect: pick the most-frequent name.
        freq.into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(name, _)| name)
            .unwrap_or_default()
    };

    log::debug!(
        "fide-indexer: chess-results player name detected as {player_norm:?} \
         (hint was {hint_name:?})"
    );

    // ── Assign FIDE IDs ───────────────────────────────────────────────────
    for g in &mut games {
        if normalise_name(&g.game.players.white.name) == player_norm {
            g.white_fide_id = fide_id;
        }
        if normalise_name(&g.game.players.black.name) == player_norm {
            g.black_fide_id = fide_id;
        }
    }

    games
}

// ─── Visitor implementation ───────────────────────────────────────────────────

/// State accumulated while visiting a single PGN game's header tags.
#[derive(Default)]
struct RawGame {
    event: Option<String>,
    site: Option<String>,
    date: Option<String>,
    round: Option<String>,
    white_name: String,
    black_name: String,
    white_rating: u16,
    black_rating: u16,
    /// `None`  = Result tag absent / invalid → skip game.
    /// `Some(None)` = draw; `Some(Some(c))` = `c` won.
    winner: Option<Option<Color>>,
    fen: Option<String>,
    sans: Vec<SanPlus>,
}

/// Collects parsed games (FIDE IDs are filled in after the full parse).
#[derive(Default)]
struct GameCollector {
    games: Vec<MastersGameWithId>,
}

impl Visitor for GameCollector {
    type Tags = RawGame;
    type Movetext = RawGame;
    type Output = ();

    fn begin_tags(&mut self) -> ControlFlow<(), RawGame> {
        ControlFlow::Continue(RawGame::default())
    }

    fn tag(&mut self, g: &mut RawGame, name: &[u8], value: RawTag<'_>) -> ControlFlow<()> {
        match name {
            b"Event" => {
                g.event = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"Site" => g.site = Some(value.decode_utf8().unwrap_or_default().into_owned()),
            b"Date" | b"UTCDate" => {
                g.date = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"Round" => {
                g.round = Some(value.decode_utf8().unwrap_or_default().into_owned())
            }
            b"White" => {
                g.white_name = value.decode_utf8().unwrap_or_default().into_owned()
            }
            b"Black" => {
                g.black_name = value.decode_utf8().unwrap_or_default().into_owned()
            }
            b"WhiteElo" => {
                g.white_rating = value
                    .decode_utf8()
                    .unwrap_or_default()
                    .trim()
                    .parse::<u16>()
                    .unwrap_or(0);
            }
            b"BlackElo" => {
                g.black_rating = value
                    .decode_utf8()
                    .unwrap_or_default()
                    .trim()
                    .parse::<u16>()
                    .unwrap_or(0);
            }
            b"Result" => {
                g.winner =
                    KnownOutcome::from_ascii(value.as_bytes()).ok().map(|o| o.winner());
            }
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

    fn begin_movetext(&mut self, g: RawGame) -> ControlFlow<(), RawGame> {
        // Skip games with a missing / unrecognised Result tag.
        if g.winner.is_none() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(g)
        }
    }

    fn san(&mut self, g: &mut RawGame, san: SanPlus) -> ControlFlow<()> {
        g.sans.push(san);
        ControlFlow::Continue(())
    }

    fn end_game(&mut self, g: RawGame) -> () {
        // Convert SAN → UCI, aborting the game on any illegal move.
        let mut pos: Chess = match g.fen.as_deref() {
            Some(fen_str) => match fen_str.parse::<Fen>() {
                Ok(f) => match f.into_position(CastlingMode::Standard) {
                    Ok(p) => p,
                    Err(_) => return,
                },
                Err(_) => return,
            },
            None => Chess::default(),
        };

        let mut uci_moves: Vec<UciMove> = Vec::with_capacity(g.sans.len());
        for san_plus in &g.sans {
            match san_plus.san.to_move(&pos) {
                Ok(m) => {
                    uci_moves.push(UciMove::from_move(m, CastlingMode::Standard));
                    pos.play_unchecked(m);
                }
                Err(_) => return, // illegal move — discard game
            }
        }

        // Require a parseable date (PGN standard says YYYY.MM.DD).
        let date = match g.date.as_deref().and_then(|d| d.parse::<LaxDate>().ok()) {
            Some(d) => d,
            None => return,
        };

        let event = g.event.as_deref().unwrap_or("");
        let date_str = g.date.as_deref().unwrap_or("????.??.??");
        let round = g.round.as_deref().unwrap_or("?");
        let id = make_game_id(event, &g.white_name, &g.black_name, date_str, round);

        // FIDE IDs are filled in after the full parse (second pass in parse_pgn).
        let winner = g.winner.unwrap(); // guaranteed Some(_) by begin_movetext

        self.games.push(MastersGameWithId {
            id,
            game: MastersGame {
                event: g.event.unwrap_or_default(),
                site: g.site.unwrap_or_default(),
                date,
                round: g.round.unwrap_or_default(),
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
                winner,
                moves: uci_moves,
            },
            white_fide_id: 0,
            black_fide_id: 0,
        });
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Normalise a player name so minor case and whitespace differences don't
/// break the White/Black side detection.
fn normalise_name(name: &str) -> String {
    name.to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Compute the same stable 8-char base-62 game ID used by `import-caissify`.
/// Key: SHA-1(event ‖ NUL ‖ white ‖ NUL ‖ black ‖ NUL ‖ date ‖ NUL ‖ round).
fn make_game_id(event: &str, white: &str, black: &str, date: &str, round: &str) -> GameId {
    let mut h = Sha1::new();
    h.update(event.as_bytes());
    h.update(b"\x00");
    h.update(white.as_bytes());
    h.update(b"\x00");
    h.update(black.as_bytes());
    h.update(b"\x00");
    h.update(date.as_bytes());
    h.update(b"\x00");
    h.update(round.as_bytes());
    let hash = h.finalize();

    // First 6 bytes as LE u64, reduced mod 62^8.
    let mut n: u64 = 0;
    for &b in hash[..6].iter().rev() {
        n = (n << 8) | u64::from(b);
    }
    n %= 62u64.pow(8);

    // Encode as the same base-62 alphabet used by `GameId::Display`.
    let mut s = String::with_capacity(8);
    for _ in 0..8 {
        let rem = n % 62;
        s.write_char(char::from(if rem >= 36 {
            (rem - 36) as u8 + b'a'
        } else if rem >= 10 {
            (rem - 10) as u8 + b'A'
        } else {
            rem as u8 + b'0'
        }))
        .unwrap();
        n /= 62;
    }
    s.parse().expect("valid base-62 game ID")
}
