/// import-fide — Downloads and imports FIDE monthly rating lists.
///
/// Downloads the complete FIDE player list (players_list_xml.zip / FOA master
/// list) which contains ALL registered FIDE players, including those with only
/// rapid/blitz ratings and inactive players. Parses the XML and posts batches
/// to the explorer's PUT /import/fide endpoint.
///
/// Usage:
///   import-fide --endpoint http://localhost:9002 --month 2026-03
///   import-fide --endpoint http://localhost:9002  # defaults to current month
use std::{
    io::{BufRead, Read},
    str::FromStr,
    time::Duration,
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use quick_xml::{Reader, events::Event};
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::json;
use time::OffsetDateTime;
use zip::ZipArchive;

#[derive(Parser)]
#[command(name = "import-fide")]
struct Opt {
    /// Explorer HTTP endpoint.
    #[arg(long, default_value = "http://localhost:9002")]
    endpoint: String,

    /// Month to import (YYYY-MM). Defaults to current UTC month.
    #[arg(long)]
    month: Option<String>,

    /// Number of players per POST batch.
    #[arg(long, default_value = "2000")]
    batch_size: usize,

    /// Timeout for each HTTP request (seconds).
    #[arg(long, default_value = "120")]
    timeout: u64,
}

#[derive(Debug, Default, Serialize)]
struct PlayerRecord {
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
    #[serde(default)]
    games_rapid: u16,
    #[serde(default)]
    games_blitz: u16,
    k_standard: u8,
    #[serde(default)]
    k_rapid: u8,
    #[serde(default)]
    k_blitz: u8,
}

fn current_month() -> String {
    let now = OffsetDateTime::now_utc();
    format!("{}-{:02}", now.year(), u8::from(now.month()))
}

fn download_zipped_xml(client: &Client, url: &str) -> Vec<u8> {
    eprintln!("Downloading {url} …");
    let response = client
        .get(url)
        .send()
        .expect("download FIDE zip")
        .error_for_status()
        .expect("FIDE zip HTTP error");

    let bytes = response.bytes().expect("read FIDE zip bytes");
    eprintln!("Downloaded {} MB", bytes.len() / 1_048_576);

    // Extract first file from ZIP
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor).expect("open FIDE zip");
    let mut xml_file = archive.by_index(0).expect("zip entry 0");
    let mut xml = Vec::new();
    xml_file.read_to_end(&mut xml).expect("read xml from zip");
    xml
}

/// Parse FIDE XML using quick-xml event reader.
/// Returns a map from fide_id → PlayerRecord.
fn parse_xml(xml: &[u8]) -> Vec<PlayerRecord> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut players: Vec<PlayerRecord> = Vec::with_capacity(1_000_000);
    let mut current: Option<PlayerRecord> = None;
    let mut current_tag = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_owned();
                if tag == "player" {
                    current = Some(PlayerRecord::default());
                }
                current_tag = tag;
            }
            Ok(Event::Text(e)) => {
                if let Some(ref mut player) = current {
                    let text = e.unescape().unwrap_or_default();
                    let text = text.trim();
                    if text.is_empty() {
                        buf.clear();
                        continue;
                    }
                    match current_tag.as_str() {
                        "fideid" => player.fide_id = text.parse().unwrap_or(0),
                        "name" => player.name = text.to_owned(),
                        "country" => player.country = text.to_owned(),
                        "sex" => player.sex = text.to_owned(),
                        "title" => player.title = text.to_owned(),
                        "w_title" => player.w_title = text.to_owned(),
                        "o_title" => player.o_title = text.to_owned(),
                        "foa_title" => player.foa_title = text.to_owned(),
                        "birthday" => player.birth_year = text.parse().unwrap_or(0),
                        "flag" => {
                            // FIDE: "i" or "wi" = inactive, "w" = active woman,
                            // anything else (or absent) = active.
                            player.flag = if text.to_ascii_lowercase().contains('i') {
                                "inactive".to_owned()
                            } else {
                                "active".to_owned()
                            };
                        }
                        "rating" => player.standard = text.parse().unwrap_or(0),
                        "games" => player.games_standard = text.parse().unwrap_or(0),
                        "k" => { player.k_standard = text.parse().unwrap_or(0); player.k_rapid = player.k_standard; player.k_blitz = player.k_standard; }
                        "rapid_rating" => player.rapid = text.parse().unwrap_or(0),
                        "blitz_rating" => player.blitz = text.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let tag = std::str::from_utf8(e.name().as_ref()).unwrap_or("");
                if tag == "player" {
                    if let Some(player) = current.take() {
                        if player.fide_id > 0 {
                            players.push(player);
                        }
                    }
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("XML parse error: {e}");
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    players
}

fn post_batch(client: &Client, endpoint: &str, month: &str, batch: &[PlayerRecord]) {
    let url = format!("{endpoint}/import/fide");
    let body = json!({
        "month": month,
        "players": batch,
    });

    let resp = client
        .put(&url)
        .json(&body)
        .send()
        .expect("POST fide batch");

    if !resp.status().is_success() {
        eprintln!(
            "Import batch failed: {} — {}",
            resp.status(),
            resp.text().unwrap_or_default()
        );
    }
}

fn main() {
    let opt = Opt::parse();
    let month = opt.month.unwrap_or_else(current_month);

    eprintln!("Importing FIDE ratings for month: {month}");

    let client = Client::builder()
        .timeout(Duration::from_secs(opt.timeout))
        .user_agent("caissify-explorer/import-fide")
        .build()
        .expect("http client");

    // players_list_xml.zip is the FOA master list: ALL ~1.8M registered FIDE
    // players including profiles AND rating data (standard/rapid/blitz) for
    // rated players. Single download — no merge needed.
    let xml = download_zipped_xml(
        &client,
        "https://ratings.fide.com/download/players_list_xml.zip",
    );

    eprintln!("Parsing XML ({} MB) …", xml.len() / 1_048_576);
    let players = parse_xml(&xml);
    let total = players.len();
    eprintln!("Parsed {total} player records");

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{elapsed_precise} [{bar:40}] {pos}/{len} players ({per_sec}, ETA {eta})",
            )
            .unwrap()
            .progress_chars("=> "),
    );

    let mut imported = 0usize;
    for chunk in players.chunks(opt.batch_size) {
        post_batch(&client, &opt.endpoint, &month, chunk);
        imported += chunk.len();
        pb.set_position(imported as u64);
    }

    pb.finish_with_message("done");
    eprintln!("Imported {imported}/{total} players for {month}");
}
