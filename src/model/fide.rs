use std::collections::HashMap;

use bytes::{Buf, BufMut};
use serde::Serialize;

use crate::model::{Month, read_uint, write_uint};

// ─── FidePlayer ───────────────────────────────────────────────────────────────

/// Stored in `fide_player` CF.
/// Key:   [4-byte FIDE ID LE]
/// Value: binary-encoded FidePlayer
#[derive(Debug, Clone, Serialize)]
pub struct FidePlayer {
    pub fide_id: u32,
    pub name: String,
    pub country: String,
    /// "M" or "F"
    pub sex: String,
    /// GM, IM, FM, CM, NM, etc.
    pub title: String,
    /// Women's title (WGM, WIM, WFM, WCM)
    pub w_title: String,
    /// FIDE Online Arena title
    pub o_title: String,
    /// FOA (FIDE Online Arena) title
    pub foa_title: String,
    /// Year of birth (0 = unknown)
    pub birth_year: u16,
    pub flag: FideFlag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FideFlag {
    Active,
    Inactive,
    /// Not in the current list (historical only)
    Unknown,
}

impl FidePlayer {
    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u32_le(self.fide_id);
        write_uint(buf, self.name.len() as u64);
        buf.put_slice(self.name.as_bytes());
        write_uint(buf, self.country.len() as u64);
        buf.put_slice(self.country.as_bytes());
        write_uint(buf, self.sex.len() as u64);
        buf.put_slice(self.sex.as_bytes());
        write_uint(buf, self.title.len() as u64);
        buf.put_slice(self.title.as_bytes());
        buf.put_u16_le(self.birth_year);
        buf.put_u8(match self.flag {
            FideFlag::Active => 0,
            FideFlag::Inactive => 1,
            FideFlag::Unknown => 2,
        });
        // v2 fields — appended after flag for backward-compatible reads
        write_uint(buf, self.w_title.len() as u64);
        buf.put_slice(self.w_title.as_bytes());
        write_uint(buf, self.o_title.len() as u64);
        buf.put_slice(self.o_title.as_bytes());
        write_uint(buf, self.foa_title.len() as u64);
        buf.put_slice(self.foa_title.as_bytes());
    }

    pub fn read<B: Buf>(buf: &mut B) -> FidePlayer {
        let fide_id = buf.get_u32_le();

        let name_len = read_uint(buf) as usize;
        let name = read_string(buf, name_len);

        let country_len = read_uint(buf) as usize;
        let country = read_string(buf, country_len);

        let sex_len = read_uint(buf) as usize;
        let sex = read_string(buf, sex_len);

        let title_len = read_uint(buf) as usize;
        let title = read_string(buf, title_len);

        let birth_year = buf.get_u16_le();
        let flag = match buf.get_u8() {
            0 => FideFlag::Active,
            1 => FideFlag::Inactive,
            _ => FideFlag::Unknown,
        };

        // v2 fields — old records may not have these; default to empty string
        let (w_title, o_title, foa_title) = if buf.remaining() > 0 {
            let w_len = read_uint(buf) as usize;
            let w = read_string(buf, w_len);
            let o_len = read_uint(buf) as usize;
            let o = read_string(buf, o_len);
            let foa_len = read_uint(buf) as usize;
            let foa = read_string(buf, foa_len);
            (w, o, foa)
        } else {
            (String::new(), String::new(), String::new())
        };

        FidePlayer {
            fide_id,
            name,
            country,
            sex,
            title,
            w_title,
            o_title,
            foa_title,
            birth_year,
            flag,
        }
    }

    pub fn fide_id_key(fide_id: u32) -> [u8; 4] {
        fide_id.to_le_bytes()
    }
}

/// Compute a stable 64-bit FNV-1a hash of the normalised form of `name`.
///
/// Used as the 8-byte prefix for `caissify_game_by_player` keys so that
/// any formatting variant of the same player name (comma-separated FIDE,
/// space-separated Western, all-caps, etc.) maps to the same bucket.
/// The hash is deterministic and version-independent.
pub fn player_name_hash(name: &str) -> u64 {
    let normalized = normalize_name(name);
    fnv64(normalized.as_bytes())
}

fn fnv64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut h = FNV_OFFSET;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

fn read_string<B: Buf>(buf: &mut B, len: usize) -> String {
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8_lossy(&bytes).into_owned()
}

// ─── FideRatingSnapshot ───────────────────────────────────────────────────────

/// One monthly rating snapshot per player per rating type.
///
/// Stored in `fide_rating_history` CF.
/// Key:   [4-byte FIDE ID LE][2-byte Month LE]
/// Value: binary-encoded FideRatingSnapshot (10 bytes fixed)
#[derive(Debug, Clone, Copy, Serialize)]
pub struct FideRatingSnapshot {
    /// 0 = not rated
    pub standard: u16,
    pub rapid: u16,
    pub blitz: u16,
    /// Number of games used for standard rating (capped at u16::MAX)
    pub games_standard: u16,
    /// Number of games used for rapid rating
    pub games_rapid: u16,
    /// Number of games used for blitz rating
    pub games_blitz: u16,
    /// K-factor for standard rating (10, 20, or 40)
    pub k_standard: u8,
    /// K-factor for rapid rating
    pub k_rapid: u8,
    /// K-factor for blitz rating
    pub k_blitz: u8,
}

impl FideRatingSnapshot {
    pub const SIZE: usize = 2 + 2 + 2 + 2 + 2 + 2 + 1 + 1 + 1; // 15

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16_le(self.standard);
        buf.put_u16_le(self.rapid);
        buf.put_u16_le(self.blitz);
        buf.put_u16_le(self.games_standard);
        buf.put_u16_le(self.games_rapid);
        buf.put_u16_le(self.games_blitz);
        buf.put_u8(self.k_standard);
        buf.put_u8(self.k_rapid);
        buf.put_u8(self.k_blitz);
    }

    pub fn read<B: Buf>(buf: &mut B) -> FideRatingSnapshot {
        // Tolerant of the old 9-byte format that predates games_rapid/blitz and k_rapid/blitz.
        let standard       = if buf.remaining() >= 2 { buf.get_u16_le() } else { 0 };
        let rapid          = if buf.remaining() >= 2 { buf.get_u16_le() } else { 0 };
        let blitz          = if buf.remaining() >= 2 { buf.get_u16_le() } else { 0 };
        let games_standard = if buf.remaining() >= 2 { buf.get_u16_le() } else { 0 };
        let games_rapid    = if buf.remaining() >= 2 { buf.get_u16_le() } else { 0 };
        let games_blitz    = if buf.remaining() >= 2 { buf.get_u16_le() } else { 0 };
        let k_standard     = if buf.remaining() >= 1 { buf.get_u8()     } else { 0 };
        let k_rapid        = if buf.remaining() >= 1 { buf.get_u8()     } else { 0 };
        let k_blitz        = if buf.remaining() >= 1 { buf.get_u8()     } else { 0 };
        FideRatingSnapshot { standard, rapid, blitz, games_standard, games_rapid, games_blitz, k_standard, k_rapid, k_blitz }
    }
}

// ─── FideRatingKey ────────────────────────────────────────────────────────────

/// Composite key for `fide_rating_history`.
#[derive(Debug, Clone, Copy)]
pub struct FideRatingKey {
    pub fide_id: u32,
    pub month: Month,
}

impl FideRatingKey {
    pub const SIZE: usize = 4 + 2; // 6

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[..4].copy_from_slice(&self.fide_id.to_le_bytes());
        buf[4..].copy_from_slice(&u16::from(self.month).to_le_bytes());
        buf
    }

    /// Exclusive upper bound for all months of a given FIDE ID.
    pub fn upper_bound(fide_id: u32) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[..4].copy_from_slice(&fide_id.saturating_add(1).to_le_bytes());
        buf
    }
}

// ─── FideNameIndex ────────────────────────────────────────────────────────────

/// In-memory index from player name → FIDE ID, loaded at startup from the
/// `fide_player` column family.
///
/// Four independent sub-indexes are maintained, tried in order during
/// `lookup` until a match is found:
///
/// 1. **Sorted-token** (`by_sorted`): split on non-alpha, drop tokens ≤ 1 char,
///    lowercase, sort alphabetically, join.  
///    → `"Carlsen, Magnus"` and `"Magnus Carlsen"` both → `"carlsen magnus"`.
///
/// 2. **Exact-lowercase** (`by_exact_lower`): just `name.to_lowercase()`.  
///    → `"Carlsen, Magnus"` → `"carlsen, magnus"`.
///
/// 3. **Last-name-only** (`by_last_name`): everything before the first comma,
///    lowercased; kept only when it is **unambiguous** (exactly one player has
///    that surname cluster).  
///    → `"Carlsen, Magnus"` → `"carlsen"`.  
///    Catches abbreviated first names like `"Carlsen, M."`.
///
/// 4. **Abbreviated compound surname** (`by_abbreviated`): for players with a
///    multi-word surname, stores a key built from just the *first* surname
///    component plus the full given name, sorted and normalised.  
///    → `"Vargas Arteaga, Alexis"` also stores key `"alexis vargas"`.  
///    This lets PGN names like `"Vargas, Alexis"` (which drop the second
///    surname component) resolve to the correct FIDE ID.
///
/// All sub-indexes mark collisions as `None` — when two players share the
/// same key the slot is a tombstone and `lookup` skips to the next tier.
pub struct FideNameIndex {
    /// Sorted-token normalised key → FIDE ID
    by_sorted: HashMap<String, Option<u32>>,
    /// Raw lowercase key → FIDE ID
    by_exact_lower: HashMap<String, Option<u32>>,
    /// Last-name-only key → FIDE ID (only unambiguous entries kept)
    by_last_name: HashMap<String, Option<u32>>,
    /// Abbreviated compound-surname key → FIDE ID
    by_abbreviated: HashMap<String, Option<u32>>,
}

impl Default for FideNameIndex {
    fn default() -> Self {
        FideNameIndex {
            by_sorted: HashMap::new(),
            by_exact_lower: HashMap::new(),
            by_last_name: HashMap::new(),
            by_abbreviated: HashMap::new(),
        }
    }
}

/// Extract the "last name" component from a FIDE-format name like
/// `"Carlsen, Magnus"`.  Returns the part before the first comma,
/// trimmed and lowercased.  Falls back to the whole string lowercased when
/// there is no comma.
fn last_name_key(name: &str) -> String {
    name.split(',')
        .next()
        .unwrap_or(name)
        .trim()
        .to_lowercase()
}

/// Build a "first-surname-component + first-name" abbreviated key for
/// players with compound surnames.
///
/// `"Vargas Arteaga, Alexis"` → surname part = `"Vargas Arteaga"`,
/// first component = `"Vargas"`, first name = `"Alexis"` →
/// sorted key `"alexis vargas"`.
///
/// Returns `None` when the name has no comma (can't split surname/given),
/// when the surname has only one component (no abbreviation possible), or
/// when the result is identical to the full sorted-token key (no benefit).
fn abbreviated_compound_key(name: &str) -> Option<String> {
    let mut parts = name.splitn(2, ',');
    let surname_full = parts.next()?.trim();
    let given = parts.next()?.trim();

    // Only meaningful if surname has at least two space-separated words.
    let first_surname_component = surname_full.split_whitespace().next()?;
    if surname_full.split_whitespace().count() < 2 {
        return None; // single-word surname — tier 1 already covers this
    }

    // Build a sorted-token key from just the first surname component + given name.
    let abbreviated = format!("{} {}", first_surname_component, given);
    let key = normalize_name(&abbreviated);
    if key.is_empty() {
        return None;
    }
    Some(key)
}

impl FideNameIndex {
    pub fn new() -> Self {
        FideNameIndex::default()
    }

    /// Insert one FIDE player into all four sub-indexes.
    pub fn insert(&mut self, player: &FidePlayer) {
        let id = player.fide_id;

        // 1. Sorted-token key
        let sorted_key = normalize_name(&player.name);
        if !sorted_key.is_empty() {
            let slot = self.by_sorted.entry(sorted_key).or_insert(Some(id));
            if *slot != Some(id) {
                *slot = None;
            }
        }

        // 2. Exact-lowercase key
        let exact_key = player.name.to_lowercase();
        if !exact_key.is_empty() {
            let slot = self.by_exact_lower.entry(exact_key).or_insert(Some(id));
            if *slot != Some(id) {
                *slot = None;
            }
        }

        // 3. Last-name-only key
        let ln_key = last_name_key(&player.name);
        if !ln_key.is_empty() {
            let slot = self.by_last_name.entry(ln_key).or_insert(Some(id));
            if *slot != Some(id) {
                *slot = None; // collision — two players share this last name
            }
        }

        // 4. Abbreviated compound-surname key (e.g. "Vargas Arteaga, Alexis" → "alexis vargas")
        if let Some(abbrev_key) = abbreviated_compound_key(&player.name) {
            let slot = self.by_abbreviated.entry(abbrev_key).or_insert(Some(id));
            if *slot != Some(id) {
                *slot = None;
            }
        }
    }

    /// Look up a FIDE ID by raw player name.
    ///
    /// Tries the four tiers in order; returns the first unambiguous match.
    /// Returns `None` when no tier produces a match.
    pub fn lookup(&self, name: &str) -> Option<u32> {
        // Tier 1: sorted-token normalisation (handles order/case differences)
        if let Some(&Some(id)) = self.by_sorted.get(&normalize_name(name)) {
            return Some(id);
        }
        // Tier 2: exact lowercase (same format, case-insensitive)
        if let Some(&Some(id)) = self.by_exact_lower.get(&name.to_lowercase()) {
            return Some(id);
        }
        // Tier 3: last-name-only fallback (handles abbreviated first names)
        if let Some(&Some(id)) = self.by_last_name.get(&last_name_key(name)) {
            return Some(id);
        }
        // Tier 4: abbreviated compound surname ("Vargas, Alexis" → "alexis vargas")
        // The incoming PGN name is treated as if it might be an abbreviated compound
        // surname, so we look it up in the by_abbreviated index using its sorted key.
        if let Some(&Some(id)) = self.by_abbreviated.get(&normalize_name(name)) {
            return Some(id);
        }
        None
    }

    /// Same as `lookup` but also returns a label for the matching tier.
    ///
    /// Returns `("sorted" | "exact_lower" | "last_name" | "abbreviated", fide_id)`
    /// or `("none", 0)` when unresolved.
    pub fn lookup_with_tier(&self, name: &str) -> (&'static str, u32) {
        if let Some(&Some(id)) = self.by_sorted.get(&normalize_name(name)) {
            return ("sorted", id);
        }
        if let Some(&Some(id)) = self.by_exact_lower.get(&name.to_lowercase()) {
            return ("exact_lower", id);
        }
        if let Some(&Some(id)) = self.by_last_name.get(&last_name_key(name)) {
            return ("last_name", id);
        }
        if let Some(&Some(id)) = self.by_abbreviated.get(&normalize_name(name)) {
            return ("abbreviated", id);
        }
        ("none", 0)
    }

    /// Number of unique (non-collision) entries in the primary (sorted) index.
    pub fn len(&self) -> usize {
        self.by_sorted.values().filter(|v| v.is_some()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return up to `limit` FIDE IDs whose sorted-token normalised name starts
    /// with `normalize_name(query)`.  O(N) scan — acceptable for the search
    /// endpoint (< 30 ms for 1 M entries on any modern CPU).
    pub fn search_prefix(&self, query: &str, limit: usize) -> Vec<u32> {
        let prefix = normalize_name(query);
        if prefix.is_empty() {
            return vec![];
        }
        self.by_sorted
            .iter()
            .filter(|(k, v)| v.is_some() && k.starts_with(&prefix))
            .take(limit)
            .filter_map(|(_, v)| *v)
            .collect()
    }
}

/// Normalise a player name for index lookup.
///
/// Output: lowercase alphabetic tokens (len > 1) joined by spaces, sorted.
pub fn normalize_name(name: &str) -> String {
    let mut tokens: Vec<&str> = name
        .split(|c: char| !c.is_alphabetic())
        .filter(|t| t.len() > 1)
        .collect();
    if tokens.is_empty() {
        return String::new();
    }
    // Sort so "Carlsen Magnus" and "Magnus Carlsen" produce the same key.
    tokens.sort_unstable_by_key(|t| t.to_lowercase().to_string());
    tokens
        .iter()
        .fold(String::with_capacity(name.len()), |mut s, t| {
            if !s.is_empty() {
                s.push(' ');
            }
            s.extend(t.chars().map(|c| c.to_ascii_lowercase()));
            s
        })
}
