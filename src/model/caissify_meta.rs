use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use shakmaty::Color;

use crate::model::GameId;

/// Compact per-game metadata stored in the `caissify_game_meta` column family.
///
/// Key:   GameId (6 bytes, same as caissify_game)
/// Value: 17 bytes (v3) — year(2) + white_rating(2) + black_rating(2) + result(1)
///                        + white_fide_id(4) + black_fide_id(4) + move_count(1) + month(1)
///
/// Version history:
///   v1 — 15 bytes: no move_count  → decoded with move_count = 0
///   v2 — 16 bytes: adds move_count → decoded with month = 0, is_v3 = false
///   v3 — 17 bytes: adds month     → current format, is_v3 = true
#[derive(Debug, Clone, Serialize)]
pub struct CaissifyGameMeta {
    pub year: u16,
    pub white_rating: u16,
    pub black_rating: u16,
    pub result: GameResult,
    /// 0 = unlinked
    pub white_fide_id: u32,
    /// 0 = unlinked
    pub black_fide_id: u32,
    /// Total half-moves played. 0 means unknown (decoded from a v1 record).
    /// Saturating at 255 (i.e. 255 means "255 or more").
    pub move_count: u8,
    /// Calendar month (1–12). 0 means unknown (PGN lacked a month, or decoded
    /// from a v1/v2 record before this field was added).
    pub month: u8,
    /// `true` when decoded from a v3 (17-byte) record.  Not serialised.
    /// Used by `reindex_meta` to detect v1/v2 records that need upgrading.
    #[serde(skip)]
    pub is_v3: bool,
}

/// Outcome from White's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GameResult {
    White,
    Draw,
    Black,
}

impl GameResult {
    pub fn from_winner(winner: Option<Color>) -> GameResult {
        match winner {
            Some(Color::White) => GameResult::White,
            None => GameResult::Draw,
            Some(Color::Black) => GameResult::Black,
        }
    }
}

impl CaissifyGameMeta {
    /// Byte size of a v1 (legacy) record.
    pub const SIZE_V1: usize = 2 + 2 + 2 + 1 + 4 + 4; // 15
    /// Byte size of a v2 record (adds move_count).
    pub const SIZE_V2: usize = Self::SIZE_V1 + 1; // 16
    /// Byte size of a v3 record (adds month). Current format.
    pub const SIZE: usize = Self::SIZE_V2 + 1; // 17

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16_le(self.year);
        buf.put_u16_le(self.white_rating);
        buf.put_u16_le(self.black_rating);
        buf.put_u8(match self.result {
            GameResult::White => 0,
            GameResult::Draw => 1,
            GameResult::Black => 2,
        });
        buf.put_u32_le(self.white_fide_id);
        buf.put_u32_le(self.black_fide_id);
        buf.put_u8(self.move_count); // v2
        buf.put_u8(self.month);      // v3
    }

    /// Decode a v1 (15-byte) or v2 (16-byte) record. v1 records decode with
    /// `move_count = 0` (unknown).
    pub fn read<B: Buf>(buf: &mut B) -> CaissifyGameMeta {
        let year = buf.get_u16_le();
        let white_rating = buf.get_u16_le();
        let black_rating = buf.get_u16_le();
        let result = match buf.get_u8() {
            0 => GameResult::White,
            1 => GameResult::Draw,
            2 => GameResult::Black,
            _ => panic!("invalid game result byte"),
        };
        let white_fide_id = buf.get_u32_le();
        let black_fide_id = buf.get_u32_le();
        // v2 adds move_count; v1 records have nothing left — treat as unknown.
        let move_count = if buf.remaining() >= 1 { buf.get_u8() } else { 0 };
        // v3 adds month (1–12; 0 = unknown).
        let (month, is_v3) = if buf.remaining() >= 1 {
            (buf.get_u8(), true)
        } else {
            (0, false)
        };
        CaissifyGameMeta {
            year,
            white_rating,
            black_rating,
            result,
            white_fide_id,
            black_fide_id,
            move_count,
            month,
            is_v3,
        }
    }
}

/// Key for `caissify_game_by_date` column family.
///
/// Layout (9 bytes): [2-byte Year LE][1-byte Month][6-byte GameId LE]
///
/// `month` is 1–12; 0 means unknown and sorts before any named month.
/// Prefix extractor: 2 bytes (year). Allows efficient year-range scans and
/// cursor-based pagination via `seek(year, month, game_id)`.
///
/// **Migration note**: records written before Phase 9 are 8 bytes (no month).
/// `read_any` handles both lengths gracefully; `iter_by_date` uses `read_any`
/// so old and new keys coexist until `/import/caissify/reindex-meta` upgrades
/// them all.
#[derive(Debug, Clone, Copy)]
pub struct CaissifyByDateKey {
    pub year: u16,
    /// Calendar month (1–12). 0 = unknown (legacy or month absent in PGN).
    pub month: u8,
    pub id: GameId,
}

impl CaissifyByDateKey {
    /// Byte size of current (v2) 9-byte keys.
    pub const SIZE: usize = 2 + 1 + GameId::SIZE; // 9
    /// Byte size of legacy (v1) 8-byte keys (no month byte).
    pub const SIZE_V1: usize = 2 + GameId::SIZE; // 8

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16_le(self.year);
        buf.put_u8(self.month);
        self.id.write(buf);
    }

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        self.write(&mut &mut buf[..]);
        buf
    }

    pub fn read<B: Buf>(buf: &mut B) -> CaissifyByDateKey {
        let year = buf.get_u16_le();
        let month = buf.get_u8();
        let id = GameId::read(buf);
        CaissifyByDateKey { year, month, id }
    }

    /// Read either a v1 (8-byte) or v2 (9-byte) key from a raw byte slice.
    /// v1 keys decode with `month = 0` (unknown).
    pub fn read_any(buf: &[u8]) -> Option<CaissifyByDateKey> {
        if buf.len() == Self::SIZE {
            Some(Self::read(&mut &buf[..]))
        } else if buf.len() == Self::SIZE_V1 {
            let mut b = &buf[..];
            let year = b.get_u16_le();
            let id = GameId::read(&mut b);
            Some(CaissifyByDateKey { year, month: 0, id })
        } else {
            None
        }
    }

    /// Exclusive upper-bound key for all games in years ≤ `year`.
    pub fn upper_bound(year: u16) -> [u8; Self::SIZE] {
        CaissifyByDateKey {
            year: year.saturating_add(1),
            month: 0,
            id: GameId::MIN,
        }
        .into_bytes()
    }
}

// ─── CaissifyByFideKey ────────────────────────────────────────────────────────

/// Key for the `caissify_game_by_fide` column family.
///
/// Layout (12 bytes): [4-byte FIDE ID LE][2-byte Year LE][6-byte GameId LE]
///
/// Prefix extractor: 4 bytes (FIDE ID). Enables bloom-filter-accelerated
/// per-player scans, year-range filtering within a player (seek to
/// `[fide_id][since_year][GameId::MIN]`), and cursor-based pagination.
///
/// Value: 1 byte — `0` = player was White, `1` = player was Black.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaissifyByFideKey {
    pub fide_id: u32,
    pub year: u16,
    pub id: GameId,
}

impl CaissifyByFideKey {
    pub const SIZE: usize = 4 + 2 + GameId::SIZE; // 12

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u32_le(self.fide_id);
        buf.put_u16_le(self.year);
        self.id.write(buf);
    }

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        self.write(&mut &mut buf[..]);
        buf
    }

    pub fn read<B: Buf>(buf: &mut B) -> CaissifyByFideKey {
        let fide_id = buf.get_u32_le();
        let year = buf.get_u16_le();
        let id = GameId::read(buf);
        CaissifyByFideKey { fide_id, year, id }
    }

    /// First key for this FIDE ID at or after `since_year`.
    pub fn lower_bound(fide_id: u32, since_year: u16) -> [u8; Self::SIZE] {
        CaissifyByFideKey {
            fide_id,
            year: since_year,
            id: GameId::MIN,
        }
        .into_bytes()
    }

    /// Exclusive upper-bound sentinel for all keys with FIDE ID ≤ `until_year`.
    /// Used for `seek_for_prev` to land on the actual last entry in range.
    pub fn upper_bound_sentinel(fide_id: u32, until_year: u16) -> [u8; Self::SIZE] {
        CaissifyByFideKey {
            fide_id,
            year: until_year.saturating_add(1),
            id: GameId::MIN,
        }
        .into_bytes()
    }
}

// ─── CaissifyByPlayerKey ─────────────────────────────────────────────────────

/// Key for the `caissify_game_by_player` column family.
///
/// Layout (16 bytes): [8-byte player-name hash LE][2-byte Year LE][6-byte GameId LE]
///
/// The player-name hash is `player_name_hash(name)` from `model::fide` — a
/// FNV-1a hash of the sorted-token normalised name.  Any formatting variant of
/// the same name (FIDE comma form, Western space form, all-caps, etc.) produces
/// the same hash, giving 100 % coverage independent of FIDE data.
///
/// Prefix extractor: 8 bytes (player hash).  Enables bloom-filter-accelerated
/// per-player prefix scans, year-range filtering, and cursor pagination.
///
/// Value: 1 byte — `0` = player was White, `1` = player was Black.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaissifyByPlayerKey {
    /// FNV-1a hash of normalised player name.
    pub hash: u64,
    pub year: u16,
    pub id: GameId,
}

impl CaissifyByPlayerKey {
    pub const SIZE: usize = 8 + 2 + GameId::SIZE; // 16

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u64_le(self.hash);
        buf.put_u16_le(self.year);
        self.id.write(buf);
    }

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        self.write(&mut &mut buf[..]);
        buf
    }

    pub fn read<B: Buf>(buf: &mut B) -> CaissifyByPlayerKey {
        let hash = buf.get_u64_le();
        let year = buf.get_u16_le();
        let id = GameId::read(buf);
        CaissifyByPlayerKey { hash, year, id }
    }

    /// First key for this player hash at or after `since_year`.
    pub fn lower_bound(hash: u64, since_year: u16) -> [u8; Self::SIZE] {
        CaissifyByPlayerKey { hash, year: since_year, id: GameId::MIN }.into_bytes()
    }

    /// Exclusive upper-bound sentinel for all keys with this hash and
    /// year ≤ `until_year`.
    pub fn upper_bound_sentinel(hash: u64, until_year: u16) -> [u8; Self::SIZE] {
        CaissifyByPlayerKey {
            hash,
            year: until_year.saturating_add(1),
            id: GameId::MIN,
        }
        .into_bytes()
    }
}

// ─── CaissifyByPositionKey ────────────────────────────────────────────────────

/// Key for the `caissify_game_by_position` column family.
///
/// Layout (20 bytes): [12-byte KeyPrefix][2-byte Year LE][6-byte GameId LE]
///
/// Prefix extractor: 12 bytes (same KeyPrefix as the `caissify` opening-stats
/// CF). This enables bloom-filter-accelerated per-position seeks and efficient
/// year-range scans, plus cursor-based pagination over all games through a
/// given position.
///
/// Value: empty (GameId is embedded in the key; year is kept for sorting).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaissifyByPositionKey {
    /// First 12 bytes of a `KeyPrefix` (from `KeyPrefix::key_bytes()`).
    pub prefix: [u8; 12], // 12 = KeyPrefix::SIZE
    pub year: u16,
    pub id: GameId,
}

impl CaissifyByPositionKey {
    pub const SIZE: usize = 12 + 2 + GameId::SIZE; // 20

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_slice(&self.prefix);
        buf.put_u16_le(self.year);
        self.id.write(buf);
    }

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        self.write(&mut &mut buf[..]);
        buf
    }

    pub fn read<B: bytes::Buf>(buf: &mut B) -> CaissifyByPositionKey {
        let mut prefix = [0u8; 12];
        buf.copy_to_slice(&mut prefix);
        let year = buf.get_u16_le();
        let id = GameId::read(buf);
        CaissifyByPositionKey { prefix, year, id }
    }

    /// First key for this position at or after `since_year`.
    pub fn lower_bound(prefix: [u8; 12], since_year: u16) -> [u8; Self::SIZE] {
        CaissifyByPositionKey {
            prefix,
            year: since_year,
            id: GameId::MIN,
        }
        .into_bytes()
    }

    /// Exclusive upper-bound sentinel for all keys with this position and
    /// year ≤ `until_year`. Used with `seek_for_prev`.
    pub fn upper_bound_sentinel(prefix: [u8; 12], until_year: u16) -> [u8; Self::SIZE] {
        CaissifyByPositionKey {
            prefix,
            year: until_year.saturating_add(1),
            id: GameId::MIN,
        }
        .into_bytes()
    }
}

// ─── CaissifyByRatingKey ──────────────────────────────────────────────────────

/// Key for the `caissify_game_by_rating` column family.
///
/// Layout (10 bytes): [2-byte max_rating big-endian][2-byte Year LE][6-byte GameId LE]
///
/// `max_rating = max(white_rating, black_rating)`.
/// Big-endian rating bytes mean higher ratings sort last in forward iteration
/// and **first** in a reverse (`seek_for_prev`) scan — i.e. `reverse=true`
/// returns the highest-rated games first, which is the default.
///
/// Year LE sub-sorts by date within the same rating band.
///
/// Prefix extractor: none (whole-key bloom filter).
/// Value: empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaissifyByRatingKey {
    /// max(white_rating, black_rating), stored big-endian in the key.
    pub max_rating: u16,
    pub year: u16,
    pub id: GameId,
}

impl CaissifyByRatingKey {
    pub const SIZE: usize = 2 + 2 + GameId::SIZE; // 10

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16(self.max_rating); // big-endian
        buf.put_u16_le(self.year);
        self.id.write(buf);
    }

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        self.write(&mut &mut buf[..]);
        buf
    }

    pub fn read<B: Buf>(buf: &mut B) -> CaissifyByRatingKey {
        let max_rating = buf.get_u16(); // big-endian
        let year = buf.get_u16_le();
        let id = GameId::read(buf);
        CaissifyByRatingKey { max_rating, year, id }
    }

    /// First key at or below `rating_ceiling` (exclusive upper bound for year).
    /// Used as the starting seek point for a reverse (highest-first) scan.
    pub fn upper_bound_sentinel(rating_ceiling: u16, until_year: u16) -> [u8; Self::SIZE] {
        CaissifyByRatingKey {
            max_rating: rating_ceiling,
            year: until_year.saturating_add(1),
            id: GameId::MIN,
        }
        .into_bytes()
    }

    /// First key at or above `rating_floor`.
    pub fn lower_bound(rating_floor: u16, since_year: u16) -> [u8; Self::SIZE] {
        CaissifyByRatingKey {
            max_rating: rating_floor,
            year: since_year,
            id: GameId::MIN,
        }
        .into_bytes()
    }
}
