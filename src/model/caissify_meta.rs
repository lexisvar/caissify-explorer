use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use shakmaty::Color;

use crate::model::GameId;

/// Compact per-game metadata stored in the `caissify_game_meta` column family.
///
/// Key:   GameId (6 bytes, same as caissify_game)
/// Value: 15 bytes — year(2) + white_rating(2) + black_rating(2) + result(1)
///                   + white_fide_id(4) + black_fide_id(4)
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
    /// Byte size on disk.
    pub const SIZE: usize = 2 + 2 + 2 + 1 + 4 + 4; // 15

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
    }

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
        CaissifyGameMeta {
            year,
            white_rating,
            black_rating,
            result,
            white_fide_id,
            black_fide_id,
        }
    }
}

/// Key for `caissify_game_by_date` column family.
///
/// Layout (8 bytes): [2-byte Year LE][6-byte GameId LE]
///
/// Prefix extractor: 2 bytes (year). Allows efficient year-range scans and
/// cursor-based pagination via `seek(year, game_id)`.
#[derive(Debug, Clone, Copy)]
pub struct CaissifyByDateKey {
    pub year: u16,
    pub id: GameId,
}

impl CaissifyByDateKey {
    pub const SIZE: usize = 2 + GameId::SIZE; // 8

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16_le(self.year);
        self.id.write(buf);
    }

    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        self.write(&mut &mut buf[..]);
        buf
    }

    pub fn read<B: Buf>(buf: &mut B) -> CaissifyByDateKey {
        let year = buf.get_u16_le();
        let id = GameId::read(buf);
        CaissifyByDateKey { year, id }
    }

    /// Exclusive upper-bound key for all games in years ≤ `year`.
    pub fn upper_bound(year: u16) -> [u8; Self::SIZE] {
        CaissifyByDateKey {
            year: year.saturating_add(1),
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
