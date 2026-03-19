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
