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
    pub title: String,
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

        FidePlayer {
            fide_id,
            name,
            country,
            sex,
            title,
            birth_year,
            flag,
        }
    }

    pub fn fide_id_key(fide_id: u32) -> [u8; 4] {
        fide_id.to_le_bytes()
    }
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
    /// K-factor for standard (13, 20, or 40)
    pub k_factor: u8,
}

impl FideRatingSnapshot {
    pub const SIZE: usize = 2 + 2 + 2 + 2 + 1; // 9

    pub fn write<B: BufMut>(&self, buf: &mut B) {
        buf.put_u16_le(self.standard);
        buf.put_u16_le(self.rapid);
        buf.put_u16_le(self.blitz);
        buf.put_u16_le(self.games_standard);
        buf.put_u8(self.k_factor);
    }

    pub fn read<B: Buf>(buf: &mut B) -> FideRatingSnapshot {
        FideRatingSnapshot {
            standard: buf.get_u16_le(),
            rapid: buf.get_u16_le(),
            blitz: buf.get_u16_le(),
            games_standard: buf.get_u16_le(),
            k_factor: buf.get_u8(),
        }
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
