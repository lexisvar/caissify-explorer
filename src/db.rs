use std::{path::PathBuf, time::Instant};

use clap::Parser;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType,
    MergeOperands, Options, ReadOptions, SliceTransform, WriteBatch,
    properties::{ESTIMATE_NUM_KEYS, OPTIONS_STATISTICS},
};

use crate::{
    api::{HistoryWanted, LichessQueryFilter, Limits},
    model::{
        CaissifyByDateKey, CaissifyByFideKey, CaissifyByPlayerKey, CaissifyByPositionKey,
        CaissifyByRatingKey, CaissifyGameMeta, FideNameIndex, FidePlayer, FideRatingKey,
        FideRatingSnapshot, GameId, History, HistoryBuilder, Key, KeyPrefix, LichessEntry,
        LichessGame, MastersEntry, MastersGame, Month, PlayerEntry, PlayerStatus,
        PreparedResponse, UserId, Year,
    },
};
// Re-export so callers don't need to import from model
pub use crate::model::{MastersEntry as CaissifyEntry, MastersGame as CaissifyGame};

#[derive(Parser)]
pub struct DbOpt {
    /// Path to RocksDB database.
    #[arg(long, default_value = "_db")]
    db: PathBuf,
    /// Tune compaction readahead for spinning disks.
    #[arg(long)]
    db_compaction_readahead: bool,
    /// Size of RocksDB block cache in bytes. Use the majority of the systems
    /// RAM, leaving some memory for the operating system.
    #[arg(long, default_value = "4294967296")]
    db_cache: usize,
    /// Rate limits for writes to disk in bytes per second. This is used to
    /// limit the speed of indexing and importing (flushes and compactions),
    /// so that enough bandwidth remains to respond to queries. Use a sustained
    /// rate that your disks can comfortably handle.
    #[arg(long, default_value = "10485760")]
    db_rate_limit: i64,
}

#[derive(Default)]
pub struct DbMetrics {
    pub block_index_miss: u64,
    pub block_index_hit: u64,
    pub block_filter_miss: u64,
    pub block_filter_hit: u64,
    pub block_data_miss: u64,
    pub block_data_hit: u64,
}

impl DbMetrics {
    fn read_options_statistics(&mut self, s: &str) {
        fn count(line: &str, prefix: &str) -> Option<u64> {
            line.strip_prefix(prefix)
                .and_then(|suffix| suffix.strip_prefix(" COUNT : "))
                .and_then(|suffix| suffix.parse().ok())
        }

        for line in s.lines() {
            if let Some(c) = count(line, "rocksdb.block.cache.index.miss") {
                self.block_index_miss = c;
            } else if let Some(c) = count(line, "rocksdb.block.cache.index.hit") {
                self.block_index_hit = c;
            } else if let Some(c) = count(line, "rocksdb.block.cache.filter.miss") {
                self.block_filter_miss = c;
            } else if let Some(c) = count(line, "rocksdb.block.cache.filter.hit") {
                self.block_filter_hit = c;
            } else if let Some(c) = count(line, "rocksdb.block.cache.data.miss") {
                self.block_data_miss = c;
            } else if let Some(c) = count(line, "rocksdb.block.cache.data.hit") {
                self.block_data_hit = c;
            }
        }
    }

    pub fn to_influx_string(&self) -> String {
        [
            format!("block_index_miss={}u", self.block_index_miss),
            format!("block_index_hit={}u", self.block_index_hit),
            format!("block_filter_miss={}u", self.block_filter_miss),
            format!("block_filter_hit={}u", self.block_filter_hit),
            format!("block_data_miss={}u", self.block_data_miss),
            format!("block_data_hit={}u", self.block_data_hit),
        ]
        .join(",")
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CacheHint {
    ply: u32,
}

impl CacheHint {
    pub fn from_ply(ply: u32) -> CacheHint {
        CacheHint { ply }
    }

    pub fn always() -> CacheHint {
        CacheHint { ply: 0 }
    }

    pub fn should_fill_cache(&self) -> bool {
        let percent = if self.ply < 15 {
            return true;
        } else if self.ply < 20 {
            5
        } else if self.ply < 25 {
            2
        } else {
            1
        };

        fastrand::u32(0..100) < percent
    }
}

// Note on usage in async contexts: All database operations are blocking
// (https://github.com/facebook/rocksdb/issues/3254). Calls should be run in a
// thread-pool to avoid blocking other requests.
pub struct Database {
    pub inner: DB,
}

type MergeFn = fn(key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>>;

struct Column<'a> {
    name: &'a str,
    prefix: Option<usize>,
    merge: Option<(&'a str, MergeFn)>,
    cache: &'a Cache,
}

impl Column<'_> {
    fn descriptor(self) -> ColumnFamilyDescriptor {
        // Mostly using modern defaults from
        // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning.
        let mut table_opts = BlockBasedOptions::default();
        table_opts.set_block_cache(self.cache);
        table_opts.set_block_size(64 * 1024); // Spinning disks
        table_opts.set_cache_index_and_filter_blocks(true);
        table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        table_opts.set_hybrid_ribbon_filter(10.0, 1);
        table_opts.set_whole_key_filtering(self.prefix.is_none()); // Only prefix seeks for positions
        table_opts.set_format_version(5);

        let mut cf_opts = Options::default();
        cf_opts.set_block_based_table_factory(&table_opts);
        cf_opts.set_compression_type(DBCompressionType::Lz4);
        cf_opts.set_bottommost_compression_type(DBCompressionType::Zstd);
        cf_opts.set_level_compaction_dynamic_level_bytes(false); // Infinitely growing database

        cf_opts.set_use_direct_io_for_flush_and_compaction(true);

        cf_opts.set_prefix_extractor(match self.prefix {
            Some(prefix) => SliceTransform::create_fixed_prefix(prefix),
            None => SliceTransform::create_noop(),
        });

        if let Some((name, merge_fn)) = self.merge {
            cf_opts.set_merge_operator_associative(name, merge_fn);
        }

        ColumnFamilyDescriptor::new(self.name, cf_opts)
    }
}

impl Database {
    pub fn open(opt: DbOpt) -> Result<Database, rocksdb::Error> {
        let started_at = Instant::now();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_background_jobs(if opt.db_compaction_readahead { 2 } else { 4 });
        db_opts.set_ratelimiter(opt.db_rate_limit, 100_000, 10);
        db_opts.set_write_buffer_size(128 * 1024 * 1024); // bulk loads
        db_opts.set_track_and_verify_wals_in_manifest(true);

        db_opts.set_use_direct_io_for_flush_and_compaction(true);

        db_opts.enable_statistics();

        if opt.db_compaction_readahead {
            db_opts.set_compaction_readahead_size(2 * 1024 * 1024);
        }

        let cache = Cache::new_lru_cache(opt.db_cache);

        let inner = DB::open_cf_descriptors(
            &db_opts,
            opt.db,
            vec![
                // Masters database
                Column {
                    name: "masters",
                    prefix: Some(KeyPrefix::SIZE),
                    merge: Some(("masters_merge", masters_merge)),
                    cache: &cache,
                }
                .descriptor(),
                Column {
                    name: "masters_game",
                    prefix: None,
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Lichess database
                Column {
                    name: "lichess",
                    prefix: Some(KeyPrefix::SIZE),
                    merge: Some(("lichess_merge", lichess_merge)),
                    cache: &cache,
                }
                .descriptor(),
                Column {
                    name: "lichess_game",
                    prefix: None,
                    merge: Some(("lichess_game_merge", lichess_game_merge)),
                    cache: &cache,
                }
                .descriptor(),
                // Player database (also shares lichess_game)
                Column {
                    name: "player",
                    prefix: Some(KeyPrefix::SIZE),
                    merge: Some(("player_merge", player_merge)),
                    cache: &cache,
                }
                .descriptor(),
                Column {
                    name: "player_status",
                    prefix: None,
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // CaissifyDB — custom game database
                Column {
                    name: "caissify",
                    prefix: Some(KeyPrefix::SIZE),
                    merge: Some(("caissify_merge", caissify_merge)),
                    cache: &cache,
                }
                .descriptor(),
                Column {
                    name: "caissify_game",
                    prefix: None,
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Compact per-game metadata (year, ratings, result, FIDE IDs)
                Column {
                    name: "caissify_game_meta",
                    prefix: None,
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Secondary index: year → GameId (for paginated listing)
                Column {
                    name: "caissify_game_by_date",
                    prefix: Some(2), // 2-byte Year LE prefix
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Secondary index: FIDE player → games
                // Key: [4-byte FIDE ID LE][2-byte Year LE][6-byte GameId]
                // Value: [1-byte color: 0=white 1=black]
                // Prefix extractor: 4 bytes (FIDE ID) — enables bloom-filter
                // per-player seeks and efficient year-range scans.
                Column {
                    name: "caissify_game_by_fide",
                    prefix: Some(4), // 4-byte FIDE ID prefix
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Primary name-based player → games index (100 % coverage).
                // Key: [8-byte FNV-1a hash of normalised name LE][2-byte Year LE][6-byte GameId]
                // Value: [1-byte color: 0=white 1=black]
                // Written at import time for every game regardless of FIDE data.
                // Prefix extractor: 8 bytes (player hash).
                Column {
                    name: "caissify_game_by_player",
                    prefix: Some(8), // 8-byte player-name hash prefix
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Secondary index: position → games
                // Key: [12-byte KeyPrefix][2-byte Year LE][6-byte GameId]
                // Value: empty (GameId is embedded in the key)
                // Prefix extractor: 12 bytes (same KeyPrefix as `caissify` CF)
                // One entry per unique position per game (~40/game).
                Column {
                    name: "caissify_game_by_position",
                    prefix: Some(12), // 12-byte KeyPrefix
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Secondary index: rating → games (for sort_by=rating on game list)
                // Key: [2-byte max_rating BE][2-byte Year LE][6-byte GameId]
                // Value: empty
                // No prefix extractor — whole-key bloom filter.
                // Forward iteration = ascending rating; reverse = descending (highest first).
                Column {
                    name: "caissify_game_by_rating",
                    prefix: None,
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // Content-based deduplication index
                // Key:   [20-byte SHA-1 of space-separated UCI move string]
                // Value: [6-byte GameId] — the game already stored for these moves
                // Allows cross-source dedup regardless of player name formatting.
                Column {
                    name: "caissify_game_by_moves",
                    prefix: None, // whole-key bloom filter
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // FIDE player database
                Column {
                    name: "fide_player",
                    prefix: None,
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
                // FIDE rating history: prefix = 4-byte FIDE ID
                Column {
                    name: "fide_rating_history",
                    prefix: Some(4),
                    merge: None,
                    cache: &cache,
                }
                .descriptor(),
            ],
        )?;

        let elapsed = started_at.elapsed();
        log::info!("database opened in {elapsed:.3?}");

        Ok(Database { inner })
    }

    pub fn metrics(&self) -> Result<DbMetrics, rocksdb::Error> {
        let mut metrics = DbMetrics::default();
        if let Some(options_statistics) = self.inner.property_value(OPTIONS_STATISTICS)? {
            metrics.read_options_statistics(&options_statistics);
        }
        Ok(metrics)
    }

    pub fn compact(&self) {
        self.lichess().compact();
        self.masters().compact();
        self.caissify().compact();
        log::info!("finished manual compaction");
    }

    pub fn masters(&self) -> MastersDatabase<'_> {
        MastersDatabase {
            inner: &self.inner,
            cf_masters: self.inner.cf_handle("masters").expect("cf masters"),
            cf_masters_game: self
                .inner
                .cf_handle("masters_game")
                .expect("cf masters_game"),
        }
    }

    pub fn lichess(&self) -> LichessDatabase<'_> {
        LichessDatabase {
            inner: &self.inner,
            cf_lichess: self.inner.cf_handle("lichess").expect("cf lichess"),
            cf_lichess_game: self
                .inner
                .cf_handle("lichess_game")
                .expect("cf lichess_game"),

            cf_player: self.inner.cf_handle("player").expect("cf player"),
            cf_player_status: self
                .inner
                .cf_handle("player_status")
                .expect("cf player_status"),
        }
    }

    pub fn fide(&self) -> FideDatabase<'_> {
        FideDatabase {
            inner: &self.inner,
            cf_fide_player: self
                .inner
                .cf_handle("fide_player")
                .expect("cf fide_player"),
            cf_fide_rating_history: self
                .inner
                .cf_handle("fide_rating_history")
                .expect("cf fide_rating_history"),
        }
    }

    pub fn caissify(&self) -> CaissifyDatabase<'_> {
        CaissifyDatabase {
            inner: &self.inner,
            cf_caissify: self.inner.cf_handle("caissify").expect("cf caissify"),
            cf_caissify_game: self
                .inner
                .cf_handle("caissify_game")
                .expect("cf caissify_game"),
            cf_caissify_game_meta: self
                .inner
                .cf_handle("caissify_game_meta")
                .expect("cf caissify_game_meta"),
            cf_caissify_game_by_date: self
                .inner
                .cf_handle("caissify_game_by_date")
                .expect("cf caissify_game_by_date"),
            cf_caissify_game_by_fide: self
                .inner
                .cf_handle("caissify_game_by_fide")
                .expect("cf caissify_game_by_fide"),
            cf_caissify_game_by_player: self
                .inner
                .cf_handle("caissify_game_by_player")
                .expect("cf caissify_game_by_player"),
            cf_caissify_game_by_position: self
                .inner
                .cf_handle("caissify_game_by_position")
                .expect("cf caissify_game_by_position"),
            cf_caissify_game_by_moves: self
                .inner
                .cf_handle("caissify_game_by_moves")
                .expect("cf caissify_game_by_moves"),
            cf_caissify_game_by_rating: self
                .inner
                .cf_handle("caissify_game_by_rating")
                .expect("cf caissify_game_by_rating"),
        }
    }
}

pub struct MastersDatabase<'a> {
    inner: &'a DB,
    cf_masters: &'a ColumnFamily,
    cf_masters_game: &'a ColumnFamily,
}

pub struct MastersMetrics {
    num_masters: u64,
    num_masters_game: u64,
}

impl MastersMetrics {
    pub fn to_influx_string(&self) -> String {
        [
            format!("masters={}u", self.num_masters),
            format!("masters_game={}u", self.num_masters_game),
        ]
        .join(",")
    }
}

impl MastersDatabase<'_> {
    pub fn compact(&self) {
        log::info!("running manual compaction for masters ...");
        compact_column(self.inner, self.cf_masters);
        log::info!("running manual compaction for masters_game ...");
        compact_column(self.inner, self.cf_masters_game);
    }

    pub fn estimate_metrics(&self) -> Result<MastersMetrics, rocksdb::Error> {
        Ok(MastersMetrics {
            num_masters: self
                .inner
                .property_int_value_cf(self.cf_masters, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_masters_game: self
                .inner
                .property_int_value_cf(self.cf_masters_game, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
        })
    }

    pub fn has_game(&self, id: GameId) -> Result<bool, rocksdb::Error> {
        self.inner
            .get_pinned_cf(self.cf_masters_game, id.to_bytes())
            .map(|maybe_entry| maybe_entry.is_some())
    }

    pub fn game(&self, id: GameId) -> Result<Option<MastersGame>, rocksdb::Error> {
        Ok(self
            .inner
            .get_pinned_cf(self.cf_masters_game, id.to_bytes())?
            .map(|buf| serde_json::from_slice(&buf).expect("deserialize masters game")))
    }

    pub fn games<I: IntoIterator<Item = GameId>>(
        &self,
        ids: I,
    ) -> Result<Vec<Option<MastersGame>>, rocksdb::Error> {
        self.inner
            .batched_multi_get_cf(
                self.cf_masters_game,
                &ids.into_iter().map(|id| id.to_bytes()).collect::<Vec<_>>(),
                false,
            )
            .into_iter()
            .map(|maybe_buf_or_err| {
                maybe_buf_or_err.map(|maybe_buf| {
                    maybe_buf
                        .map(|buf| serde_json::from_slice(&buf).expect("deserialize masters game"))
                })
            })
            .collect()
    }

    pub fn has(&self, key: Key) -> Result<bool, rocksdb::Error> {
        self.inner
            .get_pinned_cf(self.cf_masters, key.into_bytes())
            .map(|maybe_entry| maybe_entry.is_some())
    }

    pub fn read(
        &self,
        key: KeyPrefix,
        since: Year,
        until: Year,
        cache_hint: CacheHint,
    ) -> Result<MastersEntry, rocksdb::Error> {
        let mut entry = MastersEntry::default();

        let mut opt = ReadOptions::default();
        opt.fill_cache(cache_hint.should_fill_cache());
        opt.set_prefix_same_as_start(true);
        opt.set_iterate_lower_bound(key.with_year(since).into_bytes());
        opt.set_iterate_upper_bound(key.with_year(until.add_years_saturating(1)).into_bytes());

        let mut iter = self.inner.raw_iterator_cf_opt(self.cf_masters, opt);
        iter.seek_to_first();

        while let Some(mut value) = iter.value() {
            entry.extend_from_reader(&mut value);
            iter.next();
        }

        iter.status().map(|_| entry)
    }

    pub fn batch(&self) -> MastersBatch<'_> {
        MastersBatch {
            db: self,
            batch: WriteBatch::default(),
        }
    }
}

pub struct MastersBatch<'a> {
    db: &'a MastersDatabase<'a>,
    batch: WriteBatch,
}

impl MastersBatch<'_> {
    pub fn merge(&mut self, key: Key, entry: MastersEntry) {
        let mut buf = Vec::with_capacity(MastersEntry::SIZE_HINT);
        entry.write(&mut buf);
        self.batch
            .merge_cf(self.db.cf_masters, key.into_bytes(), buf);
    }

    pub fn put_game(&mut self, id: GameId, game: &MastersGame) {
        self.batch.put_cf(
            self.db.cf_masters_game,
            id.to_bytes(),
            serde_json::to_vec(game).expect("serialize masters game"),
        );
    }

    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.db.inner.write(self.batch)
    }
}

// ─── CaissifyDB ───────────────────────────────────────────────────────────────

pub struct CaissifyDatabase<'a> {
    inner: &'a DB,
    cf_caissify: &'a ColumnFamily,
    cf_caissify_game: &'a ColumnFamily,
    cf_caissify_game_meta: &'a ColumnFamily,
    cf_caissify_game_by_date: &'a ColumnFamily,
    cf_caissify_game_by_fide: &'a ColumnFamily,
    cf_caissify_game_by_player: &'a ColumnFamily,
    cf_caissify_game_by_position: &'a ColumnFamily,
    cf_caissify_game_by_moves: &'a ColumnFamily,
    cf_caissify_game_by_rating: &'a ColumnFamily,
}

pub struct CaissifyMetrics {
    num_caissify: u64,
    num_caissify_game: u64,
    num_caissify_game_meta: u64,
    num_caissify_game_by_date: u64,
    pub num_caissify_game_by_fide: u64,
    pub num_caissify_game_by_player: u64,
    pub num_caissify_game_by_position: u64,
    pub num_caissify_game_by_moves: u64,
    pub num_caissify_game_by_rating: u64,
}

impl CaissifyMetrics {
    pub fn to_influx_string(&self) -> String {
        [
            format!("caissify={}u", self.num_caissify),
            format!("caissify_game={}u", self.num_caissify_game),
            format!("caissify_game_meta={}u", self.num_caissify_game_meta),
            format!("caissify_game_by_date={}u", self.num_caissify_game_by_date),
            format!("caissify_game_by_fide={}u", self.num_caissify_game_by_fide),
            format!("caissify_game_by_player={}u", self.num_caissify_game_by_player),
            format!(
                "caissify_game_by_position={}u",
                self.num_caissify_game_by_position
            ),
            format!(
                "caissify_game_by_moves={}u",
                self.num_caissify_game_by_moves
            ),
            format!(
                "caissify_game_by_rating={}u",
                self.num_caissify_game_by_rating
            ),
        ]
        .join(",")
    }
}

impl CaissifyDatabase<'_> {
    pub fn compact(&self) {
        log::info!("running manual compaction for caissify ...");
        compact_column(self.inner, self.cf_caissify);
        log::info!("running manual compaction for caissify_game ...");
        compact_column(self.inner, self.cf_caissify_game);
        log::info!("running manual compaction for caissify_game_meta ...");
        compact_column(self.inner, self.cf_caissify_game_meta);
        log::info!("running manual compaction for caissify_game_by_date ...");
        compact_column(self.inner, self.cf_caissify_game_by_date);
        log::info!("running manual compaction for caissify_game_by_fide ...");
        compact_column(self.inner, self.cf_caissify_game_by_fide);
        log::info!("running manual compaction for caissify_game_by_player ...");
        compact_column(self.inner, self.cf_caissify_game_by_player);
        log::info!("running manual compaction for caissify_game_by_position ...");
        compact_column(self.inner, self.cf_caissify_game_by_position);
        log::info!("running manual compaction for caissify_game_by_moves ...");
        compact_column(self.inner, self.cf_caissify_game_by_moves);
        log::info!("running manual compaction for caissify_game_by_rating ...");
        compact_column(self.inner, self.cf_caissify_game_by_rating);
    }

    pub fn estimate_metrics(&self) -> Result<CaissifyMetrics, rocksdb::Error> {
        Ok(CaissifyMetrics {
            num_caissify: self
                .inner
                .property_int_value_cf(self.cf_caissify, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game: self
                .inner
                .property_int_value_cf(self.cf_caissify_game, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_meta: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_meta, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_by_date: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_by_date, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_by_fide: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_by_fide, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_by_player: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_by_player, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_by_position: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_by_position, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_by_moves: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_by_moves, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_caissify_game_by_rating: self
                .inner
                .property_int_value_cf(self.cf_caissify_game_by_rating, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
        })
    }

    pub fn has_game(&self, id: GameId) -> Result<bool, rocksdb::Error> {
        self.inner
            .get_pinned_cf(self.cf_caissify_game, id.to_bytes())
            .map(|maybe_entry| maybe_entry.is_some())
    }

    /// Return true if a game with the given SHA-1 move-sequence fingerprint is
    /// already stored. Used for cross-source deduplication independent of
    /// player name formatting.
    pub fn has_by_moves(&self, fingerprint: &[u8; 20]) -> Result<bool, rocksdb::Error> {
        self.inner
            .get_pinned_cf(self.cf_caissify_game_by_moves, fingerprint)
            .map(|maybe_entry| maybe_entry.is_some())
    }

    pub fn game(&self, id: GameId) -> Result<Option<MastersGame>, rocksdb::Error> {
        Ok(self
            .inner
            .get_pinned_cf(self.cf_caissify_game, id.to_bytes())?
            .map(|buf| serde_json::from_slice(&buf).expect("deserialize caissify game")))
    }

    pub fn games<I: IntoIterator<Item = GameId>>(
        &self,
        ids: I,
    ) -> Result<Vec<Option<MastersGame>>, rocksdb::Error> {
        self.inner
            .batched_multi_get_cf(
                self.cf_caissify_game,
                &ids.into_iter().map(|id| id.to_bytes()).collect::<Vec<_>>(),
                false,
            )
            .into_iter()
            .map(|maybe_buf_or_err| {
                maybe_buf_or_err.map(|maybe_buf| {
                    maybe_buf
                        .map(|buf| serde_json::from_slice(&buf).expect("deserialize caissify game"))
                })
            })
            .collect()
    }

    pub fn has(&self, key: Key) -> Result<bool, rocksdb::Error> {
        self.inner
            .get_pinned_cf(self.cf_caissify, key.into_bytes())
            .map(|maybe_entry| maybe_entry.is_some())
    }

    pub fn read(
        &self,
        key: KeyPrefix,
        since: Year,
        until: Year,
        cache_hint: CacheHint,
    ) -> Result<MastersEntry, rocksdb::Error> {
        let mut entry = MastersEntry::default();

        let mut opt = ReadOptions::default();
        opt.fill_cache(cache_hint.should_fill_cache());
        opt.set_prefix_same_as_start(true);
        opt.set_iterate_lower_bound(key.with_year(since).into_bytes());
        opt.set_iterate_upper_bound(key.with_year(until.add_years_saturating(1)).into_bytes());

        let mut iter = self.inner.raw_iterator_cf_opt(self.cf_caissify, opt);
        iter.seek_to_first();

        while let Some(mut value) = iter.value() {
            entry.extend_from_reader(&mut value);
            iter.next();
        }

        iter.status().map(|_| entry)
    }

    /// Retrieve compact metadata for a single game (used by paginated list).
    pub fn game_meta(&self, id: GameId) -> Result<Option<CaissifyGameMeta>, rocksdb::Error> {
        Ok(self
            .inner
            .get_pinned_cf(self.cf_caissify_game_meta, id.to_bytes())?
            .map(|buf| CaissifyGameMeta::read(&mut &buf[..])))
    }

    /// Backfill `caissify_game_meta`, `caissify_game_by_date`, and
    /// `caissify_game_by_rating` for any game that was imported before Phase 0
    /// or Phase 8 respectively. Safe to run multiple times:
    /// - Games whose meta record is missing are fully indexed.
    /// - Games with a 15-byte (v1) meta record are upgraded to v2 (adds
    ///   `move_count`) and also get a `caissify_game_by_rating` entry.
    /// - Games with a 16-byte (v2) meta record are skipped (already current).
    ///
    /// Returns the number of records written/updated.
    pub fn reindex_meta(&self) -> Result<u64, rocksdb::Error> {
        let mut count = 0u64;
        let mut iter = self.inner.raw_iterator_cf(self.cf_caissify_game);
        iter.seek_to_first();

        while let Some((key_bytes, value_bytes)) = iter.item() {
            if key_bytes.len() == GameId::SIZE {
                let id = GameId::read(&mut &key_bytes[..]);

                // Check whether an up-to-date (v2) meta record already exists.
                // Also read existing FIDE IDs so we don't overwrite them when
                // upgrading a v1 record.
                let existing_meta = self
                    .inner
                    .get_pinned_cf(self.cf_caissify_game_meta, id.to_bytes())?
                    .map(|buf| CaissifyGameMeta::read(&mut &buf[..]));

                let needs_write = match &existing_meta {
                    None => true,          // missing entirely
                    Some(m) if m.move_count == 0 => true, // v1 or not-yet-populated
                    _ => false,             // v2 with real move_count — skip
                };

                if needs_write {
                    let game: MastersGame =
                        serde_json::from_slice(value_bytes).expect("deserialize caissify game");

                    let year = u16::from(game.date.year());
                    let move_count = (game.moves.len() as u64).min(u8::MAX as u64) as u8;
                    // Preserve any FIDE IDs already linked by the fide-link pass.
                    let (white_fide_id, black_fide_id) = existing_meta
                        .as_ref()
                        .map(|m| (m.white_fide_id, m.black_fide_id))
                        .unwrap_or((0, 0));
                    let meta = CaissifyGameMeta {
                        year,
                        white_rating: game.players.white.rating,
                        black_rating: game.players.black.rating,
                        result: crate::model::GameResult::from_winner(game.winner),
                        white_fide_id,
                        black_fide_id,
                        move_count,
                    };

                    let max_rating = meta.white_rating.max(meta.black_rating);

                    let mut batch = WriteBatch::default();

                    let mut meta_buf = Vec::with_capacity(CaissifyGameMeta::SIZE);
                    meta.write(&mut meta_buf);
                    batch.put_cf(self.cf_caissify_game_meta, id.to_bytes(), meta_buf);

                    batch.put_cf(
                        self.cf_caissify_game_by_date,
                        CaissifyByDateKey { year, id }.into_bytes(),
                        [],
                    );

                    batch.put_cf(
                        self.cf_caissify_game_by_rating,
                        CaissifyByRatingKey { max_rating, year, id }.into_bytes(),
                        [],
                    );

                    self.inner.write(batch)?;
                    count += 1;
                }
            }

            iter.next();
        }

        iter.status().map(|_| count)
    }

    /// Cursor-resumable variant of `reindex_meta`.
    ///
    /// Processes up to `chunk_size` games starting from `cursor` (exclusive).
    /// Returns `(processed_count, next_cursor)`.  `next_cursor = None` means
    /// the table has been exhausted — the caller should stop.
    ///
    /// This is intentionally bounded so an HTTP handler can call it within a
    /// semaphore slot and return promptly, letting the client drive the loop.
    pub fn reindex_meta_from(
        &self,
        cursor: Option<GameId>,
        chunk_size: usize,
    ) -> Result<(u64, Option<GameId>), rocksdb::Error> {
        let mut count = 0u64;
        let mut last_id: Option<GameId> = None;
        let mut seen = 0usize;

        let mut iter = self.inner.raw_iterator_cf(self.cf_caissify_game);
        match cursor {
            Some(c) => {
                // seek to the cursor key itself, then step past it.
                iter.seek(c.to_bytes());
                if iter.valid() {
                    iter.next();
                }
            }
            None => iter.seek_to_first(),
        }

        while let Some((key_bytes, value_bytes)) = iter.item() {
            if seen >= chunk_size {
                break;
            }
            if key_bytes.len() != GameId::SIZE {
                iter.next();
                continue;
            }
            let id = GameId::read(&mut &key_bytes[..]);
            last_id = Some(id);
            seen += 1;

            let existing_meta = self
                .inner
                .get_pinned_cf(self.cf_caissify_game_meta, id.to_bytes())?
                .map(|buf| CaissifyGameMeta::read(&mut &buf[..]));

            let needs_write = match &existing_meta {
                None => true,
                Some(m) if m.move_count == 0 => true,
                _ => false,
            };

            if needs_write {
                let game: MastersGame =
                    serde_json::from_slice(value_bytes).expect("deserialize caissify game");
                let year = u16::from(game.date.year());
                let move_count = (game.moves.len() as u64).min(u8::MAX as u64) as u8;
                let (white_fide_id, black_fide_id) = existing_meta
                    .as_ref()
                    .map(|m| (m.white_fide_id, m.black_fide_id))
                    .unwrap_or((0, 0));
                let meta = CaissifyGameMeta {
                    year,
                    white_rating: game.players.white.rating,
                    black_rating: game.players.black.rating,
                    result: crate::model::GameResult::from_winner(game.winner),
                    white_fide_id,
                    black_fide_id,
                    move_count,
                };
                let max_rating = meta.white_rating.max(meta.black_rating);
                let mut batch = WriteBatch::default();
                let mut meta_buf = Vec::with_capacity(CaissifyGameMeta::SIZE);
                meta.write(&mut meta_buf);
                batch.put_cf(self.cf_caissify_game_meta, id.to_bytes(), meta_buf);
                batch.put_cf(
                    self.cf_caissify_game_by_date,
                    CaissifyByDateKey { year, id }.into_bytes(),
                    [],
                );
                batch.put_cf(
                    self.cf_caissify_game_by_rating,
                    CaissifyByRatingKey { max_rating, year, id }.into_bytes(),
                    [],
                );
                self.inner.write(batch)?;
                count += 1;
            }

            iter.next();
        }

        iter.status()?;
        // If we consumed exactly chunk_size keys there may be more; return the
        // last id as the cursor.  Otherwise we exhausted the table.
        let next_cursor = if seen >= chunk_size { last_id } else { None };
        Ok((count, next_cursor))
    }

    /// Iterate games sorted by (year, GameId). Returns up to `limit` entries
    /// starting from an inclusive `cursor` key.
    ///
    /// If `reverse` is true the iterator runs backwards — useful for
    /// "newest first" pagination. In reverse mode the `cursor` is the
    /// *last* key seen on the previous page (exclusive lower bound).
    pub fn iter_by_date(
        &self,
        since_year: u16,
        until_year: u16,
        cursor: Option<CaissifyByDateKey>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<CaissifyByDateKey>, rocksdb::Error> {
        // No ReadOptions bounds: iterate_lower/upper_bound interacts badly with
        // the 2-byte prefix extractor on this CF (seek_for_prev on a sentinel key
        // whose prefix doesn't exist in data returns invalid immediately).
        // Instead, use seek_to_first/seek_to_last (bloom-filter-free absolute
        // seeks) and enforce year bounds manually in the loop.
        let mut opt = ReadOptions::default();
        opt.fill_cache(true);

        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_date, opt);

        if reverse {
            match cursor {
                Some(c) => {
                    // The cursor is the last key returned on the previous page
                    // (exclusive lower bound): position AT it then step past it.
                    iter.seek_for_prev(c.into_bytes());
                    if iter.valid() {
                        iter.prev();
                    }
                }
                // seek_to_last() seeks to the absolute last key without relying
                // on bloom filters or needing a valid prefix — always works.
                // Then the loop filters by year < since_year.
                None => iter.seek_to_last(),
            }
        } else {
            match cursor {
                Some(c) => {
                    // Exclusive: step past the cursor.
                    iter.seek(c.into_bytes());
                    if iter.valid() {
                        iter.next();
                    }
                }
                // seek_to_first() seeks to the absolute first key, same reasoning.
                None => iter.seek_to_first(),
            }
        }

        let mut results = Vec::with_capacity(limit);
        while results.len() < limit {
            let Some(key_bytes) = iter.key() else {
                break;
            };
            if key_bytes.len() >= CaissifyByDateKey::SIZE {
                let entry = CaissifyByDateKey::read(&mut &key_bytes[..]);
                if reverse {
                    // Going newest→oldest: stop once we've passed the lower bound.
                    if entry.year < since_year {
                        break;
                    }
                    // Skip entries above the upper bound (only happens when no cursor
                    // and until_year < current maximum year in the DB).
                    if entry.year <= until_year {
                        results.push(entry);
                    }
                } else {
                    // Going oldest→newest: stop once we've passed the upper bound.
                    if entry.year > until_year {
                        break;
                    }
                    // Skip entries below the lower bound (only when no cursor
                    // and since_year > current minimum year in the DB).
                    if entry.year >= since_year {
                        results.push(entry);
                    }
                }
            }
            if reverse {
                iter.prev();
            } else {
                iter.next();
            }
        }

        iter.status().map(|_| results)
    }

    /// Iterate games for a specific FIDE player sorted by (year, GameId).
    ///
    /// Returns up to `limit + 1` `(CaissifyByFideKey, is_black)` pairs after
    /// applying optional `color_filter`.  The `+ 1` sentinel allows the caller
    /// to detect whether a next page exists (`len > limit`).
    ///
    /// Uses a scan budget of `limit × 8` raw entries when color filtering is
    /// active, which handles even extreme white/black imbalances gracefully.
    pub fn iter_by_fide(
        &self,
        fide_id: u32,
        since_year: u16,
        until_year: u16,
        color_filter: Option<bool>,
        cursor: Option<CaissifyByFideKey>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(CaissifyByFideKey, bool)>, rocksdb::Error> {
        let want = limit + 1; // +1 for has-more detection
        let raw_budget = if color_filter.is_some() {
            want * 8
        } else {
            want
        };

        let mut opt = ReadOptions::default();
        opt.fill_cache(true);
        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_fide, opt);

        if reverse {
            match cursor {
                Some(c) => {
                    iter.seek_for_prev(c.into_bytes());
                    if iter.valid() {
                        iter.prev();
                    }
                }
                None => {
                    iter.seek_for_prev(CaissifyByFideKey::upper_bound_sentinel(
                        fide_id, until_year,
                    ));
                }
            }
        } else {
            match cursor {
                Some(c) => {
                    iter.seek(c.into_bytes());
                    if iter.valid() {
                        iter.next();
                    }
                }
                None => {
                    iter.seek(CaissifyByFideKey::lower_bound(fide_id, since_year));
                }
            }
        }

        let mut results = Vec::with_capacity(want);
        let mut scanned = 0usize;

        loop {
            if results.len() >= want || scanned >= raw_budget {
                break;
            }
            let Some(key_bytes) = iter.key() else {
                break;
            };
            if key_bytes.len() < CaissifyByFideKey::SIZE {
                break;
            }
            let entry = CaissifyByFideKey::read(&mut &key_bytes[..]);
            // Stop if we've drifted outside this player's key space.
            if entry.fide_id != fide_id {
                break;
            }
            let is_black = iter.value().is_some_and(|v| v.first() == Some(&1));

            scanned += 1;

            if reverse {
                if entry.year < since_year {
                    break;
                }
                if entry.year <= until_year {
                    if color_filter.map_or(true, |b| b == is_black) {
                        results.push((entry, is_black));
                    }
                }
                iter.prev();
            } else {
                if entry.year > until_year {
                    break;
                }
                if entry.year >= since_year
                    && color_filter.map_or(true, |b| b == is_black)
                {
                    results.push((entry, is_black));
                }
                iter.next();
            }
        }

        iter.status().map(|_| results)
    }

    /// Count games for a specific FIDE player in [since_year, until_year].
    /// Streams the `caissify_game_by_fide` CF without loading any game data.
    /// Optional `color_filter` restricts to white (false) or black (true).
    pub fn count_by_fide(
        &self,
        fide_id: u32,
        since_year: u16,
        until_year: u16,
        color_filter: Option<bool>,
    ) -> Result<u64, rocksdb::Error> {
        let mut opt = ReadOptions::default();
        opt.fill_cache(false);
        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_fide, opt);
        iter.seek_for_prev(CaissifyByFideKey::upper_bound_sentinel(fide_id, until_year));
        let mut count = 0u64;
        loop {
            let Some(key_bytes) = iter.key() else { break };
            if key_bytes.len() < CaissifyByFideKey::SIZE {
                break;
            }
            let entry = CaissifyByFideKey::read(&mut &key_bytes[..]);
            if entry.fide_id != fide_id {
                break;
            }
            if entry.year < since_year {
                break;
            }
            if entry.year <= until_year {
                let is_black = iter.value().is_some_and(|v| v.first() == Some(&1));
                if color_filter.map_or(true, |b| b == is_black) {
                    count += 1;
                }
            }
            iter.prev();
        }
        iter.status().map(|_| count)
    }

    /// Iterate games for a specific player (by normalised name hash), sorted
    /// by (year, GameId). 100 % coverage — written unconditionally for every
    /// imported game, independent of FIDE data.
    ///
    /// Returns up to `limit + 1` `(CaissifyByPlayerKey, is_black)` pairs after
    /// applying optional `color_filter`.  The `+ 1` sentinel lets the caller
    /// detect whether a next page exists.
    pub fn iter_by_player(
        &self,
        hash: u64,
        since_year: u16,
        until_year: u16,
        color_filter: Option<bool>,
        cursor: Option<CaissifyByPlayerKey>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(CaissifyByPlayerKey, bool)>, rocksdb::Error> {
        let want = limit + 1;
        let raw_budget = if color_filter.is_some() { want * 8 } else { want };

        let mut opt = ReadOptions::default();
        opt.fill_cache(true);
        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_player, opt);

        if reverse {
            match cursor {
                Some(c) => {
                    iter.seek_for_prev(c.into_bytes());
                    if iter.valid() { iter.prev(); }
                }
                None => {
                    iter.seek_for_prev(CaissifyByPlayerKey::upper_bound_sentinel(hash, until_year));
                }
            }
        } else {
            match cursor {
                Some(c) => {
                    iter.seek(c.into_bytes());
                    if iter.valid() { iter.next(); }
                }
                None => {
                    iter.seek(CaissifyByPlayerKey::lower_bound(hash, since_year));
                }
            }
        }

        let mut results = Vec::with_capacity(want);
        let mut scanned = 0usize;

        loop {
            if results.len() >= want || scanned >= raw_budget { break; }
            let Some(key_bytes) = iter.key() else { break; };
            if key_bytes.len() < CaissifyByPlayerKey::SIZE { break; }
            let entry = CaissifyByPlayerKey::read(&mut &key_bytes[..]);
            if entry.hash != hash { break; }
            let is_black = iter.value().is_some_and(|v| v.first() == Some(&1));
            scanned += 1;

            if reverse {
                if entry.year < since_year { break; }
                if entry.year <= until_year
                    && color_filter.map_or(true, |b| b == is_black)
                {
                    results.push((entry, is_black));
                }
                iter.prev();
            } else {
                if entry.year > until_year { break; }
                if entry.year >= since_year
                    && color_filter.map_or(true, |b| b == is_black)
                {
                    results.push((entry, is_black));
                }
                iter.next();
            }
        }

        iter.status().map(|_| results)
    }

    /// Iterate games through a specific position, sorted by (year, GameId).
    ///
    /// Returns up to `limit + 1` `CaissifyByPositionKey` entries (the `+ 1`
    /// allows the caller to detect whether a next page exists).
    pub fn iter_by_position(
        &self,
        prefix: [u8; 12],
        since_year: u16,
        until_year: u16,
        cursor: Option<CaissifyByPositionKey>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<CaissifyByPositionKey>, rocksdb::Error> {
        let want = limit + 1;

        let mut opt = ReadOptions::default();
        opt.fill_cache(true);
        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_position, opt);

        if reverse {
            match cursor {
                Some(c) => {
                    iter.seek_for_prev(c.into_bytes());
                    if iter.valid() {
                        iter.prev();
                    }
                }
                None => {
                    iter.seek_for_prev(CaissifyByPositionKey::upper_bound_sentinel(
                        prefix, until_year,
                    ));
                }
            }
        } else {
            match cursor {
                Some(c) => {
                    iter.seek(c.into_bytes());
                    if iter.valid() {
                        iter.next();
                    }
                }
                None => {
                    iter.seek(CaissifyByPositionKey::lower_bound(prefix, since_year));
                }
            }
        }

        let mut results = Vec::with_capacity(want);

        loop {
            if results.len() >= want {
                break;
            }
            let Some(key_bytes) = iter.key() else {
                break;
            };
            if key_bytes.len() < CaissifyByPositionKey::SIZE {
                break;
            }
            let entry = CaissifyByPositionKey::read(&mut &key_bytes[..]);
            // Stop if we've drifted outside this position's key space.
            if entry.prefix != prefix {
                break;
            }
            if reverse {
                if entry.year < since_year {
                    break;
                }
                if entry.year <= until_year {
                    results.push(entry);
                }
                iter.prev();
            } else {
                if entry.year > until_year {
                    break;
                }
                if entry.year >= since_year {
                    results.push(entry);
                }
                iter.next();
            }
        }

        iter.status().map(|_| results)
    }

    /// Count all games through a specific position in [since_year, until_year].
    /// Streams the `caissify_game_by_position` CF; no game data is loaded.
    pub fn count_by_position(
        &self,
        prefix: [u8; 12],
        since_year: u16,
        until_year: u16,
    ) -> Result<u64, rocksdb::Error> {
        let mut opt = ReadOptions::default();
        opt.fill_cache(false);
        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_position, opt);
        iter.seek_for_prev(CaissifyByPositionKey::upper_bound_sentinel(prefix, until_year));
        let mut count = 0u64;
        loop {
            let Some(key_bytes) = iter.key() else { break };
            if key_bytes.len() < CaissifyByPositionKey::SIZE {
                break;
            }
            let entry = CaissifyByPositionKey::read(&mut &key_bytes[..]);
            if entry.prefix != prefix {
                break;
            }
            if entry.year < since_year {
                break;
            }
            if entry.year <= until_year {
                count += 1;
            }
            iter.prev();
        }
        iter.status().map(|_| count)
    }

    /// Returns `true` when the given game is recorded as having reached the
    /// position identified by `prefix` (O(1) point lookup — no scan).
    pub fn game_at_position(&self, prefix: [u8; 12], year: u16, id: GameId) -> bool {
        let key = CaissifyByPositionKey { prefix, year, id }.into_bytes();
        self.inner
            .get_cf(self.cf_caissify_game_by_position, key)
            .expect("game_at_position lookup")
            .is_some()
    }

    /// Iterate games sorted by (max_rating, year, GameId).
    ///
    /// `rating_floor` / `rating_ceiling` restrict the scan to keys whose
    /// max_rating is in `[rating_floor, rating_ceiling]` (inclusive). Pass
    /// `0` / `u16::MAX` for no restriction.
    ///
    /// `reverse = true` (default) returns highest-rated games first; the
    /// starting seek is the first key at or below `rating_ceiling`.
    ///
    /// Returns up to `limit + 1` entries; the extra entry lets the caller
    /// detect whether a next page exists.
    pub fn iter_by_rating(
        &self,
        rating_floor: u16,
        rating_ceiling: u16,
        since_year: u16,
        until_year: u16,
        cursor: Option<CaissifyByRatingKey>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<CaissifyByRatingKey>, rocksdb::Error> {
        let want = limit + 1;

        let mut opt = ReadOptions::default();
        opt.fill_cache(true);
        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_by_rating, opt);

        if reverse {
            match cursor {
                Some(c) => {
                    iter.seek_for_prev(c.into_bytes());
                    if iter.valid() {
                        iter.prev();
                    }
                }
                None => {
                    iter.seek_for_prev(CaissifyByRatingKey::upper_bound_sentinel(
                        rating_ceiling,
                        until_year,
                    ));
                }
            }
        } else {
            match cursor {
                Some(c) => {
                    iter.seek(c.into_bytes());
                    if iter.valid() {
                        iter.next();
                    }
                }
                None => {
                    iter.seek(CaissifyByRatingKey::lower_bound(rating_floor, since_year));
                }
            }
        }

        let mut results = Vec::with_capacity(want);

        loop {
            if results.len() >= want {
                break;
            }
            let Some(key_bytes) = iter.key() else { break };
            if key_bytes.len() < CaissifyByRatingKey::SIZE {
                break;
            }
            let entry = CaissifyByRatingKey::read(&mut &key_bytes[..]);
            if reverse {
                if entry.max_rating < rating_floor {
                    break;
                }
                if entry.year < since_year {
                    iter.prev();
                    continue;
                }
                if entry.max_rating <= rating_ceiling && entry.year <= until_year {
                    results.push(entry);
                }
                iter.prev();
            } else {
                if entry.max_rating > rating_ceiling {
                    break;
                }
                if entry.year > until_year {
                    iter.next();
                    continue;
                }
                if entry.max_rating >= rating_floor && entry.year >= since_year {
                    results.push(entry);
                }
                iter.next();
            }
        }

        iter.status().map(|_| results)
    }

    /// Cursor-paginated scan over the raw game store (`caissify_game` CF).
    ///
    /// Returns up to `limit` `(GameId, MastersGame)` pairs starting
    /// *exclusively* after `cursor`. Used by the position backfill pass.
    pub fn iter_games_from(
        &self,
        cursor: Option<GameId>,
        limit: usize,
    ) -> Result<Vec<(GameId, MastersGame)>, rocksdb::Error> {
        let mut opt = ReadOptions::default();
        opt.fill_cache(false); // back-fill scan — don't pollute the cache

        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game, opt);

        match cursor {
            Some(c) => {
                iter.seek(c.to_bytes());
                if iter.valid() {
                    iter.next();
                }
            }
            None => iter.seek_to_first(),
        }

        let mut results = Vec::with_capacity(limit);
        while results.len() < limit {
            let Some((key_bytes, value_bytes)) = iter.item() else {
                break;
            };
            if key_bytes.len() == GameId::SIZE {
                let id = GameId::read(&mut &key_bytes[..]);
                let game: MastersGame =
                    serde_json::from_slice(value_bytes).expect("deserialize caissify game");
                results.push((id, game));
            }
            iter.next();
        }

        iter.status().map(|_| results)
    }

    /// Iterate `caissify_game_meta` records starting (exclusively) from
    /// `cursor`, returning up to `limit` `(GameId, CaissifyGameMeta)` pairs.
    ///
    /// Used by the background FIDE re-linking pass to process games in batches.
    pub fn iter_meta_from(
        &self,
        cursor: Option<GameId>,
        limit: usize,
    ) -> Result<Vec<(GameId, CaissifyGameMeta)>, rocksdb::Error> {
        let mut opt = ReadOptions::default();
        opt.fill_cache(false); // back-fill scan — don't pollute the cache

        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_caissify_game_meta, opt);

        match cursor {
            Some(c) => {
                iter.seek(c.to_bytes());
                if iter.valid() {
                    iter.next(); // exclusive: step past the cursor
                }
            }
            None => iter.seek_to_first(),
        }

        let mut results = Vec::with_capacity(limit);
        while results.len() < limit {
            let Some((key_bytes, value_bytes)) = iter.item() else {
                break;
            };
            if key_bytes.len() == GameId::SIZE {
                let id = GameId::read(&mut &key_bytes[..]);
                let meta = CaissifyGameMeta::read(&mut &value_bytes[..]);
                results.push((id, meta));
            }
            iter.next();
        }

        iter.status().map(|_| results)
    }

    pub fn batch(&self) -> CaissifyBatch<'_> {
        CaissifyBatch {
            db: self,
            batch: WriteBatch::default(),
        }
    }
}

pub struct CaissifyBatch<'a> {
    db: &'a CaissifyDatabase<'a>,
    batch: WriteBatch,
}

impl CaissifyBatch<'_> {
    pub fn merge(&mut self, key: Key, entry: MastersEntry) {
        let mut buf = Vec::with_capacity(MastersEntry::SIZE_HINT);
        entry.write(&mut buf);
        self.batch
            .merge_cf(self.db.cf_caissify, key.into_bytes(), buf);
    }

    pub fn put_game(&mut self, id: GameId, game: &MastersGame) {
        self.batch.put_cf(
            self.db.cf_caissify_game,
            id.to_bytes(),
            serde_json::to_vec(game).expect("serialize caissify game"),
        );
    }

    /// Write compact metadata for fast pagination and filtering.
    pub fn put_game_meta(&mut self, id: GameId, meta: &CaissifyGameMeta) {
        let mut buf = Vec::with_capacity(CaissifyGameMeta::SIZE);
        meta.write(&mut buf);
        self.batch
            .put_cf(self.db.cf_caissify_game_meta, id.to_bytes(), buf);
    }

    /// Write the secondary date index entry.
    pub fn put_by_date(&mut self, key: CaissifyByDateKey) {
        self.batch.put_cf(
            self.db.cf_caissify_game_by_date,
            key.into_bytes(),
            [], // value is empty; GameId is embedded in the key
        );
    }

    /// Write a FIDE-player → game secondary index entry.
    ///
    /// `is_black`: true if the FIDE player was Black in this game.
    pub fn put_by_fide(&mut self, key: CaissifyByFideKey, is_black: bool) {
        self.batch.put_cf(
            self.db.cf_caissify_game_by_fide,
            key.into_bytes(),
            [is_black as u8],
        );
    }

    /// Write a player-name-hash → game secondary index entry.
    ///
    /// Called for both White and Black on every import.  Guarantees 100 %
    /// coverage regardless of FIDE data availability.
    /// `is_black`: true if the player was Black in this game.
    pub fn put_by_player(&mut self, key: CaissifyByPlayerKey, is_black: bool) {
        self.batch.put_cf(
            self.db.cf_caissify_game_by_player,
            key.into_bytes(),
            [is_black as u8],
        );
    }

    /// Write a position → game secondary index entry.
    ///
    /// One call per unique Zobrist position visited in a game (~40/game).
    /// Value is empty — the GameId is embedded in the key.
    pub fn put_by_position(&mut self, key: CaissifyByPositionKey) {
        self.batch.put_cf(
            self.db.cf_caissify_game_by_position,
            key.into_bytes(),
            [],
        );
    }

    /// Write a move-sequence fingerprint → GameId deduplication entry.
    ///
    /// `fingerprint` is the 20-byte SHA-1 of the space-separated UCI move
    /// string. `id` is the GameId being inserted, stored as the value so
    /// callers can identify the earlier duplicate when needed.
    pub fn put_by_moves(&mut self, fingerprint: [u8; 20], id: GameId) {
        self.batch.put_cf(
            self.db.cf_caissify_game_by_moves,
            fingerprint,
            id.to_bytes(),
        );
    }

    /// Write a rating → game secondary index entry.
    ///
    /// One entry per game. Key encodes `max(white_rating, black_rating)` in
    /// big-endian so a reverse iterator returns highest-rated games first.
    pub fn put_by_rating(&mut self, key: CaissifyByRatingKey) {
        self.batch.put_cf(
            self.db.cf_caissify_game_by_rating,
            key.into_bytes(),
            [], // value is empty
        );
    }

    /// Overwrite only the FIDE ID fields of an existing meta record.
    ///
    /// Used by the background re-linking pass: avoids re-reading the original
    /// meta bytes by accepting the full (already-loaded) `meta` struct.
    pub fn update_meta_fide_ids(&mut self, id: GameId, meta: &CaissifyGameMeta) {
        let mut buf = Vec::with_capacity(CaissifyGameMeta::SIZE);
        meta.write(&mut buf);
        self.batch
            .put_cf(self.db.cf_caissify_game_meta, id.to_bytes(), buf);
    }

    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.db.inner.write(self.batch)
    }
}

// ─── FideDB ───────────────────────────────────────────────────────────────────

pub struct FideDatabase<'a> {
    inner: &'a DB,
    cf_fide_player: &'a ColumnFamily,
    cf_fide_rating_history: &'a ColumnFamily,
}

pub struct FideMetrics {
    pub num_fide_player: u64,
    pub num_fide_rating_history: u64,
}

impl FideMetrics {
    pub fn to_influx_string(&self) -> String {
        [
            format!("fide_player={}u", self.num_fide_player),
            format!("fide_rating_history={}u", self.num_fide_rating_history),
        ]
        .join(",")
    }
}

impl FideDatabase<'_> {
    pub fn estimate_metrics(&self) -> Result<FideMetrics, rocksdb::Error> {
        Ok(FideMetrics {
            num_fide_player: self
                .inner
                .property_int_value_cf(self.cf_fide_player, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_fide_rating_history: self
                .inner
                .property_int_value_cf(self.cf_fide_rating_history, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
        })
    }

    pub fn get_player(&self, fide_id: u32) -> Result<Option<FidePlayer>, rocksdb::Error> {
        Ok(self
            .inner
            .get_pinned_cf(self.cf_fide_player, FidePlayer::fide_id_key(fide_id))?
            .map(|buf| FidePlayer::read(&mut &buf[..])))
    }

    /// Fetch all monthly rating snapshots for a player within an optional range.
    pub fn get_rating_history(
        &self,
        fide_id: u32,
        since: Option<Month>,
        until: Option<Month>,
    ) -> Result<Vec<(Month, FideRatingSnapshot)>, rocksdb::Error> {
        let lower = FideRatingKey {
            fide_id,
            month: since.unwrap_or(Month::min_value()),
        }
        .into_bytes();
        let upper = match until {
            Some(m) => FideRatingKey {
                fide_id,
                month: m.add_months_saturating(1),
            }
            .into_bytes(),
            None => FideRatingKey::upper_bound(fide_id),
        };

        let mut opt = ReadOptions::default();
        opt.fill_cache(true);
        opt.set_iterate_lower_bound(lower);
        opt.set_iterate_upper_bound(upper);
        opt.set_prefix_same_as_start(true);

        let mut iter = self
            .inner
            .raw_iterator_cf_opt(self.cf_fide_rating_history, opt);
        iter.seek_to_first();

        let mut results = Vec::new();
        while let Some((key_bytes, value_bytes)) = iter.item() {
            if key_bytes.len() >= FideRatingKey::SIZE {
                let month_raw = u16::from_le_bytes([key_bytes[4], key_bytes[5]]);
                if let Ok(month) = Month::try_from(month_raw) {
                    let snap = FideRatingSnapshot::read(&mut &value_bytes[..]);
                    results.push((month, snap));
                }
            }
            iter.next();
        }

        iter.status().map(|_| results)
    }

    /// Fetch only the most recent rating snapshot for a player (reverse seek).
    pub fn get_latest_rating_snapshot(
        &self,
        fide_id: u32,
    ) -> Result<Option<(Month, FideRatingSnapshot)>, rocksdb::Error> {
        // Reuse get_rating_history (which handles the prefix-bloom correctly)
        // and take the last entry.  Players have at most a few months of
        // history, so the overhead is negligible.
        let history = self.get_rating_history(fide_id, None, None)?;
        Ok(history.into_iter().last())
    }

    /// Build a `FideNameIndex` by scanning the entire `fide_player` CF.
    ///
    /// Takes ~200 ms on a warm SSD for 1 M players.  Call once at startup with
    /// `task::block_in_place` so the Tokio scheduler is not stalled.
    pub fn build_name_index(&self) -> Result<FideNameIndex, rocksdb::Error> {
        let mut index = FideNameIndex::new();
        let mut iter = self.inner.raw_iterator_cf(self.cf_fide_player);
        iter.seek_to_first();
        while let Some(value_bytes) = iter.value() {
            let player = FidePlayer::read(&mut &value_bytes[..]);
            index.insert(&player);
            iter.next();
        }
        iter.status().map(|_| index)
    }

    pub fn batch(&self) -> FideBatch<'_> {
        FideBatch {
            db: self,
            batch: WriteBatch::default(),
        }
    }
}

pub struct FideBatch<'a> {
    db: &'a FideDatabase<'a>,
    batch: WriteBatch,
}

impl FideBatch<'_> {
    pub fn put_player(&mut self, player: &FidePlayer) {
        let mut buf = Vec::new();
        player.write(&mut buf);
        self.batch.put_cf(
            self.db.cf_fide_player,
            FidePlayer::fide_id_key(player.fide_id),
            buf,
        );
    }

    pub fn put_rating_snapshot(&mut self, key: FideRatingKey, snap: &FideRatingSnapshot) {
        let mut buf = Vec::with_capacity(FideRatingSnapshot::SIZE);
        snap.write(&mut buf);
        self.batch
            .put_cf(self.db.cf_fide_rating_history, key.into_bytes(), buf);
    }

    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.db.inner.write(self.batch)
    }
}

// ─────────────────────────────────────────────────────────────────────────────

pub struct LichessDatabase<'a> {
    inner: &'a DB,

    cf_lichess: &'a ColumnFamily,
    cf_lichess_game: &'a ColumnFamily,

    cf_player: &'a ColumnFamily,
    cf_player_status: &'a ColumnFamily,
}

pub struct LichessMetrics {
    num_lichess: u64,
    num_lichess_game: u64,
    num_player: u64,
    num_player_status: u64,
}

impl LichessMetrics {
    pub fn to_influx_string(&self) -> String {
        [
            format!("lichess={}u", self.num_lichess),
            format!("lichess_game={}u", self.num_lichess_game),
            format!("player={}u", self.num_player),
            format!("player_status={}u", self.num_player_status),
        ]
        .join(",")
    }
}

impl LichessDatabase<'_> {
    pub fn compact(&self) {
        log::info!("running manual compaction for lichess ...");
        compact_column(self.inner, self.cf_lichess);
        log::info!("running manual compaction for lichess_game ...");
        compact_column(self.inner, self.cf_lichess_game);
        log::info!("running manual compaction for player ...");
        compact_column(self.inner, self.cf_player);
        log::info!("running manual compaction for player_status ...");
        compact_column(self.inner, self.cf_player_status);
    }

    pub fn estimate_metrics(&self) -> Result<LichessMetrics, rocksdb::Error> {
        Ok(LichessMetrics {
            num_lichess: self
                .inner
                .property_int_value_cf(self.cf_lichess, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_lichess_game: self
                .inner
                .property_int_value_cf(self.cf_lichess_game, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_player: self
                .inner
                .property_int_value_cf(self.cf_player, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
            num_player_status: self
                .inner
                .property_int_value_cf(self.cf_player_status, ESTIMATE_NUM_KEYS)?
                .unwrap_or(0),
        })
    }

    pub fn game(&self, id: GameId) -> Result<Option<LichessGame>, rocksdb::Error> {
        Ok(self
            .inner
            .get_pinned_cf(self.cf_lichess_game, id.to_bytes())?
            .map(|buf| LichessGame::read(&mut buf.as_ref())))
    }

    pub fn games<I: IntoIterator<Item = GameId>>(
        &self,
        ids: I,
    ) -> Result<Vec<Option<LichessGame>>, rocksdb::Error> {
        self.inner
            .batched_multi_get_cf(
                self.cf_lichess_game,
                &ids.into_iter().map(|id| id.to_bytes()).collect::<Vec<_>>(),
                false,
            )
            .into_iter()
            .map(|maybe_buf_or_err| {
                maybe_buf_or_err
                    .map(|maybe_buf| maybe_buf.map(|buf| LichessGame::read(&mut &buf[..])))
            })
            .collect()
    }

    pub fn read_lichess(
        &self,
        key: &KeyPrefix,
        filter: &LichessQueryFilter,
        limits: &Limits,
        history: HistoryWanted,
        cache_hint: CacheHint,
    ) -> Result<(PreparedResponse, Option<History>), rocksdb::Error> {
        let mut entry = LichessEntry::default();
        let mut history = match history {
            HistoryWanted::No => None,
            HistoryWanted::Yes => Some(HistoryBuilder::new_between(filter.since, filter.until)),
        };

        let mut opt = ReadOptions::default();
        opt.fill_cache(cache_hint.should_fill_cache());
        opt.set_prefix_same_as_start(true);
        opt.set_iterate_lower_bound(
            key.with_month(filter.since.unwrap_or_else(Month::min_value))
                .into_bytes(),
        );
        opt.set_iterate_upper_bound(
            key.with_month(
                filter
                    .until
                    .map_or(Month::max_value(), |m| m.add_months_saturating(1)),
            )
            .into_bytes(),
        );

        let mut iter = self.inner.raw_iterator_cf_opt(self.cf_lichess, opt);
        iter.seek_to_first();

        while let Some((key, mut value)) = iter.item() {
            entry.extend_from_reader(&mut value);

            if let Some(ref mut history) = history {
                history.record_difference(
                    Key::try_from(key)
                        .expect("lichess key size")
                        .month()
                        .expect("read lichess key suffix"),
                    entry.total(filter),
                );
            }

            iter.next();
        }

        iter.status().map(|_| {
            (
                entry.prepare(filter, limits),
                history.map(HistoryBuilder::build),
            )
        })
    }

    pub fn read_player(
        &self,
        key: &KeyPrefix,
        since: Month,
        until: Month,
        cache_hint: CacheHint,
    ) -> Result<PlayerEntry, rocksdb::Error> {
        let mut entry = PlayerEntry::default();

        let mut opt = ReadOptions::default();
        opt.fill_cache(cache_hint.should_fill_cache());
        opt.set_prefix_same_as_start(true);
        opt.set_iterate_lower_bound(key.with_month(since).into_bytes());
        opt.set_iterate_upper_bound(key.with_month(until.add_months_saturating(1)).into_bytes());

        let mut iter = self.inner.raw_iterator_cf_opt(self.cf_player, opt);
        iter.seek_to_first();

        while let Some(mut value) = iter.value() {
            entry.extend_from_reader(&mut value);
            iter.next();
        }

        iter.status().map(|_| entry)
    }

    pub fn player_status(&self, id: &UserId) -> Result<Option<PlayerStatus>, rocksdb::Error> {
        Ok(self
            .inner
            .get_pinned_cf(self.cf_player_status, id.as_lowercase_str())?
            .map(|buf| PlayerStatus::read(&mut buf.as_ref())))
    }

    pub fn put_player_status(
        &self,
        id: &UserId,
        status: &PlayerStatus,
    ) -> Result<(), rocksdb::Error> {
        let mut buf = Vec::with_capacity(PlayerStatus::SIZE_HINT);
        status.write(&mut buf);
        self.inner
            .put_cf(self.cf_player_status, id.as_lowercase_str(), buf)
    }

    pub fn batch(&self) -> LichessBatch<'_> {
        LichessBatch {
            inner: self,
            batch: WriteBatch::default(),
        }
    }
}

pub struct LichessBatch<'a> {
    inner: &'a LichessDatabase<'a>,
    batch: WriteBatch,
}

impl LichessBatch<'_> {
    pub fn merge_lichess(&mut self, key: Key, entry: LichessEntry) {
        let mut buf = Vec::with_capacity(LichessEntry::SIZE_HINT);
        entry.write(&mut buf);
        self.batch
            .merge_cf(self.inner.cf_lichess, key.into_bytes(), buf);
    }

    pub fn merge_game(&mut self, id: GameId, info: LichessGame) {
        let mut buf = Vec::with_capacity(LichessGame::SIZE_HINT);
        info.write(&mut buf);
        self.batch
            .merge_cf(self.inner.cf_lichess_game, id.to_bytes(), buf);
    }

    pub fn merge_player(&mut self, key: Key, entry: PlayerEntry) {
        let mut buf = Vec::with_capacity(PlayerEntry::SIZE_HINT);
        entry.write(&mut buf);
        self.batch
            .merge_cf(self.inner.cf_player, key.into_bytes(), buf);
    }

    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.inner.inner.write(self.batch)
    }
}

fn lichess_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut entry = LichessEntry::default();
    for mut op in existing.into_iter().chain(operands.into_iter()) {
        entry.extend_from_reader(&mut op);
    }
    let mut buf = Vec::new();
    entry.write(&mut buf);
    Some(buf)
}

fn lichess_game_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // Take latest game info, but merge index status.
    let mut info: Option<LichessGame> = None;
    let mut size_hint = 0;
    for mut op in existing.into_iter().chain(operands.into_iter()) {
        size_hint = op.len();
        let mut new_info = LichessGame::read(&mut op);
        if let Some(old_info) = info {
            new_info.indexed_player.white |= old_info.indexed_player.white;
            new_info.indexed_player.black |= old_info.indexed_player.black;
            new_info.indexed_lichess |= old_info.indexed_lichess;
        }
        info = Some(new_info);
    }
    info.map(|info| {
        let mut buf = Vec::with_capacity(size_hint);
        info.write(&mut buf);
        buf
    })
}

fn player_merge(_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut entry = PlayerEntry::default();
    for mut op in existing.into_iter().chain(operands.into_iter()) {
        entry.extend_from_reader(&mut op);
    }
    let mut buf = Vec::new();
    entry.write(&mut buf);
    Some(buf)
}

fn masters_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut entry = MastersEntry::default();
    for mut op in existing.into_iter().chain(operands.into_iter()) {
        entry.extend_from_reader(&mut op);
    }
    let mut buf = Vec::new();
    entry.write(&mut buf);
    Some(buf)
}

fn caissify_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut entry = MastersEntry::default();
    for mut op in existing.into_iter().chain(operands.into_iter()) {
        entry.extend_from_reader(&mut op);
    }
    let mut buf = Vec::new();
    entry.write(&mut buf);
    Some(buf)
}

fn compact_column(db: &DB, cf: &ColumnFamily) {
    db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
}
