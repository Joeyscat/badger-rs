use std::time::{self, Duration};

/// 1MB
const MAX_VALUE_THRESHOLD: u32 = 1 << 20;

#[derive(Debug, Clone)]
pub struct Options {
    // required options.
    pub dir: String,

    // usually modified options.
    pub sync_writes: bool,
    pub num_versions_to_keep: u32,
    pub stream_threads_num: u32,

    // find tuning options.
    pub mem_table_size: usize,
    pub base_table_size: usize,
    pub base_level_size: usize,
    pub level_size_multiplier: u32,
    pub table_size_multiplier: u32,
    pub max_levels: u32,

    pub v_log_percentile: f64,
    pub value_threshold: u32,
    pub num_memtables: u32,

    /// Changing `block_size` across DB runs will not break badger. The block size is
    /// read from the block index stored at the end of the table.
    pub block_size: u32,
    pub bloom_false_positive: f64,

    pub num_level_zero_tables: u32,
    pub num_level_zero_tables_stall: u32,

    pub value_log_file_size: usize,
    pub value_log_max_entries: usize,

    pub num_compactors: u32,
    pub compact_l0_on_close: bool,
    pub lmax_compaction: bool,
    pub zstd_compression_level: u32,

    /// When set, checksum will be validated for each entry read from the value log file.
    pub verify_value_checksum: bool,

    // encryption related options.
    pub encryption_key: Vec<u8>,
    pub encryption_key_rotation_duration: Duration,

    /// `bypass_lock_guard` will bypass the lock guard on badger. Bypassing lock
    /// guard  can cause data corruption if multiple badger instances are using
    /// the same directory. Use this options with caution.
    pub bypass_lock_guard: bool,

    /// `cv_mode` decides when db should verify checksum for SSTable blocks.
    pub cv_mode: ChecksumVerificationMode,

    /// `detect_conflicts` determines whether the transactions would be checked for
    /// conflicts. The transactions can be processed at a higher rate when
    /// conflict detection is disabled.
    pub detect_conflicts: bool,

    /// `namespace_offset` specifies the offset from where the next 8 bytes contains the namespace.
    pub namespace_offset: i64,

    /// Magic version used by the application using badger to ensure that it doesn't open the DB
    /// with incompatible data format.
    pub external_magic_version: u16,

    /// Transaction start and commit timestamps are managed by end-user.
    /// This is only useful for databases built on top of Badger (like Dgraph).
    /// Not recommanded for most users.
    _managed_txns: bool,

    // Flags for testing purposes
    _max_batch_count: u32,
    _max_batch_size: u32,

    _max_value_threshold: f64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir: "/tmp/badger".to_string(),

            sync_writes: false,
            num_versions_to_keep: 1,
            stream_threads_num: 8,

            mem_table_size: 64 << 20,
            base_table_size: 2 << 20,
            base_level_size: 10 << 20,
            level_size_multiplier: 10,
            table_size_multiplier: 2,
            max_levels: 7,

            v_log_percentile: 0.0,
            value_threshold: MAX_VALUE_THRESHOLD,
            num_memtables: 5,

            block_size: 4 * 1024,
            bloom_false_positive: 0.01,

            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 15,

            value_log_file_size: 1 << 30 - 1,
            value_log_max_entries: 1000000,

            num_compactors: 4,
            compact_l0_on_close: false,
            lmax_compaction: Default::default(),
            zstd_compression_level: 1,

            verify_value_checksum: false,

            encryption_key: Default::default(),
            encryption_key_rotation_duration: time::Duration::from_secs(60 * 60 * 24 * 10),

            bypass_lock_guard: Default::default(),
            cv_mode: Default::default(),
            detect_conflicts: true,
            namespace_offset: -1,
            external_magic_version: Default::default(),
            _managed_txns: Default::default(),

            _max_batch_count: Default::default(),
            _max_batch_size: Default::default(),

            _max_value_threshold: Default::default(),
        }
    }
}

enum CompressionType {
    // None,
    Snappy,
    // ZSTD,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Snappy
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumVerificationMode {
    NoVerification,
    OnTableRead,
    OnBlockRead,
    OnTableAndBlockRead,
}

impl Default for ChecksumVerificationMode {
    fn default() -> Self {
        Self::NoVerification
    }
}
