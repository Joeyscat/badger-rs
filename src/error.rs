#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The `value_log_file_size` option is not within the valid range.
    #[error("Invalid `value_log_file_size`: {0}, must be in range [1MB, 2GB)")]
    ValueLogSize(usize),

    /// Key isn't found on a txn.get.
    #[error("Key not found")]
    KeyNotFound,

    /// Too many writes are fit into a single transaction.
    #[error("Txn is too big to fit into one request")]
    TxnTooBig,

    /// A transaction conflicts with another transaction. This can happen
    /// if the read rows had been updated concurrently by another transaction.
    #[error("Transaction Conflict. Please retry")]
    Conflict,

    /// An update function is called on a read-only transaction.
    #[error("No sets or deletes are allowed in a read-only transaction")]
    ReadOnlyTxn,

    /// A previously discarded transaction is re-used.
    #[error("This transaction has been discarded. Create a new one")]
    DiscardedTxn,

    /// An empty key is passed on an update function.
    #[error("Key cannot be empty")]
    EmptyKey,

    /// The key has a special `!badger!` prefix, reserved for internal usage.
    #[error("Key is using a reserved `!badger!` prefix")]
    InvalidKey,

    /// The read/write key belongs to any banned namespace.
    #[error("Key is using banned prefix")]
    BannedKey,

    /// Threshold is set to zero, and value log GC is called. In such a case,
    /// GC can't be run.
    #[error("Value log GC can't run because `threshold` is set to zero")]
    ThresholdZero,

    /// A call for value log GC doesn't result in a log file rewrite.
    #[error("Value log GC attempt didn't result in any clean up")]
    NoRewrite,

    /// A value log GC is called either while another GC is running, or
    /// after DB::close has been called.
    #[error("Value log GC request rejected")]
    Rejected,

    /// The user request is invalid.
    #[error("Invalid request")]
    InvalidRequest,

    /// The user tries to use an API which isn't allowed due to external
    /// managemant of transactions, while using ManagedDB.
    #[error("Invalid API request. Not allowed to perform this action using ManagedDB")]
    ManagedTxn,

    /// The user tries to use an API which is allowed only when `namespace_offset`
    /// is non-negative.
    #[error(
        "Invalid API request. Not allowed to perform this action when `namespace_mode` is not set."
    )]
    NamespaceMode,

    /// A data dump made previously cannot be loaded into the database.
    #[error("Data dump cannot be read")]
    InvalidDump,

    /// The user passes in zero `bandwidth` for sequence.
    #[error("`bandwidth` must be greater than zero")]
    ZeroBandwidth,

    /// opt.read_only is used on Windows.
    #[error("Read-onl mode is not supported on Windows")]
    WindowsNotSupported,

    /// opt.read_only is used on Plan 9.
    #[error("Read-onl mode is not supported on Plan 9")]
    Plan9NotSupported,

    /// The value log gets corrupt, and requires truncation of corrupt data
    /// to allow Badger to run properly.
    #[error("Log truncate required to run DB. This might result in data loss")]
    TruncateNeeded,

    /// The user called `drop_all`. During the process of dropping all data from
    /// Badger, we stop accepting new writes, by returning this error.
    #[error("Writes are blocked, possibly due to `drop_all` or `close`")]
    BlockedWrites,

    /// The storage key is not matched with the key previously given.
    #[error("Encryption key mismatch")]
    EncryptionKeyMismatch,

    /// The datakey id is invalid.
    #[error("Invalid datakey id")]
    InvalidDataKeyID,

    /// Length of encryption keys is invalid.
    #[error("Encryption key's length should be either 16, 24, or 32 bytes")]
    InvalidEncryptionKey,

    /// `db.run_value_log_gc` is called in in-memory mode.
    #[error("Cannot run value log GC when DB is opened in InMemory mode")]
    GCInMemoryMode,

    /// A get operation is performed after closing the DB.
    #[error("DB Closed")]
    DBClosed,

    #[error("Manifest has bad magic")]
    ManifestBadMagic,

    #[error("Manifest has checksum mismatch")]
    ManifestBadChecksum,

    #[error("Manifest version unsupported.\nExpected: {0}, got {1}")]
    ManifestVersionUnsupport(u16, u16),

    #[error("Manifest external magic number doesn't match.\nExpected: {0}, got: {1}")]
    ManifestExtMagicMismatch(u16, u16),
}
