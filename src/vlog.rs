pub const MAX_VLOG_FILE_SIZE: u32 = u32::MAX;

/// size of vlog header.
/// +----------------+------------------+
/// | keyID(8 bytes) |  baseIV(12 bytes)|
/// +----------------+------------------+
pub const VLOG_HEADER_SIZE: u32 = 20;
