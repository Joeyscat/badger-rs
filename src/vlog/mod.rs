mod discard;


use anyhow::Result;

use crate::option::Options;

pub const MAX_VLOG_FILE_SIZE: u32 = u32::MAX;

/// size of vlog header.
/// +----------------+------------------+
/// | keyID(8 bytes) |  baseIV(12 bytes)|
/// +----------------+------------------+
pub const VLOG_HEADER_SIZE: u32 = 20;

pub(crate) struct ValueLog {
    opt: Options
}

impl ValueLog {
    pub(crate) fn new(_opts: Options) -> Result<ValueLog> {
        todo!()
    }

    pub(crate) async fn open(&mut self) -> Result<()> {
        todo!()
    }
}
