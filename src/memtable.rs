use std::path::PathBuf;

use anyhow::Result;

use crate::{option::Options, skiplist};

pub const MEM_FILE_EXT: &str = ".mem";

pub struct MemTable {
    pub sl: skiplist::SkipList,
    wal: LogFile,
    max_version: u64,
    opt: Options,
    // buf:
}

pub async fn open_mem_table(opt: &Options, fid: u32, flags: u8) -> Result<MemTable> {
    let path = PathBuf::from(&opt.dir).join(format!("{:05}{}", fid, MEM_FILE_EXT));

    todo!()
}

impl MemTable {
    pub fn decr_ref(&self) {}
}

struct LogFile {}
