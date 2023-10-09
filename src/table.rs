use anyhow::Result;

use crate::{option::Options, util::file::MmapFile};

pub struct Table {
    mmap_file: MmapFile,

    smallest: Vec<u8>,
    id: u64,
}

impl Table {
    pub fn open(mmap_file: MmapFile, opt: Options) -> Result<Self> {
        todo!()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn smallest(&self) -> &Vec<u8> {
        &self.smallest
    }
}
