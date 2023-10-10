use anyhow::{anyhow, Result};

use crate::option::{self, ChecksumVerificationMode::*};
use crate::util::{file::MmapFile, table::parse_file_id};

pub struct Options {
    pub table_size: u64,
    pub block_size: u32,
    pub cv_mode: option::ChecksumVerificationMode,
}

impl Options {
    pub fn build_table_options(opt: option::Options) -> Self {
        Self {
            table_size: opt.base_table_size as u64,
            block_size: opt.block_size,
            cv_mode: opt.cv_mode,
        }
    }
}

pub struct Table {
    mmap_file: MmapFile,

    table_size: u64,

    smallest: Vec<u8>,
    biggest: Vec<u8>,
    id: u64,

    opt: Options,
}

impl Table {
    pub fn open(mmap_file: MmapFile, opt: Options) -> Result<Self> {
        let file = mmap_file
            .file
            .lock()
            .map_err(|e| anyhow!("accessing file with mutex: {}", e))?;
        let len = file.fd.metadata()?.len();
        let id = parse_file_id(
            file.path
                .file_name()
                .ok_or(anyhow!("invalid path"))?
                .to_str()
                .ok_or(anyhow!("invalid path"))?,
        )?;
        drop(file);

        let cv_mode = opt.cv_mode.clone();
        let mut table = Table {
            mmap_file,
            table_size: len,
            smallest: vec![],
            biggest: vec![],
            id,
            opt,
        };

        table.init_biggest_and_smallest()?;

        if cv_mode == OnTableRead || cv_mode == OnTableAndBlockRead {
            table.verify_checksum()?;
        }
        Ok(table)
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn smallest(&self) -> &Vec<u8> {
        &self.smallest
    }

    pub fn biggest(&self) -> &Vec<u8> {
        &self.biggest
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        todo!()
    }

    fn verify_checksum(&mut self) -> Result<()> {
        todo!()
    }
}
