use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;

use crate::{option::Options, table::Table};

pub struct LevelHandler {
    tables: Mutex<Vec<Table>>,

    level: u32,
    opt: Options,
}

impl LevelHandler {
    pub(crate) fn new(opt: Options, level: u32) -> Self {
        Self {
            tables: Mutex::new(vec![]),
            level,
            opt,
        }
    }

    pub(crate) fn init_table(&mut self, tables: Vec<Table>) {
        let mut tables = tables;
        if self.level == 0 {
            // Key range will overlap. Just sort by file_id in ascending order
            // because newer tables are at the end of level 0.
            tables.sort_by(|a, b| a.id().cmp(&b.id()))
        } else {
            // Sort tables by keys.
            tables.sort_by(|a, b| a.smallest().cmp(&b.smallest()))
        }
        self.tables = Mutex::new(tables);
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.level == 0 {
            return Ok(());
        }

        let tables = self.tables.lock().map_err(|e| anyhow!("{}", e))?;
        for index in 1..tables.len() {
            let a = tables.get(index - 1).unwrap();
            let b = tables.get(index).unwrap();
            if a.biggest().cmp(&b.smallest()).is_ge() {
                bail!(
                    "biggest({}) >= smallest({}), level={}, tables.len={}",
                    index - 1,
                    index,
                    self.level,
                    tables.len()
                )
            }
            if b.smallest().cmp(&b.biggest()).is_gt() {
                bail!(
                    "smallest({}) > biggest({}), level={}, tables.len={}",
                    index,
                    index,
                    self.level,
                    tables.len()
                )
            }
        }
        Ok(())
    }

    pub(crate) fn level(&self) -> u32 {
        self.level
    }

    pub(crate) fn tables(&self, level: u32) -> Result<Vec<TableInfo>> {
        let mut result = vec![];

        let ts = self.tables.lock().unwrap();
        for t in ts.iter() {
            result.push(TableInfo {
                id: t.id(),
                level,
                left: t.smallest(),
                right: t.biggest(),
                key_count: t.key_count(),
                on_disk_size: t.on_disk_size(),
                stale_data_size: t.stale_data_size(),
                uncompressed_size: t.uncompressed_size(),
                max_version: t.max_version(),
                index_size: t.index_size(),
                bloom_filter_size: t.bloom_filter_size(),
            });
        }

        Ok(result)
    }
}

pub(crate) struct TableInfo {
    id: u64,
    level: u32,
    left: Bytes,
    right: Bytes,
    key_count: u32,
    on_disk_size: u32,
    stale_data_size: u32,
    uncompressed_size: u32,
    max_version: u64,
    index_size: usize,
    bloom_filter_size: usize,
}

impl TableInfo {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn level(&self) -> u32 {
        self.level
    }

    pub(crate) fn max_version(&self) -> u64 {
        self.max_version
    }
}
