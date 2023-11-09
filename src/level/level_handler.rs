use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};

use crate::{option::Options, table::Table};

pub struct LevelHandler {
    tables: Mutex<Vec<Table>>,

    level: u32,
    opt: Options,
}

impl LevelHandler {
    pub fn new(opt: Options, level: u32) -> Self {
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

    pub fn validate(&self) -> Result<()> {
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
}
