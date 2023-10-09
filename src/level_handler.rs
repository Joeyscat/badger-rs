use std::sync::Mutex;

use crate::{option::Options, table::Table, util::compare_keys};

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

    pub fn init_table(&mut self, tables: Vec<Table>) {
        let mut tables = tables;
        if self.level == 0 {
            // Key range will overlap. Just sort by file_id in ascending order
            // because newer tables are at the end of level 0.
            tables.sort_by(|a, b| a.id().cmp(&b.id()))
        } else {
            // Sort tables by keys.
            tables.sort_by(|a, b| compare_keys(a.smallest(), b.smallest()))
        }
        self.tables = Mutex::new(tables);
    }
}
