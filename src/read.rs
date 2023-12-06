use anyhow::Result;

use crate::{db::DBInner, level::level_handler::TableInfo};

impl DBInner {
    pub(crate) fn tables(&self) -> Result<Vec<TableInfo>> {
        self.lc.tables()
    }
}
