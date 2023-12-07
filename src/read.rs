use anyhow::Result;
use bytes::Bytes;

use crate::{db::DBInner, level::level_handler::TableInfo, value::ValueStruct};

impl DBInner {
    pub(crate) fn tables(&self) -> Result<Vec<TableInfo>> {
        self.lc.tables()
    }

    pub(crate) async fn get(&self, key: &Bytes) -> Result<ValueStruct> {
        todo!()
    }
}
