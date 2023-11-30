use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    db::DBInner,
    entry::Entry,
    iterator::Item,
    iterator::{Iterator, IteratorOptions},
};

pub struct Txn {
    read_ts: u64,
    db: Arc<DBInner>,

    update: bool,
}

impl Txn {
    pub(crate) fn new(db: Arc<DBInner>, update: bool) -> Self {
        Self {
            read_ts: 0,
            db,
            update,
        }
    }

    pub(crate) fn set_read_ts(&mut self, read_ts: u64) {
        self.read_ts = read_ts;
    }

    pub fn commit(self) -> Result<()> {
        unimplemented!()
    }

    pub fn discard(&self) {
        unimplemented!()
    }

    pub async fn set<B: Into<Bytes>>(&mut self, key: B, value: B) -> Result<()> {
        self.set_entry(Entry::new(key.into(), value.into())).await
    }

    pub async fn get<B: Into<Bytes>>(&self, _key: B) -> Result<Item> {
        unimplemented!()
    }

    pub async fn delete<B: Into<Bytes>>(&mut self, key: B) -> Result<()> {
        self.modify(Entry::delete(key.into())).await
    }

    pub async fn new_iterator(&self, _opt: IteratorOptions) -> Result<Iterator> {
        unimplemented!()
    }

    pub async fn set_entry(&mut self, e: Entry) -> Result<()> {
        self.modify(e).await
    }

    async fn modify(&mut self, _e: Entry) -> Result<()> {
        // TODO checks

        unimplemented!()
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        self.discard()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use test_log::test;

    use crate::{entry::Entry, test::db::new_test_db};

    #[test(tokio::test)]
    async fn test_txn_simple() {
        let test_db = new_test_db(None).await.unwrap();
        let db = test_db.db;
        let mut txn = db.new_transaction(true).unwrap();

        for i in 0..10 {
            let key = Bytes::from(format!("key={}", i));
            let value = Bytes::from(format!("val={}", i));
            txn.set_entry(Entry::new(key, value))
                .await
                .expect("set_entry fail");
        }

        let item = txn.get(Bytes::from("key=8")).await.expect("get item fail");
        assert_eq!(item.value_copy(), "val=8");

        txn.commit().unwrap();
    }
}
