use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;

use crate::{
    db::DBInner,
    entry::Entry,
    error::Error,
    iterator::Item,
    iterator::{Iterator, IteratorOptions},
};

pub(crate) const BADGER_PREFIX: &[u8] = b"!badger!";
pub(crate) const TXN_KEY: &[u8] = b"!badger!txn";
pub(crate) const BANNED_NS_KEY: &[u8] = b"!badger!banned";

pub struct Txn {
    read_ts: u64,
    size: u32,
    count: u32,
    db: Arc<DBInner>,

    conflict_keys: HashMap<u64, ()>,

    pending_writes: HashMap<Bytes, Entry>,

    discarded: bool,
    update: bool,
}

impl Txn {
    pub(crate) fn new(db: Arc<DBInner>, update: bool) -> Self {
        Self {
            read_ts: 0,
            size: TXN_KEY.len() as u32 + 10,
            count: 1,
            db,
            conflict_keys: Default::default(),
            pending_writes: Default::default(),
            discarded: false,
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

    async fn modify(&mut self, mut e: Entry) -> Result<()> {
        const MAX_KEY_SIZE: usize = 65000;
        let key = e.key();
        if !self.update {
            bail!(Error::ReadOnlyTxn)
        } else if self.discarded {
            bail!(Error::DiscardedTxn)
        } else if key.len() == 0 {
            bail!(Error::EmptyKey)
        } else if key.starts_with(BADGER_PREFIX) {
            bail!(Error::InvalidKey)
        } else if key.len() > MAX_KEY_SIZE {
            return Txn::exceeds_size("Key", MAX_KEY_SIZE, key);
        } else if e.value().len() > self.db.opt.value_log_file_size {
            return Txn::exceeds_size("Value", self.db.opt.value_log_file_size, e.value());
        }

        self.db.is_banned(key).await?;

        self.check_size(&mut e)?;

        if self.db.opt.detect_conflicts {
            let fp = todo!();
            self.conflict_keys.insert(fp, ());
        }

        self.pending_writes.insert(e.key().clone(), e);

        Ok(())
    }

    fn check_size(&mut self, e: &mut Entry) -> Result<()> {
        let count = self.count + 1;
        let size = self.size + e.estimate_size_and_set_threshold(self.db.value_threshold()) + 10;
        if count >= self.db.opt.max_batch_count || size >= self.db.opt.max_batch_size {
            bail!(Error::TxnTooBig)
        }

        self.count = count;
        self.size = size;
        Ok(())
    }

    fn exceeds_size(prefix: &str, max: usize, data: &Bytes) -> Result<()> {
        let end = if data.len() > 1 << 10 {
            1 << 10
        } else {
            data.len()
        };

        let s = match std::str::from_utf8(&data[..end]) {
            Ok(s) => s,
            Err(_) => "(convert bytes to string failed)",
        };

        Err(anyhow!(
            "{} with size {} exceeded {} limit. {}:\n{}",
            prefix,
            data.len(),
            max,
            prefix,
            s
        ))
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

    use crate::{entry::Entry, test::db::new_test_db, txn::Txn};

    #[test(tokio::test)]
    async fn test_txn_simple() {
        let test_db = new_test_db(None).await.unwrap();
        let db = test_db.db;
        let mut txn = db.new_transaction(true).await.unwrap();

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
