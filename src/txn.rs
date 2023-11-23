use anyhow::Result;
use bytes::Bytes;

use crate::{
    entry::Entry,
    iterator::Item,
    iterator::{Iterator, IteratorOptions},
};

pub struct Txn {}

impl Txn {
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
