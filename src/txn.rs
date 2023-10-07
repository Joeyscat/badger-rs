use anyhow::Result;

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

    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.set_entry(Entry::new(key, value)).await
    }

    pub async fn get(&self, _key: Vec<u8>) -> Result<Item> {
        unimplemented!()
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.modify(Entry::delete(key)).await
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
