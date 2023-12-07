use bytes::Bytes;

use crate::{entry::Entry, value::ValueStruct};

pub struct IteratorOptions {}

pub struct Iterator {}

impl std::iter::Iterator for Iterator {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct Item {
    key: Bytes,
    vptr: Bytes,
    value: Bytes,
    version: u64,
    expires_at: u64,
}

impl Item {
    pub(crate) fn from_entry(e: &Entry, read_ts: u64) -> Item {
        Item {
            key: e.key().clone(),
            vptr: Default::default(),
            value: e.value().clone(),
            version: read_ts,
            expires_at: e.expires_at(),
        }
    }

    pub(crate) fn from_value_struct(vs: &ValueStruct, key: &Bytes) -> Item {
        Item {
            key: key.clone(),
            vptr: vs.value.clone(),
            value: Default::default(),
            version: vs.version,
            expires_at: vs.expires_at,
        }
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }
}
