use bytes::Bytes;

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
    pub fn key_copy(&self) -> Bytes {
        self.key.clone()
    }

    pub fn value_copy(&self) -> Bytes {
        self.value.clone()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }
}
