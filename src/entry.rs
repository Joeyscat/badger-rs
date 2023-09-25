const BIT_DELETE: u8 = 1 << 0;
const BIT_VALUE_POINTER: u8 = 1 << 1;
const BIT_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
const BIT_MERGE_ENTRY: u8 = 1 << 3;
const BIT_TXN: u8 = 1 << 6;
const BIT_FIN_TXN: u8 = 1 << 7;

pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub version: u64,
    pub offset: u32,
    pub user_meta: u8,
    pub meta: u8,

    pub header_len: u32,
    pub val_threshold: u32,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            ..Entry::default()
        }
    }

    pub fn delete(key: Vec<u8>) -> Self {
        Self {
            key,
            meta: BIT_DELETE,
            ..Entry::default()
        }
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            key: Default::default(),
            value: Default::default(),
            expires_at: Default::default(),
            version: Default::default(),
            offset: Default::default(),
            user_meta: Default::default(),
            meta: Default::default(),
            header_len: Default::default(),
            val_threshold: Default::default(),
        }
    }
}
