use std::io::{BufRead, Read};

use anyhow::Result;

pub const BIT_DELETE: u8 = 1 << 0;
pub const BIT_VALUE_POINTER: u8 = 1 << 1;
pub const BIT_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
pub const BIT_MERGE_ENTRY: u8 = 1 << 3;
pub const BIT_TXN: u8 = 1 << 6;
pub const BIT_FIN_TXN: u8 = 1 << 7;

pub const MAX_HEADER_SIZE: u32 = 22;

pub struct ValuePointer {
    pub fid: u32,
    pub len: u32,
    pub offset: u32,
}

pub struct Header {
    key_len: u32,
    value_len: u32,
    expires_at: u64,
    meta: u8,
    user_meta: u8,
}

impl Header {
    pub fn decode_from(reader: impl BufRead) -> Result<Self> {
        todo!()
    }

    /// Encode encodes the header into []byte. The provided []byte should be atleast 5 bytes. The
    /// function will panic if out []byte isn't large enough to hold all the values.
    /// The encoded header looks like
    /// +------+----------+------------+--------------+-----------+
    /// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
    /// +------+----------+------------+--------------+-----------+
    pub fn encode(&self) {}
}

pub struct HashReader {}

impl Read for HashReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        todo!()
    }
}

impl BufRead for HashReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        todo!()
    }

    fn consume(&mut self, amt: usize) {
        todo!()
    }
}

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

    pub fn key_with_ts(key: Vec<u8>, ts: u64) -> Vec<u8> {
        todo!()
    }

    pub fn parse_ts(key: &Vec<u8>) -> u64 {
        if key.len() < 8 {
            return 0;
        }
        let mut bs = [0; 8];
        bs.copy_from_slice(&key[key.len() - 8..]);
        u64::from_be_bytes(bs)
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
