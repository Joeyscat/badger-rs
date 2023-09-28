use std::{cell::RefCell, io::Read, rc::Rc};

use anyhow::Result;

use crate::manifest::CASTAGNOLI;

pub const BIT_DELETE: u8 = 1 << 0;
pub const BIT_VALUE_POINTER: u8 = 1 << 1;
pub const BIT_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
pub const BIT_MERGE_ENTRY: u8 = 1 << 3;
pub const BIT_TXN: u8 = 1 << 6;
pub const BIT_FIN_TXN: u8 = 1 << 7;

pub const MAX_HEADER_SIZE: usize = 22;

pub const CRC_SIZE: usize = 4;

#[derive(Debug, Clone, Copy)]
pub struct ValuePointer {
    pub fid: u32,
    pub len: u32,
    pub offset: u32,
}

pub struct Header {
    pub key_len: usize,
    pub value_len: usize,
    pub expires_at: u64,
    pub meta: u8,
    pub user_meta: u8,
}

impl Header {
    pub fn decode_from<R: Read>(mut reader: R) -> Result<Self> {
        let mut header = Header {
            key_len: 0,
            value_len: 0,
            expires_at: 0,
            meta: 0,
            user_meta: 0,
        };

        let mut buf = [0; 1];
        reader.read_exact(&mut buf)?;
        header.meta = buf[0].clone();
        reader.read_exact(&mut buf)?;
        header.user_meta = buf[0].clone();
        let mut buf = [0; 8];
        reader.read_exact(&mut buf)?;
        header.key_len = u64::from_be_bytes(buf) as usize;
        let mut buf = [0; 8];
        reader.read_exact(&mut buf)?;
        header.value_len = u64::from_be_bytes(buf) as usize;

        Ok(header)
    }

    /// Encode encodes the header into []byte. The provided []byte should be atleast 5 bytes. The
    /// function will panic if out []byte isn't large enough to hold all the values.
    /// The encoded header looks like
    /// +------+----------+------------+--------------+-----------+
    /// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
    /// +------+----------+------------+--------------+-----------+
    pub fn encode(&self) {}
}

pub struct HashReader<'a, R: ?Sized> {
    count: usize,
    hash: crc::Digest<'a, u32>,
    inner: Rc<RefCell<R>>,
}

impl<'a, R: Read> HashReader<'a, R> {
    pub fn new(inner: Rc<RefCell<R>>) -> HashReader<'a, R> {
        let hash = CASTAGNOLI.digest();
        Self {
            inner,
            hash,
            count: 0,
        }
    }

    pub fn sum32(&self) -> u32 {
        self.hash.clone().finalize()
    }

    pub fn count(&self) -> usize {
        return self.count;
    }
}

impl<'a, R: ?Sized + Read> Read for HashReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.borrow_mut().read(buf)?;
        self.count += bytes_read;

        self.hash.update(&buf[..bytes_read]);

        Ok(bytes_read)
    }
}

#[derive(Debug, Clone)]
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
