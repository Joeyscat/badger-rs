use anyhow::{anyhow, bail, Result};
use integer_encoding::VarIntReader;
use std::{cell::RefCell, io::ErrorKind::UnexpectedEof, io::Read, rc::Rc, sync::Arc};

use crate::{error::Error, manifest::CASTAGNOLI};

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
    pub key_len: u64,
    pub value_len: u64,
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

        let mut buf = [0; 2];
        match reader.read_exact(&mut buf) {
            Err(e) if e.kind() == UnexpectedEof => bail!(Error::VLogTruncate),
            Err(e) => bail!(e),
            _ => {}
        };
        header.meta = buf[0];
        header.user_meta = buf[1];

        header.key_len = reader
            .read_varint::<u64>()
            .map_err(|e| anyhow!("read_varint(key_len) error:{}", e))?;
        header.value_len = reader
            .read_varint::<u64>()
            .map_err(|e| anyhow!("read_varint(value_len) error:{}", e))?;
        header.expires_at = reader
            .read_varint::<u64>()
            .map_err(|e| anyhow!("read_varint(expires_at) error:{}", e))?;

        Ok(header)
    }

    /// Encode encodes the header into []byte. The provided []byte should be atleast 5 bytes. The
    /// function will panic if out []byte isn't large enough to hold all the values.
    /// The encoded header looks like
    /// +------+----------+------------+--------------+-----------+
    /// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
    /// +------+----------+------------+--------------+-----------+
    pub fn encode(&self) {
        todo!()
    }
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
    pub value: Arc<Vec<u8>>,
    pub expires_at: u64,
    pub version: u64,
    pub offset: u32,
    pub user_meta: u8,
    pub meta: u8,

    pub header_len: u32,
    pub val_threshold: u32,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Arc<Vec<u8>>) -> Self {
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
