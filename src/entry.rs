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

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ValuePointer {
    fid: u32,
    len: u32,
    offset: u32,
}

impl ValuePointer {
    pub(crate) fn new(fid: u32, len: u32, offset: u32) -> Self {
        Self { fid, len, offset }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }
}

pub(crate) struct Header {
    pub key_len: u64,
    pub value_len: u64,
    pub expires_at: u64,
    pub meta: u8,
    pub user_meta: u8,
}

impl Header {
    pub(crate) fn decode_from<R: Read>(mut reader: R) -> Result<Self> {
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
    pub(crate) fn encode(&self) {
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
    key: Vec<u8>,
    expires_at: u64,
    value: Arc<Vec<u8>>,
    version: u64,
    user_meta: u8,
    meta: u8,

    offset: u32,
    header_len: u32,
    value_threshold: u32,
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

    pub(crate) fn skip_vlog_and_set_threshold(&mut self, threshole: u32) -> bool {
        if self.value_threshold == 0 {
            self.value_threshold = threshole;
        }

        self.value.len() < self.value_threshold as usize
    }

    #[allow(dead_code)]
    pub(crate) fn estimate_size_and_set_threshold(&mut self, threshole: u32) -> u32 {
        if self.value_threshold == 0 {
            self.value_threshold = threshole;
        }

        let k = self.key.len();
        let v = self.value.len();
        if v < self.value_threshold as usize {
            return (k + v + 2) as u32; // meta, user_meta
        }
        return (k + 12 + 2) as u32; // 12 for value_pointer, 2 for metas.
    }

    pub(crate) fn get_key(&self) -> &Vec<u8> {
        &self.key
    }

    pub(crate) fn get_value(&self) -> Arc<Vec<u8>> {
        self.value.clone()
    }

    pub(crate) fn get_expires_at(&self) -> u64 {
        self.expires_at
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: u64) {
        self.expires_at = expires_at
    }

    pub(crate) fn get_offset(&self) -> u32 {
        self.offset
    }

    pub(crate) fn set_offset(&mut self, offset: u32) {
        self.offset = offset
    }

    pub(crate) fn get_header_len(&self) -> u32 {
        self.header_len
    }

    pub(crate) fn set_header_len(&mut self, header_len: u32) {
        self.header_len = header_len
    }

    pub(crate) fn get_meta(&self) -> u8 {
        self.meta
    }

    pub(crate) fn set_meta(&mut self, meta: u8) {
        self.meta = meta
    }

    pub(crate) fn get_user_meta(&self) -> u8 {
        self.user_meta
    }

    pub(crate) fn set_user_meta(&mut self, user_meta: u8) {
        self.user_meta = user_meta
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
            value_threshold: Default::default(),
        }
    }
}
