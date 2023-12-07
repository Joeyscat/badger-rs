use anyhow::{anyhow, bail, Result};
use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use integer_encoding::{VarInt, VarIntReader};
use std::{
    cell::RefCell,
    fmt::{Debug, Display},
    io::Read,
    io::{BufRead, ErrorKind::UnexpectedEof},
    rc::Rc,
    time::UNIX_EPOCH,
};

use crate::{error::Error, manifest::CASTAGNOLI, util::hash::HashReader};

pub(crate) const MAX_HEADER_SIZE: usize = 22;
pub(crate) const CRC_SIZE: usize = 4;
pub(crate) const VP_SIZE: usize = std::mem::size_of::<ValuePointer>();

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Meta(u8);
bitflags! {
    impl Meta: u8 {
        const DELETE = 1 << 0;
        const VALUE_POINTER = 1 << 1;
        const DISCARD_EARLIER_VERSIONS = 1 << 2;
        const MERGE_ENTRY = 1 << 3;
        const TXN = 1 << 6;
        const FIN_TXN = 1 << 7;
    }
}

impl Debug for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

impl Display for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

pub(crate) fn is_deleted_or_expired(meta: Meta, expires_at: u64) -> bool {
    if meta.contains(Meta::DELETE) {
        return true;
    }
    if expires_at == 0 {
        return false;
    }

    return expires_at
        <= std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as u64;
}

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

    pub(crate) fn encode(&self) -> Vec<u8> {
        unsafe {
            let v: &[u8] = std::slice::from_raw_parts((self as *const Self) as *const u8, VP_SIZE);
            v.to_vec()
        }
    }

    pub fn decode(data: &[u8]) -> Self {
        assert_eq!(VP_SIZE, data.len());
        let s: Self = Default::default();
        unsafe {
            let v: &mut [u8] =
                std::slice::from_raw_parts_mut((&s as *const Self) as *mut u8, VP_SIZE);
            std::ptr::copy_nonoverlapping(data.as_ptr(), v.as_mut_ptr(), VP_SIZE);
        }
        s
    }
}

#[derive(Debug, Default)]
pub(crate) struct Header {
    pub key_len: u64,
    pub value_len: u64,
    pub expires_at: u64,
    pub meta: u8,
    pub user_meta: u8,
}

impl Header {
    pub(crate) fn decode_from<R: Read>(mut reader: R) -> Result<Self> {
        let mut header = Header::default();

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
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MAX_HEADER_SIZE);
        buf.resize(MAX_HEADER_SIZE, 0);
        buf.insert(0, self.meta);
        buf.insert(1, self.user_meta);
        let mut off = 2;
        off += self.key_len.encode_var(&mut buf[off..]);
        off += self.value_len.encode_var(&mut buf[off..]);
        off += self.expires_at.encode_var(&mut buf[off..]);

        buf.resize(off, 0);
        buf
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    key: Bytes,
    expires_at: u64,
    value: Bytes,
    version: u64,
    user_meta: u8,
    meta: Meta,

    offset: u32,
    header_len: u32,
    value_threshold: u32,
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self {
            key,
            value,
            ..Entry::default()
        }
    }

    pub fn delete(key: Bytes) -> Self {
        Self {
            key,
            meta: Meta::DELETE,
            ..Entry::default()
        }
    }

    pub(crate) fn skip_vlog(&self, threshole: usize) -> bool {
        self.value.len() < threshole
    }

    pub(crate) fn decode_from_reader<R: BufRead>(
        reader: Rc<RefCell<R>>,
        offset: usize,
    ) -> Result<Self> {
        let mut tee = HashReader::new(Rc::clone(&reader));
        let header = Header::decode_from(&mut tee)?;
        let header_len = tee.count();

        if header.key_len > 1 << 16 {
            bail!(Error::VLogTruncate)
        }

        let mut buf = BytesMut::zeroed((header.key_len + header.value_len) as usize);
        match tee.read_exact(&mut buf) {
            Err(e) if e.kind() == UnexpectedEof => bail!(Error::VLogTruncate),
            Err(e) => bail!(e),
            _ => {}
        };
        let (k, v) = buf.split_at(header.key_len as usize);

        let mut bufx = [0; CRC_SIZE];
        match reader.borrow_mut().read_exact(&mut bufx) {
            Err(e) if e.kind() == UnexpectedEof => bail!(Error::VLogTruncate),
            Err(e) => bail!(e),
            _ => {}
        };
        let crc = u32::from_be_bytes(bufx);
        if crc != tee.sum32() {
            bail!(Error::VLogTruncate);
        }

        // TODO optimize bytes copy
        let mut ent = Entry::new(k.to_vec().into(), v.to_vec().into());
        ent.set_expires_at(header.expires_at);
        ent.set_offset(offset as u32);
        ent.set_header_len(header_len as u32);
        ent.set_meta(Meta::from_bits_retain(header.meta));
        ent.set_user_meta(header.user_meta);

        Ok(ent)
    }

    /// encode_with_buf will encode entry to the buf
    /// layout of entry
    /// +--------+-----+-------+-------+
    /// | header | key | value | crc32 |
    /// +--------+-----+-------+-------+
    pub(crate) fn encode_with_buf(&self, buf: &mut BytesMut, _offset: usize) -> Result<u32> {
        let header = Header {
            key_len: self.key().len() as u64,
            value_len: self.value().len() as u64,
            expires_at: self.expires_at(),
            meta: self.meta().bits(),
            user_meta: self.user_meta(),
        };
        let header_buf = header.encode();

        let mut hash = CASTAGNOLI.digest();

        buf.put_slice(&header_buf);
        hash.update(&header_buf);
        buf.put_slice(&self.key());
        hash.update(&self.key());
        buf.put_slice(&self.value());
        hash.update(&self.value());

        let sum = hash.finalize();
        buf.put_u32(sum);

        let n = header_buf.len() + self.key().len() + self.value().len() + CRC_SIZE;
        Ok(n as u32)
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

    pub(crate) fn key(&self) -> &Bytes {
        &self.key
    }

    pub(crate) fn value(&self) -> &Bytes {
        &self.value
    }

    pub(crate) fn set_value<B: Into<Bytes>>(&mut self, value: B) {
        self.value = value.into()
    }

    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    pub(crate) fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    pub(crate) fn expires_at(&self) -> u64 {
        self.expires_at
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: u64) {
        self.expires_at = expires_at
    }

    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }

    pub(crate) fn set_offset(&mut self, offset: u32) {
        self.offset = offset
    }

    pub(crate) fn header_len(&self) -> u32 {
        self.header_len
    }

    pub(crate) fn set_header_len(&mut self, header_len: u32) {
        self.header_len = header_len
    }

    pub(crate) fn meta(&self) -> Meta {
        self.meta
    }

    pub(crate) fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }

    pub(crate) fn set_meta(&mut self, meta: Meta) {
        self.meta = meta
    }

    pub(crate) fn user_meta(&self) -> u8 {
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
