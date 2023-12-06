use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};
use prost::Message;

use crate::fb::BlockOffset;
use crate::option::{
    self,
    ChecksumVerificationMode::{self, *},
};
use crate::table::BlockIterator;
use crate::util::bloom;
use crate::util::file::open_mmap_file;
use crate::util::iter::IteratorI as _;
use crate::util::num::{bytes_to_u32, bytes_to_u32_vec};
use crate::util::{file::MmapFile, table::parse_file_id};
use crate::{fb, pb, util};

use super::{Builder, Iterator};

#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Maximum size of the table.
    pub table_size: u64,
    /// The false positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// The size of each block inside SSTable in bytes.
    pub block_size: u32,

    pub cv_mode: option::ChecksumVerificationMode,
}

impl Options {}

impl From<option::Options> for Options {
    fn from(value: option::Options) -> Self {
        Self {
            table_size: value.base_table_size as u64,
            bloom_false_positive: 0_f64,
            block_size: value.block_size,
            cv_mode: value.cv_mode,
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            table_size: Default::default(),
            bloom_false_positive: Default::default(),
            block_size: Default::default(),
            cv_mode: Default::default(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Table(Arc<TableInner>);

impl Deref for Table {
    type Target = TableInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Table {
    pub(crate) fn open(mmap_file: MmapFile, opt: Options) -> Result<Self> {
        let file = mmap_file
            .file
            .lock()
            .map_err(|e| anyhow!("accessing file with mutex: {}", e))?;
        let len = file.fd.metadata()?.len();
        let id = parse_file_id(file.filename()?)?;
        drop(file);

        let (has_bloom_filter, index_buf, index_size, _cheap) =
            TableInner::init_index(&mmap_file, len as usize)?;
        let (smallest, biggest) =
            TableInner::get_biggest_and_smallest(&index_buf, &mmap_file, opt.cv_mode)?;

        let cv_mode = opt.cv_mode.clone();
        let inner = TableInner {
            mmap_file,
            table_size: len,
            index_buf,
            _cheap,
            smallest,
            biggest,
            id,
            opt,
            index_size,
            has_bloom_filter,
        };

        let table = Table(Arc::new(inner));

        if cv_mode == OnTableRead || cv_mode == OnTableAndBlockRead {
            table.verify_checksum()?;
        }
        Ok(table)
    }

    pub(crate) async fn create<P: AsRef<Path>>(filepath: P, builder: Builder) -> Result<Self> {
        let opts = builder.opts;
        let bd = builder.done();
        let mut mfile = match open_mmap_file(
            filepath,
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .create_new(true),
            bd.size as usize,
        )
        .await
        {
            Ok((mfile, true)) => mfile, // expected
            Ok((mfile, false)) => {
                bail!("file already exists: {:?}", mfile.filename())
            }
            Err(e) => {
                bail!("failed to create file: {}", e)
            }
        };

        let written = bd.dump(&mut mfile.as_mut());
        assert_eq!(written, mfile.as_ref().len() as u32, "written != data.len");

        mfile.sync()?;

        Self::open(mfile, opts)
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn index_size(&self) -> usize {
        self.index_size
    }

    pub(crate) fn stale_data_size(&self) -> u32 {
        self.get_table_index().unwrap().stale_data_size()
    }

    pub(crate) fn smallest(&self) -> Bytes {
        self.smallest.clone()
    }

    pub(crate) fn biggest(&self) -> Bytes {
        self.biggest.clone()
    }

    pub(crate) fn has_bloom_filter(&self) -> bool {
        self.has_bloom_filter
    }

    pub(crate) fn does_not_have(&self, hash: u32) -> Result<bool> {
        if !self.has_bloom_filter {
            return Ok(false);
        }

        Ok(!bloom::Filter::may_contain(
            self.get_table_index()?
                .bloom_filter()
                .ok_or(anyhow!("Get bloom filter bytes error"))?
                .bytes(),
            hash,
        ))
    }

    pub(crate) fn max_version(&self) -> u64 {
        self._cheap.max_version
    }

    fn offsets_len(&self) -> usize {
        self._cheap.offsets_len
    }

    pub(crate) fn key_count(&self) -> u32 {
        self._cheap.key_count
    }

    pub(crate) fn on_disk_size(&self) -> u32 {
        self._cheap.on_disk_size
    }

    pub(crate) fn uncompressed_size(&self) -> u32 {
        self._cheap.uncompressed_size
    }

    pub(crate) fn bloom_filter_size(&self) -> usize {
        self._cheap.bloom_filter_len
    }

    pub(crate) fn new_iterator(&self) -> Iterator {
        Iterator::new(self.clone())
    }
}

pub(crate) struct TableInner {
    mmap_file: MmapFile,

    table_size: u64,

    index_buf: Bytes,
    _cheap: CheapIndex,

    smallest: Bytes,
    biggest: Bytes,
    id: u64,

    index_size: usize,
    has_bloom_filter: bool,

    opt: Options,
}

impl TableInner {
    pub(crate) fn block(&self, idx: isize) -> Result<Block> {
        assert!(idx >= 0);
        let idx: usize = idx as usize;
        if idx >= self.offsets_len() {
            bail!("block out of index")
        }

        let block_offset = self.offsets(idx)?;
        let block = Self::blockx(block_offset, &self.mmap_file, self.opt.cv_mode)?;

        Ok(block)
    }

    pub(crate) fn blockx(
        block_offset: BlockOffset<'_>,
        mmap_file: &MmapFile,
        cv_mode: ChecksumVerificationMode,
    ) -> Result<Block> {
        let data = mmap_file
            .read(block_offset.offset() as usize, block_offset.len() as usize)
            .map_err(|e| {
                let filename = mmap_file.filename().unwrap();
                anyhow!(
                    "failed to read from file, {} at offset {} and len {}: {}",
                    filename,
                    block_offset.offset(),
                    block_offset.len(),
                    e
                )
            })?;

        let mut read_pos = data.len() - 4;
        let checksum_len = bytes_to_u32(&data[read_pos..read_pos + 4]) as usize;

        if checksum_len > data.len() {
            bail!("invalid checksum length. Either the data is corrupted or the table options are incorrectly set")
        }

        read_pos -= checksum_len;
        let checksum = data[read_pos..read_pos + checksum_len].to_vec();

        read_pos -= 4;
        let num_entries = bytes_to_u32(&data[read_pos..read_pos + 4]) as usize;
        let entries_index_start = read_pos - (num_entries * 4);
        let entries_index_end = read_pos;

        let entry_offsets = bytes_to_u32_vec(&data[entries_index_start..entries_index_end]);

        let data = data[..read_pos + 4].to_vec();

        let block = Block {
            offset: block_offset.offset(),
            data,
            checksum,
            checksum_len: checksum_len as u16,
            entries_index_start: entries_index_start as u32,
            entry_offsets,
        };

        if cv_mode == OnBlockRead || cv_mode == OnTableAndBlockRead {
            block.verify_checksum()?;
        }

        Ok(block)
    }

    fn verify_checksum(&self) -> Result<()> {
        let index = self.get_table_index()?;
        for i in 0..index.offsets().unwrap().len() {
            let block = self.block(i as isize)?;

            if !(self.opt.cv_mode == OnBlockRead || self.opt.cv_mode == OnTableAndBlockRead) {
                block.verify_checksum()?;
            }
        }

        Ok(())
    }

    pub(crate) fn init_index(
        mmap_file: &MmapFile,
        table_size: usize,
    ) -> Result<(bool, Bytes, usize, CheapIndex)> {
        let mut read_pos = table_size;

        // read checksum len
        read_pos -= 4;
        let mut buf = [0; 4];
        buf.copy_from_slice(&Self::read_or_panic(mmap_file, read_pos, 4));
        let checksum_len = u32::from_be_bytes(buf);
        // if checksum_len < 0 {
        //     bail!("checksum.len < 0. Data corrupted")
        // }

        // read checksum
        read_pos -= checksum_len as usize;
        let buf = Self::read_or_panic(mmap_file, read_pos, checksum_len as usize);
        let x = BytesMut::from(buf.as_slice());
        let expected_checksum = pb::Checksum::decode(x)?;

        // read index size from the footer
        read_pos -= 4;
        let mut buf = [0; 4];
        buf.copy_from_slice(&Self::read_or_panic(mmap_file, read_pos, 4));
        let index_size = u32::from_be_bytes(buf) as usize;

        // read index
        read_pos -= index_size;
        let index_start = read_pos;
        let buf = Self::read_or_panic(mmap_file, read_pos, index_size);

        util::verify_checksum(&buf, expected_checksum).map_err(|e| {
            anyhow!(
                "failed to verify checksum for table {}: {}",
                mmap_file.filename().unwrap(),
                e
            )
        })?;

        let index_buf = Self::read_or_panic(&mmap_file, index_start, index_size);
        let index_buf = Bytes::from(index_buf);
        let index = Self::to_table_index(&index_buf)?;

        let cheap = CheapIndex {
            max_version: index.max_version(),
            key_count: index.key_count(),
            uncompressed_size: index.uncompressed_size(),
            on_disk_size: index.on_disk_size(),
            bloom_filter_len: index.bloom_filter().unwrap().len(),
            offsets_len: index.offsets().unwrap().len(),
        };
        let mut has_bloom_filter = false;
        if let Some(bf) = index.bloom_filter() {
            has_bloom_filter = bf.len() > 0;
        }

        Ok((has_bloom_filter, index_buf, index_size, cheap))
    }

    fn to_table_index(index_buf: &Bytes) -> Result<fb::TableIndex> {
        Ok(flatbuffers::root::<fb::TableIndex>(&index_buf)?)
    }

    fn get_table_index(&self) -> Result<fb::TableIndex> {
        Self::to_table_index(&self.index_buf)
    }

    fn get_biggest_and_smallest(
        index_buf: &Bytes,
        mmap_file: &MmapFile,
        cv_mode: ChecksumVerificationMode,
    ) -> Result<(Bytes, Bytes)> {
        let index = Self::to_table_index(index_buf)?;
        let offsets = match index.offsets() {
            Some(x) => x,
            None => panic!("get block offset fail"),
        };
        let smallest = Bytes::from(offsets.get(0).key().unwrap().bytes().to_vec());

        let last_block_idx = offsets
            .iter()
            .last()
            .ok_or_else(|| anyhow!("get last offset failed"))?;
        let last_block = Self::blockx(last_block_idx, mmap_file, cv_mode)?;
        let mut bi = BlockIterator::new(last_block);
        assert!(
            bi.seek_to_last()?,
            "BlockIterator.seek_to_last() no success"
        );

        let biggest = bi.key().to_vec().into();

        Ok((smallest, biggest))
    }

    pub(crate) fn offsets(&self, idx: usize) -> Result<fb::BlockOffset<'_>> {
        let block_offset: fb::BlockOffset<'_> = match self.get_table_index()?.offsets() {
            Some(x) => x.get(idx),
            None => panic!("get block offset fail"),
        };
        return Ok(block_offset);
    }

    pub(crate) fn offsets_len(&self) -> usize {
        self._cheap.offsets_len
    }

    fn read_or_panic(mmap_file: &MmapFile, offset: usize, size: usize) -> Vec<u8> {
        match mmap_file.read(offset, size) {
            Ok(d) => d,
            Err(e) => panic!("mfile read error: {}", e),
        }
    }
}

struct CheapIndex {
    max_version: u64,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    bloom_filter_len: usize,
    offsets_len: usize,
}

impl CheapIndex {
    fn empty() -> CheapIndex {
        CheapIndex {
            max_version: 0,
            key_count: 0,
            uncompressed_size: 0,
            on_disk_size: 0,
            bloom_filter_len: 0,
            offsets_len: 0,
        }
    }
}

#[derive(Default)]
pub(crate) struct Block {
    offset: u32,
    pub(crate) data: Vec<u8>,
    checksum: Vec<u8>,
    checksum_len: u16,
    pub(crate) entries_index_start: u32,
    pub(crate) entry_offsets: Vec<u32>,
}

impl Block {
    fn verify_checksum(&self) -> Result<()> {
        let expected_checksum = pb::Checksum::decode(BytesMut::from(self.checksum.as_slice()))?;
        util::verify_checksum(&self.data, expected_checksum)
            .map_err(|e| anyhow!("failed to verify checksum for block: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        option::{self, ChecksumVerificationMode},
        table::builder::Builder,
        test::{
            bt,
            table::{build_test_table, get_test_options, key},
        },
        util::{
            file::open_mmap_file,
            iter::IteratorI,
            kv::{key_with_ts, parse_key},
        },
        value::ValueStruct,
    };
    use rand::RngCore;
    use temp_dir::TempDir;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_iterator() {
        for n in vec![99, 100, 101] {
            let opts = get_test_options();
            let tbl = build_test_table("key", n, opts).await.unwrap();
            let mut iter = tbl.new_iterator();
            assert!(iter.seek_to_first().unwrap());
            let mut count = 0;
            while iter.valid().unwrap() {
                let v = iter.value_struct().unwrap();
                let expected_key = key_with_ts(key("key", count).into_bytes(), 0);
                assert_eq!(&expected_key, iter.key());
                let expected_value = count.to_string().into_bytes();
                assert_eq!(&expected_value, v.value.as_ref());
                count += 1;
                iter.next().unwrap();
            }
        }
    }

    #[test(tokio::test)]
    async fn test_seek_to_first() {
        for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
            let opts = get_test_options();
            let tbl = build_test_table("key", n, opts).await.unwrap();
            let mut iter = tbl.new_iterator();
            assert!(iter.seek_to_first().unwrap());
            let v = iter.value_struct().unwrap();
            assert_eq!(&Vec::from("0"), v.value.as_ref());
            assert_eq!(b'A', v.meta.bits());
        }
    }

    #[test(tokio::test)]
    async fn test_seek_to_last() {
        for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
            let opts = get_test_options();
            let tbl = build_test_table("key", n, opts).await.unwrap();
            let mut iter = tbl.new_iterator();

            assert!(iter.seek_to_last().unwrap());
            let v = iter.value_struct().unwrap();
            assert_eq!(&Vec::from((n - 1).to_string()), v.value.as_ref());
            assert_eq!(b'A', v.meta.bits());

            assert!(iter.prev().unwrap());
            let v = iter.value_struct().unwrap();
            assert_eq!(&Vec::from((n - 2).to_string()), v.value.as_ref());
            assert_eq!(b'A', v.meta.bits());
        }
    }

    #[test(tokio::test)]
    async fn test_seek() {
        let opts = get_test_options();
        let tbl = build_test_table("k", 10000, opts).await.unwrap();
        let mut iter = tbl.new_iterator();

        for (in_, valid, out) in vec![
            ("abc", true, "k0000"),
            ("k0100", true, "k0100"),
            ("k0100b", true, "k0101"),
            ("k1234", true, "k1234"),
            ("k1234b", true, "k1235"),
            ("k9999", true, "k9999"),
            ("z", false, ""),
        ] {
            assert_eq!(valid, iter.seek(&key_with_ts(Vec::from(in_), 0)).unwrap());
            assert_eq!(valid, iter.valid().unwrap());
            if valid {
                assert_eq!(out.as_bytes(), parse_key(iter.key()));
            }
        }
    }

    #[test(tokio::test)]
    async fn test_seek_for_prev() {
        let opts = get_test_options();
        let tbl = build_test_table("k", 10000, opts).await.unwrap();
        let mut iter = tbl.new_iterator();

        for (in_, valid, out) in vec![
            ("abc", false, ""),
            ("k0100", true, "k0100"),
            ("k0100b", true, "k0100"),
            ("k1234", true, "k1234"),
            ("k1234b", true, "k1234"),
            ("k9999", true, "k9999"),
            ("z", true, "k9999"),
        ] {
            println!("seek_for_prev({})", in_);
            assert_eq!(
                valid,
                iter.seek_for_prev(&key_with_ts(Vec::from(in_), 0)).unwrap()
            );
            assert_eq!(valid, iter.valid().unwrap());
            if valid {
                assert_eq!(out.as_bytes(), parse_key(iter.key()));
            }
        }
    }

    #[test(tokio::test)]
    async fn test_iterate_from_start() {
        for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
            println!("test_iterator_from_start, n={}", n);
            let opts = get_test_options();
            let tbl = build_test_table("key", n, opts).await.unwrap();
            let mut iter = tbl.new_iterator();

            assert!(iter.seek_to_first().unwrap());

            let mut count = 0;
            while iter.valid().unwrap() {
                let v = iter.value_struct().unwrap();
                assert_eq!(&count.to_string().into_bytes(), v.value.as_ref());
                assert_eq!(b'A', v.meta.bits());
                count += 1;
                iter.next().unwrap();
            }
            assert_eq!(n, count);
        }
    }

    #[test(tokio::test)]
    async fn test_iterate_from_end() {
        for n in vec![99, 100, 101, 199, 200, 250, 9999, 10000] {
            println!("test_iterator_from_end, n={}", n);
            let opts = get_test_options();
            let tbl = build_test_table("key", n, opts).await.unwrap();
            let mut iter = tbl.new_iterator();

            assert!(!iter.seek(&key_with_ts(Vec::from("zzzzzz"), 0)).unwrap()); // seek to end, an invalid element.
            assert!(!iter.valid().unwrap());
            for i in (0..n).rev() {
                assert!(iter.prev().unwrap());
                let v = iter.value_struct().unwrap();
                assert_eq!(&i.to_string().into_bytes(), v.value.as_ref());
                assert_eq!(b'A', v.meta.bits());
            }
            assert!(!iter.prev().unwrap());
        }
    }

    #[test(tokio::test)]
    async fn test_iterate_seek_and_next() {
        let opts = get_test_options();
        let tbl = build_test_table("key", 10000, opts).await.unwrap();
        let mut iter = tbl.new_iterator();

        let mut kid = 1010;
        assert!(iter
            .seek(&key_with_ts(Vec::from(key("key", kid)), 0))
            .unwrap());

        while iter.valid().unwrap() {
            assert_eq!(parse_key(iter.key()), key("key", kid).as_bytes());
            kid += 1;
            iter.next().unwrap();
        }
        assert!(!iter.prev().unwrap());
        assert_eq!(10000, kid);

        assert!(!iter
            .seek(&key_with_ts(Vec::from(key("key", 99999)), 0))
            .unwrap());

        assert!(iter
            .seek(&key_with_ts(Vec::from(key("key", -1)), 0))
            .unwrap());
        assert_eq!(parse_key(iter.key()), key("key", 0).as_bytes());
    }

    #[test(tokio::test)]
    async fn test_iterate_back_and_forth() {
        let opts = get_test_options();
        let tbl = build_test_table("key", 10000, opts).await.unwrap();
        let mut iter = tbl.new_iterator();

        assert!(iter
            .seek(&key_with_ts(Vec::from(key("key", 1010)), 0))
            .unwrap());

        assert!(iter.prev().unwrap());
        assert!(iter.prev().unwrap());
        assert_eq!(parse_key(iter.key()), key("key", 1008).as_bytes());

        assert!(iter.next().unwrap());
        assert!(iter.next().unwrap());
        assert_eq!(parse_key(iter.key()), key("key", 1010).as_bytes());

        assert!(iter
            .seek(&key_with_ts(Vec::from(key("key", 2000)), 0))
            .unwrap());
        assert_eq!(parse_key(iter.key()), key("key", 2000).as_bytes());

        assert!(iter.prev().unwrap());
        assert_eq!(parse_key(iter.key()), key("key", 1999).as_bytes());

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(parse_key(iter.key()), key("key", 0).as_bytes());
    }

    #[test(tokio::test)]
    async fn test_init_index() {
        let test_dir = TempDir::new().unwrap();
        bt::write_with_cli(test_dir.path().to_str().unwrap());

        let opt = option::Options::default();
        let (mfile, _) = open_mmap_file(
            format!("{}/000001.sst", test_dir.path().to_str().unwrap()).as_str(),
            std::fs::File::options().read(true).write(true),
            0,
        )
        .await
        .unwrap();
        let table_size = mfile.file.lock().unwrap().fd.metadata().unwrap().len();

        let (has_bloom_filter, index_buf, index_size, _cheap) =
            TableInner::init_index(&mfile, table_size as usize).unwrap();

        let table_inner = TableInner {
            mmap_file: mfile,
            table_size,
            index_buf,
            _cheap,
            smallest: Default::default(),
            biggest: Default::default(),
            id: 1,
            has_bloom_filter,
            index_size,
            opt: opt.into(),
        };
        let t = Table(Arc::new(table_inner));

        println!("table: {}", t.id());
        let index = t.get_table_index().unwrap();
        println!("key_count: {}", index.key_count());
        println!("max_version: {}", index.max_version());
        println!("on_disk_size: {}", index.on_disk_size());
        println!("stale_data_size: {}", index.stale_data_size());
        println!("uncompressed_size: {}", index.uncompressed_size());

        let offsets = index.offsets().unwrap();
        println!("offsets.len: {}", offsets.len());
        let offset = offsets.get(0);

        println!(
            "offsets[0].key: {}",
            offset.key().unwrap().bytes().escape_ascii().to_string()
        );
        println!("offsets[0].len: {}", offset.len());
        println!("offsets[0].offset: {}", offset.offset());
    }

    #[test(tokio::test)]
    async fn test_index_no_encryption_or_compression() {
        let mut opts = super::Options::default();
        opts.block_size = 4 * 1024;
        opts.bloom_false_positive = 0.01;
        opts.table_size = 30 << 20;

        test_index_with_options(opts).await;
    }

    async fn test_index_with_options(opts: super::Options) {
        let keys_count = 10000;

        let mut builder = Builder::new(opts);
        let mut block_first_keys = Vec::new();
        let mut block_count = 0;
        for i in 0..keys_count {
            let k = key_with_ts(format!("{:016x}", i).into(), i + 1);
            let vs = ValueStruct::new(format!("{}", i).as_bytes().to_vec());

            if i == 0 {
                block_first_keys.push(k.clone());
                block_count = 1;
            } else if builder.should_finish_block(&k, &vs) {
                block_first_keys.push(k.clone());
                block_count += 1;
            }
            builder.add(k, vs, 0);
        }

        let test_dir = TempDir::new().unwrap();
        let filepath = test_dir
            .path()
            .join(format!("{}.sst", rand::thread_rng().next_u32()));
        let t = match Table::create(filepath.clone(), builder).await {
            Ok(t) => t,
            Err(e) => panic!("{}", e),
        };

        let offsets_len = t.offsets_len();
        let max_version = t.max_version();

        let idx = t.get_table_index().unwrap();
        assert_eq!(block_count, offsets_len, "block count should be equal");
        for i in 0..offsets_len {
            let offset = idx.offsets().unwrap().get(i);
            assert_eq!(
                block_first_keys[i],
                offset.key().unwrap().bytes().to_vec(),
                "block first key should be equal"
            );
        }
        assert_eq!(keys_count, max_version, "max version should be equal");
        drop(t);
        std::fs::remove_file(filepath).unwrap();
    }

    #[test(tokio::test)]
    async fn test_checksum() {
        let mut opts = get_test_options();
        opts.cv_mode = ChecksumVerificationMode::OnTableAndBlockRead;

        let t = build_test_table("k", 10000, opts).await.unwrap();

        t.verify_checksum().unwrap();
    }

    #[test(tokio::test)]
    async fn test_max_version() {
        let opts = get_test_options();
        let mut b = Builder::new(opts);

        let test_dir = TempDir::new().unwrap();
        let filepath = test_dir
            .path()
            .join(format!("{}.sst", rand::thread_rng().next_u32()));
        const N: u64 = 1000;
        for i in 0..N {
            b.add(
                key_with_ts(format!("foo:{}", i).into_bytes(), i + 1),
                ValueStruct::new(vec![]),
                0,
            );
        }

        let tbl = Table::create(filepath, b).await.unwrap();
        assert_eq!(N, tbl.max_version());
    }
}
