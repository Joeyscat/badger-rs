use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use anyhow::{anyhow, bail, Result};
use bytes::BytesMut;
use prost::Message;

use crate::option::{self, ChecksumVerificationMode::*};
use crate::util::bloom;
use crate::util::file::open_mmap_file;
use crate::util::iter::Iterator as _;
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

pub(crate) struct Table {
    inner: Rc<RefCell<TableInner>>,
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

        let cv_mode = opt.cv_mode.clone();
        let inner = TableInner {
            mmap_file,
            table_size: len,
            index_buf: vec![],
            _cheap: CheapIndex::empty(),
            smallest: vec![],
            biggest: vec![],
            id,
            opt,
            index_start: 0,
            index_size: 0,
            has_bloom_filter: false,
        };
        let mut table = Table {
            inner: Rc::new(RefCell::new(inner)),
        };

        table.init_biggest_and_smallest()?;

        if cv_mode == OnTableRead || cv_mode == OnTableAndBlockRead {
            table.inner.borrow_mut().verify_checksum()?;
        }
        Ok(table)
    }

    pub(crate) async fn create<P: AsRef<Path>>(filepath: P, builder: Builder) -> Result<Self> {
        let opts = builder.opts;
        let bd = builder.done();
        let mfile = match open_mmap_file(
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

        let written = bd.dump(&mut mfile.data.borrow_mut());
        assert_eq!(
            written,
            mfile.data.borrow().len() as u32,
            "written != data.len"
        );

        mfile.sync()?;

        Self::open(mfile, opts)
    }

    pub(crate) fn id(&self) -> u64 {
        self.inner.borrow().id
    }

    pub(crate) fn smallest(&self) -> Vec<u8> {
        self.inner.borrow().smallest.to_vec()
    }

    pub(crate) fn biggest(&self) -> Vec<u8> {
        self.inner.borrow().biggest.to_vec()
    }

    pub(crate) fn has_bloom_filter(&self) -> bool {
        self.inner.borrow().has_bloom_filter
    }

    pub(crate) fn does_not_have(&self, hash: u32) -> Result<bool> {
        if !self.inner.borrow().has_bloom_filter {
            return Ok(false);
        }

        Ok(!bloom::Filter::may_contain(
            self.inner
                .borrow()
                .get_table_index()?
                .bloom_filter()
                .ok_or(anyhow!("Get bloom filter bytes error"))?
                .bytes(),
            hash,
        ))
    }

    fn max_version(&self) -> u64 {
        self.inner.borrow()._cheap.max_version
    }

    fn offsets_len(&self) -> usize {
        self.inner.borrow()._cheap.offsets_len
    }

    pub(crate) fn new_iterator(&self) -> Iterator {
        Iterator::new(Rc::clone(&self.inner))
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner
            .init_index()
            .map_err(|e| anyhow!("failed to read index: {}", e))?;

        let block_offset = inner.offsets(0)?;
        inner.smallest = block_offset.key().unwrap().bytes().to_vec();
        drop(inner);

        let mut it = self.new_iterator();

        if it.seek_to_last()? {
            self.inner.borrow_mut().biggest = it.key().to_vec();
            return Ok(());
        }
        bail!(
            "failed to initialize biggest for table {}",
            self.inner.borrow().mmap_file.filename()?
        )
    }
}

pub(crate) struct TableInner {
    mmap_file: MmapFile,

    table_size: u64,

    index_buf: Vec<u8>,
    _cheap: CheapIndex,

    smallest: Vec<u8>,
    biggest: Vec<u8>,
    id: u64,

    index_start: usize,
    index_size: usize,
    has_bloom_filter: bool,

    opt: Options,
}

impl TableInner {
    pub(crate) fn block(&self, idx: usize) -> Result<Block> {
        if idx >= self.offsets_len() {
            bail!("block out of index")
        }

        let block_offset = self.offsets(idx)?;
        let data = self
            .mmap_file
            .read(block_offset.offset() as usize, block_offset.len() as usize)
            .map_err(|e| {
                let filename = self.mmap_file.filename().unwrap();
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

        if self.opt.cv_mode == OnBlockRead || self.opt.cv_mode == OnTableAndBlockRead {
            block.verify_checksum()?;
        }

        Ok(block)
    }

    fn verify_checksum(&mut self) -> Result<()> {
        todo!()
    }

    fn init_index(&mut self) -> Result<()> {
        let mut read_pos = self.table_size as usize;

        // read checksum len
        read_pos -= 4;
        let mut buf = [0; 4];
        buf.copy_from_slice(&self.read_or_panic(read_pos, 4));
        let checksum_len = u32::from_be_bytes(buf);
        // if checksum_len < 0 {
        //     bail!("checksum.len < 0. Data corrupted")
        // }

        // read checksum
        read_pos -= checksum_len as usize;
        let buf = self.read_or_panic(read_pos, checksum_len as usize);
        let x = BytesMut::from(buf.as_slice());
        let expected_checksum = pb::Checksum::decode(x)?;

        // read index size from the footer
        read_pos -= 4;
        let mut buf = [0; 4];
        buf.copy_from_slice(&self.read_or_panic(read_pos, 4));
        self.index_size = u32::from_be_bytes(buf) as usize;

        // read index
        read_pos -= self.index_size;
        self.index_start = read_pos;
        let buf = self.read_or_panic(read_pos, self.index_size);

        util::verify_checksum(&buf, expected_checksum).map_err(|e| {
            anyhow!(
                "failed to verify checksum for table {}: {}",
                self.mmap_file.filename().unwrap(),
                e
            )
        })?;

        self.read_table_index_buf()?;
        let index = self.get_table_index()?;
        let cheap = CheapIndex {
            max_version: index.max_version(),
            key_count: index.key_count(),
            un_compressed_size: index.uncompressed_size(),
            on_disk_size: index.on_disk_size(),
            bloom_filter_len: index.bloom_filter().unwrap().len(),
            offsets_len: index.offsets().unwrap().len(),
        };
        if let Some(bf) = index.bloom_filter() {
            self.has_bloom_filter = bf.len() > 0;
        }
        self._cheap = cheap;

        Ok(())
    }

    /// read table index to buffer. call `get_table_index` when we need a `fb::TableIndex`
    fn read_table_index_buf(&mut self) -> Result<()> {
        let data = self.read_or_panic(self.index_start, self.index_size);
        self.index_buf = data;
        Ok(())
    }

    fn get_table_index(&self) -> Result<fb::TableIndex> {
        Ok(flatbuffers::root::<fb::TableIndex>(&self.index_buf)?)
    }

    pub(crate) fn offsets(&self, idx: usize) -> Result<fb::BlockOffset<'_>> {
        let block_offset: fb::BlockOffset<'_> = match self.get_table_index()?.offsets() {
            Some(x) => x.get(idx),
            None => panic!("get block offset fail"),
        };
        return Ok(block_offset);
    }

    fn read_or_panic(&self, offset: usize, size: usize) -> Vec<u8> {
        match self.mmap_file.read(offset, size) {
            Ok(d) => d,
            Err(e) => panic!("mfile read error: {}", e),
        }
    }

    pub(crate) fn offsets_len(&self) -> usize {
        self._cheap.offsets_len
    }
}

struct CheapIndex {
    max_version: u64,
    key_count: u32,
    un_compressed_size: u32,
    on_disk_size: u32,
    bloom_filter_len: usize,
    offsets_len: usize,
}

impl CheapIndex {
    fn empty() -> CheapIndex {
        CheapIndex {
            max_version: 0,
            key_count: 0,
            un_compressed_size: 0,
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
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::{CheapIndex, Table, TableInner};
    use crate::{
        option,
        table::builder::Builder,
        test::bt,
        util::{file::open_mmap_file, kv::key_with_ts},
        value::ValueStruct,
    };
    use rand::RngCore;
    use std::{cell::RefCell, rc::Rc};
    use temp_dir::TempDir;
    use test_log::test;

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
        let table_inner = TableInner {
            mmap_file: mfile,
            table_size,
            index_buf: vec![],
            _cheap: CheapIndex::empty(),
            smallest: vec![],
            biggest: vec![],
            id: 1,
            index_start: 0,
            index_size: 0,
            has_bloom_filter: false,
            opt: opt.into(),
        };
        let t = Table {
            inner: Rc::new(RefCell::new(table_inner)),
        };

        println!("table: {}", t.id());
        let mut inner = t.inner.borrow_mut();
        inner.init_index().unwrap();

        let index = inner.get_table_index().unwrap();
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
            let vs = ValueStruct::new(format!("{}", i).into_bytes().into());

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
        let tbl = match Table::create(filepath.clone(), builder).await {
            Ok(t) => t,
            Err(e) => panic!("{}", e),
        };

        let offsets_len = tbl.offsets_len();
        let max_version = tbl.max_version();
        let mut inner = tbl.inner.borrow_mut();
        inner.init_index().unwrap();
        let idx = inner.get_table_index().unwrap();
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
        drop(inner);
        drop(tbl);
        std::fs::remove_file(filepath).unwrap();
    }
}
