use std::path::Path;

use anyhow::{anyhow, bail, Result};
use bytes::BytesMut;
use prost::Message;

use crate::option::{self, ChecksumVerificationMode::*};
use crate::util::file::open_mmap_file;
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

pub struct Table {
    mmap_file: MmapFile,

    table_size: u64,

    index_buf: Vec<u8>,

    smallest: Vec<u8>,
    biggest: Vec<u8>,
    id: u64,

    index_start: usize,
    index_size: usize,
    has_bloom_filter: bool,

    opt: Options,
}

impl Table {
    pub fn open(mmap_file: MmapFile, opt: Options) -> Result<Self> {
        let file = mmap_file
            .file
            .lock()
            .map_err(|e| anyhow!("accessing file with mutex: {}", e))?;
        let len = file.fd.metadata()?.len();
        let id = parse_file_id(file.filename()?)?;
        drop(file);

        let cv_mode = opt.cv_mode.clone();
        let mut table = Table {
            mmap_file,
            table_size: len,
            index_buf: vec![],
            smallest: vec![],
            biggest: vec![],
            id,
            opt,
            index_start: 0,
            index_size: 0,
            has_bloom_filter: false,
        };

        table.init_biggest_and_smallest()?;

        if cv_mode == OnTableRead || cv_mode == OnTableAndBlockRead {
            table.verify_checksum()?;
        }
        Ok(table)
    }

    pub async fn create<P: AsRef<Path>>(filepath: P, builder: Builder) -> Result<Self> {
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

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn smallest(&self) -> &Vec<u8> {
        &self.smallest
    }

    pub fn biggest(&self) -> &Vec<u8> {
        &self.biggest
    }

    pub fn has_bloom_filter(&self) -> bool {
        self.has_bloom_filter
    }

    pub fn new_iterator(&self, opt: usize) -> Iterator {
        todo!()
    }

    fn init_biggest_and_smallest(&mut self) -> Result<()> {
        self.init_index()
            .map_err(|e| anyhow!("failed to read index: {}", e))?;

        let index = self.get_table_index()?;

        let block_offset: fb::BlockOffset<'_> = match index.offsets() {
            Some(x) => x.get(0),
            None => panic!("get block offset fail"),
        };

        self.smallest = block_offset.key().unwrap().bytes().to_vec();

        todo!()
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
        if let Some(bf) = index.bloom_filter() {
            self.has_bloom_filter = bf.len() > 0;
        }

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

    fn read_or_panic(&self, offset: usize, size: usize) -> Vec<u8> {
        match self.mmap_file.read(offset, size) {
            Ok(d) => d,
            Err(e) => panic!("mfile read error: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Table;
    use crate::{
        option,
        table::builder::Builder,
        test::bt,
        util::{file::open_mmap_file, kv::key_with_ts},
        value::ValueStruct,
    };
    use rand::RngCore;
    use std::env::temp_dir;
    use temp_dir::TempDir;

    #[tokio::test]
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
        let mut t = Table {
            mmap_file: mfile,
            table_size,
            index_buf: vec![],
            smallest: vec![],
            biggest: vec![],
            id: 1,
            index_start: 0,
            index_size: 0,
            has_bloom_filter: false,
            opt: opt.into(),
        };

        t.init_index().unwrap();
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

    #[tokio::test]
    async fn test_index_no_encryption_or_compression() {
        let mut opts = super::Options::default();
        opts.block_size = 4 * 1024;
        opts.bloom_false_positive = 0.01;
        opts.table_size = 30 << 20;

        test_index_with_options(opts).await;
    }

    async fn test_index_with_options(opts: super::Options) {
        let keys_count = 100000;

        let mut builder = Builder::new(opts);
        let mut block_first_keys = Vec::new();
        let mut block_count = 0;
        for i in 0..keys_count {
            let k = key_with_ts(format!("{:016x}", i).into(), i);
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

        let filepath = temp_dir().join(format!("{}.sst", rand::thread_rng().next_u32()));
        let mut tbl = match Table::create(filepath.clone(), builder).await {
            Ok(t) => t,
            Err(e) => panic!("{}", e),
        };

        tbl.init_index().unwrap();
        let idx = tbl.get_table_index().unwrap();
        let off_len = idx.offsets().unwrap().len();
        assert_eq!(block_count, off_len, "block count should be equal");
        for i in 0..off_len {
            let offset = idx.offsets().unwrap().get(i);
            assert_eq!(
                block_first_keys[i],
                offset.key().unwrap().bytes().to_vec(),
                "block first key should be equal"
            );
        }
        assert_eq!(keys_count, idx.max_version(), "max version should be equal");
        drop(tbl);
        std::fs::remove_file(filepath).unwrap();
    }
}
