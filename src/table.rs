use std::io;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use prost::Message;

use crate::option::{self, ChecksumVerificationMode::*};
use crate::util::{file::MmapFile, table::parse_file_id};
use crate::{fb, pb, util};

pub struct Options {
    pub table_size: u64,
    pub block_size: u32,
    pub cv_mode: option::ChecksumVerificationMode,
}

impl Options {
    pub fn build_table_options(opt: option::Options) -> Self {
        Self {
            table_size: opt.base_table_size as u64,
            block_size: opt.block_size,
            cv_mode: opt.cv_mode,
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

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn smallest(&self) -> &Vec<u8> {
        &self.smallest
    }

    pub fn biggest(&self) -> &Vec<u8> {
        &self.biggest
    }

    pub fn new_iterator(&self, opt: usize) -> iter::Iterator {
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

        util::verify_checksum(buf, expected_checksum).map_err(|e| {
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

    fn read(&self, offset: usize, size: usize) -> Result<Vec<u8>> {
        let d = self.mmap_file.data.borrow();
        if offset + size > d.len() {
            return Err(anyhow::Error::new(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "early eof",
            )));
        }
        Ok(d[offset..offset + size].to_vec())
    }

    fn read_or_panic(&self, offset: usize, size: usize) -> Vec<u8> {
        match self.read(offset, size) {
            Ok(d) => d,
            Err(e) => panic!("mfile read error: {}", e),
        }
    }
}

mod iter {
    use std::rc::Rc;

    use super::Table;

    pub struct Iterator {
        table: Rc<Table>,
        bpos: usize,
    }
}

#[cfg(test)]
mod tests {
    use crate::{option::Options, util::file::open_mmap_file};

    use super::Table;

    #[tokio::test]
    async fn test_init_index() {
        let opt = Options::default();
        let (mfile, _) = open_mmap_file(
            "/tmp/x/badger-helloworld/000001.sst",
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
            opt: crate::table::Options::build_table_options(opt),
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
}
