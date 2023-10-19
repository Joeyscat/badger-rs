use std::ops::{Div, Mul};

use integer_encoding::VarInt;
use prost::Message;

use crate::{
    fb::{self, BlockOffsetT},
    pb::{self, checksum::Algorithm::Crc32c},
    util::{
        bloom::{self, bloom_bits_per_key, Filter},
        calculate_checksum,
        kv::{parse_key, parse_ts},
    },
    value::ValueStruct,
};

use super::Options;

const PADDING: u32 = 256;

pub struct Builder {
    cur_block: Bblock,
    block_list: Vec<Bblock>,

    len_offsets: u32,
    key_hashes: Vec<u32>,
    max_version: u64,
    on_disk_size: u32,

    pub(crate) opts: Options,
}

impl Builder {
    pub fn new(opts: Options) -> Builder {
        Builder {
            cur_block: Bblock::new(opts.block_size + PADDING),
            block_list: vec![],
            len_offsets: 0,
            key_hashes: vec![],
            max_version: 0,
            on_disk_size: 0,
            opts,
        }
    }

    pub fn add(&mut self, key: Vec<u8>, value: ValueStruct, value_len: u32) {
        if self.should_finish_block(&key, &value) {
            self.finish_block();

            self.cur_block = Bblock::new(self.opts.block_size + PADDING);
        }

        self.add_helper(key, value, value_len)
    }

    /// finishes the table by appending the index.
    ///
    /// The table structure looks like
    /// +---------+------------+-----------+---------------+
    /// | Block 1 | Block 2    | Block 3   | Block 4       |
    /// +---------+------------+-----------+---------------+
    /// | Block 5 | Block 6    | Block ... | Block N       |
    /// +---------+------------+-----------+---------------+
    /// | Index   | Index Size | Checksum  | Checksum Size |
    /// +---------+------------+-----------+---------------+
    ///
    /// In case the data is encrypted, the "IV" is added to the end of the index.
    pub fn finish(self) -> Vec<u8> {
        let bd = self.done();
        if bd.size == 0 {
            return vec![];
        }
        let mut buf = vec![0; bd.size as usize];
        let written = bd.dump(&mut buf);
        assert_eq!(written, bd.size);
        buf
    }

    pub(crate) fn done(mut self) -> BuildData {
        self.finish_block();

        let mut bd = BuildData::empty();
        if self.block_list.len() == 0 {
            return bd;
        }

        let f = if self.opts.bloom_false_positive > 0_f64 {
            let bits = bloom_bits_per_key(
                self.key_hashes.len() as isize,
                self.opts.bloom_false_positive,
            );
            Filter::new(&self.key_hashes, bits)
        } else {
            Filter::empty()
        };

        let (index, data_size) = self.build_index(f.bloom());
        let checksum = self.calculate_checksum(&index);

        bd.size = data_size + (index.len() + checksum.len()) as u32 + 4 + 4;
        bd.index = index;
        bd.checksum = checksum;
        bd.block_list = self.block_list;

        bd
    }

    fn add_helper(&mut self, key: Vec<u8>, value: ValueStruct, value_len: u32) {
        self.key_hashes.push(bloom::hash(parse_key(&key)));

        let version = parse_ts(&key);
        if version > self.max_version {
            self.max_version = version;
        }
        let key_len = key.len();
        // diff_key store the difference of key with base_key.
        let diff_key = if self.cur_block.base_key.len() == 0 {
            self.cur_block.base_key.extend_from_slice(&key);
            key
        } else {
            self.key_diff(&key)
        };
        assert!(key_len - diff_key.len() <= u16::MAX as usize);
        assert!(diff_key.len() <= u16::MAX as usize);

        // store current entry's offset
        self.cur_block.entry_offsets.push(self.cur_block.end as u32);

        let overlap = (key_len - diff_key.len()) as u16;
        let diff = diff_key.len() as u16;
        // layout header(overlap,diff), diff_key, value
        self.append(overlap.to_be_bytes().to_vec());
        self.append(diff.to_be_bytes().to_vec());
        self.append(diff_key);
        self.append(value.encode_to_vec());

        self.on_disk_size += value_len;
    }

    fn key_diff(&self, key: &Vec<u8>) -> Vec<u8> {
        let mut index: usize = 0;
        let base_key = &self.cur_block.base_key;
        for i in 0..key.len() {
            if i >= base_key.len() {
                break;
            }
            if key.get(i).unwrap() != self.cur_block.base_key.get(i).unwrap() {
                index = i;
                break;
            }
        }
        key[index..].to_vec()
    }

    pub(crate) fn should_finish_block(&self, key: &Vec<u8>, value: &ValueStruct) -> bool {
        let mut entrys_offsets_size = self.cur_block.entry_offsets.len() as u32;
        if entrys_offsets_size == 0 {
            return false;
        }

        // 4: size of list
        // 8: sum64 in checksum proto
        // 4: chechsum length
        entrys_offsets_size = (entrys_offsets_size + 1) * 4 + (4 + 8 + 4);
        assert!(entrys_offsets_size < u32::MAX);

        // 6: header size for entry
        let estimated_size = self.cur_block.end
            + 6
            + key.len()
            + value.encoded_size()
            + entrys_offsets_size as usize;
        let estimated_size = estimated_size as u32;
        assert!(self.cur_block.end as u32 + estimated_size < u32::MAX);

        return estimated_size > self.opts.block_size;
    }

    ///
    /// Structure of Block.
    /// +-------------------+---------------------+--------------------+--------------+------------------+
    /// | Entry1            | Entry2              | Entry3             | Entry4       | Entry5           |
    /// +-------------------+---------------------+--------------------+--------------+------------------+
    /// | Entry6            | ...                 | ...                | ...          | EntryN           |
    /// +-------------------+---------------------+--------------------+--------------+------------------+
    /// | Block Meta(contains list of offsets used| Block Meta Size    | Block        | Checksum Size    |
    /// | to perform binary search in the block)  | (4 Bytes)          | Checksum     | (4 Bytes)        |
    /// +-----------------------------------------+--------------------+--------------+------------------+
    ///
    /// In case the data is encrypted, the "IV" is added to the end of the block.
    fn finish_block(&mut self) {
        if self.cur_block.entry_offsets.len() == 0 {
            return;
        }

        let entry_offsets_len = self.cur_block.entry_offsets.len() as u32;

        self.cur_block
            .entry_offsets
            .clone()
            .iter()
            .for_each(|off| self.append(off.encode_to_vec()));

        self.append(entry_offsets_len.encode_var_vec());

        let checksum = self.calculate_checksum(&self.cur_block.data);
        let checksum_len = checksum.len() as u32;
        self.append(checksum);
        self.append(checksum_len.encode_var_vec());

        self.block_list.push(self.cur_block.clone());

        self.len_offsets +=
            ((self.cur_block.base_key.len() as f64).div(4_f64).ceil() as u32).mul(4) + 40;
    }

    fn append(&mut self, data: Vec<u8>) {
        let add_size = data.len();
        self.cur_block.data.extend_from_slice(&data);
        self.cur_block.end += add_size;
    }

    fn calculate_checksum(&self, data: &[u8]) -> Vec<u8> {
        let cs = pb::Checksum {
            algo: Crc32c.into(),
            sum: calculate_checksum(data, Crc32c),
        };
        cs.encode_to_vec()
    }

    fn build_index(&mut self, bloom: &[u8]) -> (Vec<u8>, u32) {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let bg_off = if bloom.len() != 0 {
            builder.create_vector(&bloom)
        } else {
            flatbuffers::WIPOffset::new(0)
        };

        let (bo_list, data_size) =
            self.block_list
                .iter()
                .fold((vec![], 0), |(mut bo_list, mut data_size), bl| {
                    bo_list.push(BlockOffsetT {
                        key: Some(bl.base_key.to_vec()),
                        offset: data_size,
                        len: bl.end as u32,
                    });
                    data_size += bl.end as u32;
                    (bo_list, data_size)
                });
        self.on_disk_size += data_size;
        let x = fb::TableIndexT {
            offsets: Some(bo_list),
            bloom_filter: Some(bg_off.encode_to_vec()),
            max_version: self.max_version,
            key_count: self.key_hashes.len() as u32,
            uncompressed_size: 0,
            on_disk_size: self.on_disk_size,
            stale_data_size: 0,
        }
        .pack(&mut builder);
        builder.finish(x, None);
        let buf = builder.finished_data();

        // TODO let on_disk_size = self.on_disk_size + buf.len() as u32;

        (buf.to_vec(), data_size)
    }
}

#[derive(Debug, Clone)]
struct Bblock {
    data: Vec<u8>,
    base_key: Vec<u8>,
    entry_offsets: Vec<u32>,
    end: usize, // TODO remove??
}

impl Bblock {
    fn new(size: u32) -> Bblock {
        Bblock {
            data: Vec::with_capacity(size as usize),
            base_key: vec![],
            entry_offsets: vec![],
            end: 0,
        }
    }
}

pub(crate) struct BuildData {
    block_list: Vec<Bblock>,
    index: Vec<u8>,
    checksum: Vec<u8>,
    pub(crate) size: u32,
}

impl BuildData {
    pub(crate) fn dump(&self, buf: &mut [u8]) -> u32 {
        let mut written = 0;

        self.block_list.iter().for_each(|b| {
            buf[written..written + b.end].copy_from_slice(&b.data[..b.end]);
            written += b.end;
        });

        let len = self.index.len() as u32;
        buf[written..written + len as usize].copy_from_slice(&self.index);
        written += len as usize;
        buf[written..written + 4].copy_from_slice(&len.to_be_bytes());
        written += 4;

        let len = self.checksum.len() as u32;
        buf[written..written + len as usize].copy_from_slice(&self.checksum);
        written += len as usize;
        buf[written..written + 4].copy_from_slice(&len.to_be_bytes());
        written += 4;

        written as u32
    }

    fn empty() -> BuildData {
        BuildData {
            block_list: vec![],
            index: vec![],
            checksum: vec![],
            size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use std::env::temp_dir;

    use crate::{
        table::{Options, Table},
        util::kv::key_with_ts,
        value::ValueStruct,
    };

    use super::Builder;

    fn build_test_builder(key_counts: u32, opts: Options) -> Builder {
        let mut builder = Builder::new(opts);
        for i in 0..key_counts {
            builder.add(
                key_with_ts(format!("{:016x}", i).into(), i as u64),
                ValueStruct::new(format!("value{:04}", i).into_bytes().into()),
                0,
            );
        }
        builder
    }

    async fn build_test_table(prefix: &str, n: u32, opts: Options) -> Table {
        let opts = if opts.block_size == 0 {
            let mut temp = opts.clone();
            temp.block_size = 4 * 1024;
            temp
        } else {
            opts
        };
        assert!(n <= 10000);

        let mut kvs = Vec::with_capacity(n as usize);
        for i in 0..n {
            kvs.push((key(prefix, i), format!("{}", i)));
        }

        return build_table(kvs, opts).await;
    }

    async fn build_table(mut kvs: Vec<(String, String)>, opts: Options) -> Table {
        kvs.sort_by_key(|e| e.0.as_ptr());

        let builder = Builder::new(opts);
        let filepath = temp_dir().join(format!("{}.sst", rand::thread_rng().next_u32()));

        let table = Table::create(filepath, builder).await;
        assert!(table.is_ok());

        return table.unwrap();
    }

    fn key(prefix: &str, i: u32) -> String {
        format!("{}{:04}", prefix, i)
    }

    #[test]
    fn test_empty_builder() {
        let mut opts = Options::default();
        opts.bloom_false_positive = 0.1;
        let builder = Builder::new(opts);
        let empty_bytes: Vec<u8> = Vec::new();
        assert_eq!(empty_bytes, builder.finish(), "the builder should be empty");
    }

    #[tokio::test]
    async fn test_without_bloom_filter() {
        let mut opts = Options::default();
        opts.bloom_false_positive = 0.0;
        let builder = Builder::new(opts);
        let tab = build_test_table("p", 1000, opts).await;
        assert!(!tab.has_bloom_filter(), "shoud not has bloom filter");

        let iter = tab.new_iterator(0);
        todo!()
    }

    #[test]
    fn test_with_bloom_filter() {
        todo!()
    }

    #[test]
    fn test_dump_build_data() {
        let opts = Options::default();
        let builder = build_test_builder(100, opts);
        let bd = builder.done();
        let mut buf = vec![0; bd.size as usize];
        let written = bd.dump(&mut buf);
        assert_eq!(written, bd.size);
    }
}
