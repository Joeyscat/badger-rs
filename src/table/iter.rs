use std::{cell::RefCell, rc::Rc};

use anyhow::Result;
use log::{error, warn};

use crate::{
    table::{Header, HEADER_SIZE},
    util::iter::IteratorI,
    value::ValueStruct,
};

use super::{Block, TableInner};

#[derive(Default)]
struct BlockIterator {
    data: Vec<u8>,
    idx: isize,
    base_key: Vec<u8>,
    key: Vec<u8>,
    value: Vec<u8>,
    block: Block,
    prev_overlap: u16,
}

impl BlockIterator {
    fn new(block: Block) -> BlockIterator {
        let mut bi = BlockIterator::default();
        bi.data = block.data[..block.entries_index_start as usize].to_vec();
        bi.block = block;
        bi
    }

    fn entry_offsets(&self) -> &[u32] {
        &self.block.entry_offsets
    }

    fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    fn set_idx(&mut self, idx: isize) -> Result<bool> {
        self.idx = idx;
        if self.idx < 0 || self.idx as usize >= self.entry_offsets().len() {
            return Ok(false);
        }
        let idx = idx as usize;

        if self.base_key.len() == 0 {
            let base_header = Header::decode(&self.data[0..HEADER_SIZE]);
            self.base_key =
                (self.data[HEADER_SIZE..HEADER_SIZE + base_header.diff as usize]).to_owned()
        }

        let start_offset = self.entry_offsets()[idx] as usize;
        let end_offset = if idx + 1 == self.entry_offsets().len() {
            self.data.len()
        } else {
            self.entry_offsets()[idx + 1] as usize
        };
        let entry_data = &self.data[start_offset..end_offset];
        let header = Header::decode(&entry_data[0..HEADER_SIZE]);

        if header.overlap > self.prev_overlap {
            let x = self.key[..self.prev_overlap as usize].to_vec();
            self.key = vec![];
            self.key.extend_from_slice(&x);
            self.key.extend_from_slice(
                &self.base_key[self.prev_overlap as usize..header.overlap as usize],
            );
        }
        self.prev_overlap = header.overlap;
        let value_offset = HEADER_SIZE + header.diff as usize;
        let diff_key = &entry_data[HEADER_SIZE..value_offset];
        self.key = self.key[..header.overlap as usize].to_vec();
        self.key.extend_from_slice(diff_key);
        self.value = entry_data[value_offset..].to_vec();

        Ok(true)
    }
}

impl IteratorI for BlockIterator {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        let start_index = 0;
        let entry_index = match (start_index..self.entry_offsets().len())
            .collect::<Vec<usize>>()
            .binary_search_by(|idx| {
                self.set_idx(*idx as isize).unwrap();
                self.key().cmp(key)
            }) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.set_idx(entry_index as isize)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        if !self.seek(key)? {
            return Ok(false);
        }
        self.prev()
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        self.set_idx(0)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.set_idx(self.entry_offsets().len() as isize - 1)
    }

    fn prev(&mut self) -> Result<bool> {
        self.set_idx(self.idx - 1)
    }

    fn next(&mut self) -> Result<bool> {
        self.set_idx(self.idx + 1)
    }

    fn key(&self) -> &[u8] {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &self.value
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.idx >= 0 && (self.idx as usize) < self.entry_offsets().len())
    }
}

pub struct Iterator {
    table: Rc<RefCell<TableInner>>,
    bpos: usize,
    bi: BlockIterator,
}

impl Iterator {
    pub(crate) fn new(table: Rc<RefCell<TableInner>>) -> Iterator {
        let mut iter = Iterator {
            table,
            bpos: 0,
            bi: BlockIterator::default(),
        };

        iter
    }

    pub fn value_struct(&self) -> Result<ValueStruct> {
        let data = self.value();
        ValueStruct::decode(data)
    }

    fn seek_from(&mut self, key: &[u8]) -> Result<bool> {
        self.bpos = 0;

        let t = self.table.borrow();
        let idx = match (0..t.offsets_len())
            .collect::<Vec<usize>>()
            .binary_search_by(|idx| {
                t.offsets(*idx)
                    .expect(format!("no block offset found for index: {}", idx).as_str())
                    .key()
                    .unwrap()
                    .bytes()
                    .cmp(key)
            }) {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };
        drop(t);
        if idx == 0 {
            return self.seek_helper(0, key);
        }

        if !self.seek_helper(idx, key)? {
            if idx + 1 >= self.table.borrow().offsets_len() {
                return Ok(false);
            }
            return self.seek_helper(idx + 1, key);
        }

        Ok(true)
    }

    fn seek_helper(&mut self, block_idx: usize, key: &[u8]) -> Result<bool> {
        self.bpos = block_idx;
        if self.bpos > self.table.borrow().offsets_len() {
            return Ok(false);
        }
        let block = self.table.borrow().block(self.bpos)?;
        self.bi = BlockIterator::new(block);
        self.bi.seek(key)
    }
}

impl IteratorI for Iterator {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.seek_from(key)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        if !self.seek_from(key)? {
            return Ok(false);
        }
        if self.key() != key {
            return self.prev();
        }
        Ok(true)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        if self.table.borrow().offsets_len() == 0 {
            return Ok(false);
        }
        self.bpos = 0;
        let block = self.table.borrow().block(self.bpos)?;
        self.bi = BlockIterator::new(block);
        self.bi.seek_to_first()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let num_blocks = self.table.borrow().offsets_len();
        if num_blocks == 0 {
            return Ok(false);
        }
        self.bpos = num_blocks - 1;
        let block = self.table.borrow().block(self.bpos)?;
        self.bi = BlockIterator::new(block);
        self.bi.seek_to_last()
    }

    fn prev(&mut self) -> Result<bool> {
        if self.bi.is_empty() {
            let block = match self.table.borrow().block(self.bpos) {
                Ok(b) => b,
                Err(e) => {
                    error!("read block from table error: {}", e);
                    return Ok(false);
                }
            };
            self.bi = BlockIterator::new(block);
            return self.bi.seek_to_last();
        }

        if self.bi.prev()? {
            return Ok(true);
        }

        if self.bpos == 0 {
            return Ok(false);
        }
        self.bpos -= 1;
        self.bi = BlockIterator::default();
        self.prev()
    }

    fn next(&mut self) -> Result<bool> {
        if self.bpos >= self.table.borrow().offsets_len() {
            return Ok(false);
        }

        if self.bi.is_empty() {
            let block = match self.table.borrow().block(self.bpos) {
                Ok(b) => b,
                Err(e) => {
                    warn!("read block from table error: {}", e);
                    return Ok(false);
                }
            };
            self.bi = BlockIterator::new(block);
            return self.bi.seek_to_first();
        }

        if self.bi.next()? {
            return Ok(true);
        }

        self.bpos += 1;
        self.bi = BlockIterator::default();
        self.next()
    }

    fn key(&self) -> &[u8] {
        self.bi.key()
    }

    fn value(&self) -> &[u8] {
        self.bi.value()
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.bpos < self.table.borrow().offsets_len() && self.bi.valid()?)
    }
}
