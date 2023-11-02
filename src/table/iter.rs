use std::{cell::RefCell, rc::Rc};

use anyhow::{anyhow, bail, Result};
use log::{error, warn};

use crate::{util::iter::Iterator as Iter, value::ValueStruct};

use super::{Block, TableInner};

#[derive(Default)]
struct BlockIterator {
    data: Vec<u8>,
    idx: usize,
    idx_back: usize,
    base_key: Vec<u8>,
    key: Vec<u8>,
    entry_offsets: Vec<u32>,
    block: Option<Block>,
    prev_overlap: u16,
}

impl BlockIterator {
    fn new(block: Block) -> BlockIterator {
        let mut bi = BlockIterator::default();
        bi.block = Some(block);
        // TODO init
        bi
    }

    fn is_empty(&self) -> bool {
        self.block.is_none()
    }

    fn set_idx(&mut self, idx: usize) -> Result<()> {
        todo!()
    }
}

impl Iter for BlockIterator {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        todo!()
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        todo!()
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        todo!()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        todo!()
    }

    fn prev(&mut self) -> Result<bool> {
        todo!()
    }

    fn next(&mut self) -> Result<bool> {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn valid(&self) -> Result<bool> {
        todo!()
    }
}

pub struct Iterator {
    table: Rc<RefCell<TableInner>>,
    bpos: usize,
    bi: BlockIterator,
}

impl Iterator {
    pub(crate) fn new(table: Rc<RefCell<TableInner>>) -> Iterator {
        Iterator {
            table,
            bpos: 0,
            bi: BlockIterator::default(),
        }
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
        todo!()
    }
}

impl Iter for Iterator {
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
