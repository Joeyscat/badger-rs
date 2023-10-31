use std::{cell::RefCell, rc::Rc};

use log::{error, warn};

use crate::value::ValueStruct;

use super::{Block, TableInner};

struct BlockIterator {
    block: Option<Block>,
}

impl BlockIterator {
    fn empty() -> BlockIterator {
        BlockIterator { block: None }
    }

    fn new(block: Block) -> BlockIterator {
        BlockIterator { block: Some(block) }
    }

    fn data(&self) -> &[u8] {
        todo!()
    }

    fn clean_data(&mut self) {
        todo!()
    }

    fn is_empty(&self) -> bool {
        self.block.is_none()
    }
}

impl std::iter::Iterator for BlockIterator {
    type Item = (Vec<u8>, ValueStruct);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl std::iter::DoubleEndedIterator for BlockIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct Iterator {
    table: Rc<RefCell<TableInner>>,
    bpos: usize,
    bpos_back: usize,
    bi: BlockIterator,
    bi_back: BlockIterator,
}

impl Iterator {
    pub(crate) fn new(table: Rc<RefCell<TableInner>>) -> Iterator {
        let mut bpos_back = table.borrow().offsets_len();
        if bpos_back != 0 {
            bpos_back -= 1;
        }
        Iterator {
            table,
            bpos: 0,
            bpos_back,
            bi: BlockIterator::empty(),
            bi_back: BlockIterator::empty(),
        }
    }
}

impl std::iter::Iterator for Iterator {
    type Item = (Vec<u8>, ValueStruct);

    fn next(&mut self) -> Option<Self::Item> {
        if self.bpos >= self.table.borrow().offsets_len() {
            return None;
        }

        if self.bi.is_empty() {
            let block = match self.table.borrow().block(self.bpos) {
                Ok(b) => b,
                Err(e) => {
                    warn!("read block from table error: {}", e);
                    return None;
                }
            };
            self.bi = BlockIterator::new(block);
            return self.bi.next();
        }

        let r = self.bi.next();
        if r.is_some() {
            return r;
        }

        self.bpos += 1;
        self.bi = BlockIterator::empty();
        self.next()
    }
}

impl std::iter::DoubleEndedIterator for Iterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.bi_back.is_empty() {
            let block = match self.table.borrow().block(self.bpos_back) {
                Ok(b) => b,
                Err(e) => {
                    error!("read block from table error: {}", e);
                    return None;
                }
            };
            self.bi_back = BlockIterator::new(block);
            return self.bi_back.next_back();
        }

        let r = self.bi_back.next_back();
        if r.is_some() {
            return r;
        }

        if self.bpos_back == 0 {
            return None;
        }
        self.bpos_back -= 1;
        self.bi_back = BlockIterator::empty();
        self.next_back()
    }
}
