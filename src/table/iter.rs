use std::rc::Rc;

use log::warn;

use crate::value::ValueStruct;

use super::{Block, Table};

struct BlockIterator {
    block: Rc<Block>,
}

impl BlockIterator {
    fn set_block(&mut self, block: Rc<Block>) {
        self.block = block;
    }

    fn data(&self) -> &[u8] {
        todo!()
    }

    fn clean_data(&mut self) {
        todo!()
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
    table: Rc<Table>,
    bpos: usize,
    bi: BlockIterator,
}

impl std::iter::Iterator for Iterator {
    type Item = (Vec<u8>, ValueStruct);

    fn next(&mut self) -> Option<Self::Item> {
        if self.bpos >= self.table.offsets_len() {
            return None;
        }

        if self.bi.data().len() == 0 {
            let block = match self.table.block(self.bpos) {
                Ok(b) => b,
                Err(e) => {
                    warn!("read block from table error: {}", e);
                    return None;
                }
            };

            self.bi.set_block(block);
            return self.bi.next();
        }

        let r = self.bi.next();
        if r.is_some() {
            return r;
        }

        self.bpos += 1;
        self.bi.clean_data();
        self.next()
    }
}

impl std::iter::DoubleEndedIterator for Iterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
