use std::rc::Rc;


use crate::value::ValueStruct;

use super::Table;

pub struct Iterator {
    table: Rc<Table>,
    bpos: usize,
}

impl std::iter::Iterator for Iterator {
    type Item = (Vec<u8>, ValueStruct);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
