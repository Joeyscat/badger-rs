use crate::item::Item;

pub struct IteratorOptions {}

pub struct Iterator {}

impl std::iter::Iterator for Iterator {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
