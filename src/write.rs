use crate::entry::{Entry, ValuePointer};

pub(crate) struct WriteReq {
    entries_vptrs: Vec<(Entry, ValuePointer)>,
}

impl WriteReq {
    pub(crate) fn entries_vptrs(&mut self) -> &Vec<(Entry, ValuePointer)> {
        &self.entries_vptrs
    }

    pub(crate) fn entries_vptrs_mut(&mut self) -> &mut Vec<(Entry, ValuePointer)> {
        &mut self.entries_vptrs
    }
}
