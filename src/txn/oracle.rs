use std::sync::Mutex;

use crate::option::Options;

pub(crate) struct Oracle{
    next_txn_ts: Mutex<u64>,
}

impl Oracle {
    pub(crate)fn new(opt: Options) ->Self{
        todo!()
    }

    pub(crate)fn read_ts(&self) ->u64 {
        todo!()
    }

    pub(crate)fn next_ts(&self)->u64 {
        todo!()
    }
}