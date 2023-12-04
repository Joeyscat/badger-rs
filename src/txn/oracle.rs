use std::{collections::HashMap, sync::Mutex};

use anyhow::{anyhow, Result};

use crate::option::Options;

use super::WaterMark;

pub(crate) struct Oracle {
    txnx: Mutex<Txnx>,

    txn_mark: WaterMark,
    read_mark: WaterMark,
}

struct Txnx {
    next_txn_ts: u64,
    committed_txns: Vec<CommittedTxn>,
}

struct CommittedTxn {
    ts: u64,
    conflict_keys: HashMap<u64, ()>,
}

impl Oracle {
    pub(crate) fn new(opt: Options) -> Self {
        todo!()
    }

    pub(crate) async fn read_ts(&self) -> Result<u64> {
        let txnx = self.txnx.lock().map_err(|e| anyhow!("txnx: {}", e))?;
        let read_ts = txnx.next_txn_ts - 1;
        self.read_mark.begin(read_ts).await;
        drop(txnx);

        assert!(self.txn_mark.wait_for_mark(read_ts).await.is_ok());
        Ok(read_ts)
    }

    pub(crate) fn next_ts(&self) -> Result<u64> {
        let txnx = self.txnx.lock().map_err(|e| anyhow!("txnx: {}", e))?;
        Ok(txnx.next_txn_ts)
    }

    pub(crate) fn incre_next_ts(&mut self) -> Result<()> {
        let txnx = self.txnx.get_mut().map_err(|e| anyhow!("txnx: {}", e))?;
        txnx.next_txn_ts += 1;
        Ok(())
    }
}
