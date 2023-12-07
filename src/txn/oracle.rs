use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, bail, Result};
use tokio::sync::Notify;

use crate::option::Options;

use super::WaterMark;

pub(crate) struct Oracle {
    txnx: Mutex<Txnx>,

    txn_mark: WaterMark,
    pub(crate) read_mark: WaterMark,

    close: Arc<Notify>,
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
    pub(crate) fn new(_opt: Options) -> Self {
        let close = Arc::new(Notify::new());
        let txn_mark_close_rx = Arc::clone(&close);
        let read_mark_close_rx = Arc::clone(&close);
        let txn_mark = WaterMark::new("badger.TxnTimestamp".to_string(), txn_mark_close_rx);
        let read_mark = WaterMark::new("badger.PendingReads".to_string(), read_mark_close_rx);

        Self {
            txnx: Mutex::new(Txnx {
                next_txn_ts: 0,
                committed_txns: vec![],
            }),
            txn_mark,
            read_mark,
            close,
        }
    }

    pub(crate) fn stop(&self) {
        self.close.notify_waiters()
    }

    pub(crate) async fn read_ts(&self) -> Result<u64> {
        let txnx = self.txnx.lock().map_err(|e| anyhow!("txnx: {}", e))?;
        let read_ts = txnx.next_txn_ts - 1;
        self.read_mark.begin(read_ts).await;
        drop(txnx);

        assert!(self.txn_mark.wait_for_mark(read_ts).await.is_ok());
        Ok(read_ts)
    }

    pub(crate) fn next_txn_ts(&self) -> Result<u64> {
        let txnx = self.txnx.lock().map_err(|e| anyhow!("txnx: {}", e))?;
        Ok(txnx.next_txn_ts)
    }

    pub(crate) fn set_next_txn_ts(&mut self, v: u64) -> Result<()> {
        match self.txnx.get_mut() {
            Ok(e) => Ok(e.next_txn_ts = v),
            Err(e) => bail!("{}", e),
        }
    }

    pub(crate) fn incre_next_ts(&mut self) -> Result<()> {
        let txnx = self.txnx.get_mut().map_err(|e| anyhow!("txnx: {}", e))?;
        txnx.next_txn_ts += 1;
        Ok(())
    }
}
