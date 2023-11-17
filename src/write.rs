use anyhow::{bail, Result};
use tokio::sync::{mpsc, oneshot};

use crate::{
    db::DBInner,
    entry::{Entry, ValuePointer},
    error::Error,
    util::MEM_ORDERING,
};

pub(crate) struct WriteReq {
    entries_vptrs: Vec<(Entry, ValuePointer)>,
    result_tx: Option<oneshot::Sender<Result<()>>>,
}

impl WriteReq {
    pub(crate) fn new(mut entries: Vec<Entry>, send_result: oneshot::Sender<Result<()>>) -> Self {
        let entries_vptrs = entries
            .drain(..)
            .map(|e| (e, ValuePointer::default()))
            .collect();

        Self {
            entries_vptrs,
            result_tx: Some(send_result),
        }
    }

    pub(crate) fn entries_vptrs(&mut self) -> &Vec<(Entry, ValuePointer)> {
        &self.entries_vptrs
    }

    pub(crate) fn entries_vptrs_mut(&mut self) -> &mut Vec<(Entry, ValuePointer)> {
        &mut self.entries_vptrs
    }
}

impl DBInner {
    async fn send_to_write_tx(&self, entries: Vec<Entry>) -> Result<oneshot::Receiver<Result<()>>> {
        if self.block_writes.load(MEM_ORDERING) {
            bail!(Error::BlockedWrites)
        }

        let (result_tx, result_rx) = oneshot::channel();
        let req = WriteReq::new(entries, result_tx);
        self.write_tx.send(req).await?;

        Ok(result_rx)
    }

    pub(crate) async fn do_writes(&self, write_rx: mpsc::Receiver<WriteReq>) {
        todo!()
    }

    fn write_requests(&self, reqs: &Vec<WriteReq>) -> Result<()> {
        todo!()
    }

    fn write_to_memtable(&self, req: &WriteReq) -> Result<()> {
        todo!()
    }
}
