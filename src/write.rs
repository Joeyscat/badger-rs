use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use log::{debug, error};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot, Notify},
    time::sleep,
};

use crate::{
    db::DB,
    entry::{Entry, ValuePointer},
    error::Error,
    util::MEM_ORDERING,
};

const KV_WRITE_CH_CAPACITY: usize = 1000;

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

impl DB {
    async fn send_to_write_tx(&self, entries: Vec<Entry>) -> Result<oneshot::Receiver<Result<()>>> {
        if self.block_writes.load(MEM_ORDERING) {
            bail!(Error::BlockedWrites)
        }

        let (result_tx, result_rx) = oneshot::channel();
        let req = WriteReq::new(entries, result_tx);
        self.write_tx.send(req).await?;

        Ok(result_rx)
    }

    pub(crate) async fn do_writes(
        self,
        mut write_rx: mpsc::Receiver<WriteReq>,
        close: Arc<Notify>,
    ) {
        let notify_send = Arc::new(Notify::new());
        let notify_recv = notify_send.clone();
        notify_send.notify_one();
        let mut write_req_buf = Vec::with_capacity(10);
        async fn write_reqs(db: DB, reqs: Vec<WriteReq>, notify_send: Arc<Notify>) {
            if let Err(e) = db.write_requests(reqs).await {
                error!("Write Request Error: {}", e);
            }
            notify_send.notify_one();
        }

        loop {
            select! {
                Some(req) = write_rx.recv() => {
                    write_req_buf.push(req);
                }
                _=close.notified() =>{
                    while let Some(req) = write_rx.recv().await {
                        write_req_buf.push(req);
                    }
                    notify_recv.notified();
                    write_reqs(self.clone(), write_req_buf, notify_send.clone()).await;
                    return ;
                }
                else=>{
                    error!("write_rx closed!!!");
                },
            }

            'a: loop {
                if write_req_buf.len() >= 3 * KV_WRITE_CH_CAPACITY {
                    notify_recv.notified();
                    spawn(write_reqs(self.clone(), write_req_buf, notify_send.clone()));
                    write_req_buf = Vec::with_capacity(10);
                    break 'a;
                }

                select! {
                    Some(req) = write_rx.recv() => {
                        write_req_buf.push(req);
                    }
                    _=notify_recv.notified() => {
                        spawn(write_reqs(self.clone(), write_req_buf, notify_send.clone()));
                        write_req_buf = Vec::with_capacity(10);
                        break 'a;
                    }
                    _=close.notified() =>{
                        while let Some(req) = write_rx.recv().await {
                            write_req_buf.push(req);
                        }
                        notify_recv.notified();
                        write_reqs(self.clone(), write_req_buf, notify_send.clone()).await;
                        return ;
                    }
                    else=>{
                        error!("write_rx closed!!!");
                    },
                }
            }
        }
    }

    async fn write_requests(&self, mut reqs: Vec<WriteReq>) -> Result<()> {
        if reqs.len() == 0 {
            return Ok(());
        }

        debug!("write_requests called. Writing to value log");
        self.vlog.write(&mut reqs).await?;

        debug!("Writing to memtable");
        let mut count = 0;
        for req in reqs {
            if req.entries_vptrs.len() == 0 {
                continue;
            }
            count += req.entries_vptrs.len();
            let mut i = 0;
            while !self.ensure_room_for_write().await? {
                i += 1;
                if i % 100 == 0 {
                    debug!("Making room for writes")
                }
                sleep(Duration::from_millis(10)).await;
            }

            self.write_to_memtable(&req).await?;
        }

        // TODO
        // debug!("Sending updates to subscribers");
        // self.publisher.send_updates(reqs)?;

        debug!("{} entries written", count);
        todo!()
    }

    async fn write_to_memtable(&self, req: &WriteReq) -> Result<()> {
        todo!()
    }

    async fn ensure_room_for_write(&self) -> Result<bool> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
