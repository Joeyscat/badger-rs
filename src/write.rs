use std::{mem::replace, sync::Arc};

use anyhow::{anyhow, bail, Result};
use log::{debug, error};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot, Notify},
};

use crate::{
    db::DB,
    entry::{Entry, Meta, ValuePointer},
    error::Error,
    memtable::MemTable,
    util::MEM_ORDERING,
};

pub(crate) const KV_WRITE_CH_CAPACITY: usize = 1000;

pub(crate) struct WriteReq {
    entries_vptrs: Vec<(Entry, ValuePointer)>,
    result: Result<()>,
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
            result: Ok(()),
            result_tx: Some(send_result),
        }
    }

    pub(crate) fn entries_vptrs(&self) -> &Vec<(Entry, ValuePointer)> {
        &self.entries_vptrs
    }

    pub(crate) fn entries_vptrs_mut(&mut self) -> &mut Vec<(Entry, ValuePointer)> {
        &mut self.entries_vptrs
    }

    pub(crate) fn set_result(&mut self, result: Result<()>) {
        self.result = result;
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
        let done = |e: anyhow::Error, reqs: &mut Vec<WriteReq>| {
            let ex = Arc::new(e);
            reqs.iter_mut().for_each(|r| {
                r.set_result(Err(anyhow!(Arc::clone(&ex))));
            });
            ex
        };

        debug!("write_requests called. Writing to value log");
        if let Err(e) = self.vlog.write(&mut reqs).await {
            bail!(done(e, &mut reqs));
        };

        debug!("Writing to memtable");
        let mut count = 0;
        let mut err = None;
        for req in reqs.iter_mut() {
            if req.entries_vptrs.len() == 0 {
                continue;
            }
            count += req.entries_vptrs.len();

            if let Err(e) = self.ensure_room_for_write().await {
                err = Some(e);
                break;
            }

            if let Err(e) = self.write_to_memtable(req).await {
                err = Some(e);
                break;
            }
        }
        if let Some(e) = err {
            bail!(done(e, &mut reqs));
        }

        // TODO
        // debug!("Sending updates to subscribers");
        // self.publisher.send_updates(reqs)?;

        debug!("{} entries written", count);
        Ok(())
    }

    async fn write_to_memtable(&self, req: &mut WriteReq) -> Result<()> {
        let mut mt = self.mt.as_ref().unwrap().write().await;
        for (ent, vp) in req.entries_vptrs.iter_mut() {
            if let Err(e) = if ent.skip_vlog(self.opt.value_threshold) {
                ent.meta_mut().remove(Meta::VALUE_POINTER);
                mt.put(ent).await
            } else {
                ent.meta_mut().insert(Meta::VALUE_POINTER);
                ent.set_value(vp.encode());
                mt.put(ent).await
            } {
                bail!("Write to mem_table error: {}", e)
            };
        }

        if self.opt.sync_writes {
            mt.sync_wal()?;
        }

        Ok(())
    }

    async fn ensure_room_for_write(&self) -> Result<()> {
        let mt = self.mt.as_ref().unwrap();
        if !mt.read().await.is_full() {
            return Ok(());
        }
        debug!("Making room for writes");

        let mt_new = self.new_mem_table().await?;
        let mut mt = mt.write().await;
        let mt = replace(&mut *mt, mt_new);
        let mt = Arc::new(mt);

        self.flush_tx.send(Arc::clone(&mt)).await?;
        self.imm.write().await.push(Arc::clone(&mt));

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
