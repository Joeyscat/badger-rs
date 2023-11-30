use std::{
    collections::HashMap,
    ops::Deref,
    sync::{atomic, Arc},
};

use anyhow::Result;
use scopeguard::defer;
use tokio::{
    select, spawn,
    sync::{
        mpsc::{self, Receiver, Sender},
        Notify,
    },
};

use crate::util::MEM_ORDERING;

pub(crate) enum Mark {
    Begin(u64),
    BeginMany(Vec<u64>),
    Done(u64),
    DoneMany(Vec<u64>),
    Wait(u64, Arc<Notify>),
}

#[derive(Debug, Clone)]
pub(crate) struct WaterMark(Arc<WaterMarkInner>);

#[derive(Debug)]
pub(crate) struct WaterMarkInner {
    donw_until: atomic::AtomicU64,
    last_index: atomic::AtomicU64,
    name: String,
    mark_tx: Sender<Mark>,
}

impl Deref for WaterMark {
    type Target = WaterMarkInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WaterMark {
    pub(crate) fn new(name: String, close: Arc<Notify>) -> WaterMark {
        let (mark_tx, mark_rx) = mpsc::channel(100);

        let wm = WaterMark(Arc::new(WaterMarkInner {
            name,
            mark_tx,
            donw_until: Default::default(),
            last_index: Default::default(),
        }));

        spawn(wm.clone().process(mark_rx, close));

        wm
    }

    pub(crate) async fn begin(&self, index: u64) {
        self.last_index.store(index, MEM_ORDERING);
        self.send_mark(Mark::Begin(index)).await;
    }

    pub(crate) async fn done(&self, index: u64) {
        self.send_mark(Mark::Done(index)).await;
    }

    pub(crate) async fn send_mark(&self, mark: Mark) {
        match self.mark_tx.send(mark).await {
            Err(e) => panic!("send mark error: {}", e),
            _ => (),
        };
    }

    pub(crate) fn done_until(&self) -> u64 {
        self.donw_until.load(MEM_ORDERING)
    }

    pub(crate) fn set_done_until(&mut self, v: u64) {
        self.donw_until.store(v, MEM_ORDERING)
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.last_index.load(MEM_ORDERING)
    }

    pub(crate) async fn wait_for_mark(&self, index: u64) -> Result<()> {
        if self.done_until() >= index {
            return Ok(());
        }

        let wait = Arc::new(Notify::new());
        self.send_mark(Mark::Wait(index, Arc::clone(&wait))).await;

        wait.notified().await;

        Ok(())
    }

    async fn process(self, mut recv: Receiver<Mark>, close: Arc<Notify>) {
        defer!(close.notify_one());

        let mut waiters: HashMap<u64, Vec<Arc<Notify>>> = HashMap::new();

        let process_one =
            |index: u64, done: bool, waiters: &mut HashMap<u64, Vec<Arc<Notify>>>| todo!();

        loop {
            select! {
                _ = close.notified()=>return,
                Some(mark) = recv.recv() => {
                    match mark {
                        Mark::Begin(index) => process_one(index, false, &mut waiters),
                        Mark::BeginMany(_) => todo!(),
                        Mark::Done(index) => process_one(index, true, &mut waiters),
                        Mark::DoneMany(_) => todo!(),
                        Mark::Wait(index, waiter) => {
                            if self.done_until() >= index {
                                waiter.notify_one();
                            } else {
                                match waiters.get_mut(&index) {
                                    Some(v) => {
                                        v.push(waiter);
                                    }
                                    None => {
                                        waiters.insert(index, vec![waiter]);
                                    }
                                };
                            }
                        }
                    }
                },
            }
        }
    }
}
