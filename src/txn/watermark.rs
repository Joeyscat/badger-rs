use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
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
    done_until: atomic::AtomicU64,
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
            done_until: Default::default(),
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
        self.done_until.load(MEM_ORDERING)
    }

    pub(crate) fn set_done_until(&mut self, v: u64) {
        self.done_until.store(v, MEM_ORDERING)
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
        let mut heap = BinaryHeap::new();
        let mut pending: HashMap<u64, i32> = HashMap::new();

        let mut process_one =
            |index: u64, done: bool, waiters: &mut HashMap<u64, Vec<Arc<Notify>>>| {
                let delta = if done { 1 } else { -1 };
                match pending.get_mut(&index) {
                    Some(prev) => {
                        *prev += delta;
                    }
                    None => {
                        heap.push(Reverse(index));
                        pending.insert(index, delta);
                    }
                };

                let done_until = self.done_until();
                assert!(
                    done_until <= index,
                    "Name: {}, done_until: {done_until}, index: {index}",
                    &self.name
                );

                let mut until = done_until;
                while !heap.is_empty() {
                    let min = heap.peek().expect("must return a value").0;
                    if pending.get(&min).unwrap().is_positive() {
                        break;
                    }
                    heap.pop();
                    pending.remove(&min);
                    until = min;
                }

                if until != done_until {
                    assert!(self
                        .done_until
                        .compare_exchange(done_until, until, MEM_ORDERING, MEM_ORDERING)
                        .is_ok());
                }

                if until - done_until <= waiters.len() as u64 {
                    for idx in done_until + 1..=until {
                        if let Some(ns) = waiters.get(&idx) {
                            ns.iter().for_each(|i| i.notify_one());
                            waiters.remove(&idx);
                        }
                    }
                } else {
                    for idx in 0..(waiters.len() as u64).min(until + 1) {
                        let ns = waiters.get(&idx).unwrap();
                        ns.iter().for_each(|i| i.notify_one());
                        waiters.remove(&idx);
                    }
                }
            };

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
