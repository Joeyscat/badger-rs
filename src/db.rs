use std::{
    ops::Deref,
    sync::{atomic, Arc},
};

use anyhow::{anyhow, bail, Result};
use log::{error, info};
use tokio::{
    fs::read_dir,
    spawn,
    sync::{
        mpsc::{self, Sender},
        Notify, RwLock,
    },
};

use crate::{
    error::Error,
    level::level::LevelsController,
    manifest::{open_or_create_manifest_file, ManifestFile},
    memtable::{open_mem_table, MemTable, MEM_FILE_EXT},
    option::Options,
    txn::{Oracle, Txn},
    util::MEM_ORDERING,
    vlog::ValueLog,
    write::{WriteReq, KV_WRITE_CH_CAPACITY},
};

pub struct DB(Arc<DBInner>);

impl DB {
    pub fn new_transaction(&self, update: bool) -> Result<Txn> {
        let mut txn = Txn::new(Arc::clone(&self.0), update);

        let read_ts = self.orc.read_ts();
        txn.set_read_ts(read_ts);

        Ok(txn)
    }
}

impl Deref for DB {
    type Target = DBInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct DBInner {
    // dir_lock_guard: x,
    // value_dir_guard: x,

    // closers: closers,
    pub(crate) mt: Option<Arc<RwLock<MemTable>>>,
    pub(crate) imm: RwLock<Vec<Arc<MemTable>>>,

    pub(crate) next_mem_fid: atomic::AtomicU32,

    pub(crate) opt: Options,
    pub(crate) manifest: Arc<RwLock<ManifestFile>>,
    pub(crate) lc: LevelsController,
    pub(crate) vlog: ValueLog,
    pub(crate) write_tx: Sender<WriteReq>,
    pub(crate) flush_tx: Sender<Arc<MemTable>>,
    // close_once: std::sync::Once,
    pub(crate) block_writes: atomic::AtomicBool,
    // is_closed: atomic::AtomicBool,
    pub(crate) orc: Oracle,
}

impl Clone for DB {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl DB {
    pub async fn open(opt: Options) -> Result<DB> {
        Self::check_options(&opt)?;

        let mf = open_or_create_manifest_file(&opt).await?;
        let mm = mf.manifest.lock().await;
        let lc = LevelsController::new(opt.clone(), &mm).await?;
        drop(mm);
        let mf = Arc::new(RwLock::new(mf));

        let vlog = ValueLog::open(opt.clone()).await?;

        let (write_tx, write_rx) = mpsc::channel(KV_WRITE_CH_CAPACITY);
        let (flush_tx, flush_rx) = mpsc::channel(opt.num_memtables as usize);

        let mut inner = DBInner {
            mt: None,
            lc,
            imm: RwLock::new(Vec::with_capacity(opt.num_memtables as usize)),
            next_mem_fid: Default::default(),
            opt: opt.clone(),
            manifest: Arc::clone(&mf),
            vlog,
            write_tx,
            flush_tx,
            // close_once: todo!(),
            block_writes: false.into(),
            // is_closed: todo!(),
            orc: Oracle::new(opt.clone()),
        };

        inner
            .open_mem_tables()
            .await
            .map_err(|e| anyhow!("Opening memtables error: {}", e))?;

        inner.mt = Some(Arc::new(RwLock::new(
            inner
                .new_mem_table()
                .await
                .map_err(|e| anyhow!("Cannot create memtable: {}", e))?,
        )));

        let inner = Arc::new(inner);
        let db = DB(inner);

        let write_close_send = Arc::new(Notify::new());
        let write_close_recv = write_close_send.clone();
        spawn(db.clone().do_writes(write_rx, write_close_recv));

        // TODO flush memtable

        Ok(db)
    }

    fn check_options(opt: &Options) -> Result<()> {
        if !(opt.value_log_file_size < 2 << 30 && opt.value_log_file_size >= 1 << 20) {
            anyhow::bail!(Error::ValueLogSize(opt.value_log_file_size))
        }
        Ok(())
    }
}

impl DBInner {
    pub fn close(self) -> Result<()> {
        unimplemented!()
    }

    pub fn update(&self, _f: fn(txn: &Txn) -> Result<()>) -> Result<()> {
        unimplemented!()
    }

    pub fn view(&self, _f: fn(txn: &Txn) -> Result<()>) -> Result<()> {
        unimplemented!()
    }
}

impl DBInner {
    async fn open_mem_tables(&self) -> Result<()> {
        let dir = self.opt.dir.clone();
        let mut entries = read_dir(dir.as_str()).await?;
        let mut fids = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let filename = entry
                .file_name()
                .into_string()
                .expect("String convert fail");
            if !filename.ends_with(MEM_FILE_EXT) {
                continue;
            }
            let fid = filename
                .strip_suffix(MEM_FILE_EXT)
                .expect(&format!("Strip suffix for {} error", filename))
                .parse::<u32>()?;

            fids.push(fid);
        }

        fids.sort();
        for fid in &fids {
            let (mt, _) = open_mem_table(
                self.opt.clone(),
                fid.to_owned(),
                std::fs::File::options().read(true).write(true),
            )
            .await?;

            if mt.sl.is_empty() {
                info!("The skiplist is empty and the corresponding mem file needs to be deleted.");
                mt.wal.delete()?;
                continue;
            }
            self.imm.write().await.push(Arc::new(mt));
        }
        if !fids.is_empty() {
            self.next_mem_fid.store(
                fids.last()
                    .expect("Fetching last fid from a non empty vector")
                    .to_owned(),
                MEM_ORDERING,
            );
        }
        self.next_mem_fid.fetch_add(1, MEM_ORDERING);
        Ok(())
    }

    pub(crate) async fn new_mem_table(&self) -> Result<MemTable> {
        match open_mem_table(
            self.opt.clone(),
            self.next_mem_fid.load(MEM_ORDERING),
            std::fs::File::options().read(true).write(true).create(true),
        )
        .await
        {
            Ok((mt, true)) => {
                self.next_mem_fid.fetch_add(1, MEM_ORDERING);
                return Ok(mt);
            }
            Ok((_, _)) => {
                bail!(
                    "File {:05}{} already exists",
                    self.next_mem_fid.load(MEM_ORDERING),
                    MEM_FILE_EXT
                )
            }
            Err(e) => {
                error!(
                    "Got error for id({}): {}",
                    self.next_mem_fid.load(MEM_ORDERING),
                    e
                );
                bail!("new_mem_table error: {}", e)
            }
        }
    }
}

// impl Display for DB {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

//     }
// }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::test::bt;
    use temp_dir::TempDir;
    use test_log::test;

    async fn create_test_db(opt: Options) -> DB {
        let mf = open_or_create_manifest_file(&opt).await.unwrap();
        let mm = mf.manifest.lock().await;
        let lc = LevelsController::new(opt.clone(), &mm).await.unwrap();
        drop(mm);
        let manifest = Arc::new(RwLock::new(mf));
        let (write_tx, _) = mpsc::channel(KV_WRITE_CH_CAPACITY);
        let (flush_tx, _) = mpsc::channel(opt.num_memtables as usize);
        let orc = Oracle::new(opt.clone());
        DB(Arc::new(DBInner {
            mt: None,
            imm: RwLock::new(Vec::with_capacity(opt.num_memtables as usize)),
            next_mem_fid: Default::default(),
            manifest,
            lc,
            vlog: ValueLog::open(opt.clone()).await.unwrap(),
            write_tx,
            flush_tx,
            block_writes: true.into(),
            opt,
            orc,
        }))
    }

    #[test(tokio::test)]
    async fn test_new_mem_table() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let db = create_test_db(opt).await;
        db.next_mem_fid.store(1, MEM_ORDERING);

        let mt = db.new_mem_table().await.unwrap();

        println!("{}", mt);
    }

    #[test(tokio::test)]
    async fn test_open_mem_tables() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let db = create_test_db(opt).await;

        db.open_mem_tables().await.unwrap();

        println!("{}", &db.imm.read().await.len());
    }
}
