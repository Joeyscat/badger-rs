use std::{
    collections::HashMap,
    ops::Deref,
    sync::{atomic, Arc},
};

use anyhow::{bail, Result};
use bytes::Bytes;
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
    vlog::ValueLog,
    write::{WriteReq, KV_WRITE_CH_CAPACITY},
};

pub struct DB(Arc<DBInner>);

impl DB {
    pub async fn new_transaction(&self, update: bool) -> Result<Txn> {
        let mut txn = Txn::new(Arc::clone(&self.0), update);

        let read_ts = self.orc.read_ts().await?;
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
    pub(crate) mt: Arc<RwLock<MemTable>>,
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
    pub(crate) bannedNamespaces: RwLock<HashMap<u64, ()>>,
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

        let (imm, mut next_mem_fid) = Self::open_mem_tables(&opt).await?;
        let mt = Self::new_mem_table(&opt, next_mem_fid).await?;
        next_mem_fid += 1;

        let max_version = Self::max_version(&mt, &imm, &lc).await?;
        let mut orc = Oracle::new(opt.clone());
        orc.set_next_txn_ts(max_version)?;
        info!("Set next_txn_ts to {}", orc.next_txn_ts()?);

        let vlog = ValueLog::open(opt.clone()).await?;
        orc.incre_next_ts()?;

        let (write_tx, write_rx) = mpsc::channel(KV_WRITE_CH_CAPACITY);
        let (flush_tx, flush_rx) = mpsc::channel(opt.num_memtables as usize);

        let db = DB(Arc::new(DBInner {
            mt: Arc::new(RwLock::new(mt)),
            lc,
            imm: RwLock::new(imm),
            next_mem_fid: next_mem_fid.into(),
            opt: opt.clone(),
            manifest: Arc::clone(&mf),
            vlog,
            write_tx,
            flush_tx,
            // close_once: todo!(),
            block_writes: false.into(),
            // is_closed: todo!(),
            orc,
            bannedNamespaces: Default::default(),
        }));

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

    async fn max_version(
        mt: &MemTable,
        imm: &Vec<Arc<MemTable>>,
        lc: &LevelsController,
    ) -> Result<u64> {
        let mut max_version = 0;

        let mut update = |v| {
            if v > max_version {
                max_version = v;
            }
        };
        update(mt.max_version());

        for mt in imm.iter() {
            update(mt.max_version());
        }

        for ti in lc.tables()? {
            update(ti.max_version());
        }

        Ok(max_version)
    }

    async fn open_mem_tables(opt: &Options) -> Result<(Vec<Arc<MemTable>>, u32)> {
        let mut imm = Vec::with_capacity(opt.num_memtables as usize);
        let mut next_mem_fid = 0;

        let dir = opt.dir.clone();
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
                opt.clone(),
                fid.to_owned(),
                std::fs::File::options().read(true).write(true),
            )
            .await?;

            if mt.sl.is_empty() {
                info!("The skiplist is empty and the corresponding mem file needs to be deleted.");
                mt.wal.delete()?;
                continue;
            }
            imm.push(Arc::new(mt));
        }
        if !fids.is_empty() {
            next_mem_fid = fids
                .last()
                .expect("Fetching last fid from a non empty vector")
                .to_owned();
        }
        next_mem_fid += 1;
        Ok((imm, next_mem_fid))
    }

    pub(crate) async fn new_mem_table(opt: &Options, next_mem_fid: u32) -> Result<MemTable> {
        match open_mem_table(
            opt.clone(),
            next_mem_fid.to_owned(),
            std::fs::File::options().read(true).write(true).create(true),
        )
        .await
        {
            Ok((mt, true)) => {
                return Ok(mt);
            }
            Ok((_, _)) => {
                bail!(
                    "File {:05}{} already exists",
                    next_mem_fid.to_owned(),
                    MEM_FILE_EXT
                )
            }
            Err(e) => {
                error!("Got error for id({}): {}", next_mem_fid.to_owned(), e);
                bail!("new_mem_table error: {}", e)
            }
        }
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
    pub(crate) async fn is_banned(&self, key: &Bytes) -> Result<()> {
        if self.opt.namespace_offset < 0 {
            return Ok(());
        }
        let off = self.opt.namespace_offset as usize;
        if key.len() <= off + 8 {
            return Ok(());
        }
        let mut bs = [0; 8];
        bs.copy_from_slice(&key[off..off + 8]);
        let num = u64::from_be_bytes(bs);
        if self.bannedNamespaces.read().await.contains_key(&num) {
            bail!(Error::BannedKey)
        }
        Ok(())
    }

    pub(crate) fn value_threshold(&self) -> usize {
        self.opt.value_threshold
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

        let (imm, mut next_mem_fid) = DB::open_mem_tables(&opt).await.unwrap();
        let mt = DB::new_mem_table(&opt, next_mem_fid).await.unwrap();
        next_mem_fid += 1;

        let max_version = DB::max_version(&mt, &imm, &lc).await.unwrap();
        let mut orc = Oracle::new(opt.clone());
        orc.set_next_txn_ts(max_version).unwrap();

        let vlog = ValueLog::open(opt.clone()).await.unwrap();
        orc.incre_next_ts().unwrap();

        let (write_tx, _) = mpsc::channel(KV_WRITE_CH_CAPACITY);
        let (flush_tx, _) = mpsc::channel(opt.num_memtables as usize);

        DB(Arc::new(DBInner {
            mt: Arc::new(RwLock::new(mt)),
            imm: RwLock::new(imm),
            next_mem_fid: next_mem_fid.into(),
            manifest,
            lc,
            vlog,
            write_tx,
            flush_tx,
            block_writes: true.into(),
            opt,
            orc,
            bannedNamespaces: Default::default(),
        }))
    }

    #[test(tokio::test)]
    async fn test_new_mem_table() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let (imm, mut next_mem_fid) = DB::open_mem_tables(&opt).await.unwrap();
        let mt = DB::new_mem_table(&opt, next_mem_fid).await.unwrap();

        println!("{}", mt);
    }

    #[test(tokio::test)]
    async fn test_open_mem_tables() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();

        let (imm, _) = DB::open_mem_tables(&opt).await.unwrap();

        println!("{}", imm.len());
    }
}
