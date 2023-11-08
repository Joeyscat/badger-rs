use std::{cell::RefCell, rc::Rc};

use anyhow::{anyhow, bail, Result};
use log::{error, info};
use tokio::{fs::read_dir, sync::Mutex};

use crate::{
    error::Error,
    level::LevelsController,
    manifest::{open_or_create_manifest_file, ManifestFile},
    memtable::{open_mem_table, MemTable, MEM_FILE_EXT},
    option::Options,
    txn::Txn,
};

pub struct DB {
    // dir_lock_guard: x,
    // value_dir_guard: x,

    // closers: closers,
    mt: Option<Mutex<MemTable>>,
    imm: Mutex<Vec<MemTable>>,

    next_mem_fid: u32,

    opt: Options,
    manifest: Rc<RefCell<ManifestFile>>,
    lc: LevelsController,
    // vlog: ValueLog,
    // write_ch: Receiver<Request>,
    // flush_ch: Receiver<MemTable>,
    // close_once: std::sync::Once,

    // block_writes: atomic::AtomicU32,
    // is_closed: atomic::AtomicU32,
}

impl DB {
    pub async fn open(opt: Options) -> Result<Self> {
        Self::check_options(&opt)?;

        let mf = open_or_create_manifest_file(&opt).await?;
        let lc = LevelsController::new(
            opt.clone(),
            Rc::clone(&Rc::new(mf.manifest.lock().await.clone())),
        )
        .await?;
        let mf = Rc::new(RefCell::new(mf));

        let mut db = DB {
            mt: None,
            lc,
            imm: Mutex::new(Vec::with_capacity(opt.num_memtables as usize)),
            next_mem_fid: 0,
            opt: opt.clone(),
            manifest: Rc::clone(&mf),
            // flush_ch: todo!(),
            // close_once: todo!(),
            // block_writes: todo!(),
            // is_closed: todo!(),
        };

        db.open_mem_tables()
            .await
            .map_err(|e| anyhow!("Opening memtables error: {}", e))?;

        db.mt = Some(Mutex::new(
            db.new_mem_table()
                .await
                .map_err(|e| anyhow!("Cannot create memtable: {}", e))?,
        ));

        Ok(db)
    }

    fn check_options(opt: &Options) -> Result<()> {
        if !(opt.value_log_file_size < 2 << 30 && opt.value_log_file_size >= 1 << 20) {
            anyhow::bail!(Error::ValueLogSize(opt.value_log_file_size))
        }
        Ok(())
    }

    pub fn new_transaction(&self, _update: bool) -> Result<Txn> {
        unimplemented!()
    }

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

impl DB {
    async fn open_mem_tables(&mut self) -> Result<()> {
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
            self.imm.get_mut().push(mt);
        }
        if !fids.is_empty() {
            self.next_mem_fid = fids
                .last()
                .expect("Fetching last fid from a non empty vector")
                .to_owned();
        }
        self.next_mem_fid += 1;
        Ok(())
    }

    async fn new_mem_table(&mut self) -> Result<MemTable> {
        match open_mem_table(
            self.opt.clone(),
            self.next_mem_fid,
            std::fs::File::options().read(true).write(true).create(true),
        )
        .await
        {
            Ok((mt, true)) => {
                self.next_mem_fid += 1;
                return Ok(mt);
            }
            Ok((_, _)) => {
                bail!(
                    "File {:05}{} already exists",
                    self.next_mem_fid,
                    MEM_FILE_EXT
                )
            }
            Err(e) => {
                error!("Got error for id({}): {}", self.next_mem_fid, e);
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
    use super::*;
    use crate::test::bt;
    use temp_dir::TempDir;
    use test_log::test;

    async fn create_test_db(opt: Options) -> DB {
        let mf = open_or_create_manifest_file(&opt).await.unwrap();
        let lc = LevelsController::new(
            opt.clone(),
            Rc::clone(&&Rc::new(mf.manifest.lock().await.clone())),
        )
        .await
        .unwrap();
        let manifest = Rc::new(RefCell::new(mf));
        DB {
            mt: None,
            imm: Mutex::new(Vec::with_capacity(opt.num_memtables as usize)),
            next_mem_fid: 0,
            opt,
            manifest,
            lc,
        }
    }

    #[test(tokio::test)]
    async fn test_new_mem_table() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let mut db = create_test_db(opt).await;
        db.next_mem_fid = 1;

        let mt = db.new_mem_table().await.unwrap();

        println!("{}", mt);
    }

    #[test(tokio::test)]
    async fn test_open_mem_tables() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let mut db = create_test_db(opt).await;

        db.open_mem_tables().await.unwrap();

        println!("{}", &db.imm.lock().await.len());
    }
}
