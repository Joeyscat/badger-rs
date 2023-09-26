use std::sync::atomic;

use anyhow::{anyhow, bail, Result};
use log::error;
use tokio::{
    fs::read_dir,
    sync::{mpsc::Receiver, Mutex},
};

use crate::{
    error::Error,
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
    manifest: ManifestFile,
    // lc: LevelsController,
    // vlog: ValueLog,
    // write_ch: Receiver<Request>,
    flush_ch: Receiver<MemTable>,
    close_once: std::sync::Once,

    block_writes: atomic::AtomicU32,
    is_closed: atomic::AtomicU32,
}

async fn check_options(opt: &Options) -> Result<()> {
    if !(opt.value_log_file_size < 2 << 30 && opt.value_log_file_size >= 1 << 20) {
        anyhow::bail!(Error::ValueLogSize(opt.value_log_file_size))
    }
    Ok(())
}

pub async fn open(opt: Options) -> Result<DB> {
    check_options(&opt).await?;

    let manifest_file = open_or_create_manifest_file(&opt).await?;

    let mut _db = DB {
        mt: None,
        imm: Mutex::new(Vec::with_capacity(opt.num_memtables as usize)),
        next_mem_fid: 0,
        opt,
        manifest: manifest_file,
        flush_ch: todo!(),
        close_once: todo!(),
        block_writes: todo!(),
        is_closed: todo!(),
    };

    _db.open_mem_tables()
        .await
        .map_err(|e| anyhow!("Opening memtables error: {}", e))?;

    _db.mt = Some(Mutex::new(
        _db.new_mem_table()
            .await
            .map_err(|e| anyhow!("Cannot create memtable: {}", e))?,
    ));

    unimplemented!()
}

impl DB {
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
                std::fs::File::options().write(true),
            )
            .await?;

            if mt.sl.is_empty() {
                mt.decr_ref();
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
            std::fs::File::options().write(true).create(true),
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

impl Drop for DB {
    fn drop(&mut self) {
        todo!()
    }
}
