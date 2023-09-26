use std::{
    path::{Path, PathBuf},
    sync::atomic,
    sync::atomic::Ordering::Relaxed,
};

use anyhow::{anyhow, bail, Result};
use log::error;
use rand::seq::SliceRandom;
use tokio::{fs::remove_file, sync::Mutex};

use crate::{option::Options, pb, skiplist, vlog::VLOG_HEADER_SIZE};

pub const MEM_FILE_EXT: &str = ".mem";

pub struct MemTable {
    pub sl: crossbeam_skiplist::SkipList<Vec<u8>, skiplist::ValueStruct>,
    guard: crossbeam_epoch::Guard,
    pub wal: LogFile,
    // max_version: u64,
    // opt: Options,
    buf: bytes::BytesMut,
}

pub async fn open_mem_table(
    opt: Options,
    fid: u32,
    oopt: &std::fs::OpenOptions,
) -> Result<(MemTable, bool)> {
    let wal = LogFile::open(opt.clone(), fid, oopt, 2 * opt.mem_table_size).await?;

    let mt = MemTable {
        sl: crossbeam_skiplist::SkipList::new(crossbeam_epoch::Collector::new()),
        guard: crossbeam_epoch::pin(),
        wal: wal,
        // max_version: 0,
        // opt: opt,
        buf: bytes::BytesMut::new(),
    };

    todo!()
}

pub async fn open_mmap_file<P: AsRef<Path>>(
    path: P,
    oopt: &std::fs::OpenOptions,
    sz: u32,
) -> Result<(MmapFile, bool)> {
    let mut new_file = false;
    let mut ooptx = oopt.clone();
    let fd = ooptx.truncate(true).open(&path)?;
    let meta = fd.metadata()?;

    let mut file_size = meta.len();
    if sz > 0 && file_size == 0 {
        fd.set_len(sz as u64)
            .map_err(|e| anyhow!("Truncate error: {}", e))?;
        file_size = sz as u64;
        new_file = true;
    }

    let buf = unsafe { memmap2::MmapMut::map_mut(&fd)? };

    Ok((
        MmapFile {
            data: buf,
            file: Mutex::new(Filex {
                fd: fd,
                path: path.as_ref().to_path_buf(),
            }),
        },
        new_file,
    ))
}

impl MemTable {
    pub fn decr_ref(&self) {
        todo!()
    }
}

pub struct LogFile {
    mmap_file: MmapFile,
    path: String,
    fid: u32,
    size: atomic::AtomicU32,
    // data_key: pb::DataKey,
    base_iv: Vec<u8>,
    write_at: u32,
    opt: Options,
}

impl LogFile {
    pub async fn open(
        opt: Options,
        fid: u32,
        oopt: &std::fs::OpenOptions,
        file_size: u32,
    ) -> Result<Self> {
        let path = PathBuf::from(&opt.dir).join(format!("{:05}{}", fid, MEM_FILE_EXT));

        let (mmapfile, new_file) = open_mmap_file(&path, oopt, file_size)
            .await
            .map_err(|e| anyhow!("Open mmap file error: {}", e))?;
        let mut lf = LogFile {
            mmap_file: mmapfile,
            path: path.to_string_lossy().to_string(),
            fid,
            size: Default::default(),
            // data_key: Default::default(),
            base_iv: Vec::with_capacity(12),
            write_at: Default::default(),
            opt,
        };

        if new_file {
            if let Err(e) = lf.bootstrap() {
                let _ = remove_file(path).await;
                bail!(e)
            }
            lf.size.store(VLOG_HEADER_SIZE, Relaxed);
        }
        lf.size.store(lf.mmap_file.data.len() as u32, Relaxed);

        if lf.size.load(Relaxed) < VLOG_HEADER_SIZE {
            return Ok(lf);
        }

        let mut buf = [0; 8];
        buf.copy_from_slice(&lf.mmap_file.data[..8]);
        if u64::from_be_bytes(buf) != 0 {
            bail!("Unsupport encryption yet, found keyid not 0")
        }
        lf.base_iv.resize(12, 0);
        lf.base_iv.copy_from_slice(&lf.mmap_file.data[8..20]);

        return Ok(lf);
    }

    fn bootstrap(&mut self) -> Result<()> {
        let mut buf = [0; 20];
        let mut rng = rand::thread_rng();
        buf.shuffle(&mut rng);
        self.mmap_file.data[..20].copy_from_slice(&buf);

        self.zero_next_entry();

        Ok(())
    }

    fn zero_next_entry(&mut self) {
        let start = self.write_at as usize;
        let mut end = (self.write_at + 22) as usize;

        if end > self.mmap_file.data.len() {
            end = self.mmap_file.data.len();
        }
        if end - start <= 0 {
            return;
        }

        self.mmap_file.data[start..end].fill(0_u8);
    }
}

pub struct MmapFile {
    data: memmap2::MmapMut,
    file: Mutex<Filex>,
}

pub struct Filex {
    fd: std::fs::File,
    path: PathBuf,
}

impl Drop for MmapFile {
    fn drop(&mut self) {
        let p = &self.file.blocking_lock().path.clone();

        if let Err(e) = self.file.get_mut().fd.set_len(0) {
            error!("Truncate file({:#?}) error: {}", p, e);
        }

        if let Err(e) = std::fs::remove_file(&self.file.blocking_lock().path) {
            error!("Remove file({:#?}) error: {}", p, e);
        }
    }
}
