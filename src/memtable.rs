use std::{
    cell::RefCell,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    rc::Rc,
    sync::atomic,
    sync::atomic::Ordering::Relaxed,
};

use anyhow::{anyhow, bail, Result};
use log::{debug, error};
use rand::seq::SliceRandom;
use tokio::{fs::remove_file, sync::Mutex};

use crate::{
    entry::Entry,
    entry::{ValuePointer, MAX_HEADER_SIZE},
    error::Error,
    option::Options,
    skiplist::{self, ValueStruct},
    vlog::VLOG_HEADER_SIZE,
};

pub const MEM_FILE_EXT: &str = ".mem";

pub struct MemTable {
    pub sl: crossbeam_skiplist::SkipList<Vec<u8>, skiplist::ValueStruct>,
    guard: crossbeam_epoch::Guard,
    pub wal: LogFile,
    max_version: atomic::AtomicU64,
    // opt: Options,
    buf: bytes::BytesMut,
}

pub async fn open_mem_table(
    opt: Options,
    fid: u32,
    oopt: &std::fs::OpenOptions,
) -> Result<(MemTable, bool)> {
    let (wal, is_new_file) = LogFile::open(opt.clone(), fid, oopt, 2 * opt.mem_table_size).await?;

    let mut mt = MemTable {
        sl: crossbeam_skiplist::SkipList::new(crossbeam_epoch::Collector::new()),
        guard: crossbeam_epoch::pin(),
        wal,
        max_version: Default::default(),
        // opt: opt,
        buf: Default::default(),
    };

    if is_new_file {
        return Ok((mt, is_new_file));
    }

    mt.update_skip_list()?;

    Ok((mt, false))
}

pub async fn open_mmap_file<P: AsRef<Path>>(
    path: P,
    oopt: &std::fs::OpenOptions,
    sz: usize,
) -> Result<(MmapFile, bool)> {
    let mut is_new_file = false;
    let mut ooptx = oopt.clone();
    let fd = ooptx
        .truncate(true)
        .open(&path)
        .map_err(|e| anyhow!("Open file error: {}", e))?;
    let meta = fd.metadata()?;

    let mut file_size = meta.len() as usize;
    if sz > 0 && file_size == 0 {
        fd.set_len(sz as u64)
            .map_err(|e| anyhow!("Truncate error: {}", e))?;
        file_size = sz;
        is_new_file = true;
    }

    let path = path.as_ref().to_path_buf();
    let mmap_mut = unsafe {
        memmap2::MmapOptions::new()
            .len(file_size)
            .map_mut(&fd)
            .map_err(|e| {
                anyhow!(
                    "Mmapping {} with size {} error: {}",
                    path.to_string_lossy(),
                    file_size,
                    e
                )
            })?
    };

    if file_size == 0 {
        match path.to_owned().parent() {
            None => {
                bail!("Get parent of path {} fail", path.to_string_lossy())
            }
            Some(p) => {
                let p = p.to_owned();
                tokio::task::spawn(async move {
                    if let Ok(f) = std::fs::File::open(p) {
                        let _ = f.sync_all();
                    }
                });
            }
        }
    }

    Ok((
        MmapFile {
            data: Rc::new(RefCell::new(mmap_mut)),
            file: Mutex::new(Filex { fd, path }),
        },
        is_new_file,
    ))
}

impl MemTable {
    pub fn decr_ref(&self) {
        todo!()
    }

    fn update_skip_list(&mut self) -> Result<()> {
        let end_off = self.wal.iterate(true, 0, self.replay_func())?;

        if (end_off as u32) < self.wal.size.load(Relaxed) {
            bail!(
                "{}, end offset {} < size {}",
                Error::TruncateNeeded,
                end_off,
                self.wal.size.load(Relaxed)
            )
        }

        self.wal.truncate(end_off)
    }

    fn replay_func(&self) -> impl FnMut(Entry, ValuePointer) -> Result<()> + '_ {
        let mut first = true;

        move |e: Entry, _vp: ValuePointer| -> Result<()> {
            if first {
                debug!(
                    "First key={}",
                    String::from_utf8(e.key.to_vec())
                        .map_or("UNKOWN(decode utf8 fail)".to_string(), |s| s),
                );
                first = false;
            }
            let ts = Entry::parse_ts(&e.key);
            if ts > self.max_version.load(Relaxed) {
                self.max_version.store(ts, Relaxed);
            }
            let v = ValueStruct {
                meta: e.meta,
                user_meta: e.user_meta,
                expires_at: e.expires_at,
                value: e.value,
                version: 0,
            };

            self.sl.insert(e.key, v, &self.guard);
            Ok(())
        }
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
}

impl LogFile {
    pub async fn open(
        opt: Options,
        fid: u32,
        oopt: &std::fs::OpenOptions,
        file_size: usize,
    ) -> Result<(Self, bool)> {
        let path = PathBuf::from(&opt.dir).join(format!("{:05}{}", fid, MEM_FILE_EXT));

        let (mmapfile, is_new_file) = open_mmap_file(&path, oopt, file_size)
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
        };

        if is_new_file {
            if let Err(e) = lf.bootstrap() {
                let _ = remove_file(path).await;
                bail!(e)
            }
            lf.size.store(VLOG_HEADER_SIZE, Relaxed);
        }
        lf.size
            .store(lf.mmap_file.data.borrow().len() as u32, Relaxed);

        if lf.size.load(Relaxed) < VLOG_HEADER_SIZE {
            return Ok((lf, false));
        }

        let mut buf = [0; 8];
        buf.copy_from_slice(&(lf.mmap_file.data.borrow()[..8]));
        if u64::from_be_bytes(buf) != 0 {
            bail!("Unsupport encryption yet, found keyid not 0")
        }
        lf.base_iv.resize(12, 0);
        lf.base_iv
            .copy_from_slice(&(lf.mmap_file.data.borrow()[8..20]));

        return Ok((lf, is_new_file));
    }

    /// bootstrap will initialize the log file with key id and baseIV.
    /// The below figure shows the layout of log file.
    /// +----------------+------------------+------------------+
    /// | keyID(8 bytes) |  baseIV(12 bytes)|	  entry...     |
    /// +----------------+------------------+------------------+
    fn bootstrap(&mut self) -> Result<()> {
        let mut buf = [0; 20];
        let mut rng = rand::thread_rng();
        buf.shuffle(&mut rng);
        self.mmap_file.data.borrow_mut()[..20].copy_from_slice(&buf);

        self.zero_next_entry();

        Ok(())
    }

    fn zero_next_entry(&mut self) {
        let start = self.write_at as usize;
        let mut end = (self.write_at + MAX_HEADER_SIZE) as usize;

        if end > self.mmap_file.data.borrow().len() {
            end = self.mmap_file.data.borrow().len();
        }
        if end - start <= 0 {
            return;
        }

        self.mmap_file.data.borrow_mut()[start..end].fill(0_u8);
    }

    fn iterate<F>(&self, read_only: bool, offset: usize, f: F) -> Result<usize>
    where
        F: FnMut(Entry, ValuePointer) -> Result<()>,
    {
        let mut offset = offset;
        if offset == 0 {
            offset = VLOG_HEADER_SIZE as usize;
        }

        let reader = BufReader::new(self.mmap_file.new_reader(offset));
        todo!()
    }

    fn truncate(&self, offset: usize) -> Result<()> {
        todo!()
    }
}

pub struct MmapFile {
    data: Rc<RefCell<memmap2::MmapMut>>,
    file: Mutex<Filex>,
}
impl MmapFile {
    fn new_reader(&self, offset: usize) -> MmapReader {
        MmapReader {
            data: Rc::clone(&self.data),
            offset,
        }
    }
}

struct MmapReader {
    data: Rc<RefCell<memmap2::MmapMut>>,
    offset: usize,
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        todo!()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_open_log_file() {
        let mut opt = Options::default();
        let fid = 1;
        let oopt = std::fs::OpenOptions::new();
        let r = LogFile::open(opt, fid, &oopt, 0).await;
        let (mt, is_new) = r.unwrap();
    }

    #[tokio::test]
    async fn test_open_mem_table() {
        // let r = open_mem_table(opt, fid, oopt).await;
        // let (mt, is_new) = r.unwrap();
    }
}
