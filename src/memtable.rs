use std::{
    cell::RefCell,
    fmt::Display,
    io::BufReader,
    ops::{AddAssign, Deref, DerefMut},
    path::{Path, PathBuf},
    rc::Rc,
    sync::atomic,
};

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};
use log::debug;
use rand::seq::SliceRandom;
use tokio::fs::remove_file;

use crate::{
    entry::Entry,
    entry::{Meta, ValuePointer, CRC_SIZE, MAX_HEADER_SIZE},
    error::Error,
    option::Options,
    util::{
        file::{open_mmap_file, MmapFile},
        kv::parse_ts,
        MEM_ORDERING,
    },
    value::ValueStruct,
    vlog::VLOG_HEADER_SIZE,
};

pub const MEM_FILE_EXT: &str = ".mem";

pub(crate) struct MemTable {
    pub(crate) sl: crossbeam_skiplist::SkipMap<Bytes, ValueStruct>,
    pub(crate) wal: LogFile,
    max_version: atomic::AtomicU64,
    opt: Options,
    buf: bytes::BytesMut,
}

impl Display for MemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(sl: {}, wal: {}, max_version: {}, buf: [u8;{}])",
            self.sl.len(),
            self.wal,
            self.max_version.load(MEM_ORDERING),
            self.buf.len()
        )
    }
}

pub(crate) async fn open_mem_table(
    opt: Options,
    fid: u32,
    oopt: &std::fs::OpenOptions,
) -> Result<(MemTable, bool)> {
    let path = Path::new(&opt.dir).join(format!("{:05}{}", fid, MEM_FILE_EXT));
    let (wal, is_new_file) = LogFile::open(path, fid, oopt, 2 * opt.mem_table_size).await?;

    let mut mt = MemTable {
        sl: crossbeam_skiplist::SkipMap::new(),
        wal,
        max_version: Default::default(),
        opt: opt,
        buf: Default::default(),
    };

    if is_new_file {
        return Ok((mt, is_new_file));
    }

    mt.update_skip_list().await?;

    Ok((mt, false))
}

impl MemTable {
    pub(crate) fn sync_wal(&self) -> Result<()> {
        self.wal.sync()
    }

    pub(crate) fn is_full(&self) -> bool {
        // TODO check skiplist mem_size
        self.wal.write_at >= self.opt.mem_table_size
    }

    pub(crate) async fn put(&mut self, ent: &Entry) -> Result<()> {
        self.wal.write_entry(&mut self.buf, ent).await?;

        if ent.meta().contains(Meta::FIN_TXN) {
            return Ok(());
        }

        self.sl.insert(
            ent.key().clone(),
            ValueStruct {
                meta: ent.meta(),
                user_meta: ent.user_meta(),
                expires_at: ent.expires_at(),
                value: ent.value().clone(),
                version: ent.version(),
            },
        );
        let ts = parse_ts(&ent.key());
        if ts > self.max_version.load(MEM_ORDERING) {
            self.max_version.store(ts, MEM_ORDERING);
        }

        Ok(())
    }

    async fn update_skip_list(&mut self) -> Result<()> {
        let end_off = self.wal.iterate(0, self.replay_func())?;

        let read_only = false;
        if end_off < self.wal.size.load(MEM_ORDERING) && read_only {
            bail!(
                "{}, end offset {} < size {}",
                Error::TruncateNeeded,
                end_off,
                self.wal.size.load(MEM_ORDERING)
            )
        }

        self.wal
            .truncate(end_off)
            .await
            .map_err(|e| anyhow!("Truncate logfile error: {}", e))
    }

    fn replay_func(&self) -> impl FnMut(Entry, ValuePointer) -> Result<()> + '_ {
        let mut first = true;

        move |e: Entry, _vp: ValuePointer| -> Result<()> {
            if first {
                debug!(
                    "First key={}",
                    String::from_utf8(e.key().to_vec())
                        .map_or("UNKOWN(decode utf8 fail)".to_string(), |s| s),
                );
                first = false;
            }
            let ts = parse_ts(e.key());
            if ts > self.max_version.load(MEM_ORDERING) {
                self.max_version.store(ts, MEM_ORDERING);
            }
            let v = ValueStruct {
                meta: e.meta(),
                user_meta: e.user_meta(),
                expires_at: e.expires_at(),
                value: e.value().clone(),
                version: 0,
            };

            self.sl.insert(e.key().clone(), v);
            Ok(())
        }
    }

    pub(crate) fn max_version(&self) -> u64 {
        self.max_version.load(MEM_ORDERING)
    }
}

pub(crate) struct LogFile {
    mmap_file: MmapFile,
    path: String,
    fid: u32,
    size: atomic::AtomicU32,
    // data_key: pb::DataKey,
    base_iv: Vec<u8>,
    write_at: usize,
}

impl Deref for LogFile {
    type Target = MmapFile;

    fn deref(&self) -> &Self::Target {
        &self.mmap_file
    }
}

impl DerefMut for LogFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap_file
    }
}

impl LogFile {
    pub async fn open(
        path: PathBuf,
        fid: u32,
        oopt: &std::fs::OpenOptions,
        file_size: usize,
    ) -> Result<(Self, bool)> {
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
            lf.size.store(VLOG_HEADER_SIZE, MEM_ORDERING);
        }
        lf.size
            .store(lf.mmap_file.as_ref().len() as u32, MEM_ORDERING);

        if lf.size.load(MEM_ORDERING) < VLOG_HEADER_SIZE {
            return Ok((lf, false));
        }

        let mut buf = [0; 8];
        buf.copy_from_slice(&(lf.mmap_file.as_ref()[..8]));
        if u64::from_be_bytes(buf) != 0 {
            bail!("Unsupport encryption yet, found keyid not 0")
        }
        lf.base_iv.resize(12, 0);
        lf.base_iv.copy_from_slice(&(lf.mmap_file.as_ref()[8..20]));

        return Ok((lf, is_new_file));
    }

    /// bootstrap will initialize the log file with key id and baseIV.
    /// The below figure shows the layout of log file.
    /// +----------------+------------------+------------------+
    /// | keyID(8 bytes) |  baseIV(12 bytes)|	  entry...     |
    /// +----------------+------------------+------------------+
    fn bootstrap(&mut self) -> Result<()> {
        let mut buf = [0; 20];

        buf[..8].copy_from_slice(&u64::to_be_bytes(0));
        let mut rng = rand::thread_rng();
        buf[8..].shuffle(&mut rng);
        self.mmap_file.write_slice(0, &buf)?;

        self.zero_next_entry();

        Ok(())
    }

    fn zero_next_entry(&mut self) {
        let start = self.write_at as usize;
        let mut end = start + MAX_HEADER_SIZE;

        if end > self.mmap_file.as_ref().len() {
            end = self.mmap_file.as_ref().len();
        }
        if end - start <= 0 {
            return;
        }

        self.mmap_file.as_mut()[start..end].fill(0_u8);
    }

    pub(crate) fn iterate<F>(&self, offset: u32, mut f: F) -> Result<u32>
    where
        F: FnMut(Entry, ValuePointer) -> Result<()>,
    {
        let mut offset = offset;
        if offset == 0 {
            offset = VLOG_HEADER_SIZE;
        }

        let reader = BufReader::new(self.mmap_file.new_reader(offset as usize));
        let reader = Rc::new(RefCell::new(reader));

        let mut last_commit = 0;
        let mut valid_end_offset = offset;
        let mut entries = vec![];
        let mut vptrs = vec![];

        loop {
            let ent = match Entry::decode_from_reader(Rc::clone(&reader), offset as usize) {
                Ok(ent) if ent.key().is_empty() => break,
                Ok(ent) => ent,
                // We have not reached the end of the file buf the entry we read is
                // zero. This happens because we have truncated the file and zero'ed
                // it out.
                Err(e) if matches!(e.downcast_ref::<Error>(), Some(Error::VLogTruncate)) => {
                    break;
                }
                Err(e) => bail!(e),
            };

            let ent_len =
                ent.header_len() + (ent.key().len() + ent.value().len() + CRC_SIZE) as u32;
            let vp = ValuePointer::new(self.fid, ent_len, ent.offset());
            offset += vp.len();

            match ent.meta() {
                meta if meta.contains(Meta::TXN) => {
                    let txn_ts = parse_ts(ent.key());
                    if last_commit == 0 {
                        last_commit = txn_ts;
                    }
                    if last_commit != txn_ts {
                        break;
                    }
                    entries.push(ent);
                    vptrs.push(vp);
                }

                meta if meta.contains(Meta::FIN_TXN) => {
                    let txn_ts: u64 = match String::from_utf8(ent.value().to_vec()) {
                        Ok(s) => match s.parse() {
                            Ok(i) => i,
                            _ => break,
                        },
                        _ => break,
                    };
                    if last_commit != txn_ts {
                        break;
                    }
                    // Got the end of txn. Now we can store them.
                    last_commit = 0;
                    valid_end_offset = offset;

                    let mut index = 0;
                    for entx in &entries {
                        let vpx = vptrs.get(index).unwrap();
                        index += 1;
                        if let Err(e) = f(entx.clone(), vpx.clone()) {
                            bail!("Iterate function error: {}. file={}", e, self.path)
                        }
                    }

                    entries.clear();
                    vptrs.clear();
                }

                _ => {
                    if last_commit != 0 {
                        // This is most likely an entry which was moved as part of GC.
                        // We should't get this entry in the middle of a transaction.
                        break;
                    }
                    valid_end_offset = offset;

                    if let Err(e) = f(ent, vp) {
                        bail!("Iterate function error: {}. file={}", e, self.path)
                    }
                }
            }
        }

        Ok(valid_end_offset)
    }

    async fn write_entry(&mut self, buf: &mut BytesMut, ent: &Entry) -> Result<()> {
        buf.clear();
        let plen = ent.encode_with_buf(buf, self.write_at)?;
        let offset = self.write_at;

        self.write_slice(offset, &buf)?;
        self.write_at.add_assign(plen as usize);

        self.zero_next_entry();
        Ok(())
    }

    pub(crate) async fn truncate(&mut self, offset: u32) -> Result<()> {
        if self
            .mmap_file
            .file
            .lock()
            .map_err(|e| anyhow!("Get locked fd error: {}", e))?
            .fd
            .metadata()?
            .len() as u32
            == offset
        {
            return Ok(());
        }
        self.size.store(offset, MEM_ORDERING);
        self.mmap_file.truncate(offset as u64)
    }

    pub(crate) async fn donw_writing(&mut self, offset: u32) -> Result<()> {
        self.sync()?;

        self.truncate(offset).await
    }

    pub(crate) fn delete(self) -> Result<()> {
        self.mmap_file.delete()
    }

    pub(crate) fn get_fid(&self) -> u32 {
        self.fid
    }

    pub(crate) fn get_size(&self) -> u32 {
        self.size.load(MEM_ORDERING)
    }

    pub(crate) fn set_size(&self, s: u32) {
        self.size.store(s, MEM_ORDERING);
    }

    pub(crate) fn get_path(&self) -> &str {
        &self.path
    }
}

impl Display for LogFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(mmap_file: {}, path: {}, fid: {}, size: {}, base_iv: [u8;{}], write_at: {})",
            self.mmap_file,
            self.path,
            self.fid,
            self.size.load(MEM_ORDERING),
            self.base_iv.len(),
            self.write_at
        )
    }
}

#[cfg(test)]
mod tests {
    use temp_dir::TempDir;

    use super::*;
    use crate::test::bt;

    #[tokio::test]
    async fn test_log_file_open() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let fid = 1;
        let path = Path::new(&opt.dir).join(format!("{:05}{}", fid, MEM_FILE_EXT));
        let r = LogFile::open(
            path,
            fid,
            std::fs::File::options().read(true).write(true).create(true),
            opt.mem_table_size,
        )
        .await;
        match r.unwrap() {
            (lf, true) => {
                println!("(lf, true)");
                println!("{}", lf);
            }
            (lf, _) => {
                println!("(lf, false)");
                println!("{}", lf);
            }
        };
    }

    #[tokio::test]
    async fn test_open_mem_table() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let r = open_mem_table(
            opt,
            1,
            std::fs::File::options().read(true).write(true).create(true),
        )
        .await;

        match r.unwrap() {
            (mt, true) => {
                println!("(mt, true)");
                println!("{}", mt);
            }
            (mt, _) => {
                println!("(mt, false)");
                println!("{}", mt);
            }
        };
    }
}
