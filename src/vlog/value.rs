use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    path::{Path, PathBuf},
    sync::{atomic, Arc},
};

use crate::{memtable::LogFile, option::Options, util::MEM_ORDERING};
use anyhow::{anyhow, bail, Result};
use log::info;
use tokio::{fs::read_dir, sync::RwLock};

use super::discard::DiscardStats;

pub const MAX_VLOG_FILE_SIZE: u32 = u32::MAX;
pub const VLOG_FILE_EXT: &str = ".vlog";

/// size of vlog header.
/// +----------------+------------------+
/// | keyID(8 bytes) |  baseIV(12 bytes)|
/// +----------------+------------------+
pub const VLOG_HEADER_SIZE: u32 = 20;

pub(crate) struct ValueLog {
    files_map: RwLock<BTreeMap<u32, Arc<RwLock<LogFile>>>>,
    max_fid: atomic::AtomicU32,
    files_tobe_deleted: Vec<u32>,
    discard_stats: DiscardStats,

    writeable_log_offset: atomic::AtomicU32,
    num_entries_written: atomic::AtomicU32,
    opt: Options,
}

impl ValueLog {
    pub(crate) async fn open(opt: Options) -> Result<ValueLog> {
        let discard_stats: DiscardStats = DiscardStats::new(&opt.dir).await?;
        let (fids, max_fid) = Self::populate_files_map(&opt.dir).await?;

        let mut files_map = BTreeMap::new();
        let fids = Self::sort_fids(&vec![], &fids);
        for fid in fids {
            let path = Self::fpath(&opt.dir, fid);
            let (log_file, is_new) = LogFile::open(
                path.clone(),
                fid,
                File::options().read(true).write(true).create(false),
                opt.value_log_file_size * 2,
            )
            .await
            .map_err(|e| anyhow!("Unable to open log file: {:?}. Error={}", path, e))?;
            assert!(!is_new);

            if log_file.get_size() == VLOG_HEADER_SIZE && fid != max_fid {
                info!("Deleting empty file: {}", log_file.get_path());
                log_file.delete()?;
                continue;
            }

            files_map.insert(fid, Arc::new(RwLock::new(log_file)));
        }
        let files_map_len = files_map.len();
        let value_log = ValueLog {
            files_map: RwLock::new(files_map),
            max_fid: max_fid.into(),
            files_tobe_deleted: vec![],
            discard_stats,
            writeable_log_offset: 0.into(),
            num_entries_written: 0.into(),
            opt,
        };

        if files_map_len == 0 {
            value_log
                .create_vlog_file()
                .await
                .map_err(|e| anyhow!("Error while creating log file in ValueLog::open: {}", e))?;
        }

        let last = value_log.get_latest_logfile().await?;
        let mut last_w = last.write().await;
        let last_off = last_w.iterate(VLOG_HEADER_SIZE, |_, _| Ok(()))?;
        last_w.truncate(last_off).await?;
        drop(last_w);

        value_log
            .create_vlog_file()
            .await
            .map_err(|e| anyhow!("Error while creating log file in ValueLog::open: {}", e))?;

        Ok(value_log)
    }

    pub(crate) async fn create_vlog_file(&self) -> Result<Arc<RwLock<LogFile>>> {
        let fid = self.max_fid.fetch_add(1, MEM_ORDERING) + 1;
        let path = Self::fpath(&self.opt.dir, fid);
        let (log_file, is_new) = LogFile::open(
            path,
            fid,
            File::options().read(true).write(true).create_new(true),
            self.opt.value_log_file_size * 2,
        )
        .await?;
        assert!(is_new);
        let log_file = Arc::new(RwLock::new(log_file));
        self.files_map
            .write()
            .await
            .insert(fid, Arc::clone(&log_file));
        self.writeable_log_offset
            .store(VLOG_HEADER_SIZE, MEM_ORDERING);
        self.num_entries_written.store(0, MEM_ORDERING);

        Ok(log_file)
    }

    // return file id vector, and max file id
    async fn populate_files_map<P: AsRef<Path>>(dir: P) -> Result<(Vec<u32>, u32)> {
        let mut entries = read_dir(dir.as_ref())
            .await
            .map_err(|e| anyhow!("Unable to open log dir: {:?}. Error={}", dir.as_ref(), e))?;
        let mut fid_map = HashMap::new();
        let mut max_fid = 0;
        while let Some(entry) = entries.next_entry().await? {
            let filename = entry.file_name().into_string().expect("String conert fail");
            if !filename.ends_with(VLOG_FILE_EXT) {
                continue;
            }
            let fid = filename
                .strip_suffix(VLOG_FILE_EXT)
                .expect(&format!("Strip suffix for {} error", filename))
                .parse::<u32>()
                .map_err(|e| anyhow!("Unable to parse log id: {}. Error={}", filename, e))?;
            if fid_map.contains_key(&fid) {
                bail!("Duplicate file found: {}. Please delete one.", filename)
            }
            if fid > max_fid {
                max_fid = fid;
            }
            fid_map.insert(fid, ());
        }

        let x: Vec<u32> = fid_map.into_keys().collect();
        Ok((x, max_fid))
    }

    fn sort_fids(files_tobe_deleted: &[u32], fids: &[u32]) -> Vec<u32> {
        let mut x = fids
            .to_vec()
            .into_iter()
            .filter(|&i| !files_tobe_deleted.contains(&i))
            .collect::<Vec<u32>>();
        x.sort();
        x
    }

    fn fpath(dir: &str, fid: u32) -> PathBuf {
        Path::new(dir).join(format!("{:06}.vlog", fid))
    }

    pub(crate) async fn get_latest_logfile(&self) -> Result<Arc<RwLock<LogFile>>> {
        Ok(Arc::clone(
            self.files_map
                .read()
                .await
                .get(&self.max_fid.load(MEM_ORDERING))
                .expect("get_latest_logfile failed"),
        ))
    }

    pub(crate) fn woffset(&self) -> u32 {
        self.writeable_log_offset.load(MEM_ORDERING)
    }

    pub(crate) fn get_opt(&self) -> &Options {
        &self.opt
    }

    pub(crate) fn get_writeable_log_offset(&self) -> u32 {
        self.writeable_log_offset.load(MEM_ORDERING)
    }

    pub(crate) fn writeable_log_offset_fetchadd(&self, s: u32) -> u32 {
        self.writeable_log_offset.fetch_add(s, MEM_ORDERING)
    }

    pub(crate) fn get_num_entries_written(&self) -> u32 {
        self.num_entries_written.load(MEM_ORDERING)
    }

    pub(crate) fn num_entries_written_fetchadd(&self, n: u32) -> u32 {
        self.num_entries_written.fetch_add(n, MEM_ORDERING)
    }

    pub(crate) fn get_value_threshold(&self) -> u32 {
        self.opt.value_threshold
    }

    pub(crate) fn get_discard_stats(&self) -> &DiscardStats {
        &self.discard_stats
    }
}
