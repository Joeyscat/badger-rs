use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::atomic,
};

use crate::{memtable::LogFile, option::Options, util::MEM_ORDERING};
use anyhow::{anyhow, bail, Result};
use log::info;
use tokio::fs::read_dir;

use super::discard::DiscardStats;

pub const MAX_VLOG_FILE_SIZE: u32 = u32::MAX;
pub const VLOG_FILE_EXT: &str = ".vlog";

/// size of vlog header.
/// +----------------+------------------+
/// | keyID(8 bytes) |  baseIV(12 bytes)|
/// +----------------+------------------+
pub const VLOG_HEADER_SIZE: u32 = 20;

pub(crate) struct ValueLog {
    files_map: HashMap<u32, LogFile>,
    max_fid: u32,
    files_tobe_deleted: Vec<u32>,
    pub(crate) discard_stats: DiscardStats,

    writeable_log_offset: atomic::AtomicU32,
    num_entries_written: u32,
    opt: Options,
}

impl ValueLog {
    pub(crate) async fn open(opt: Options) -> Result<ValueLog> {
        let discard_stats: DiscardStats = DiscardStats::new(&opt.dir).await?;
        let (fids, max_fid) = Self::populate_files_map(&opt.dir).await?;
        let fids = Self::sort_fids(&vec![], &fids);

        let mut files_map = HashMap::new();
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

            if log_file.size.load(MEM_ORDERING) == VLOG_HEADER_SIZE && fid != max_fid {
                info!("Deleting empty file: {}", log_file.path);
                log_file.delete()?;
                continue;
            }

            files_map.insert(fid, log_file);
        }
        let mut value_log = ValueLog {
            files_map,
            max_fid,
            files_tobe_deleted: vec![],
            discard_stats,
            writeable_log_offset: Default::default(),
            num_entries_written: 0,
            opt,
        };

        if value_log.files_map.len() == 0 {
            value_log
                .create_vlog_file()
                .await
                .map_err(|e| anyhow!("Error while creating log file in ValueLog::open: {}", e))?;
        }

        let last = value_log
            .files_map
            .get_mut(&value_log.max_fid)
            .expect("invalid fid");
        let last_off = last.iterate(VLOG_HEADER_SIZE, |_, _| Ok(()))?;
        last.truncate(last_off).await?;

        value_log
            .create_vlog_file()
            .await
            .map_err(|e| anyhow!("Error while creating log file in ValueLog::open: {}", e))?;

        Ok(value_log)
    }

    async fn create_vlog_file(&mut self) -> Result<()> {
        let fid = self.max_fid + 1;
        let path = Self::fpath(&self.opt.dir, fid);
        let (log_file, is_new) = LogFile::open(
            path,
            fid,
            File::options().read(true).write(true).create_new(true),
            self.opt.value_log_file_size * 2,
        )
        .await?;
        assert!(is_new);
        self.files_map.insert(fid, log_file);
        self.max_fid = fid;
        self.writeable_log_offset
            .store(VLOG_HEADER_SIZE, MEM_ORDERING);
        self.num_entries_written = 0;

        Ok(())
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
}
