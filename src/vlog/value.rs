use std::{collections::HashMap, fs::File, path::Path};

use crate::{memtable::LogFile, option::Options};
use anyhow::{anyhow, Result};
use log::info;
use std::sync::atomic::Ordering::Relaxed;

use super::discard::DiscardStats;

pub const MAX_VLOG_FILE_SIZE: u32 = u32::MAX;

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
    opt: Options,
}

impl ValueLog {
    pub(crate) async fn new(opt: Options) -> Result<ValueLog> {
        let discard_stats: DiscardStats = DiscardStats::new(opt.clone()).await?;
        let (fids, max_fid) = Self::populate_files_map(&opt.dir).await?;
        let fids = Self::sort_fids(&vec![], &fids);

        let mut files_map = HashMap::new();
        for fid in fids {
            let (log_file, is_new) = LogFile::open(
                opt.clone(),
                fid,
                File::options().read(true).write(true).create(false),
                opt.value_log_file_size * 2,
            )
            .await?;
            assert!(!is_new);

            if log_file.size.load(Relaxed) == VLOG_HEADER_SIZE && fid != max_fid {
                info!("Deleting empty file: {}", log_file.path);
                log_file.delete()?;
                continue;
            }

            files_map.insert(fid, log_file);
        }

        let last = files_map.get_mut(&max_fid).expect("invalid fid");
        let last_off = last.iterate(VLOG_HEADER_SIZE, |_, vp| todo!())?;
        last.truncate(last_off).await?;

        let mut value_log = ValueLog {
            files_map,
            max_fid,
            files_tobe_deleted: vec![],
            discard_stats,
            opt,
        };

        if value_log.files_map.len() == 0 {
            value_log.create_vlog_file()?;
        }

        Ok(value_log)
    }

    pub(crate) async fn open(&mut self) -> Result<()> {
        todo!()
    }

    fn create_vlog_file(&mut self) -> Result<()> {
        todo!()
    }

    // return file id vector, and max file id
    async fn populate_files_map<P: AsRef<Path>>(dir: P) -> Result<(Vec<u32>, u32)> {
        todo!()
    }

    fn sort_fids(files_tobe_deleted: &[u32], fids: &[u32]) -> Vec<u32> {
        todo!()
    }
}
