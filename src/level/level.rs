use anyhow::{anyhow, bail, Result};
use log::info;
use std::{collections::HashMap, fs::remove_file, rc::Rc, sync::atomic::AtomicU64};

use crate::{
    level::compaction::LevelCompactStatus,
    manifest::Manifest,
    option::Options,
    table::Table,
    util::{
        self,
        file::{open_mmap_file, sync_dir},
    },
};

use super::{compaction::CompactStatus, level_handler::LevelHandler};

pub struct LevelsController {
    next_file_id: AtomicU64,
    l0_stalls_ms: AtomicU64,

    levels: Vec<LevelHandler>,
    manifest: Rc<Manifest>,
    opt: Options,

    cstatus: CompactStatus,
}

impl LevelsController {
    pub async fn new(opt: Options, mf: Rc<Manifest>) -> Result<Self> {
        assert!(opt.num_level_zero_tables_stall > opt.num_level_zero_tables);
        let mut levels = Vec::with_capacity(opt.max_levels as usize);
        let mut levelsx = Vec::with_capacity(opt.max_levels as usize);

        for i in 0..opt.max_levels {
            levels.push(LevelHandler::new(opt.clone(), i));
            levelsx.push(LevelCompactStatus::new())
        }
        let dir = opt.dir.to_owned();
        revert_to_manifest(opt.clone(), &mf, util::get_id_map(dir.clone())?)?;

        // TODO Parallelization
        let mut tables: Vec<Vec<Table>> = Vec::with_capacity(opt.max_levels as usize);
        let mut max_file_id: u64 = 0;
        let mut num_opened: u32 = 0;
        for (file_id, tm) in &mf.tables {
            let file_id = file_id.to_owned();
            let filename = util::table::new_filename(file_id, &dir);
            if file_id > max_file_id {
                max_file_id = file_id;
            }

            let (mfile, _) = open_mmap_file(
                filename.clone(),
                std::fs::File::options().read(true).write(true),
                0,
            )
            .await?;
            let topt = opt.clone().into();
            let t = match Table::open(mfile, topt) {
                Ok(t) => t,
                // Err(e) =>{} ignore table which checksum mismatch
                Err(e) => {
                    bail!("Opening table {}: {}", filename, e)
                }
            };
            match tables.get_mut(tm.level as usize) {
                Some(v) => {
                    v.push(t);
                }
                None => {
                    let mut v = Vec::new();
                    v.push(t);
                    tables.insert(tm.level as usize, v);
                }
            };

            num_opened += 1;
            info!(
                "{}/{} tables opened: {}",
                num_opened,
                mf.tables.len(),
                filename
            );
        }

        for index in 0..tables.len() {
            if let Some(h) = levels.get_mut(index) {
                h.init_table(tables.remove(index));
            }
        }

        let lc = Self {
            next_file_id: (max_file_id + 1).into(),
            l0_stalls_ms: 0.into(),
            levels,
            manifest: mf,
            opt,
            cstatus: CompactStatus {
                levels: levelsx,
                tables: HashMap::new(),
            },
        };

        lc.validate()?;

        sync_dir(dir)?;

        Ok(lc)
    }

    fn validate(&self) -> Result<()> {
        for l in &self.levels {
            l.validate()?;
        }
        Ok(())
    }
}

fn revert_to_manifest(opt: Options, mf: &Manifest, id_map: HashMap<u64, ()>) -> Result<()> {
    for ele in mf.tables.keys() {
        if !id_map.contains_key(ele) {
            bail!("file does not exist for table {}", ele)
        }
    }

    for ele in id_map.keys() {
        if !mf.tables.contains_key(ele) {
            info!("Table file {} not referrenced in MANIFEST", ele);
            let filename = util::table::new_filename(ele.to_owned(), &opt.dir);
            remove_file(filename).map_err(|e| anyhow!("Removing table error: {}", e))?;
        }
    }

    Ok(())
}
