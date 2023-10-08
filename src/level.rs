use std::{cell::RefCell, collections::HashMap, fs::remove_file, rc::Rc};

use anyhow::{anyhow, bail, Result};
use log::{info, warn};

use crate::{
    compaction::{CompactStatus, LevelCompactStatus},
    level_handler::LevelHandler,
    manifest::Manifest,
    option::Options,
    util,
};

pub struct LevelsController {
    levels: Vec<LevelHandler>,
    manifest: Rc<Manifest>,
    opt: Options,

    cstatus: CompactStatus,
}

impl LevelsController {
    pub fn new(opt: Options, mf: Rc<Manifest>) -> Result<Self> {
        assert!(opt.num_level_zero_tables_stall > opt.num_level_zero_tables);
        let mut levels = Vec::with_capacity(opt.max_levels as usize);
        let mut levelsx = Vec::with_capacity(opt.max_levels as usize);

        for i in 0..opt.max_levels {
            levels.push(LevelHandler::new(opt.clone(), i));
            levelsx.push(LevelCompactStatus::new())
        }
        revert_to_manifest(opt.clone(), &mf, util::get_id_map(opt.dir.to_owned())?)?;

        let s = Self {
            levels,
            manifest: mf,
            opt,
            cstatus: CompactStatus {
                levels: levelsx,
                tables: HashMap::new(),
            },
        };

        todo!()
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
