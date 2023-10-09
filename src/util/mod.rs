pub mod file;
pub mod table;

use std::{collections::HashMap, fs, path::Path, cmp::Ordering};

use anyhow::Result;

pub fn get_id_map<P: AsRef<Path>>(dir: P) -> Result<HashMap<u64, ()>> {
    let m = fs::read_dir(dir)?
        .filter_map(|s| s.ok())
        .filter_map(|s| s.file_name().into_string().ok())
        .filter_map(|s| table::parse_file_id(&s).ok())
        .map(|s| (s, ()))
        .collect::<HashMap<u64, ()>>();
    Ok(m)
}


pub fn compare_keys(k1: &Vec<u8>, k2: &Vec<u8>)->Ordering {
    todo!()
}