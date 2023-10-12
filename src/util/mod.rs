pub mod file;
pub mod table;

use std::{collections::HashMap, fs, path::Path};

use anyhow::{bail, Result};

use crate::{manifest::CASTAGNOLI, pb};

pub fn get_id_map<P: AsRef<Path>>(dir: P) -> Result<HashMap<u64, ()>> {
    let m = fs::read_dir(dir)?
        .filter_map(|s| s.ok())
        .filter_map(|s| s.file_name().into_string().ok())
        .filter_map(|s| table::parse_file_id(&s).ok())
        .map(|s| (s, ()))
        .collect::<HashMap<u64, ()>>();
    Ok(m)
}

pub fn verify_checksum(data: Vec<u8>, expected: pb::Checksum) -> Result<()> {
    let actual = calculate_checksum(data, expected.algo());
    if actual != expected.sum {
        bail!(
            "checksum mismatch, actual: {}, expected: {}",
            actual,
            expected.sum
        )
    }
    Ok(())
}

pub fn calculate_checksum(data: Vec<u8>, ca: pb::checksum::Algorithm) -> u64 {
    return match ca {
        pb::checksum::Algorithm::Crc32c => CASTAGNOLI.checksum(&data) as u64,
        pb::checksum::Algorithm::XxHash64 => panic!("xxhash not supported"),
    };
}
