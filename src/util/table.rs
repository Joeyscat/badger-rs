use std::path::Path;

use anyhow::{anyhow, Result};

const FILE_SUFFIX: &str = ".sst";

/// 001.sst => 1
pub fn parse_file_id(name: &str) -> Result<u64> {
    let s = Path::new(name)
        .file_name()
        .ok_or(anyhow!("invalid filename: {}", name))?
        .to_str()
        .ok_or(anyhow!("convert filename string error"))?;

    let id = s
        .strip_suffix(FILE_SUFFIX)
        .ok_or(anyhow!("invalid filename: {}", name))?
        .parse()?;
    assert!(id > 0);
    Ok(id)
}

pub fn id_to_filename(id: u64) -> String {
    return format!("{:05}{}", id, FILE_SUFFIX);
}

pub fn new_filename(id: u64, dir: &str) -> String {
    match Path::new(dir)
        .join(id_to_filename(id))
        .into_os_string()
        .into_string()
    {
        Ok(s) => s,
        Err(s) => panic!("build new filename({:?}) failed", s),
    }
}
