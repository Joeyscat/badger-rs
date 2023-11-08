pub(crate) mod bloom;
pub(crate) mod file;
pub(crate) mod iter;
pub(crate) mod table;

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

pub fn verify_checksum(data: &Vec<u8>, expected: pb::Checksum) -> Result<()> {
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

pub fn calculate_checksum(data: &[u8], ca: pb::checksum::Algorithm) -> u64 {
    return match ca {
        pb::checksum::Algorithm::Crc32c => CASTAGNOLI.checksum(data) as u64,
        pb::checksum::Algorithm::XxHash64 => panic!("xxhash not supported"),
    };
}

pub(crate) mod kv {
    use std::cmp::Ordering;

    pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
        let x = a[..a.len() - 8].cmp(&b[..b.len() - 8]);
        if x.is_ne() {
            return x;
        }
        a[a.len() - 8..].cmp(&b[b.len() - 8..])
    }

    pub fn parse_key(key: &[u8]) -> Vec<u8> {
        if key.len() == 0 {
            return vec![];
        }

        return key[..key.len() - 8].to_vec();
    }

    pub fn key_with_ts(mut key: Vec<u8>, ts: u64) -> Vec<u8> {
        key.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
        key
    }

    pub fn parse_ts(key: &Vec<u8>) -> u64 {
        if key.len() < 8 {
            return 0;
        }
        let mut bs = [0; 8];
        bs.copy_from_slice(&key[key.len() - 8..]);
        u64::MAX - u64::from_be_bytes(bs)
    }
}

pub(crate) mod num {
    pub fn bytes_to_u32(src: &[u8]) -> u32 {
        let mut buf = [0; 4];
        buf.copy_from_slice(src);
        u32::from_be_bytes(buf)
    }

    pub fn bytes_to_u32_vec(src: &[u8]) -> Vec<u32> {
        assert!(src.len() % 4 == 0, "src length must be a multiple of 4");
        let words = src
            .chunks(4)
            .map(|four_bytes| u32::from_be_bytes(four_bytes.try_into().unwrap()))
            .collect::<Vec<_>>();

        words
    }
}
