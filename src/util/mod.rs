pub mod bloom;
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

pub mod kv {
    pub fn parse_key(key: &Vec<u8>) -> Vec<u8> {
        if key.len() == 0 {
            return vec![];
        }

        return key[..key.len() - 8].to_vec();
    }

    pub fn key_with_ts(mut key: Vec<u8>, ts: u64) -> Vec<u8> {
        key.extend_from_slice(&ts.to_be_bytes());
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
        let num_32_bit_words = src.len() / 4;
        let words = src
            .chunks(4)
            .map(|four_bytes| four_bytes.iter().map(|&byte| byte as u32).sum())
            .collect::<Vec<_>>();

        if src.len() % 4 == 0 {
            words
        } else {
            words.split_at(words.len() - num_32_bit_words).1.to_vec()
        }
    }
}
