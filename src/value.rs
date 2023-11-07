use std::{fmt::Display, sync::Arc};

use anyhow::{anyhow, Result};
use integer_encoding::VarInt;

pub struct ValueStruct {
    pub meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub value: Arc<Vec<u8>>,

    pub version: u64, // This field is not serialized. Only for internal usage.
}

impl ValueStruct {
    pub fn new(value: Arc<Vec<u8>>) -> ValueStruct {
        ValueStruct {
            meta: 0,
            user_meta: 0,
            expires_at: 0,
            value,
            version: 0,
        }
    }

    pub fn encoded_size(&self) -> usize {
        let sz = self.value.len() + 2; // meta, usermeta
        let enc = self.expires_at.required_space();
        sz + enc
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.push(self.meta);
        buf.push(self.user_meta);
        buf.extend_from_slice(&self.expires_at.encode_var_vec());
        buf.extend_from_slice(&self.value);
        buf
    }

    pub fn decode(data: &[u8]) -> Result<ValueStruct> {
        let meta = data[0];
        let user_meta = data[0];
        let (expires_at, sz) = u64::decode_var(&data[2..]).ok_or(anyhow!(""))?;
        let value = &data[sz + 2..];

        Ok(ValueStruct {
            meta,
            user_meta,
            expires_at,
            value: Arc::new(value.to_vec()),
            version: 0,
        })
    }
}

impl Display for ValueStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(meta: {}, user_meta: {}, expires_at: {}, value: [u8;{}], version: {})",
            self.meta,
            self.user_meta,
            self.expires_at,
            self.value.len(),
            self.version
        )
    }
}
