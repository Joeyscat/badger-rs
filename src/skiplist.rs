use std::fmt::Display;

use integer_encoding::VarInt;

pub struct ValueStruct {
    pub meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub value: Vec<u8>,

    pub version: u64, // This field is not serialized. Only for internal usage.
}

impl ValueStruct {
    pub fn new(value: Vec<u8>) -> ValueStruct {
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
