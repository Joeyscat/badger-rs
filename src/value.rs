use std::fmt::Display;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use integer_encoding::VarInt;

use crate::entry::Meta;

#[derive(Default)]
pub struct ValueStruct {
    pub meta: Meta,
    pub user_meta: u8,
    pub expires_at: u64,
    pub value: Bytes,

    pub version: u64, // This field is not serialized. Only for internal usage.
}

impl ValueStruct {
    pub fn new<B: Into<Bytes>>(value: B) -> ValueStruct {
        ValueStruct {
            value: value.into(),
            ..Default::default()
        }
    }

    pub fn encoded_size(&self) -> usize {
        let sz = self.value.len() + 2; // meta, usermeta
        let enc = self.expires_at.required_space();
        sz + enc
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.push(self.meta.bits());
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
            meta: Meta::from_bits_retain(meta),
            user_meta,
            expires_at,
            value: value.to_vec().into(),
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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, io::BufReader, rc::Rc};

    use bytes::BytesMut;

    use crate::entry::Entry;

    #[test]
    fn test_entry() {
        let ent = Entry::new("key".into(), "value".into());
        let mut buf = BytesMut::new();
        let _ = ent.encode_with_buf(&mut buf, 0).unwrap();

        let reader = BufReader::new(buf.as_ref());
        let ent_1 = Entry::decode_from_reader(Rc::new(RefCell::new(reader)), 0).unwrap();

        assert_eq!(ent.key(), ent_1.key(), "key mismatch");
        assert_eq!(ent.value(), ent_1.value(), "value mismatch");
        assert_eq!(ent.meta(), ent_1.meta(), "meta mismatch");
        assert_eq!(ent.user_meta(), ent_1.user_meta(), "user_meta mismatch");
        assert_eq!(ent.expires_at(), ent_1.expires_at(), "expires_at mismatch");
    }
}
