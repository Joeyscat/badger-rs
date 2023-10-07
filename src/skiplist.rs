use std::fmt::Display;

pub struct SkipList {
    inner: crossbeam_skiplist::SkipList<Vec<u8>, ValueStruct>,
}

pub struct ValueStruct {
    pub meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub value: Vec<u8>,

    pub version: u64, // This field is not serialized. Only for internal usage.
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
