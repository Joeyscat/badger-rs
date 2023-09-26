pub struct ValueStruct {
    pub meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub value: Vec<u8>,

    pub version: u64, // This field is not serialized. Only for internal usage.
}

impl ValueStruct {}
