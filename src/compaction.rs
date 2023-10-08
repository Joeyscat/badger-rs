use std::collections::HashMap;

pub struct CompactStatus {
    pub levels: Vec<LevelCompactStatus>,
    pub tables: HashMap<u64, ()>
}

pub struct LevelCompactStatus{

}

impl LevelCompactStatus {
    pub fn new()->Self {
        Self {  }
    }
}