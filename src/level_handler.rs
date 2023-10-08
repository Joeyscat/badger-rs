use crate::option::Options;

pub struct LevelHandler {
    level: u32,
    opt: Options,
}

impl LevelHandler {
    pub fn new(opt: Options, level: u32) -> Self {
        Self { level, opt }
    }
}
