use std::{path::Path, sync::Mutex};

use anyhow::Result;
use bytes::Buf;
use log::info;

use crate::{
    option::Options,
    util::file::{open_mmap_file, MmapFile},
};

const DISCARD_FNAME: &str = "DISCARD";

pub(crate) struct DiscardStats {
    mfile: MmapFile,
    next_empty_slot: Mutex<usize>,
    opt: Options,
}

impl DiscardStats {
    pub(crate) async fn new(opt: Options) -> Result<Self> {
        let fname = Path::new(&opt.value_dir).join(DISCARD_FNAME);

        let (mfile, is_new) = open_mmap_file(
            fname,
            &std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
            1 << 20,
        )
        .await?;

        let mut lf = DiscardStats {
            mfile,
            opt,
            next_empty_slot: Mutex::new(0),
        };
        if is_new {
            lf.zero_out()?;
        }

        for slot in 0..lf.max_slot() {
            if lf.get(16 * slot)? == 0 {
                *lf.next_empty_slot.lock().unwrap() = slot;
                break;
            }
        }

        lf.sort();
        info!(
            "Discard stats next_empty_slot: {}",
            *lf.next_empty_slot.lock().unwrap()
        );

        Ok(lf)
    }

    pub(crate) fn update(&mut self, fid: u64, discard: i64) -> Result<i64> {
        let mut next_empty_slot = *self.next_empty_slot.lock().unwrap();
        let idx = match (0..next_empty_slot)
            .collect::<Vec<usize>>()
            .binary_search_by(|slot| self.get(slot * 16).unwrap().cmp(&fid))
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if idx < next_empty_slot && self.get(idx * 16)? == fid {
            let off = idx * 16 + 8;
            let cur_disc = self.get(off)?;
            if discard == 0 {
                return Ok(cur_disc as i64);
            }
            if discard < 0 {
                self.set(off, 0)?;
                return Ok(0);
            }
            self.set(off, cur_disc + discard as u64)?;
            return Ok(cur_disc as i64 + discard);
        }

        if discard <= 0 {
            return Ok(0);
        }

        let idx = next_empty_slot;
        self.set(idx * 16, fid)?;
        self.set(idx * 16 + 8, discard as u64)?;

        next_empty_slot += 1;
        loop {
            if next_empty_slot < self.max_slot() {
                break;
            }
            let l = self.mfile.data.borrow().len() as u64;
            self.mfile.truncate(l * 2)?;
        }
        self.zero_out()?;

        self.sort();

        Ok(discard)
    }

    pub(crate) fn iterate<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, u64),
    {
        for slot in 0..*self.next_empty_slot.lock().unwrap() {
            let idx = 16 * slot;
            f(self.get(idx)?, self.get(idx + 8)?);
        }
        Ok(())
    }

    pub(crate) fn max_discard(&self) -> Result<(u32, u64)> {
        let mut max_fid = 0;
        let mut max_val = 0;

        self.iterate(|fid, val| {
            if max_val < val {
                max_val = val;
                max_fid = fid;
            }
        })?;

        return Ok((max_fid as u32, max_val));
    }

    fn zero_out(&mut self) -> Result<()> {
        let x = *self.next_empty_slot.lock().unwrap();
        self.set(x * 16, 0)?;
        self.set(x * 16 + 8, 0)?;
        Ok(())
    }

    fn max_slot(&self) -> usize {
        return self.mfile.data.borrow().len();
    }

    fn get(&self, offset: usize) -> Result<u64> {
        return Ok(u64::from_be_bytes(
            self.mfile.data.borrow()[offset..offset + 8].try_into()?,
        ));
    }

    fn set(&mut self, offset: usize, value: u64) -> Result<()> {
        self.mfile.data.borrow_mut()[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn sort(&mut self) {
        let x = *self.next_empty_slot.lock().unwrap() * 16;
        let slice = &mut self.mfile.as_mut()[..x];
        let chunks = unsafe { slice.as_chunks_unchecked_mut::<16>() };
        chunks.sort_unstable_by(|a, b| a.as_ref().get_u64().cmp(&b.as_ref().get_u64()));
    }
}

#[cfg(test)]
mod tests {
    use temp_dir::TempDir;

    use crate::{db::DB, option::Options};

    use super::DiscardStats;

    #[tokio::test]
    async fn test_discard_stats() {
        let test_dir = TempDir::new().unwrap();

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        opt.value_dir = opt.dir.clone();
        let mut ds = DiscardStats::new(opt).await.unwrap();
        assert_eq!(*ds.next_empty_slot.lock().unwrap(), 0);
        let (fid, _) = ds.max_discard().unwrap();
        assert_eq!(fid, 0);

        for i in 0..20 {
            assert_eq!(i as i64 * 100, ds.update(i, i as i64 * 100).unwrap());
        }
        ds.iterate(|id, val| {
            assert_eq!(id * 100, val);
        })
        .unwrap();
        for i in 0..10 {
            assert_eq!(0, ds.update(i, -1).unwrap());
        }
        ds.iterate(|id, val| {
            if id < 10 {
                assert_eq!(0, val);
            }
            assert_eq!(id * 100, val);
        })
        .unwrap();
    }

    #[tokio::test]
    async fn test_reload_discard_stats() {
        let test_dir = TempDir::new().unwrap();

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        opt.value_dir = opt.dir.clone();

        let mut db = DB::open(opt.clone()).await.unwrap();
        let ds = &mut db.vlog.discard_stats;

        ds.update(1, 1).unwrap();
        ds.update(2, 1).unwrap();
        ds.update(1, -1).unwrap();
        db.close().unwrap();

        let mut db = DB::open(opt).await.unwrap();
        let ds = &mut db.vlog.discard_stats;

        assert_eq!(0, ds.update(1, 0).unwrap());
        assert_eq!(1, ds.update(2, 0).unwrap());
    }
}
