use std::{path::Path, sync::Mutex};

use anyhow::Result;
use bytes::Buf;
use log::info;

use crate::util::file::{open_mmap_file, MmapFile};

const DISCARD_FNAME: &str = "DISCARD";

pub(crate) struct DiscardStats(Mutex<DiscardStatsInner>);

struct DiscardStatsInner {
    mfile: MmapFile,
    next_empty_slot: usize,
}

impl DiscardStats {
    pub(crate) async fn new(dir: &str) -> Result<Self> {
        Ok(DiscardStats(Mutex::new(DiscardStatsInner::new(dir).await?)))
    }

    pub(crate) fn update(&mut self, fid: u64, discard: i64) -> Result<i64> {
        self.0.lock().unwrap().update(fid, discard)
    }

    pub(crate) fn iterate<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(u64, u64),
    {
        self.0.lock().unwrap().iterate(f)
    }

    pub(crate) fn max_discard(&self) -> Result<(u32, u64)> {
        self.0.lock().unwrap().max_discard()
    }
}

impl DiscardStatsInner {
    async fn new(dir: &str) -> Result<Self> {
        let fname = Path::new(dir).join(DISCARD_FNAME);

        let (mfile, is_new) = open_mmap_file(
            fname,
            &std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
            1 << 20,
        )
        .await?;

        let mut lf = DiscardStatsInner {
            mfile,
            next_empty_slot: 0,
        };
        if is_new {
            lf.zero_out()?;
        }

        for slot in 0..lf.max_slot() {
            let x = lf.get(16 * slot)?;
            if x == 0 {
                lf.next_empty_slot = slot;
                break;
            }
        }

        lf.sort();
        info!("Discard stats next_empty_slot: {}", lf.next_empty_slot);

        Ok(lf)
    }

    fn update(&mut self, fid: u64, discard: i64) -> Result<i64> {
        let idx = match (0..self.next_empty_slot)
            .collect::<Vec<usize>>()
            .binary_search_by(|slot| self.get(slot * 16).unwrap().cmp(&fid))
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if idx < self.next_empty_slot && self.get(idx * 16)? == fid {
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

        let idx = self.next_empty_slot;
        self.set(idx * 16, fid)?;
        self.set(idx * 16 + 8, discard as u64)?;

        self.next_empty_slot += 1;
        loop {
            if self.next_empty_slot < self.max_slot() {
                break;
            }
            let l = self.mfile.as_ref().len() as u64;
            self.mfile.truncate(l * 2)?;
        }
        self.zero_out()?;

        self.sort();

        Ok(discard)
    }

    fn iterate<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, u64),
    {
        for slot in 0..self.next_empty_slot {
            let idx = 16 * slot;
            f(self.get(idx)?, self.get(idx + 8)?);
        }
        Ok(())
    }

    fn max_discard(&self) -> Result<(u32, u64)> {
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
        self.set(self.next_empty_slot * 16, 0)?;
        self.set(self.next_empty_slot * 16 + 8, 0)?;
        Ok(())
    }

    fn max_slot(&self) -> usize {
        return self.mfile.as_ref().len();
    }

    fn get(&self, offset: usize) -> Result<u64> {
        return Ok(u64::from_be_bytes(
            self.mfile.as_ref()[offset..offset + 8].try_into()?,
        ));
    }

    fn set(&mut self, offset: usize, value: u64) -> Result<()> {
        self.mfile.as_mut()[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn sort(&mut self) {
        let x = self.next_empty_slot * 16;
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
        let mut ds = DiscardStats::new(&opt.dir).await.unwrap();
        assert_eq!(ds.0.lock().unwrap().next_empty_slot, 0);
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
            } else {
                assert_eq!(id * 100, val);
            }
        })
        .unwrap();
    }

    #[tokio::test]
    async fn test_reload_discard_stats() {
        let test_dir = TempDir::new().unwrap();

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();

        let mut db = DB::open(opt.clone()).await.unwrap();
        let ds = &mut db.vlog.discard_stats;

        ds.update(1, 1).unwrap();
        ds.update(2, 1).unwrap();
        ds.update(1, -1).unwrap();
        // db.close().unwrap();
        drop(db);

        let mut dbs = DB::open(opt).await.unwrap();
        let ds2 = &mut dbs.vlog.discard_stats;

        assert_eq!(0, ds2.update(1, 0).unwrap());
        assert_eq!(1, ds2.update(2, 0).unwrap());
    }
}
