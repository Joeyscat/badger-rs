use anyhow::{bail, Result};

use crate::{
    entry::{Entry, Meta, ValuePointer},
    util::DEFAULT_PAGE_SIZE,
    vlog::MAX_VLOG_FILE_SIZE,
    write::WriteReq,
};

use super::ValueLog;

impl ValueLog {
    pub(crate) async fn write(&self, reqs: &mut Vec<WriteReq>) -> Result<()> {
        self.validate_writes(reqs)?;

        let mut cur_logfile = self.get_latest_logfile().await?;
        let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_PAGE_SIZE.to_owned());
        for req in reqs.iter_mut() {
            let mut cur_logfile_w = cur_logfile.write().await;
            let entries_vptrs = req.entries_vptrs_mut();
            let mut value_sizes = Vec::with_capacity(entries_vptrs.len());
            let mut written = 0;

            for (ent, vp) in entries_vptrs {
                buf.clear();
                value_sizes.push(ent.get_value().len());

                if ent.skip_vlog(self.get_value_threshold()) {
                    *vp = ValuePointer::default();
                    continue;
                }
                let tmp_meta = ent.get_meta();

                ent.get_meta_mut().remove(Meta::TXN.union(Meta::FIN_TXN));
                let plen = self.encode_entry(&mut buf, ent, self.woffset())?;
                ent.set_meta(tmp_meta);
                *vp = ValuePointer::new(cur_logfile_w.get_fid(), plen, self.woffset());

                // write
                if buf.len() != 0 {
                    let n = buf.len() as u32;
                    let start_offset = self.writeable_log_offset_fetchadd(n);
                    let end_offset = start_offset + n;
                    if end_offset as usize >= cur_logfile_w.as_ref().len() {
                        cur_logfile_w.truncate(end_offset).await?;
                    }

                    cur_logfile_w.write_slice(start_offset as usize, &buf)?;
                    cur_logfile_w.set_size(end_offset);
                }

                written += 1;
            }

            self.num_entries_written_fetchadd(written);

            // to disk
            if self.woffset() as usize > self.get_opt().value_log_file_size
                || self.get_num_entries_written() as usize > self.get_opt().value_log_max_entries
            {
                cur_logfile_w.donw_writing(self.woffset()).await?;

                let new_logfile = self.create_vlog_file().await?;
                drop(cur_logfile_w);
                cur_logfile = new_logfile;
            }
        }

        // to disk
        if self.woffset() as usize > self.get_opt().value_log_file_size
            || self.get_num_entries_written() as usize > self.get_opt().value_log_max_entries
        {
            let mut cur_logfile_w = cur_logfile.write().await;
            cur_logfile_w.donw_writing(self.woffset()).await?;

            let _ = self.create_vlog_file().await?;
        }

        Ok(())
    }

    fn encode_entry(&self, buf: &mut Vec<u8>, ent: &Entry, offset: u32) -> Result<u32> {
        todo!()
    }

    fn validate_writes(&self, reqs: &Vec<WriteReq>) -> Result<()> {
        let mut vlog_offset = self.woffset() as u64;

        for req in reqs {
            let size = Self::estimate_request_size(req);
            let estimated_vlog_offset = vlog_offset + size;
            if estimated_vlog_offset > MAX_VLOG_FILE_SIZE as u64 {
                bail!(
                    "Request size offset {} is bigger than maximum offset {}",
                    estimated_vlog_offset,
                    MAX_VLOG_FILE_SIZE
                )
            }

            if estimated_vlog_offset >= self.get_opt().value_log_file_size as u64 {
                vlog_offset = 0;
                continue;
            }
            vlog_offset = estimated_vlog_offset;
        }

        Ok(())
    }

    fn estimate_request_size(req: &WriteReq) -> u64 {
        todo!()
    }
}
