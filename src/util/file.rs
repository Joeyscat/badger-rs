use std::{
    fmt::Display,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    slice,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, bail, Result};
use log::error;

pub(crate) fn sync_dir<P: AsRef<Path>>(dir: P) -> Result<()> {
    std::fs::File::open(&dir)?
        .sync_all()
        .map_err(|e| anyhow!("Sync {:?} error: {}", dir.as_ref(), e))
}

pub(crate) struct MmapFile {
    pub data: Arc<RwLock<memmap2::MmapMut>>,
    pub file: std::sync::Mutex<Filex>,
}

impl AsRef<[u8]> for MmapFile {
    fn as_ref(&self) -> &[u8] {
        let data = self.data.read().unwrap();
        unsafe { slice::from_raw_parts(data.as_ptr() as _, data.len()) }
    }
}

impl AsMut<[u8]> for MmapFile {
    fn as_mut(&mut self) -> &mut [u8] {
        let mut data = self.data.write().unwrap();
        unsafe { slice::from_raw_parts_mut(data.as_mut_ptr() as _, data.len()) }
    }
}

impl MmapFile {
    pub fn new(data: Arc<RwLock<memmap2::MmapMut>>, file: Filex) -> Self {
        Self {
            data,
            file: std::sync::Mutex::new(file),
        }
    }

    pub(crate) fn write_slice(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        self.as_mut()[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    pub fn read(&self, offset: usize, size: usize) -> Result<Vec<u8>> {
        let d = self.data.read().unwrap();
        if offset + size > d.len() {
            return Err(anyhow::Error::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "early eof",
            )));
        }
        Ok(d[offset..offset + size].to_vec())
    }

    pub fn new_reader(&self, offset: usize) -> MmapReader {
        MmapReader {
            data: Arc::clone(&self.data),
            offset,
        }
    }

    pub fn sync(&self) -> Result<()> {
        self.data
            .write()
            .unwrap()
            .flush()
            .map_err(|e| anyhow!("Flush mmapfile error: {}", e))
    }

    pub fn truncate(&mut self, max_size: u64) -> Result<()> {
        self.sync()?;
        self.file
            .lock()
            .map_err(|e| anyhow!("Get locked fd error: {}", e))?
            .fd
            .set_len(max_size as u64)
            .map_err(|e| anyhow!("Truncate mmapfile error: {}", e))?;

        unsafe {
            self.data
                .write()
                .unwrap()
                .remap(
                    max_size as usize,
                    memmap2::RemapOptions::new().may_move(true),
                )
                .map_err(|e| anyhow!("Remap file error: {}", e))
        }
    }

    pub fn delete(self) -> Result<()> {
        drop(self.data); // TODO munmap?

        let p = &self
            .file
            .lock()
            .map_err(|e| anyhow!("Get locked fd error: {}", e))?
            .path
            .clone();

        if let Err(e) = self
            .file
            .lock()
            .map_err(|e| anyhow!("Get locked fd error: {}", e))?
            .fd
            .set_len(0)
        {
            error!("Truncate file({:#?}) error: {}", p, e);
        }

        std::fs::remove_file(p).map_err(|e| anyhow!("Remove file({:#?}) error: {}", p, e))
    }

    pub fn path(&self) -> Result<String> {
        Ok(self
            .file
            .lock()
            .map_err(|e| anyhow!("Get locked fd error: {}", e))?
            .path()?
            .to_string())
    }

    pub fn filename(&self) -> Result<String> {
        Ok(self
            .file
            .lock()
            .map_err(|e| anyhow!("Get locked fd error: {}", e))?
            .filename()?
            .to_string())
    }
}

impl Display for MmapFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(data: [u8;{}], file: _)",
            self.data.read().unwrap().len(),
        )
    }
}

pub async fn open_mmap_file<P: AsRef<Path>>(
    path: P,
    oopt: &std::fs::OpenOptions,
    sz: usize,
) -> Result<(MmapFile, bool)> {
    let mut is_new_file = false;
    let fd = oopt
        .open(&path)
        .map_err(|e| anyhow!("Open file({:?}) error: {}", &path.as_ref(), e))?;
    let meta = fd.metadata()?;

    let mut file_size = meta.len() as usize;
    if sz > 0 && file_size == 0 {
        fd.set_len(sz as u64)
            .map_err(|e| anyhow!("Truncate error: {}", e))?;
        file_size = sz;
        is_new_file = true;
    }

    let path = path.as_ref().to_path_buf();
    let mmap_mut = unsafe {
        memmap2::MmapOptions::new()
            .len(file_size)
            .map_mut(&fd)
            .map_err(|e| anyhow!("Mmapping {:?} with size {} error: {}", path, file_size, e))?
    };

    if file_size == 0 {
        match path.to_owned().parent() {
            None => {
                bail!("Get parent of path {} fail", path.to_string_lossy())
            }
            Some(p) => {
                let p = p.to_owned();
                tokio::task::spawn(async move {
                    if let Ok(f) = std::fs::File::open(p) {
                        let _ = f.sync_all();
                    }
                });
            }
        }
    }

    Ok((
        MmapFile::new(Arc::new(RwLock::new(mmap_mut)), Filex::new(fd, path)),
        is_new_file,
    ))
}

pub struct MmapReader {
    data: Arc<RwLock<memmap2::MmapMut>>,
    offset: usize,
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset > self.data.read().unwrap().len() {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }

        let bytes_to_read = std::cmp::min(buf.len(), self.data.read().unwrap().len() - self.offset);

        buf[..bytes_to_read]
            .copy_from_slice(&self.data.write().unwrap()[self.offset..self.offset + bytes_to_read]);
        self.offset += bytes_to_read;

        Ok(bytes_to_read)
    }
}

pub struct Filex {
    pub fd: std::fs::File,
    pub path: PathBuf,
}

impl Filex {
    pub fn new(fd: std::fs::File, path: PathBuf) -> Self {
        Self { fd, path }
    }

    pub fn path(&self) -> Result<&str> {
        self.path.to_str().ok_or(anyhow!("convert to string error"))
    }

    pub fn filename(&self) -> Result<&str> {
        self.path
            .file_name()
            .ok_or(anyhow!("has no valid filename"))?
            .to_str()
            .ok_or(anyhow!("convert to string error"))
    }
}

impl Display for Filex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(fd: {:?}, path: {:?})", self.fd, self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::open_mmap_file;

    #[tokio::test]
    async fn test_mmap_read_write() {
        let path = format!("/tmp/mmaptest-{}", rand::random::<u64>());
        let (mut mfile, new) = open_mmap_file(
            path.clone(),
            &std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
            1 << 20,
        )
        .await
        .unwrap();
        assert!(new);

        let mut buf = vec![0u8; 1024];
        for i in 0..1024 {
            buf[i] = i as u8;
        }

        mfile.write_slice(0, &buf).unwrap();
        mfile.sync().unwrap();

        let (mfile, new) = open_mmap_file(
            path,
            &std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
            0,
        )
        .await
        .unwrap();
        assert!(!new);

        assert_eq!(mfile.as_ref()[..1024], buf[..]);
    }
}
