use std::{
    cell::RefCell,
    fmt::Display,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::{anyhow, bail, Result};
use log::error;

pub struct MmapFile {
    pub data: Rc<RefCell<memmap2::MmapMut>>,
    pub file: std::sync::Mutex<Filex>,
}

impl MmapFile {
    pub fn new(data: Rc<RefCell<memmap2::MmapMut>>, file: Filex) -> Self {
        Self {
            data,
            file: std::sync::Mutex::new(file),
        }
    }
    pub fn new_reader(&self, offset: usize) -> MmapReader {
        MmapReader {
            data: Rc::clone(&self.data),
            offset,
        }
    }

    pub fn sync(&self) -> Result<()> {
        self.data
            .borrow_mut()
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
                .borrow_mut()
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
}

impl Display for MmapFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(data: [u8;{}], file: _)", self.data.borrow().len(),)
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
        MmapFile::new(Rc::new(RefCell::new(mmap_mut)), Filex::new(fd, path)),
        is_new_file,
    ))
}

pub struct MmapReader {
    data: Rc<RefCell<memmap2::MmapMut>>,
    offset: usize,
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset > self.data.borrow().len() {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }

        let bytes_to_read = std::cmp::min(buf.len(), self.data.borrow().len() - self.offset);

        buf[..bytes_to_read]
            .copy_from_slice(&self.data.borrow_mut()[self.offset..self.offset + bytes_to_read]);
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
}

impl Display for Filex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(fd: {:?}, path: {:?})", self.fd, self.path)
    }
}
