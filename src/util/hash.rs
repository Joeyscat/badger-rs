use crate::manifest::CASTAGNOLI;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::{cell::RefCell, rc::Rc};

pub struct HashReader<'a, R: ?Sized> {
    count: usize,
    hash: crc::Digest<'a, u32>,
    inner: Rc<RefCell<R>>,
}

impl<'a, R: Read> HashReader<'a, R> {
    pub fn new(inner: Rc<RefCell<R>>) -> HashReader<'a, R> {
        let hash = CASTAGNOLI.digest();
        Self {
            inner,
            hash,
            count: 0,
        }
    }

    pub fn sum32(&self) -> u32 {
        self.hash.clone().finalize()
    }

    pub fn count(&self) -> usize {
        return self.count;
    }
}

impl<'a, R: ?Sized + Read> Read for HashReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.borrow_mut().read(buf)?;
        self.count += bytes_read;

        self.hash.update(&buf[..bytes_read]);

        Ok(bytes_read)
    }
}

// pub struct HashWriter<'a, W: ?Sized> {
//     count: usize,
//     hash: crc::Digest<'a, u32>,
//     inner: W,
// }

// impl<'a, W: Write> HashWriter<'a, W> {
//     pub fn new(inner: W) -> HashWriter<'a, W> {
//         let hash = CASTAGNOLI.digest();
//         Self {
//             inner,
//             hash,
//             count: 0,
//         }
//     }

//     pub fn sum32(&self) -> u32 {
//         self.hash.clone().finalize()
//     }

//     pub fn count(&self) -> usize {
//         return self.count;
//     }
// }

// impl<'a, W: ?Sized + Write> Write for HashWriter<'a, W> {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         let n = self.inner.write(buf)?;
//         self.count += n;
//         self.hash.update(&buf[..n]);
//         Ok(n)
//     }

//     fn flush(&mut self) -> std::io::Result<()> {
//         self.inner.flush()
//     }
// }

pub(crate) fn mem_hash(data: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}
