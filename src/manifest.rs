use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind::UnexpectedEof,
    path::Path,
};

use anyhow::{anyhow, bail, Result};
use bytes::BytesMut;
use crc::{Crc, CRC_32_ISCSI};
use prost::Message;
use tokio::{
    fs::{rename, File},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{
    error::Error,
    option::Options,
    pb::{self},
    util::file::sync_dir,
};

const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST-REWRITE";

const MAGIC_TEXT: &[u8; 4] = b"Bdgr";
const BADGER_MAGIC_VERSION: u16 = 8;

pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Manifest represents the contents of the MENIFEST file in a Badger store.
///
/// The Manifest file describe the startup state of the db -- all LSM files and
/// what level they're at.
///
/// It consists of a sequence of [`pb::ManifestChangeSet`] objects.
/// Each of these if treated atomically, and contains a sequence of
/// [`ManifestChange`]'s (file creations/deletions) which we use to
/// reconstruct the manifest at startup.
#[derive(Debug, Clone)]
pub struct Manifest {
    pub levels: Vec<LevelManifest>,
    pub tables: HashMap<u64, TableManifest>,

    /// Contains total number of creation and deletion changes in the manifest
    /// -- used to compute whether it'd be useful to rewrite the manifest.
    pub creations: u32,
    pub deletions: u32,
}

impl Manifest {
    pub fn new() -> Self {
        Self {
            levels: vec![],
            tables: HashMap::new(),
            creations: 0,
            deletions: 0,
        }
    }

    fn as_changes(&self) -> Vec<pb::ManifestChange> {
        let mut changes = Vec::with_capacity(self.tables.len());
        for (id, tm) in &self.tables {
            changes.push(new_create_change(id.to_owned(), tm.level as u32, tm.key_id));
        }
        changes
    }
}

fn new_create_change(id: u64, level: u32, key_id: u64) -> pb::ManifestChange {
    pb::ManifestChange {
        id,
        op: pb::manifest_change::Operation::Create.into(),
        level,
        key_id,
        encryption_algo: pb::EncryptionAlgo::Aes.into(),
        compression: 0,
    }
}

/// LevelManifest contains information about LSM tree levels
/// in the MANIFEST file.
#[derive(Debug, Clone)]
pub struct LevelManifest {
    pub tables: HashSet<u64>,
}

/// TableManifest contains information about a specific table
/// in the LSM tree.
#[derive(Debug, Clone, Copy)]
pub struct TableManifest {
    pub level: u8,
    pub key_id: u64,
}

#[derive(Debug)]
pub struct ManifestFile {
    fp: File,
    directory: String,

    external_magic: u16,

    pub manifest: Mutex<Manifest>,
}

pub async fn open_or_create_manifest_file(opt: &Options) -> Result<ManifestFile> {
    help_open_or_create_manifest_file(opt.dir.clone(), false, opt.external_magic_version).await
}

async fn help_open_or_create_manifest_file(
    dir: String,
    _read_only: bool,
    ext_magic: u16,
) -> Result<ManifestFile> {
    let path = Path::new(&dir).join(MANIFEST_FILENAME);

    let mut fp = match File::options()
        .read(true)
        .write(true)
        .open(path.as_path())
        .await
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let m = Manifest::new();
            let fp = help_rewrite(&dir, &m, ext_magic).await?;

            return Ok(ManifestFile {
                fp,
                directory: dir,
                external_magic: ext_magic,
                manifest: Mutex::new(m),
            });
        }
        Err(e) => bail!(format!("Open MANIFEST error: {}", e)),
    };

    let (manifest, trunc_offset) = replay_manifest_file(&mut fp, ext_magic).await?;
    fp.set_len(trunc_offset)
        .await
        .map_err(|e| anyhow!("Truncate MANIFEST error: {}", e))?;
    fp.seek(std::io::SeekFrom::End(0))
        .await
        .map_err(|e| anyhow!("Seek error: {}", e))?;

    Ok(ManifestFile {
        fp,
        directory: dir,
        external_magic: ext_magic,
        manifest: Mutex::new(manifest),
    })
}

async fn help_rewrite(dir: &String, m: &Manifest, ext_magic: u16) -> Result<File> {
    let rewrite_path = Path::new(&dir).join(MANIFEST_REWRITE_FILENAME);

    let mut fp = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&rewrite_path)
        .await?;

    //
    // +---------------------+-------------------------+-----------------------+
    // | magicText (4 bytes) | externalMagic (2 bytes) | badgerMagic (2 bytes) |
    // +---------------------+-------------------------+-----------------------+
    let mut buf = tokio::io::BufWriter::new(vec![]);

    buf.write_all(MAGIC_TEXT).await?;
    buf.write_u16(ext_magic).await?;
    buf.write_u16(BADGER_MAGIC_VERSION).await?;

    let changes = m.as_changes();
    let change_buf = pb::ManifestChangeSet { changes }.encode_to_vec();
    let checksum = CASTAGNOLI.checksum(&change_buf);
    buf.write_u32(change_buf.len() as u32).await?;
    buf.write_u32(checksum).await?;
    buf.write_all(&change_buf).await?;

    fp.write_all(buf.buffer()).await?;
    fp.sync_all()
        .await
        .map_err(|e| anyhow!("Sync {} error: {}", MANIFEST_REWRITE_FILENAME, e))?;

    let manifest_path = Path::new(&dir).join(MANIFEST_FILENAME);
    rename(rewrite_path, &manifest_path).await?;

    let mut fp = File::options()
        .read(true)
        .write(true)
        .open(manifest_path)
        .await?;
    fp.seek(std::io::SeekFrom::End(0))
        .await
        .map_err(|e| anyhow!("Seek error: {}", e))?;

    sync_dir(dir)?;

    Ok(fp)
}

async fn replay_manifest_file(file: &mut File, ext_magic: u16) -> Result<(Manifest, u64)> {
    let meta = file
        .metadata()
        .await
        .map_err(|e| anyhow!("Query metadata error: {}", e))?;
    let mut reader = io::BufReader::new(file);
    let mut magic_buf = [0; 4];
    reader
        .read_exact(&mut magic_buf)
        .await
        .map_err(|e| anyhow!("Read error: {}", e))?;
    if magic_buf.to_vec().cmp(&MAGIC_TEXT.to_vec()).is_ne() {
        bail!(Error::ManifestBadMagic)
    }

    let ext_version = reader.read_u16().await?;
    let version = reader.read_u16().await?;

    if ext_version != ext_magic {
        bail!(Error::ManifestExtMagicMismatch(ext_magic, ext_version))
    }
    if version != BADGER_MAGIC_VERSION {
        bail!(Error::ManifestVersionUnsupport(
            BADGER_MAGIC_VERSION,
            version
        ))
    }

    let mut build = Manifest::new();

    let mut offset = 4 + 4;
    loop {
        let length = match reader.read_u32().await {
            Ok(l) => l,
            Err(e) if e.kind() == UnexpectedEof => {
                break;
            }
            Err(e) => bail!("Read MANIFEST error: {}", e),
        };
        if length as u64 > meta.len() {
            bail!(
                "Buffer length: {} greater than file size: {}. Manifest file might be currupted.",
                length,
                meta.len()
            )
        }
        let checksum = match reader.read_u32().await {
            Ok(l) => l,
            Err(e) if e.kind() == UnexpectedEof => {
                break;
            }
            Err(e) => bail!("Read MANIFEST error: {}", e),
        };
        let mut buf = BytesMut::zeroed(length as usize);
        match reader.read_exact(&mut buf).await {
            Ok(_) => (),
            Err(e) if e.kind() == UnexpectedEof => {
                break;
            }
            Err(e) => bail!(e),
        };
        let checksum_x = CASTAGNOLI.checksum(&buf);
        if checksum_x != checksum {
            bail!(Error::ManifestBadChecksum)
        }

        let cs = pb::ManifestChangeSet::decode(buf)?;

        apply_change_set(&mut build, cs)?;

        offset += 4 + 4 + length
    }

    Ok((build, offset as u64))
}

fn apply_change_set(mf: &mut Manifest, cs: pb::ManifestChangeSet) -> Result<()> {
    for c in cs.changes {
        apply_manifest_change(mf, c)?;
    }
    Ok(())
}

fn apply_manifest_change(mf: &mut Manifest, change: pb::ManifestChange) -> Result<()> {
    match change.op() {
        pb::manifest_change::Operation::Create => {
            if mf.tables.contains_key(&change.id) {
                bail!("MANIFEST invalid, table {} exists", change.id)
            }
            mf.tables.insert(
                change.id,
                TableManifest {
                    level: change.level as u8,
                    key_id: change.key_id,
                },
            );
            while mf.levels.len() <= change.level as usize {
                mf.levels.push(LevelManifest {
                    tables: HashSet::new(),
                })
            }
            mf.levels
                .get_mut(change.level as usize)
                .unwrap()
                .tables
                .insert(change.id);
            mf.creations += 1;
        }
        pb::manifest_change::Operation::Delete => {
            let table = match mf.tables.get(&change.id) {
                None => bail!("MANIFEST removes non-existing table {}", change.id),
                Some(v) => v,
            };
            mf.levels
                .get_mut(table.level as usize)
                .unwrap()
                .tables
                .remove(&change.id);
            mf.tables.remove(&change.id);
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {

    use temp_dir::TempDir;

    use crate::test::bt;

    use super::*;
    #[test]
    fn test_crc32() {
        let r = CASTAGNOLI.checksum("hello world".as_bytes());
        println!("{}", r);

        let mut d = CASTAGNOLI.digest();
        d.update("hello world".as_bytes());
        println!("{}", d.finalize());

        let mut d = CASTAGNOLI.digest();
        d.update("hello".as_bytes());
        d.update(" world".as_bytes());
        println!("{}", d.finalize());
    }

    #[tokio::test]
    async fn test_open_manifest_file() {
        let test_dir = TempDir::new().unwrap();
        bt::initdb_with_cli(test_dir.path().to_str().unwrap());

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let r = open_or_create_manifest_file(&opt).await;
        println!("{:#?}", r.unwrap())
    }

    #[tokio::test]
    async fn test_create_manifest_file() {
        let test_dir = TempDir::new().unwrap();

        let mut opt = Options::default();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let r = open_or_create_manifest_file(&opt).await;
        println!("{:#?}", r.unwrap())
    }
}
