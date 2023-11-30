pub(crate) mod db {

    use anyhow::Result;
    use temp_dir::TempDir;

    use crate::{db::DB, option::Options};

    pub(crate) struct TestDB {
        pub(crate) db: DB,
        #[allow(dead_code)]
        dir: TempDir,
    }

    pub(crate) async fn new_test_db(oopt: Option<Options>) -> Result<TestDB> {
        let mut opt = if let Some(opt) = oopt {
            opt
        } else {
            Options::default()
        };
        let test_dir = TempDir::new().unwrap();
        opt.dir = test_dir.path().to_str().unwrap().to_string();
        let db = DB::open(opt).await?;

        Ok(TestDB { db, dir: test_dir })
    }
}

pub(crate) mod bt {

    pub(crate) fn initdb_with_cli(filepath: &str) {
        println!("{}", filepath);
        let x = std::process::Command::new("bt")
            .args(vec!["-read", "-dir", filepath, "-key", "a"])
            .output()
            .expect("failed to execute <bt read>");
        assert!(x.status.success());
    }

    pub(crate) fn write_with_cli(filepath: &str) {
        println!("{}", filepath);
        let x = std::process::Command::new("bt")
            .args(vec!["-write", "-dir", filepath, "-key", "a", "-value", "b"])
            .output()
            .expect("failed to execute <bt read>");
        assert!(x.status.success());
    }

    mod tests {
        use super::initdb_with_cli;
        use temp_dir::TempDir;

        #[test]
        fn test_bt() {
            let test_dir = TempDir::new().unwrap();
            initdb_with_cli(test_dir.path().to_str().unwrap());
        }
    }
}

pub(crate) mod table {

    use anyhow::Result;
    use rand::RngCore;
    use temp_dir::TempDir;

    use crate::{
        entry::Meta,
        table::{Builder, Options, Table},
        util::kv::key_with_ts,
        value::ValueStruct,
    };
    pub(crate) fn get_test_options() -> Options {
        Options {
            block_size: 4 * 1024,
            bloom_false_positive: 0.01,
            ..Default::default()
        }
    }

    pub(crate) async fn build_test_table(prefix: &str, n: u32, mut opts: Options) -> Result<Table> {
        if opts.block_size == 0 {
            opts.block_size = 4 * 1024;
        }
        assert!(n <= 10000);

        let mut kvs = Vec::with_capacity(n as usize);
        for i in 0..n as i64 {
            kvs.push((key(prefix, i), i.to_string()));
        }

        return build_table(kvs, opts).await;
    }

    async fn build_table(kvs: Vec<(String, String)>, opts: Options) -> Result<Table> {
        let mut builder = Builder::new(opts);
        for (k, v) in kvs {
            builder.add(
                key_with_ts(k.into(), 0),
                ValueStruct {
                    meta: Meta::from_bits_retain(b'A'),
                    user_meta: 0,
                    expires_at: 0,
                    value: v.to_owned().into(),
                    version: 0,
                },
                0,
            );
        }
        let test_dir = TempDir::new()?;
        let filepath = test_dir
            .path()
            .join(format!("{}.sst", rand::thread_rng().next_u32()));

        Table::create(filepath, builder).await
    }

    pub(crate) fn key(prefix: &str, i: i64) -> String {
        format!("{}{:04}", prefix, i)
    }
}
