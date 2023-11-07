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
}

pub(crate) mod table {
    use std::sync::Arc;

    use anyhow::Result;
    use rand::RngCore;
    use temp_dir::TempDir;

    use crate::{
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
        for i in 0..n {
            kvs.push((key(prefix, i), i.to_string()));
        }

        return build_table(kvs, opts).await;
    }

    async fn build_table(mut kvs: Vec<(String, String)>, opts: Options) -> Result<Table> {
        let mut builder = Builder::new(opts);
        for (k, v) in kvs {
            builder.add(
                key_with_ts(k.into(), 0),
                ValueStruct {
                    meta: b'A',
                    user_meta: 0,
                    expires_at: 0,
                    value: Arc::new(v.into()),
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

    pub(crate) fn key(prefix: &str, i: u32) -> String {
        format!("{}{:04}", prefix, i)
    }
}
#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::bt;
    use rand::RngCore;

    #[test]
    fn test_bt() {
        let filepath = temp_dir().join(format!("badgertest-{}", rand::thread_rng().next_u32()));
        let filepath = filepath.to_str().unwrap();
        bt::initdb_with_cli(filepath);
    }
}
