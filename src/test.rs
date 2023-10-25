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
