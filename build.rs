use std::io::Result;

fn main() -> Result<()> {
    println!("build proto");
    prost_build::compile_protos(&["src/pb/badgerpb4.proto"], &["src/pb/"])?;
    Ok(())
}
