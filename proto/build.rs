use std::io::Result;

fn main() -> Result<()> {
    tonic_build::configure().compile(&["src/proto/helyim.proto"], &["src/proto"])?;
    Ok(())
}
