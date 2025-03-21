use std::{fs::read_dir, io::Result, path::PathBuf};

fn main() -> Result<()> {
    let includes = &[PathBuf::from("src/proto")];
    let mut protos = Vec::new();
    for include in includes {
        for file in read_dir(include)? {
            let file = file?;
            if file.file_type()?.is_dir() {
                continue;
            }
            let path = file.path();
            if path.extension().unwrap() == "proto" {
                protos.push(path);
            }
        }
    }
    tonic_build::configure()
        .type_attribute(
            "volume.RemoteFile",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .type_attribute(
            "volume.VolumeInfo",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .type_attribute(
            "helyim.HeartbeatResponse",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .type_attribute(
            "filer.FileId",
            "#[derive(Hash, Eq, ::serde::Serialize, ::serde::Deserialize)]",
        )
        .type_attribute(
            "filer.FileChunk",
            "#[derive(Hash, Eq, ::serde::Serialize, ::serde::Deserialize)]",
        )
        .compile_protos(&protos, includes)?;
    Ok(())
}
