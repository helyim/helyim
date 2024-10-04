pub mod directory {
    include!(concat!(env!("OUT_DIR"), "/helyim.rs"));

    impl VolumeLocation {
        pub fn new() -> Self {
            Self {
                url: String::new(),
                public_url: String::new(),
                new_vids: vec![],
                deleted_vids: vec![],
                new_ec_vids: vec![],
                deleted_ec_vids: vec![],
                leader: None,
            }
        }
    }
}

pub mod filer {
    include!(concat!(env!("OUT_DIR"), "/filer.rs"));

    impl FileChunk {
        pub fn get_fid(&self) -> String {
            match self.fid.as_ref() {
                Some(fid) => fid.to_fid_str(),
                None => String::default(),
            }
        }
    }

    impl FileId {
        pub fn to_fid_str(&self) -> String {
            format!("{},{:x}{:08x}", self.volume_id, self.file_key, self.cookie)
        }
    }
}

pub mod volume {
    include!(concat!(env!("OUT_DIR"), "/volume.rs"));
}
