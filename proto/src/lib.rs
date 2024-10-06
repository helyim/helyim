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
}

pub mod volume {
    include!(concat!(env!("OUT_DIR"), "/volume.rs"));
}
