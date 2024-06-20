use dashmap::DashMap;
use faststr::FastStr;

use crate::filer::Filer;

type BucketName = FastStr;

struct BucketOption {
    name: BucketName,
    replication: FastStr,
    fsync: bool,
}

pub struct FilerBuckets {
    dir_buckets_path: FastStr,
    buckets: DashMap<BucketName, BucketOption>,
}

impl Filer {
    pub fn load_buckets(&self) {
        todo!()
    }

    pub fn read_bucket_option(&self, bucket_name: BucketName) -> (FastStr, bool) {
        match self.buckets.buckets.get(&bucket_name) {
            Some(option) => (option.replication.clone(), option.fsync),
            None => (FastStr::empty(), false),
        }
    }
}
