use std::collections::BTreeSet;
use std::sync::RwLock;

pub struct BucketPriorityQueue {
    bucket_ids: RwLock<BTreeSet<u64>>,
}

impl BucketPriorityQueue {
    pub fn new() -> BucketPriorityQueue {
        BucketPriorityQueue {
            bucket_ids: RwLock::new(BTreeSet::new()),
        }
    }

    pub fn len(&self) -> u64 {
        self.bucket_ids.read().unwrap().len() as u64
    }

    pub fn peek(&self) -> Option<u64> {
        self.bucket_ids.read().unwrap().first().cloned()
    }

    pub fn add_bucket(&self, bucket_id: u64) {
        // If the bucket already exists, return
        if self.bucket_ids.read().unwrap().contains(&bucket_id) {
            return;
        }

        self.bucket_ids.write().unwrap().insert(bucket_id);
    }

    pub fn remove_bucket(&self, bucket_id: &u64) {
        self.bucket_ids.write().unwrap().remove(&bucket_id);
    }
}
