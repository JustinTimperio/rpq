use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub struct BucketPriorityQueue {
    bucket_ids: RwLock<BTreeMap<u64, Box<u64>>>,
    queue: RwLock<VecDeque<Box<u64>>>,
    active_buckets: AtomicU64,
}

impl BucketPriorityQueue {
    pub fn new() -> BucketPriorityQueue {
        BucketPriorityQueue {
            bucket_ids: RwLock::new(BTreeMap::new()),
            queue: RwLock::new(VecDeque::new()),
            active_buckets: AtomicU64::new(0),
        }
    }

    pub fn len(&self) -> u64 {
        self.active_buckets.load(Ordering::Relaxed)
    }

    pub fn peek(&self) -> Option<u64> {
        self.queue.read().unwrap().front().map(|x| **x)
    }

    pub fn add_bucket(&self, bucket_id: u64) {
        // If the bucket already exists, return
        if self.bucket_ids.read().unwrap().contains_key(&bucket_id) {
            return;
        }

        self.bucket_ids
            .write()
            .unwrap()
            .insert(bucket_id, Box::new(bucket_id));
        self.queue.write().unwrap().push_back(Box::new(bucket_id));
        self.active_buckets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_bucket(&self, bucket_id: u64) {
        if self
            .bucket_ids
            .write()
            .unwrap()
            .remove(&bucket_id)
            .is_none()
        {
            return;
        }

        self.queue.write().unwrap().retain(|x| **x != bucket_id);
        self.active_buckets.fetch_sub(1, Ordering::Relaxed);
    }
}
