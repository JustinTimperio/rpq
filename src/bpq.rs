use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub struct BucketPriorityQueue {
    bucket_ids: Mutex<BTreeMap<u64, Box<u64>>>,
    queue: Mutex<VecDeque<Box<u64>>>,
    active_buckets: AtomicU64,
    mutex: Mutex<()>,
}

impl BucketPriorityQueue {
    pub fn new() -> BucketPriorityQueue {
        BucketPriorityQueue {
            bucket_ids: Mutex::new(BTreeMap::new()),
            queue: Mutex::new(VecDeque::new()),
            active_buckets: AtomicU64::new(0),
            mutex: Mutex::new(()),
        }
    }

    pub fn len(&self) -> u64 {
        self.active_buckets.load(Ordering::Relaxed)
    }

    pub fn peek(&self) -> Option<u64> {
        if let Some(bucket_id) = self.queue.lock().unwrap().front() {
            return Some(**bucket_id);
        }

        None
    }

    pub fn add_bucket(&self, bucket_id: u64) {
        // If the bucket already exists, return
        if self.bucket_ids.lock().unwrap().contains_key(&bucket_id) {
            return;
        }

        // This will lock the mutex and unlock it when it goes out of scope
        let _guard: std::sync::MutexGuard<()> = self.mutex.lock().unwrap();

        self.bucket_ids
            .lock()
            .unwrap()
            .insert(bucket_id, Box::new(bucket_id));
        self.queue.lock().unwrap().push_back(Box::new(bucket_id));
        self.active_buckets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_bucket(&self, bucket_id: u64) {
        let _guard: std::sync::MutexGuard<()> = self.mutex.lock().unwrap();

        if self.bucket_ids.lock().unwrap().remove(&bucket_id).is_none() {
            return;
        }

        self.queue.lock().unwrap().retain(|x| **x != bucket_id);
        self.active_buckets.fetch_sub(1, Ordering::Relaxed);
    }
}
