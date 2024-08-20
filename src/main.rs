use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

extern crate scopeguard;

mod bpq;
mod pq;

struct RPQ<T: Ord + Clone> {
    // total_len is the number of items across all queues
    pub bucket_count: u64,
    // non_empty_buckets is a binary heap of priorities
    pub non_empty_buckets: bpq::BucketPriorityQueue,

    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<RwLock<HashMap<u64, pq::PriorityQueue<T>>>>,
    items_in_queues: AtomicU64,
}

impl<T: Ord + Clone> RPQ<T> {
    fn new(bucket_count: u64) -> RPQ<T> {
        let buckets = Arc::new(RwLock::new(HashMap::new()));
        let items_in_queues = AtomicU64::new(0);

        for i in 0..bucket_count {
            buckets.write().unwrap().insert(i, pq::PriorityQueue::new());
        }

        RPQ {
            bucket_count,
            non_empty_buckets: bpq::BucketPriorityQueue::new(),
            buckets,
            items_in_queues,
        }
    }

    fn len(&self) -> u64 {
        self.items_in_queues.load(Ordering::Relaxed)
    }

    fn peek(&self) -> Option<pq::Item<T>> {
        let bucket_id: u64 = self.non_empty_buckets.peek()?;
        self.buckets.read().unwrap().get(&bucket_id)?.peek()
    }

    fn enqueue(&self, item: pq::Item<T>) {
        let bucket_id: u64 = item.priority;
        self.buckets
            .read()
            .unwrap()
            .get(&bucket_id)
            .unwrap()
            .enqueue(item.clone());
        self.non_empty_buckets.add_bucket(bucket_id);
        self.items_in_queues.fetch_add(1, Ordering::Relaxed);
    }

    fn dequeue(&self) -> Option<pq::Item<T>> {
        let buckets = self.buckets.read().unwrap();

        // Fetch the bucket
        let bucket_id: u64 = self.non_empty_buckets.peek()?;
        let bucket = buckets.get(&bucket_id);
        if bucket.is_none() {
            println!("Bucket is none for id: {}", bucket_id);
            return None;
        }

        // Fetch the item from the bucket
        let item = bucket.unwrap().dequeue();
        if item.is_none() {
            self.non_empty_buckets.remove_bucket(bucket_id);
            return None;
        }
        self.items_in_queues.fetch_sub(1, Ordering::Relaxed);

        // If the bucket is empty, remove it from the non_empty_buckets
        if bucket.unwrap().len() == 0 {
            self.non_empty_buckets.remove_bucket(bucket_id);
        }
        return item;
    }
}

fn main() {
    // Set Conncurrency
    let send_threads = 10;
    let receive_threads = 1;
    let bucket_count = 10;
    let message_count = 10_000_000;
    let sent_counter = Arc::new(AtomicU64::new(0));
    let received_counter = Arc::new(AtomicU64::new(0));

    // Create the RPQ
    let rpq = Arc::new(RPQ::new(bucket_count));

    // Enqueue items
    let mut send_handles = Vec::new();
    for i in 0..send_threads {
        let rpq_clone = Arc::clone(&rpq);
        let sent_clone = Arc::clone(&sent_counter);
        println!("Spawning thread {}", i);
        send_handles.push(std::thread::spawn(move || {
            loop {
                if sent_clone.load(Ordering::Relaxed) >= message_count {
                    break;
                }

                let item: pq::Item<u64> = pq::Item {
                    // Random priority between 0 and 9
                    priority: rand::thread_rng().gen_range(0..bucket_count),
                    data: 0,
                    disk_uuid: Some(Uuid::new_v4().to_string()),
                    should_escalate: false,
                    escalation_rate: None,
                    can_timeout: false,
                    timeout: None,
                    submitted_at: std::time::Instant::now(),
                    last_escalation: None,
                    index: 0,
                    batch_id: 0,
                    was_restored: false,
                };

                rpq_clone.enqueue(item);
                sent_clone.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Dequeue items
    let mut receive_handles = Vec::new();
    for i in 0..receive_threads {
        println!("Spawning dequeue thread {}", i);
        let rpq_clone = Arc::clone(&rpq);
        let recived_clone = Arc::clone(&received_counter);
        let sent_clone = Arc::clone(&sent_counter);
        receive_handles.push(std::thread::spawn(move || loop {
            if recived_clone.load(Ordering::Relaxed) >= message_count {
                if sent_clone.load(Ordering::Relaxed) == recived_clone.load(Ordering::Relaxed) {
                    break;
                }
            }

            if let Some(_item) = rpq_clone.dequeue() {
                recived_clone.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Wait for all threads to finish
    for handle in send_handles {
        handle.join().unwrap();
    }
    println!("Sent: {}", sent_counter.load(Ordering::Relaxed));

    // Wait for all threads to finish
    for handle in receive_handles {
        handle.join().unwrap();
    }
    println!("Recived: {}", received_counter.load(Ordering::Relaxed));
    println!("Total: {}", rpq.len());
}
