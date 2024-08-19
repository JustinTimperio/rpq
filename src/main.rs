use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

extern crate scopeguard;

mod bpq;
mod pq;
mod schema;

struct RPQ<T: Ord + Clone> {
    // total_len is the number of items across all queues
    pub bucket_count: u64,
    // non_empty_buckets is a binary heap of priorities
    pub non_empty_buckets: bpq::BucketPriorityQueue,

    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<RwLock<HashMap<u64, pq::PriorityQueue<T>>>>,
}

impl<T: Ord + Clone> RPQ<T> {
    fn new(bucket_count: u64) -> RPQ<T> {
        let buckets = Arc::new(RwLock::new(HashMap::new()));

        for i in 0..bucket_count {
            buckets
                .write()
                .unwrap()
                .insert(i, pq::PriorityQueue::new(bpq::BucketPriorityQueue::new()));
        }

        RPQ {
            bucket_count,
            non_empty_buckets: bpq::BucketPriorityQueue::new(),
            buckets,
        }
    }

    fn len(&self) -> u64 {
        let mut total_len: u64 = 0;

        for (_, queue) in self.buckets.read().unwrap().iter() {
            total_len += queue.len();
        }

        total_len
    }

    fn peek(&self) -> Option<schema::Item<T>> {
        let bucket_id: u64 = self.non_empty_buckets.peek()?;
        self.buckets.read().unwrap().get(&bucket_id)?.peek()
    }

    fn enqueue(&self, item: schema::Item<T>) {
        let bucket_id: u64 = item.priority;
        self.buckets
            .read()
            .unwrap()
            .get(&bucket_id)
            .unwrap()
            .enqueue(item.clone());
        self.non_empty_buckets.add_bucket(bucket_id);
    }

    fn dequeue(&self) -> Option<schema::Item<T>> {
        let bucket_id: u64 = self.non_empty_buckets.peek()?;
        let item = self.buckets.read().unwrap().get(&bucket_id)?.dequeue()?;

        if self.buckets.read().unwrap().get(&bucket_id)?.len() == 0 {
            self.non_empty_buckets.remove_bucket(bucket_id);
        }
        Some(item)
    }
}

fn main() {
    let send_threads = 10;
    let receive_threads = 1;
    let bucket_count = 10;
    let rpq = Arc::new(RPQ::new(bucket_count));

    // Enqueue items
    let mut send_handles = Vec::new();
    let sent = Arc::new(AtomicU64::new(0));
    for i in 0..send_threads {
        let rpq_clone = Arc::clone(&rpq);
        let sent_clone = Arc::clone(&sent);
        println!("Spawning thread {}", i);
        send_handles.push(std::thread::spawn(move || {
            for j in 0..10_000_000 / send_threads {
                let item: schema::Item<u64> = schema::Item {
                    // Random priority between 0 and 9
                    priority: rand::thread_rng().gen_range(0..bucket_count),
                    data: j,
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
                // This is safe because the item is only used in this thread
                // and the thread is joined before the item goes out of scope
                rpq_clone.enqueue(item);
                sent_clone.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Dequeue items
    let received = Arc::new(AtomicU64::new(0));
    let mut receive_handles = Vec::new();
    for i in 0..receive_threads {
        println!("Spawning dequeue thread {}", i);
        let rpq_clone = Arc::clone(&rpq);
        let recived_clone = Arc::clone(&received);
        receive_handles.push(std::thread::spawn(move || {
            for _ in 0..10_000_000 / receive_threads {
                loop {
                    if let Some(_item) = rpq_clone.dequeue() {
                        recived_clone.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }));
    }

    // Wait for all threads to finish
    for handle in send_handles {
        handle.join().unwrap();
    }
    println!("Sent: {}", sent.load(Ordering::Relaxed));

    // Wait for all threads to finish
    for handle in receive_handles {
        handle.join().unwrap();
    }
    println!("Recived: {}", received.load(Ordering::Relaxed));
}
