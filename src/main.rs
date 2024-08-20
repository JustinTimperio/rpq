use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

mod bpq;
mod pq;

struct RPQ<T: Ord + Clone> {
    // total_len is the number of items across all queues
    pub bucket_count: u64,

    // non_empty_buckets is a binary heap of priorities
    non_empty_buckets: bpq::BucketPriorityQueue,
    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<RwLock<HashMap<u64, pq::PriorityQueue<T>>>>,
    // items_in_queues is the number of items across all queues
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
        // Fetch the bucket_id
        let bucket_id = self.non_empty_buckets.peek();
        if bucket_id.is_none() {
            return None;
        }
        let bucket_id = bucket_id.unwrap();

        // Fetch the queue
        let queue = self.buckets.read().unwrap();
        let queue = queue.get(&bucket_id);
        if queue.is_none() {
            return None;
        }

        // Peek the item
        queue.unwrap().peek()
    }

    fn enqueue(&self, item: pq::Item<T>) {
        // Check if the item priority is greater than the bucket count
        if item.priority >= self.bucket_count {
            println!("Item priority is greater than bucket count");
            return;
        }

        // Get the bucket and enqueue the item
        let buckets = self.buckets.read().unwrap();
        let bucket = buckets.get(&item.priority);

        if bucket.is_none() {
            println!("Bucket is none for id: {}", item.priority);
            return;
        }

        // Enqueue the item and update
        bucket.unwrap().enqueue(item.clone());
        self.non_empty_buckets.add_bucket(item.priority);
        self.items_in_queues.fetch_add(1, Ordering::Relaxed);
    }

    fn dequeue(&self) -> Option<pq::Item<T>> {
        let buckets = self.buckets.read().unwrap();

        // Fetch the bucket
        let bucket_id = self.non_empty_buckets.peek();
        if bucket_id.is_none() {
            return None;
        }
        let bucket_id = bucket_id.unwrap();

        // Fetch the queue
        let queue = buckets.get(&bucket_id);
        if queue.is_none() {
            println!("Bucket is none for id: {}", bucket_id);
            return None;
        }

        // Fetch the item from the bucket
        let item = queue.unwrap().dequeue();
        if item.is_none() {
            self.non_empty_buckets.remove_bucket(bucket_id);
            return None;
        }
        self.items_in_queues.fetch_sub(1, Ordering::Relaxed);

        // If the bucket is empty, remove it from the non_empty_buckets
        if queue.unwrap().len() == 0 {
            self.non_empty_buckets.remove_bucket(bucket_id);
        }
        return item;
    }
}

fn main() {
    // Set Message Count
    let message_count = 10_000_000;

    // Set Concurrency
    let send_threads = 20;
    let receive_threads = 1;
    let bucket_count = 10;
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
        send_handles.push(std::thread::spawn(move || loop {
            if sent_clone.load(Ordering::Relaxed) >= message_count {
                break;
            }

            let item = pq::Item::new(
                rand::thread_rng().gen_range(0..bucket_count),
                0,
                Some(Uuid::new_v4().to_string()),
                false,
                None,
                false,
                None,
            );

            rpq_clone.enqueue(item);
            sent_clone.fetch_add(1, Ordering::Relaxed);
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

    // Wait for send threads to finish
    for handle in send_handles {
        handle.join().unwrap();
    }
    println!("Sent: {}", sent_counter.load(Ordering::Relaxed));

    // Wait for receive threads to finish
    for handle in receive_handles {
        handle.join().unwrap();
    }
    println!("Recived: {}", received_counter.load(Ordering::Relaxed));

    println!("Total: {}", rpq.len());
}
