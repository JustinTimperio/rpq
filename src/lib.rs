use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

mod bpq;
mod pq;

struct RPQ<T: Ord> {
    // total_len is the number of items across all queues
    pub bucket_count: u64,

    // non_empty_buckets is a binary heap of priorities
    non_empty_buckets: bpq::BucketPriorityQueue,
    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<RwLock<HashMap<u64, pq::PriorityQueue<T>>>>,
    // items_in_queues is the number of items across all queues
    items_in_queues: AtomicU64,
}

impl<T: Ord> RPQ<T> {
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

    fn enqueue(&self, item: pq::Item<T>) {
        // Check if the item priority is greater than the bucket count
        if item.priority >= self.bucket_count {
            println!("Item priority is greater than bucket count");
            return;
        }
        let priority = item.priority;

        // Get the bucket and enqueue the item
        let buckets = self.buckets.read().unwrap();
        let bucket = buckets.get(&item.priority);

        if bucket.is_none() {
            println!("Bucket is none for id: {}", priority);
            return;
        }

        // Enqueue the item and update
        bucket.unwrap().enqueue(item);
        self.non_empty_buckets.add_bucket(priority);
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
            return None;
        }

        // Fetch the item from the bucket
        let item = queue.unwrap().dequeue();
        if !item.is_none() {
            self.items_in_queues.fetch_sub(1, Ordering::Relaxed);
        }

        // If the bucket is empty, remove it from the non_empty_buckets
        if queue.unwrap().len() == 0 {
            self.non_empty_buckets.remove_bucket(&bucket_id);
        }
        return item;
    }

    fn prioritize(&self) -> Option<(u64, u64)> {
        let mut removed = 0;
        let mut escalated = 0;

        for (_, active_bucket) in self.buckets.read().unwrap().iter() {
            let results = active_bucket.prioritize();
            if results.is_none() {
                continue;
            }
            removed += results.unwrap().0;
            escalated += results.unwrap().1;
        }
        self.items_in_queues.fetch_sub(removed, Ordering::Relaxed);
        return Some((removed, escalated));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time;
    use rand::Rng;

    #[test]
    fn e2e_test() {
        // Set Message Count
        let message_count = 10_000_000;

        // Set Concurrency
        let send_threads = 1;
        let receive_threads = 1;
        let bucket_count = 10;
        let sent_counter = Arc::new(AtomicU64::new(0));
        let received_counter = Arc::new(AtomicU64::new(0));
        let removed_counter = Arc::new(AtomicU64::new(0));

        // Create the RPQ
        let rpq = Arc::new(RPQ::new(bucket_count));

        // Launch the monitoring thread
        let rpq_clone = Arc::clone(&rpq);
        let removed_clone = Arc::clone(&removed_counter);
        let received_clone = Arc::clone(&received_counter);
        let sent_clone = Arc::clone(&sent_counter);
        std::thread::spawn(move || loop {
            let mut total_removed = 0;
            let mut total_escalated = 0;

            let results = rpq_clone.prioritize();
            if !results.is_none() {
                let (removed, escalated) = results.unwrap();
                total_escalated += escalated;
                total_removed += removed;
                removed_clone.fetch_add(removed, Ordering::Relaxed);
            }

            println!(
                "Total: {} | Sent: {} | Received: {} | Removed: {} | Escalated: {} | Removed: {}",
                rpq_clone.len(),
                sent_clone.load(Ordering::Relaxed),
                received_clone.load(Ordering::Relaxed),
                removed_clone.load(Ordering::Relaxed),
                total_escalated,
                total_removed
            );
            std::thread::sleep(time::Duration::from_secs(1));
        });
        // Enqueue items
        println!("Launching {} Send Threads", send_threads);
        let mut send_handles = Vec::new();
        for _ in 0..send_threads {
            let rpq_clone = Arc::clone(&rpq);
            let sent_clone = Arc::clone(&sent_counter);
            send_handles.push(std::thread::spawn(move || loop {
                let item = pq::Item::new(
                    rand::thread_rng().gen_range(0..bucket_count),
                    0,
                    None,
                    false,
                    None,
                    true,
                    Some(std::time::Duration::from_secs(5)),
                );

                if sent_clone.load(Ordering::Relaxed) >= message_count {
                    break;
                }

                rpq_clone.enqueue(item);
                sent_clone.fetch_add(1, Ordering::Relaxed);
            }));
        }

        // Wait for send threads to finish
        for handle in send_handles {
            handle.join().unwrap();
        }

        // Dequeue items
        println!("Launching {} Receive Threads", receive_threads);
        let mut receive_handles = Vec::new();
        for _ in 0..receive_threads {
            // Clone all the shared variables
            let rpq_clone = Arc::clone(&rpq);
            let recived_clone = Arc::clone(&received_counter);
            let sent_clone = Arc::clone(&sent_counter);
            let removed_clone = Arc::clone(&removed_counter);

            // Spawn the thread
            receive_handles.push(std::thread::spawn(move || loop {
                if (sent_clone.load(Ordering::Relaxed)
                    == (recived_clone.load(Ordering::Relaxed)
                        + removed_clone.load(Ordering::Relaxed)))
                    && rpq_clone.len() == 0
                    && sent_clone.load(Ordering::Relaxed) >= message_count
                {
                    break;
                }

                let item = rpq_clone.dequeue();
                if item.is_none() {
                    continue;
                }

                recived_clone.fetch_add(1, Ordering::Relaxed);
            }));
        }

        // Wait for receive threads to finish
        for handle in receive_handles {
            handle.join().unwrap();
        }

        println!("Total: {}", rpq.len());
    }
}
