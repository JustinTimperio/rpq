use core::time;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use redb::{Database, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::interval;

mod bpq;
mod pq;

pub struct RPQ<T: Ord + Clone + Send> {
    // options is the configuration for the RPQ
    options: RPQOptions,
    // non_empty_buckets is a binary heap of priorities
    non_empty_buckets: bpq::BucketPriorityQueue,
    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<RwLock<HashMap<u64, pq::PriorityQueue<T>>>>,
    // items_in_queues is the number of items across all queues
    items_in_queues: AtomicU64,
    // disk_cache maintains a cache of items that are in the queue
    disk_cache: Arc<Database>,
    // lazy_disk_channel is the channel for lazy disk writes
    lazy_disk_writer_sender: Mutex<Sender<pq::Item<T>>>,
    // lazy_disk_reader is the receiver for lazy disk writes
    lazy_disk_writer_receiver: Mutex<Receiver<pq::Item<T>>>,
    // lazy_disk_delete_sender is the sender for lazy disk deletes
    lazy_disk_delete_sender: Mutex<Sender<pq::Item<T>>>,
    // lazy_disk_delete_receiver is the receiver for lazy disk deletes
    lazy_disk_delete_receiver: Mutex<Receiver<pq::Item<T>>>,
    // synced_batches is a map of batch ids that have been fully committed
    //synced_batches: Arc<RwLock<HashMap<u64, bool>>>,
}

pub struct RPQOptions {
    // bucket_count is the number of buckets in the RPQ
    pub bucket_count: u64,
    // disk_cache_enabled is a flag to enable or disable the disk cache
    pub disk_cache_enabled: bool,
    // disk_cache_path is the path to the disk cache
    pub database_path: String,
    // disk_cache_max_size is the maximum size of the disk cache
    pub lazy_disk_cache: bool,
    // lazy_disk_max_delay is the maximum delay for lazy disk writes
    pub lazy_disk_max_delay: time::Duration,
    // lazy_disk_cache_batch_size is the maximum batch size for lazy disk writes
    pub lazy_disk_cache_batch_size: u64,
    // buffer_size is the size of the buffer for the disk cache
    pub buffer_size: u64,
}

const DB: TableDefinition<&str, &[u8]> = TableDefinition::new("rpq");

impl<T: Ord + Clone + Send + Sync> RPQ<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    pub fn new(options: RPQOptions) -> Arc<RPQ<T>> {
        // Create base structures
        let buckets = Arc::new(RwLock::new(HashMap::new()));
        let items_in_queues = AtomicU64::new(0);
        let lazy_disk_cache_enabled = options.lazy_disk_cache;
        let disk_cache_enabled = options.disk_cache_enabled;

        // Create the lazy disk sync channel
        let (lazy_disk_writer_sender, lazy_disk_writer_receiver) =
            channel(options.buffer_size as usize);
        let (lazy_disk_delete_sender, lazy_disk_delete_receiver) =
            channel(options.buffer_size as usize);

        // Create the disk cache
        let path = options.database_path.clone();

        // Create the buckets
        for i in 0..options.bucket_count {
            buckets.write().unwrap().insert(i, pq::PriorityQueue::new());
        }

        // If the disk cache is enabled, load the items from the disk cache
        let disk_cache = Arc::new(Database::create(path).unwrap());
        let ctxn = disk_cache.begin_write().unwrap();
        ctxn.open_table(DB).unwrap();
        ctxn.commit().unwrap();

        if disk_cache_enabled {
            let read_txn = disk_cache.begin_read().unwrap();
            let table = read_txn.open_table(DB).unwrap();
            let mut cursor = table.range::<&str>(..);

            for entry in cursor.iter_mut() {
                match entry.next() {
                    Some(Ok((_key, value))) => {
                        let mut item = pq::Item::from_bytes(value.value());
                        // Mark the item as restored
                        item.set_restored();

                        let buckets = buckets.write().unwrap();
                        let bucket = buckets.get(&item.priority);
                        if bucket.is_none() {
                            continue;
                        }

                        bucket.unwrap().enqueue(item);
                    }
                    Some(Err(e)) => {
                        println!("Error reading from disk cache: {}", e);
                        continue;
                    }
                    None => {
                        break;
                    }
                }
            }
            _ = read_txn.close();
        }

        let rpq = RPQ {
            options,
            non_empty_buckets: bpq::BucketPriorityQueue::new(),
            buckets,
            items_in_queues,
            disk_cache,
            lazy_disk_writer_sender: Mutex::new(lazy_disk_writer_sender),
            lazy_disk_writer_receiver: Mutex::new(lazy_disk_writer_receiver),
            lazy_disk_delete_sender: Mutex::new(lazy_disk_delete_sender),
            lazy_disk_delete_receiver: Mutex::new(lazy_disk_delete_receiver),
            //synced_batches,
        };
        let rpq = Arc::new(rpq);

        // Launch the lazy disk writer
        if lazy_disk_cache_enabled {
            let rpq_clone = Arc::clone(&rpq);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(rpq_clone.lazy_disk_writer());
            });

            let rpq_clone = Arc::clone(&rpq);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(rpq_clone.lazy_disk_deleter());
            });
        }
        return rpq;
    }

    pub fn len(&self) -> u64 {
        let mut len = 0 as u64;
        for (_, active_bucket) in self.buckets.read().unwrap().iter() {
            len += active_bucket.len();
        }
        return len;
    }

    pub fn active_buckets(&self) -> u64 {
        self.non_empty_buckets.len()
    }

    pub async fn enqueue(&self, mut item: pq::Item<T>) {
        // Check if the item priority is greater than the bucket count
        if item.priority >= self.options.bucket_count {
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

        // If the disk cache is enabled, send the item to the lazy disk writer
        if self.options.disk_cache_enabled {
            if !item.was_restored() {
                item.set_disk_uuid();
                if self.options.lazy_disk_cache {
                    let lazy_disk_writer_sender = self.lazy_disk_writer_sender.lock().unwrap();
                    let was_sent = lazy_disk_writer_sender.send(item.clone()).await;
                    if was_sent.is_err() {
                        println!("Error sending item to lazy disk writer");
                        return;
                    }
                } else {
                    self.commit_single(item.clone());
                }
            }
        }

        // Enqueue the item and update
        bucket.unwrap().enqueue(item);
        self.non_empty_buckets.add_bucket(priority);
        self.items_in_queues.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn dequeue(&self) -> Option<pq::Item<T>> {
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

        let item_clone = item.clone();
        if self.options.disk_cache_enabled {
            if self.options.lazy_disk_cache {
                let lazy_disk_delete_sender = self.lazy_disk_delete_sender.lock().unwrap();
                let was_sent = lazy_disk_delete_sender.send(item.clone().unwrap()).await;
                if was_sent.is_err() {
                    println!("Error sending item to lazy disk delete");
                    return None;
                }
            } else {
                self.delete_single(item_clone.unwrap().get_disk_uuid().unwrap().as_ref());
            }
        }

        item
    }

    pub fn prioritize(&self) -> Option<(u64, u64)> {
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
        Some((removed, escalated))
    }

    async fn lazy_disk_writer(&self) {
        let mut write_cache = Vec::new();
        let mut ticker = interval(self.options.lazy_disk_max_delay);
        let mut receiver = self.lazy_disk_writer_receiver.lock().unwrap();
        println!("Starting lazy disk writer");

        loop {
            // Check if the write cache is full or the ticker has ticked
            tokio::select! {
                    _ = ticker.tick() => {
                        if write_cache.len() > 0 {
                            self.commit_batch(&mut write_cache);
                        }
                    },
                item = receiver.recv() => {
                    if let Some(item) = item {
                        write_cache.push(item);
                    }

                    if write_cache.len() as u64 >= self.options.lazy_disk_cache_batch_size {
                        self.commit_batch(&mut write_cache);
                    }
                }
            }
        }
    }

    async fn lazy_disk_deleter(&self) {
        let mut delete_cache = Vec::new();
        let mut ticker = interval(self.options.lazy_disk_max_delay);
        let mut receiver = self.lazy_disk_delete_receiver.lock().unwrap();

        loop {
            // Check if the write cache is full or the ticker has ticked
            tokio::select! {
                    _ = ticker.tick() => {
                        if delete_cache.len() > 0 {
                            self.delete_batch(&mut delete_cache);
                        }
                    },
                item = receiver.recv() => {
                    if let Some(item) = item {
                        delete_cache.push(item);
                    }

                    if delete_cache.len() as u64 >= self.options.lazy_disk_cache_batch_size {
                        self.delete_batch(&mut delete_cache);
                    }
                }
            }
        }
    }

    fn commit_batch(&self, write_cache: &mut Vec<pq::Item<T>>) {
        let write_txn = self.disk_cache.begin_write().unwrap();
        println!("Committing batch of {} items", write_cache.len());
        for item in write_cache.iter() {
            let mut table = write_txn.open_table(DB).unwrap();
            // Convert to bytes
            let b = item.to_bytes();
            let key = item.get_disk_uuid().unwrap();

            let was_written = table.insert(key.as_str(), &b[..]);
            if was_written.is_err() {
                println!("Error writing item to disk cache");
                continue;
            }
        }

        write_txn.commit().unwrap();
        write_cache.clear();
    }

    fn delete_batch(&self, delete_cache: &mut Vec<pq::Item<T>>) {
        let write_txn = self.disk_cache.begin_write().unwrap();
        for item in delete_cache.iter() {
            let mut table = write_txn.open_table(DB).unwrap();
            // Convert to bytes
            let key = item.get_disk_uuid().unwrap();

            let was_deleted = table.remove(key.as_str());
            if was_deleted.is_err() {
                println!("Error deleting item from disk cache");
                continue;
            }
        }

        write_txn.commit().unwrap();
        delete_cache.clear();
    }

    fn commit_single(&self, item: pq::Item<T>) {
        let write_txn = self.disk_cache.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DB).unwrap();
            // Convert to bytes
            let b = item.to_bytes();
            let key = item.get_disk_uuid().unwrap();

            let was_written = table.insert(key.as_str(), &b[..]);
            if was_written.is_err() {
                println!("Error writing item to disk cache");
                return;
            }
        }

        write_txn.commit().unwrap();
    }

    fn delete_single(&self, key: &str) {
        let write_txn = self.disk_cache.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DB).unwrap();
            let was_written = table.remove(key);
            if was_written.is_err() {
                println!("Error writing item to disk cache");
                return;
            }
        }

        write_txn.commit().unwrap();
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
        let send_threads = 4;
        let receive_threads = 1;
        let bucket_count = 10;
        let sent_counter = Arc::new(AtomicU64::new(0));
        let received_counter = Arc::new(AtomicU64::new(0));
        let removed_counter = Arc::new(AtomicU64::new(0));

        // Create the RPQ
        let options = RPQOptions {
            bucket_count,
            disk_cache_enabled: true,
            database_path: "/tmp/rpq.redb".to_string(),
            lazy_disk_cache: false,
            lazy_disk_max_delay: time::Duration::from_secs(5),
            lazy_disk_cache_batch_size: 50000,
            buffer_size: 1_000_000,
        };
        let rpq = Arc::new(RPQ::new(options));

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

            send_handles.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                runtime.block_on(async {
                    loop {
                        let item = pq::Item::new(
                            rand::thread_rng().gen_range(0..bucket_count),
                            0,
                            None,
                            false,
                            None,
                            false,
                            Some(std::time::Duration::from_secs(5)),
                        );

                        if sent_clone.load(Ordering::Relaxed) >= message_count {
                            break;
                        }

                        rpq_clone.enqueue(item).await;
                        sent_clone.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }));
        }

        // Dequeue items
        println!("Launching {} Receive Threads", receive_threads);
        let mut receive_handles = Vec::new();
        for _ in 0..receive_threads {
            // Clone all the shared variables
            let rpq_clone = Arc::clone(&rpq);
            let received_clone = Arc::clone(&received_counter);
            let sent_clone = Arc::clone(&sent_counter);
            let removed_clone = Arc::clone(&removed_counter);

            // Spawn the thread
            receive_handles.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    loop {
                        if (sent_clone.load(Ordering::Relaxed)
                            == (received_clone.load(Ordering::Relaxed)
                                + removed_clone.load(Ordering::Relaxed)))
                            && rpq_clone.len() == 0
                            && sent_clone.load(Ordering::Relaxed) >= message_count
                        {
                            break;
                        }

                        let item = rpq_clone.dequeue().await;
                        if item.is_none() {
                            continue;
                        }

                        received_clone.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }));
        }

        // Wait for send threads to finish
        for handle in send_handles {
            handle.join().unwrap();
        }

        // Wait for receive threads to finish
        for handle in receive_handles {
            handle.join().unwrap();
        }

        println!("Total: {}", rpq.len());
    }
}
