//! # RPQ
//! RPQ implements a in memory, disk cached priority queue that is optimized for high throughput and low latency
//! while still maintaining strict ordering guarantees and durability. Due to the fact that most operations are done
//! in constant time O(1) or logarithmic time O(log n), with the exception of the prioritize function which happens
//! in linear time O(n), all RPQ operations are extremely fast. A single RPQ can handle a few million transactions
//! a second and can be tuned depending on your work load.
//!
//!  # Create a new RPQ
//! The RPQ should always be created with the new function like so:
//!
//! ```rust
//! use chrono::Duration;
//! use rpq::{schema::RPQOptions, schema::Item, RPQ};
//!
//! #[tokio::main(flavor = "multi_thread")]
//! async fn main() {
//!     let message_count = 1_000;
//!
//!     let options = RPQOptions {
//!         max_priority: 10,
//!         disk_cache_enabled: true,
//!         database_path: "/tmp/rpq-prioritize.redb".to_string(),
//!         lazy_disk_cache: true,
//!         lazy_disk_write_delay: Duration::seconds(5),
//!         lazy_disk_cache_batch_size: 10_000,
//!     };
//!
//!     let r = RPQ::new(options).await;
//!     match r {
//!         Ok(_) => {}
//!         Err(e) => {
//!             println!("Error Creating RPQ: {}", e);
//!             return;
//!         }
//!     }
//!
//!     let (rpq, _restored_items) = r.unwrap();
//!
//!     for i in 0..message_count {
//!         let item = Item::new(
//!             i % 10,
//!             i,
//!             false,
//!             None,
//!             false,
//!             None,
//!         );
//!
//!         let result = rpq.enqueue(item).await;
//!         if result.is_err() {
//!             println!("Error Enqueuing: {}", result.err().unwrap());
//!             return;
//!         }
//!     }
//!
//!     for _i in 0..message_count {
//!         let result = rpq.dequeue().await;
//!             if result.is_err() {
//!                 println!("Error Dequeuing: {}", result.err().unwrap());
//!                 return;
//!             }
//!         }
//!
//!     rpq.close().await;
//! }
//! ```
//!
//! # Architecture Notes
//! In many ways, RPQ slightly compromises the performance of a traditional priority queue in order to provide
//! a variety of features that are useful when absorbing distributed load from many down or upstream services.
//! It employs a fairly novel technique that allows it to lazily write and delete items from a disk cache while
//! still maintaining data in memory. This basically means that a object can be added to the queue and then removed
//! without the disk commit ever blocking the processes sending or receiving the data. In the case that a batch of data
//! has already been removed from the queue before it is written to disk, the data is simply discarded. This
//! dramatically reduces the amount of time spent doing disk commits and allows for much better performance in the
//! case that you need disk caching and still want to maintain a high peak throughput.
//!
//! ```text
//!                 ┌───────┐
//!                 │ Item  │
//!                 └───┬───┘
//!                     │
//!                     ▼
//!              ┌─────────────┐
//!              │             │
//!              │   enqueue   │
//!              │             │
//!              │             │
//!              └──────┬──────┘
//!                     │
//!                     │
//!                     │
//! ┌───────────────┐   │    ┌──────────────┐
//! │               │   │    │              │      ┌───┐
//! │   VecDeque    │   │    │  Lazy Disk   │      │   │
//! │               │◄──┴───►│    Writer    ├─────►│ D │
//! │               │        │              │      │ i │
//! └───────┬───────┘        └──────────────┘      │ s │
//!         │                                      │ k │
//!         │                                      │   │
//!         │                                      │ C │
//!         ▼                                      │ a │
//! ┌───────────────┐         ┌─────────────┐      │ c │
//! │               │         │             │      │ h │
//! │    dequeue    │         │   Lazy Disk ├─────►│ e │
//! │               ├────────►│    Deleter  │      │   │
//! │               │         │             │      └───┘
//! └───────┬───────┘         └─────────────┘
//!         │
//!         ▼
//!      ┌──────┐
//!      │ Item │
//!      └──────┘
//! ```
use std::collections::HashMap;
use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::result::Result;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::interval;

mod disk;
pub mod pq;
pub mod schema;

/// RPQ holds private items and configuration for the RPQ.
pub struct RPQ<T: Clone + Send> {
    // options is the configuration for the RPQ
    options: schema::RPQOptions,

    // queue is the main queue that holds the items
    queue: Mutex<pq::PriorityQueue<T>>,

    // disk_cache maintains a cache of items that are in the queue
    disk_cache: Arc<Option<disk::DiskCache<T>>>,

    // lazy_disk_channel is the channel for lazy disk writes
    lazy_disk_writer_sender: UnboundedSender<schema::Item<T>>,
    // lazy_disk_reader is the receiver for lazy disk writes
    lazy_disk_writer_receiver: Mutex<UnboundedReceiver<schema::Item<T>>>,
    // lazy_disk_delete_sender is the sender for lazy disk deletes
    lazy_disk_delete_sender: Arc<UnboundedSender<schema::Item<T>>>,
    // lazy_disk_delete_receiver is the receiver for lazy disk deletes
    lazy_disk_delete_receiver: Mutex<UnboundedReceiver<schema::Item<T>>>,

    // lazy_disk_sync_handles is a map of priorities to sync handles
    lazy_disk_sync_handles: Mutex<Vec<JoinHandle<()>>>,
    // lazy_disk_batch_handler is the handler for batches
    lazy_disk_batch_handler: Mutex<BatchHandler>,
    // lazy_disk_batch_counter is the counter for batches
    lazy_disk_batch_counter: Mutex<BatchCounter>,

    // lazy_disk_batch_shutdown_receiver is the receiver for the shutdown signal
    lazy_disk_batch_shutdown_receiver: watch::Receiver<bool>,
    // lazy_disk_batch_shutdown_sender is the sender for the shutdown signal
    lazy_disk_batch_shutdown_sender: watch::Sender<bool>,
    // lazy_disk_shutdown_receiver is the receiver for the shutdown signal
    lazy_disk_shutdown_receiver: watch::Receiver<bool>,
    // lazy_disk_shutdown_sender is the sender for the shutdown signal
    lazy_disk_shutdown_sender: watch::Sender<bool>,
}

struct BatchHandler {
    // synced_batches is a map of priorities to the last synced batch
    synced_batches: HashMap<usize, bool>,
    // deleted_batches is a map of priorities to the last deleted batch
    deleted_batches: HashMap<usize, bool>,
}

struct BatchCounter {
    // message_counter is the counter for the number of messages that have been sent to the RPQ over the lifetime
    message_counter: usize,
    // batch_number is the current batch number
    batch_number: usize,
}

impl<T: Clone + Send + Sync> RPQ<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    /// Creates a new RPQ with the given options and returns the RPQ and the number of items restored from the disk cache
    pub async fn new(options: schema::RPQOptions) -> Result<(Arc<RPQ<T>>, usize), Box<dyn Error>> {
        // Create base structures
        let sync_handles = Vec::new();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let (batch_shutdown_sender, batch_shutdown_receiver) = watch::channel(false);
        let batch_handler = BatchHandler {
            synced_batches: HashMap::new(),
            deleted_batches: HashMap::new(),
        };
        let batch_counter = BatchCounter {
            message_counter: 0,
            batch_number: 0,
        };

        // Create the lazy disk sync channel
        let (lazy_disk_writer_sender, lazy_disk_writer_receiver) = unbounded_channel();
        let (lazy_disk_delete_sender, lazy_disk_delete_receiver) = unbounded_channel();
        let lazy_disk_delete_sender = Arc::new(lazy_disk_delete_sender);
        let lazy_disk_delete_sender_clone = Arc::clone(&lazy_disk_delete_sender);

        // Capture some variables
        let path = options.database_path.clone();
        let disk_cache_enabled = options.disk_cache_enabled;
        let lazy_disk_cache = options.lazy_disk_cache;
        let max_priority = options.max_priority;

        let disk_cache = if disk_cache_enabled {
            Arc::new(Some(disk::DiskCache::new(&path)))
        } else {
            Arc::new(None)
        };

        let disk_cache_clone_one = Arc::clone(&disk_cache);
        let disk_cache_clone_two = Arc::clone(&disk_cache);

        // Create the RPQ
        let rpq = RPQ {
            options,
            queue: Mutex::new(pq::PriorityQueue::new(
                max_priority,
                disk_cache_enabled,
                lazy_disk_cache,
                lazy_disk_delete_sender_clone,
                disk_cache_clone_one,
            )),

            disk_cache: disk_cache_clone_two,

            lazy_disk_sync_handles: Mutex::new(sync_handles),
            lazy_disk_writer_sender: lazy_disk_writer_sender,
            lazy_disk_writer_receiver: Mutex::new(lazy_disk_writer_receiver),
            lazy_disk_delete_sender: lazy_disk_delete_sender,
            lazy_disk_delete_receiver: Mutex::new(lazy_disk_delete_receiver),
            lazy_disk_shutdown_receiver: shutdown_receiver,
            lazy_disk_shutdown_sender: shutdown_sender,
            lazy_disk_batch_handler: Mutex::new(batch_handler),
            lazy_disk_batch_counter: Mutex::new(batch_counter),
            lazy_disk_batch_shutdown_sender: batch_shutdown_sender,
            lazy_disk_batch_shutdown_receiver: batch_shutdown_receiver,
        };
        let rpq = Arc::new(rpq);

        // Restore the items from the disk cache
        let mut restored_items: usize = 0;
        if disk_cache_enabled {
            let result = rpq.restore_from_disk().await;
            if result.is_err() {
                return Err(Box::<dyn Error>::from(result.err().unwrap()));
            }
            restored_items = result?;

            if lazy_disk_cache {
                let mut handles = rpq.lazy_disk_sync_handles.lock().await;
                let rpq_clone = Arc::clone(&rpq);
                handles.push(tokio::spawn(async move {
                    let result = rpq_clone.lazy_disk_writer().await;
                    if result.is_err() {
                        panic!("Error in lazy disk writer: {:?}", result.err().unwrap());
                    }
                }));

                let rpq_clone = Arc::clone(&rpq);
                handles.push(tokio::spawn(async move {
                    let result = rpq_clone.lazy_disk_deleter().await;
                    if result.is_err() {
                        panic!("Error in lazy disk deleter: {:?}", result.err().unwrap());
                    }
                }));
            }
        }
        Ok((rpq, restored_items))
    }

    /// Adds an item to the RPQ and returns an error if one occurs otherwise it returns ()
    pub async fn enqueue(&self, mut item: schema::Item<T>) -> Result<(), Box<dyn Error>> {
        // If the disk cache is enabled, send the item to the lazy disk writer
        if self.options.disk_cache_enabled {
            // Increment the batch number
            let mut batch_counter = self.lazy_disk_batch_counter.lock().await;
            batch_counter.message_counter += 1;
            if batch_counter.message_counter % self.options.lazy_disk_cache_batch_size == 0 {
                batch_counter.batch_number += 1;
            }
            let bn = batch_counter.batch_number;
            drop(batch_counter);

            item.set_batch_id(bn);
            if !item.was_restored() {
                item.set_disk_uuid();
                if self.options.lazy_disk_cache {
                    let lazy_disk_writer_sender = &self.lazy_disk_writer_sender;
                    let was_sent = lazy_disk_writer_sender.send(item.clone());
                    match was_sent {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(Box::<dyn Error>::from(e));
                        }
                    }
                } else {
                    let db = self.disk_cache.as_ref();
                    match db {
                        None => {
                            return Err(Box::<dyn Error>::from(IoError::new(
                                ErrorKind::InvalidInput,
                                "Error getting disk cache",
                            )));
                        }
                        Some(db) => {
                            let result = db.commit_single(item.clone());
                            match result {
                                Ok(_) => {}
                                Err(e) => {
                                    return Err(Box::<dyn Error>::from(e));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Enqueue the item
        self.queue.lock().await.enqueue(item);
        Ok(())
    }

    /// Adds a batch of items to the RPQ and returns an error if one occurs otherwise it returns ()
    pub async fn enqueue_batch(
        &self,
        mut items: Vec<schema::Item<T>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut queue = self.queue.lock().await;
        for item in items.iter_mut() {
            // If the disk cache is enabled, send the item to the lazy disk writer
            if self.options.disk_cache_enabled {
                // Increment the batch number
                let mut batch_counter = self.lazy_disk_batch_counter.lock().await;
                batch_counter.message_counter += 1;
                if batch_counter.message_counter % self.options.lazy_disk_cache_batch_size == 0 {
                    batch_counter.batch_number += 1;
                }
                let bn = batch_counter.batch_number;
                drop(batch_counter);

                item.set_batch_id(bn);
                if !item.was_restored() {
                    item.set_disk_uuid();
                    if self.options.lazy_disk_cache {
                        let lazy_disk_writer_sender = &self.lazy_disk_writer_sender;
                        let was_sent = lazy_disk_writer_sender.send(item.clone());
                        match was_sent {
                            Ok(_) => {}
                            Err(e) => {
                                return Err(Box::<dyn Error>::from(e));
                            }
                        }
                    } else {
                        let db = self.disk_cache.as_ref();
                        match db {
                            None => {
                                return Err(Box::<dyn Error>::from(IoError::new(
                                    ErrorKind::InvalidInput,
                                    "Error getting disk cache",
                                )));
                            }
                            Some(db) => {
                                let result = db.commit_single(item.clone());
                                match result {
                                    Ok(_) => {}
                                    Err(e) => {
                                        return Err(Box::<dyn Error>::from(e));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Enqueue the item
            queue.enqueue(item.clone());
        }
        Ok(())
    }

    /// Returns a Result with the next item in the RPQ or an error if one occurs
    pub async fn dequeue(&self) -> Result<Option<schema::Item<T>>, Box<dyn Error>> {
        let item = self.queue.lock().await.dequeue();
        if item.is_none() {
            return Ok(None);
        }
        let item = item.unwrap();

        if self.options.disk_cache_enabled {
            if self.options.lazy_disk_cache {
                let lazy_disk_delete_sender = &self.lazy_disk_delete_sender;
                let was_sent = lazy_disk_delete_sender.send(item.clone());
                match was_sent {
                    Ok(_) => {}
                    Err(e) => {
                        return Result::Err(Box::new(e));
                    }
                }
            } else {
                let id = match item.get_disk_uuid() {
                    Some(id) => id,
                    None => {
                        return Result::Err(Box::new(IoError::new(
                            ErrorKind::InvalidInput,
                            "Error getting disk uuid",
                        )));
                    }
                };

                let db = self.disk_cache.as_ref();
                match db {
                    None => {
                        return Result::Err(Box::new(IoError::new(
                            ErrorKind::InvalidInput,
                            "Error getting disk cache",
                        )));
                    }
                    Some(db) => {
                        let result = db.delete_single(&id);
                        if result.is_err() {
                            return Result::Err(result.err().unwrap());
                        }
                    }
                }
            }
        }

        Ok(Some(item))
    }

    /// Returns a Result with a batch of items in the RPQ or an error if one occurs
    pub async fn dequeue_batch(
        &self,
        count: usize,
    ) -> Result<Option<Vec<schema::Item<T>>>, Box<dyn Error>> {
        let mut items = Vec::new();
        let mut queue = self.queue.lock().await;
        for _ in 0..count {
            let item = queue.dequeue();
            if item.is_none() {
                break;
            }
            let item = item.unwrap();

            if self.options.disk_cache_enabled {
                if self.options.lazy_disk_cache {
                    let lazy_disk_delete_sender = &self.lazy_disk_delete_sender;
                    let was_sent = lazy_disk_delete_sender.send(item.clone());
                    match was_sent {
                        Ok(_) => {}
                        Err(e) => {
                            return Result::Err(Box::new(e));
                        }
                    }
                } else {
                    let id = match item.get_disk_uuid() {
                        Some(id) => id,
                        None => {
                            return Result::Err(Box::new(IoError::new(
                                ErrorKind::InvalidInput,
                                "Error getting disk uuid",
                            )));
                        }
                    };

                    let db = self.disk_cache.as_ref();
                    match db {
                        None => {
                            return Result::Err(Box::new(IoError::new(
                                ErrorKind::InvalidInput,
                                "Error getting disk cache",
                            )));
                        }
                        Some(db) => {
                            let result = db.delete_single(&id);
                            if result.is_err() {
                                return Result::Err(result.err().unwrap());
                            }
                        }
                    }
                }
            }
            items.push(item);
        }
        if items.is_empty() {
            return Ok(None);
        }
        Ok(Some(items))
    }

    /// Prioritize reorders the items in each bucket based on the values specified in the item.
    /// It returns a tuple with the number of items removed and the number of items escalated or and error if one occurs.
    pub async fn prioritize(&self) -> Result<(usize, usize), Box<dyn Error>> {
        self.queue.lock().await.prioritize()
    }

    /// Returns the number of items in the RPQ across all buckets
    pub async fn len(&self) -> usize {
        self.queue.lock().await.items_in_queue()
    }

    /// Returns the number of active buckets in the RPQ (buckets with items)
    pub async fn active_buckets(&self) -> usize {
        self.queue.lock().await.active_buckets()
    }

    /// Returns the number of pending batches in the RPQ for both the writer or the deleter
    pub async fn unsynced_batches(&self) -> usize {
        let batch_handler = self.lazy_disk_batch_handler.lock().await;
        batch_handler
            .synced_batches
            .iter()
            .chain(batch_handler.deleted_batches.iter())
            .filter(|&(_, synced_or_deleted)| !*synced_or_deleted)
            .count()
    }

    /// Returns the number of items in the disk cache which can be helpful for debugging or monitoring
    pub fn items_in_db(&self) -> usize {
        if self.disk_cache.is_none() {
            return 0;
        }

        let db = self.disk_cache.as_ref();
        match db {
            None => 0,
            Some(db) => db.items_in_db(),
        }
    }

    /// Closes the RPQ and waits for all the async tasks to finish
    pub async fn close(&self) {
        self.lazy_disk_shutdown_sender.send(true).unwrap();

        let mut handles = self.lazy_disk_sync_handles.lock().await;
        while let Some(handle) = handles.pop() {
            handle.await.unwrap();
        }
    }

    async fn lazy_disk_writer(&self) -> Result<(), Box<dyn Error>> {
        let mut awaiting_batches = HashMap::<usize, Vec<schema::Item<T>>>::new();
        let mut ticker = interval(self.options.lazy_disk_write_delay.to_std().unwrap());
        let mut receiver = self.lazy_disk_writer_receiver.lock().await;
        let mut shutdown_receiver = self.lazy_disk_shutdown_receiver.clone();

        loop {
            // Check if the write cache is full or the ticker has ticked
            tokio::select! {
                // Flush the cache if the ticker has ticked
                _ = ticker.tick() => {
                    let mut batch_handler = self.lazy_disk_batch_handler.lock().await;
                    for (id, batch) in awaiting_batches.iter_mut() {

                        if batch.len() >= self.options.lazy_disk_cache_batch_size {
                            if *batch_handler.deleted_batches.get(id).unwrap_or(&false) {
                                batch.clear();
                            } else {
                                let db = self.disk_cache.as_ref();
                                match db {
                                    None => {
                                        return Result::Err(Box::new(IoError::new(
                                            ErrorKind::InvalidInput,
                                            "Error getting disk cache",
                                        )));
                                    }
                                    Some(db) => {
                                            let result = db.commit_batch(batch);
                                            if result.is_err() {
                                                return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                            }
                                    }
                                }
                            }

                            batch_handler.synced_batches.insert(*id, true);
                            batch_handler.deleted_batches.insert(*id, false);
                        }
                    }
                },

                // Add the item to the cache if it is received
                item = receiver.recv() => {
                    if let Some(item) = item {
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);
                    }
                },

                // Shutdown the writer if the shutdown signal is received
                _ = shutdown_receiver.changed() => {
                    receiver.close();

                    // Pull the remaining items from the receiver
                    while let Some(item) = receiver.recv().await {
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);
                    }

                    // Commit the remaining batches
                    for (id, batch) in awaiting_batches.iter_mut() {
                        let mut batch_handler = self.lazy_disk_batch_handler.lock().await;

                        if *batch_handler.deleted_batches.get(id).unwrap_or(&false) {
                            batch.clear();
                            continue;
                        }

                        batch_handler.synced_batches.insert(*id, true);
                        batch_handler.deleted_batches.insert(*id, false);

                        let db = self.disk_cache.as_ref();
                        match db {
                            None => {
                                return Result::Err(Box::new(IoError::new(
                                    ErrorKind::InvalidInput,
                                    "Error getting disk cache",
                                )));
                            }
                            Some(db) => {
                                    let result = db.commit_batch(batch);
                                    if result.is_err() {
                                        return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                    }
                            }
                        }
                    }
                    self.lazy_disk_batch_shutdown_sender.send(true).unwrap();

                    break Ok(());
                }
            }
        }
    }

    async fn lazy_disk_deleter(&self) -> Result<(), Box<dyn Error>> {
        let mut awaiting_batches = HashMap::<usize, Vec<schema::Item<T>>>::new();
        let mut restored_items: Vec<schema::Item<T>> = Vec::new();
        let mut receiver = self.lazy_disk_delete_receiver.lock().await;
        let mut shutdown_receiver = self.lazy_disk_batch_shutdown_receiver.clone();

        loop {
            tokio::select! {
                item = receiver.recv() => {
                    // Check if the item was restored
                    if let Some(item) = item {
                        if item.was_restored() {
                            restored_items.push(item);

                            if restored_items.len() >= self.options.lazy_disk_cache_batch_size {
                                let db = self.disk_cache.as_ref();
                                match db {
                                    None => {
                                        return Result::Err(Box::new(IoError::new(
                                            ErrorKind::InvalidInput,
                                            "Error getting disk cache",
                                        )));
                                    }
                                    Some(db) => {
                                            let result = db.delete_batch(&mut restored_items);
                                            if result.is_err() {
                                                return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                            }
                                    }
                                }
                            }
                            continue;
                        }

                        // If the item was not restored, add it to the batch
                        let batch_bucket = item.get_batch_id();
                        let mut batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);

                        // Check if the batch is full
                        if batch.len() >= self.options.lazy_disk_cache_batch_size {
                            let mut batch_handler = self.lazy_disk_batch_handler.lock().await;
                            let was_synced = batch_handler.synced_batches.get(&batch_bucket).unwrap_or(&false);
                            if *was_synced {
                                let db = self.disk_cache.as_ref();
                                match db {
                                    None => {
                                        return Result::Err(Box::new(IoError::new(
                                            ErrorKind::InvalidInput,
                                            "Error getting disk cache",
                                        )));
                                    }
                                    Some(db) => {
                                            let result = db.delete_batch(&mut batch);
                                            if result.is_err() {
                                                return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                            }
                                    }
                                }

                                awaiting_batches.remove(&batch_bucket);
                            } else {
                                batch.clear();
                                awaiting_batches.remove(&batch_bucket);
                            }

                            batch_handler.deleted_batches.insert(batch_bucket, true);
                            batch_handler.synced_batches.insert(batch_bucket, false);
                        }
                    }
                },
                // Shutdown the writer if the shutdown signal is received
                _ = shutdown_receiver.changed() => {
                    receiver.close();

                    // Pull the remaining items from the receiver
                    while let Some(item) = receiver.recv().await {
                        // Check if the item was restored
                        if item.was_restored() {
                            restored_items.push(item);
                            continue;
                        }

                        // If the item was not restored, add it to the batch
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);
                    }

                    // Commit the remaining batches
                    if !restored_items.is_empty() {
                                let db = self.disk_cache.as_ref();
                                match db {
                                    None => {
                                        return Result::Err(Box::new(IoError::new(
                                            ErrorKind::InvalidInput,
                                            "Error getting disk cache",
                                        )));
                                    }
                                    Some(db) => {
                                            let result = db.delete_batch(&mut restored_items);
                                            if result.is_err() {
                                                return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                            }
                                    }
                                }
                    }
                    for (id, mut batch) in awaiting_batches.iter_mut() {
                        let mut batch_handler = self.lazy_disk_batch_handler.lock().await;
                        let was_synced = batch_handler.synced_batches.get(id).unwrap_or(&false);
                        if *was_synced {
                                let db = self.disk_cache.as_ref();
                                match db {
                                    None => {
                                        return Result::Err(Box::new(IoError::new(
                                            ErrorKind::InvalidInput,
                                            "Error getting disk cache",
                                        )));
                                    }
                                    Some(db) => {
                                            let result = db.delete_batch(&mut batch);
                                            if result.is_err() {
                                                return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                            }
                                    }
                                }
                        } else {
                            batch.clear();
                        }
                        batch_handler.deleted_batches.insert(*id, true);
                        batch_handler.synced_batches.insert(*id, false);
                    }

                    break Ok(());
                }
            }
        }
    }

    async fn restore_from_disk(&self) -> Result<usize, Box<dyn Error>> {
        let db = self.disk_cache.as_ref();
        match db {
            None => {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error getting disk cache",
                )));
            }
            Some(db) => {
                let restored_items = db.return_items_from_disk();
                if restored_items.is_err() {
                    return Err(Box::<dyn Error>::from(restored_items.err().unwrap()));
                }
                let restored_items = restored_items?;
                let total_items = restored_items.len();

                let mut queue = self.queue.lock().await;
                for item in restored_items.iter() {
                    queue.enqueue(item.clone());
                }

                Ok(total_items)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rand::Rng;
    use std::sync::atomic::Ordering;
    use std::{
        collections::VecDeque,
        error::Error,
        sync::atomic::{AtomicBool, AtomicUsize},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn order_test() {
        let message_count = 1_000_000;

        let options = schema::RPQOptions {
            max_priority: 10,
            disk_cache_enabled: false,
            database_path: "/tmp/rpq-order.redb".to_string(),
            lazy_disk_cache: false,
            lazy_disk_write_delay: Duration::seconds(5),
            lazy_disk_cache_batch_size: 5_000,
        };

        let r: Result<(Arc<RPQ<usize>>, usize), Box<dyn Error>> = RPQ::new(options).await;
        assert!(r.is_ok());
        let (rpq, _restored_items) = r.unwrap();

        let mut expected_data = HashMap::new();
        for i in 0..message_count {
            let item = schema::Item::new(i % 10, i, false, None, false, Some(Duration::seconds(5)));
            let result = rpq.enqueue(item).await;
            assert!(result.is_ok());
            let v = expected_data.entry(i % 10).or_insert(VecDeque::new());
            v.push_back(i);
        }

        for _i in 0..message_count {
            let item = rpq.dequeue().await;
            assert!(item.is_ok());
            let item = item.unwrap().unwrap();
            let v = expected_data.get_mut(&item.priority).unwrap();
            let expected_data = v.pop_front().unwrap();
            assert!(item.data == expected_data);
        }

        rpq.close().await;
        assert_eq!(rpq.len().await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prioritize_test() {
        let message_count = 1_000_000;
        let sent_counter = Arc::new(AtomicUsize::new(0));
        let received_counter = Arc::new(AtomicUsize::new(0));
        let removed_counter = Arc::new(AtomicUsize::new(0));
        let total_escalated = Arc::new(AtomicUsize::new(0));

        let options = schema::RPQOptions {
            max_priority: 10,
            disk_cache_enabled: true,
            database_path: "/tmp/rpq-prioritize.redb".to_string(),
            lazy_disk_cache: true,
            lazy_disk_write_delay: Duration::seconds(5),
            lazy_disk_cache_batch_size: 5_000,
        };

        let r: Result<(Arc<RPQ<usize>>, usize), Box<dyn Error>> = RPQ::new(options).await;
        assert!(r.is_ok());
        let (rpq, _restored_items) = r.unwrap();

        let rpq_clone = Arc::clone(&rpq);
        let (shutdown_sender, mut shutdown_receiver) = watch::channel(false);
        let removed_clone = Arc::clone(&removed_counter);
        let escalated_clone = Arc::clone(&total_escalated);
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_receiver.changed() => {
                    return;
                },
                _ = async {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let results = rpq_clone.prioritize().await;

                        if results.is_ok() {
                            let (removed, escalated) = results.unwrap();
                            removed_clone.fetch_add(removed, Ordering::SeqCst);
                            escalated_clone.fetch_add(escalated, Ordering::SeqCst);
                        } else {
                            println!("Error: {:?}", results.err().unwrap());
                        }
                    }
                } => {}
            }
        });

        for i in 0..message_count {
            let item = schema::Item::new(
                i % 10,
                i,
                true,
                Some(Duration::seconds(rand::thread_rng().gen_range(1..10))),
                true,
                Some(Duration::seconds(10)),
            );
            let result = rpq.enqueue(item).await;
            assert!(result.is_ok());
            sent_counter.fetch_add(1, Ordering::SeqCst);
        }

        loop {
            if removed_counter.load(Ordering::SeqCst) + received_counter.load(Ordering::SeqCst)
                == sent_counter.load(Ordering::SeqCst)
            {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let item = rpq.dequeue().await;
            assert!(item.is_ok());
            let item = item.unwrap();
            if item.is_none() {
                continue;
            }
            received_counter.fetch_add(1, Ordering::SeqCst);
        }

        shutdown_sender.send(true).unwrap();
        rpq.close().await;
        assert_eq!(rpq.len().await, 0);
        assert_eq!(rpq.items_in_db(), 0);
        println!(
            "Sent: {}, Received: {}, Removed: {}, Escalated: {}",
            sent_counter.load(Ordering::SeqCst),
            received_counter.load(Ordering::SeqCst),
            removed_counter.load(Ordering::SeqCst),
            total_escalated.load(Ordering::SeqCst)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disk_write_test() {
        let message_count = 500_000;

        let options = schema::RPQOptions {
            max_priority: 10,
            disk_cache_enabled: true,
            database_path: "/tmp/rpq-write.redb".to_string(),
            lazy_disk_cache: true,
            lazy_disk_write_delay: Duration::seconds(1),
            lazy_disk_cache_batch_size: 5_000,
        };

        let r: Result<(Arc<RPQ<usize>>, usize), Box<dyn Error>> = RPQ::new(options).await;
        assert!(r.is_ok());
        let (rpq, _restored_items) = r.unwrap();

        let mut expected_data = HashMap::new();
        for i in 0..message_count {
            let item = schema::Item::new(
                i % 10,
                i,
                true,
                Some(Duration::seconds(1)),
                true,
                Some(Duration::seconds(5)),
            );
            let result = rpq.enqueue(item).await;
            assert!(result.is_ok());
            let v = expected_data.entry(i % 10).or_insert(VecDeque::new());
            v.push_back(i);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        assert!(rpq.len().await == message_count);
        assert!(rpq.items_in_db() != 0);

        for _i in 0..message_count {
            let item = rpq.dequeue().await;
            assert!(item.is_ok());
            let item = item.unwrap().unwrap();
            let v = expected_data.get_mut(&item.priority).unwrap();
            let expected_data = v.pop_front().unwrap();
            assert!(item.data == expected_data);
        }

        rpq.close().await;
        assert_eq!(rpq.len().await, 0);
        assert_eq!(rpq.items_in_db(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_no_batch() {
        // Set Message Count
        let message_count = 10_000_250 as usize;

        // Set Concurrency
        let send_threads = 4 as usize;
        let receive_threads = 4 as usize;
        let bucket_count = 10 as usize;
        let sent_counter = Arc::new(AtomicUsize::new(0));
        let received_counter = Arc::new(AtomicUsize::new(0));
        let removed_counter = Arc::new(AtomicUsize::new(0));
        let total_escalated = Arc::new(AtomicUsize::new(0));
        let finished_sending = Arc::new(AtomicBool::new(false));

        // Create the RPQ
        let options = schema::RPQOptions {
            max_priority: bucket_count,
            disk_cache_enabled: true,
            database_path: "/tmp/rpq-e2e-nobatch.redb".to_string(),
            lazy_disk_cache: true,
            lazy_disk_write_delay: Duration::seconds(5),
            lazy_disk_cache_batch_size: 10_000,
        };
        let r = RPQ::new(options).await;
        assert!(r.is_ok());
        let (rpq, restored_items) = r.unwrap();

        // Launch the monitoring thread
        let rpq_clone = Arc::clone(&rpq);
        let (shutdown_sender, mut shutdown_receiver) = watch::channel(false);
        let removed_clone = Arc::clone(&removed_counter);
        let escalated_clone = Arc::clone(&total_escalated);
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_receiver.changed() => {
                    return;
                },
                _ = async {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let results = rpq_clone.prioritize().await;

                        if !results.is_ok() {
                            let (removed, escalated) = results.unwrap();
                            removed_clone.fetch_add(removed, Ordering::SeqCst);
                            escalated_clone.fetch_add(escalated, Ordering::SeqCst);
                        }
                    }
                } => {}
            }
        });

        let total_timer = std::time::Instant::now();

        // Enqueue items
        println!("Launching {} Send Threads", send_threads);
        let mut send_handles = Vec::new();
        for _ in 0..send_threads {
            let rpq_clone = Arc::clone(&rpq);
            let sent_clone = Arc::clone(&sent_counter);

            send_handles.push(tokio::spawn(async move {
                loop {
                    if sent_clone.load(Ordering::SeqCst) >= message_count {
                        break;
                    }

                    let item = schema::Item::new(
                        sent_clone.load(Ordering::SeqCst) % bucket_count,
                        0,
                        true,
                        Some(Duration::seconds(1)),
                        true,
                        Some(Duration::seconds(5)),
                    );

                    let result = rpq_clone.enqueue(item).await;
                    assert!(result.is_ok());
                    sent_clone.fetch_add(1, Ordering::SeqCst);
                }
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
            let finished_sending_clone = Arc::clone(&finished_sending);

            // Spawn the thread
            receive_handles.push(tokio::spawn(async move {
                loop {
                    if finished_sending_clone.load(Ordering::SeqCst) {
                        if received_clone.load(Ordering::SeqCst)
                            + removed_clone.load(Ordering::SeqCst)
                            >= sent_clone.load(Ordering::SeqCst) + restored_items
                        {
                            break;
                        }
                    }

                    let item = rpq_clone.dequeue().await;
                    assert!(item.is_ok());
                    if item.unwrap().is_none() {
                        continue;
                    }
                    received_clone.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        // Wait for send threads to finish
        for handle in send_handles {
            handle.await.unwrap();
        }

        finished_sending.store(true, Ordering::SeqCst);
        // Wait for receive threads to finish
        for handle in receive_handles {
            handle.await.unwrap();
        }
        shutdown_sender.send(true).unwrap();
        println!("Total Time: {}s", total_timer.elapsed().as_secs_f64());

        // Close the RPQ
        println!("Waiting for RPQ to close");
        rpq.close().await;

        println!(
            "Sent: {}, Received: {}, Removed: {}, Escalated: {} Restored: {:?}",
            sent_counter.load(Ordering::SeqCst),
            received_counter.load(Ordering::SeqCst),
            removed_counter.load(Ordering::SeqCst),
            total_escalated.load(Ordering::SeqCst),
            restored_items
        );

        assert_eq!(rpq.items_in_db(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_batch() {
        // Set Message Count
        let message_count = 10_000_250 as usize;

        // Set Concurrency
        let send_threads = 4 as usize;
        let receive_threads = 4 as usize;
        let bucket_count = 10 as usize;
        let sent_counter = Arc::new(AtomicUsize::new(0));
        let received_counter = Arc::new(AtomicUsize::new(0));
        let removed_counter = Arc::new(AtomicUsize::new(0));
        let total_escalated = Arc::new(AtomicUsize::new(0));
        let finshed_sending = Arc::new(AtomicBool::new(false));

        // Create the RPQ
        let options = schema::RPQOptions {
            max_priority: bucket_count,
            disk_cache_enabled: true,
            database_path: "/tmp/rpq-e2e-batch.redb".to_string(),
            lazy_disk_cache: true,
            lazy_disk_write_delay: Duration::seconds(5),
            lazy_disk_cache_batch_size: 10_000,
        };
        let r = RPQ::new(options).await;
        assert!(r.is_ok());
        let (rpq, restored_items) = r.unwrap();

        // Launch the monitoring thread
        let rpq_clone = Arc::clone(&rpq);
        let (shutdown_sender, mut shutdown_receiver) = watch::channel(false);
        let removed_clone = Arc::clone(&removed_counter);
        let escalated_clone = Arc::clone(&total_escalated);
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_receiver.changed() => {
                    return;
                },
                _ = async {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let results = rpq_clone.prioritize().await;

                        if !results.is_ok() {
                            let (removed, escalated) = results.unwrap();
                            removed_clone.fetch_add(removed, Ordering::SeqCst);
                            escalated_clone.fetch_add(escalated, Ordering::SeqCst);
                        }
                    }
                } => {}
            }
        });

        let total_timer = std::time::Instant::now();

        // Enqueue items
        println!("Launching {} Batch Send Threads", send_threads);
        let mut send_handles = Vec::new();
        for _ in 0..send_threads {
            let rpq_clone = Arc::clone(&rpq);
            let sent_clone = Arc::clone(&sent_counter);

            send_handles.push(tokio::spawn(async move {
                loop {
                    if sent_clone.load(Ordering::SeqCst) >= message_count {
                        break;
                    }

                    let mut batch = Vec::new();
                    for i in 0..1000 {
                        let item = schema::Item::new(
                            sent_clone.load(Ordering::SeqCst) % bucket_count,
                            i,
                            true,
                            Some(Duration::seconds(1)),
                            true,
                            Some(Duration::seconds(5)),
                        );

                        batch.push(item);
                        sent_clone.fetch_add(1, Ordering::SeqCst);
                    }

                    let result = rpq_clone.enqueue_batch(batch).await;
                    assert!(result.is_ok());
                }
                println!("Finished Sending");
            }));
        }

        // Dequeue items
        println!("Launching {} Batch Receive Threads", receive_threads);
        let mut receive_handles = Vec::new();
        for _ in 0..receive_threads {
            // Clone all the shared variables
            let rpq_clone = Arc::clone(&rpq);
            let received_clone = Arc::clone(&received_counter);
            let sent_clone = Arc::clone(&sent_counter);
            let removed_clone = Arc::clone(&removed_counter);
            let finished_sending_clone = Arc::clone(&finshed_sending);

            // Spawn the thread
            receive_handles.push(tokio::spawn(async move {
                loop {
                    if finished_sending_clone.load(Ordering::SeqCst) {
                        if received_clone.load(Ordering::SeqCst)
                            + removed_clone.load(Ordering::SeqCst)
                            >= sent_clone.load(Ordering::SeqCst) + restored_items
                        {
                            break;
                        }
                    }

                    let item = rpq_clone.dequeue_batch(1000).await;
                    assert!(item.is_ok());
                    let item = item.unwrap();
                    if item.is_none() {
                        continue;
                    }
                    for _i in item.unwrap() {
                        received_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
        }

        // Wait for send threads to finish
        for handle in send_handles {
            handle.await.unwrap();
        }

        finshed_sending.store(true, Ordering::SeqCst);
        // Wait for receive threads to finish
        for handle in receive_handles {
            handle.await.unwrap();
        }
        shutdown_sender.send(true).unwrap();
        println!("Total Time: {}s", total_timer.elapsed().as_secs_f64());
        println!(
            "Sent: {}, Received: {}, Removed: {}, Escalated: {} Restored: {}",
            sent_counter.load(Ordering::SeqCst),
            received_counter.load(Ordering::SeqCst),
            removed_counter.load(Ordering::SeqCst),
            total_escalated.load(Ordering::SeqCst),
            restored_items
        );

        // Close the RPQ
        println!("Waiting for RPQ to close");
        rpq.close().await;

        assert_eq!(rpq.items_in_db(), 0);
    }
}
