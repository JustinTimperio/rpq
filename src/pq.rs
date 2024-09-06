use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::error::Error;
use std::time::Duration as StdDuration;
use std::{sync::Arc, vec::Vec};

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;

mod ftime;
use crate::disk::DiskCache;
use crate::schema::Item;

/// PriorityQueue is a struct that holds methods for inserting, removing, and prioritizing items
/// in a queue. Items are stored in a VecDeque and are prioritized based on metadata provided by the user.
/// Items can be escalated or timed out based on the should_escalate and can_timeout fields.
pub struct PriorityQueue<T: Clone + Send> {
    items: Vec<VecDeque<Item<T>>>,
    active_buckets: BTreeSet<usize>,
    ftime: Arc<ftime::CachedTime>,
    len: usize,
    db: Arc<Option<DiskCache<T>>>,
    disk_enabled: bool,
    lazy_disk_enabled: bool,
    lazy_disk_deleter: Arc<UnboundedSender<Item<T>>>,
}

impl<T: Clone + Send> PriorityQueue<T> {
    /// This function creates a new PriorityQueue.
    pub fn new(
        buckets: usize,
        disk_enabled: bool,
        lazy_disk_enabled: bool,
        lazy_disk_deleter: Arc<UnboundedSender<Item<T>>>,
        db: Arc<Option<DiskCache<T>>>,
    ) -> PriorityQueue<T> {
        let mut pq = PriorityQueue {
            items: Vec::new(),
            active_buckets: BTreeSet::new(),
            ftime: ftime::CachedTime::new(StdDuration::from_millis(50)),
            len: 0 as usize,
            disk_enabled,
            lazy_disk_enabled,
            lazy_disk_deleter,
            db,
        };

        for i in 0..buckets {
            pq.items.insert(i, VecDeque::new());
        }

        pq
    }

    /// Returns the number of items in this queue
    pub fn items_in_queue(&self) -> usize {
        self.len
    }

    pub fn active_buckets(&self) -> usize {
        self.active_buckets.len()
    }

    /// Adds an item to the queue at the end of the VecDeque
    pub fn enqueue(&mut self, item: Item<T>) {
        let mut item = item;
        // Set the internal fields
        item.submitted_at = self.ftime.get_time().into();
        item.last_escalation = None;

        // Insert the item into the queue
        let priority = item.priority;
        self.active_buckets.insert(priority);
        self.items.get_mut(priority).unwrap().push_back(item);
        self.len += 1;
    }

    /// Removes and returns the item with the highest priority
    pub fn dequeue(&mut self) -> Option<Item<T>> {
        // This should really only ever loop once if the first pulled bucket is a miss
        loop {
            let bucket = self.active_buckets.first();
            if bucket.is_none() {
                return None;
            }
            let item = self.items.get_mut(*bucket.unwrap()).unwrap().pop_front();
            if item.is_none() {
                self.active_buckets.pop_first();
                continue;
            }
            self.len -= 1;
            return item;
        }
    }

    /// Prioritizes the items in the queue based on the priority, escalation rate, and timeout
    /// Returns a tuple of the number of items removed and the number of items swapped
    pub fn prioritize(&mut self) -> Result<(usize, usize), Box<dyn Error>>
    where
        T: Serialize + DeserializeOwned,
    {
        let mut removed = 0 as usize;
        let mut swapped = 0 as usize;

        for (_, bucket) in self.items.iter_mut().enumerate() {
            bucket.retain(|item| {
                let mut keep = true;

                // Timeout items that have been in the queue for too long
                if item.can_timeout {
                    if let (Some(timeout), Some(submitted_at)) = (item.timeout, item.submitted_at) {
                        let current_time = self.ftime.get_time();
                        let elapsed = current_time - submitted_at;

                        if elapsed >= timeout {
                            keep = false;
                            removed += 1;
                            self.len -= 1;

                            if self.disk_enabled {
                                let db = self.db.as_ref();
                                match db {
                                    Some(db) => {
                                        if self.lazy_disk_enabled {
                                            self.lazy_disk_deleter.send(item.clone()).unwrap();
                                        } else {
                                            let _ = db.delete_single(
                                                item.get_disk_uuid().as_ref().unwrap(),
                                            );
                                        }
                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                }
                keep
            });

            let mut index = 0;
            let mut last_item_was_escalated = false;
            let mut last_last_was_escalated = false;

            while index < bucket.len() {
                let item = bucket.get_mut(index).unwrap();

                if item.should_escalate {
                    if item.submitted_at.is_some() && item.escalation_rate.is_some() {
                        let current_time = self.ftime.get_time();
                        let context = (!last_item_was_escalated && !last_last_was_escalated)
                            || (last_item_was_escalated && !last_last_was_escalated);

                        // Check if we have ever escalated this item
                        if item.last_escalation.is_none() {
                            let elapsed_time = current_time - item.submitted_at.unwrap();

                            if elapsed_time >= item.escalation_rate.unwrap() {
                                if context {
                                    item.last_escalation = Some(self.ftime.get_time());
                                    if index > 0 {
                                        bucket.swap(index, index - 1);
                                    }
                                    last_last_was_escalated = last_item_was_escalated;
                                    last_item_was_escalated = true;
                                    swapped += 1;
                                }
                            } else {
                                last_last_was_escalated = last_item_was_escalated;
                                last_item_was_escalated = false;
                            }

                        // We have escalated this item before
                        } else {
                            let elapsed_time = current_time - item.last_escalation.unwrap();
                            if elapsed_time >= item.escalation_rate.unwrap() {
                                if context {
                                    item.last_escalation = Some(self.ftime.get_time());
                                    if index > 0 {
                                        bucket.swap(index, index - 1);
                                    }
                                    last_last_was_escalated = last_item_was_escalated;
                                    last_item_was_escalated = true;
                                    swapped += 1;
                                }
                            } else {
                                last_last_was_escalated = last_item_was_escalated;
                                last_item_was_escalated = false;
                            }
                        }
                    }
                }
                index += 1;
            }
        }

        Ok((removed, swapped))
    }
}
