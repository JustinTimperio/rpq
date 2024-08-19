use std::{collections::BinaryHeap, sync::MutexGuard, sync::RwLock};

use crate::{bpq, schema};

pub struct PriorityQueue<T: Ord + Clone> {
    items: RwLock<BinaryHeap<schema::Item<T>>>,
    mutex: std::sync::Mutex<()>,
    bpq: bpq::BucketPriorityQueue,
}

impl<T: Ord + Clone> PriorityQueue<T> {
    pub fn new(bucket_priority_queue: bpq::BucketPriorityQueue) -> PriorityQueue<T> {
        PriorityQueue {
            items: RwLock::new(BinaryHeap::new()),
            mutex: std::sync::Mutex::new(()),
            bpq: bucket_priority_queue,
        }
    }

    pub fn len(&self) -> u64 {
        self.items.read().unwrap().len() as u64
    }

    pub fn peek(&self) -> Option<schema::Item<T>> {
        self.items.read().unwrap().peek().cloned()
    }

    pub fn enqueue(&self, item: schema::Item<T>) {
        let _guard: MutexGuard<()> = self.mutex.lock().unwrap();
        let mut items = self.items.write().unwrap();

        // Add the item to the bpq if it's not already there
        if items.len() == 0 {
            self.bpq.add_bucket(item.priority);
        }

        items.push(item);
    }

    pub fn dequeue(&self) -> Option<schema::Item<T>> {
        let _guard: MutexGuard<()> = self.mutex.lock().unwrap();
        let mut items = self.items.write().unwrap();

        // Remove from the bpq if it's the last item
        if items.len() == 1 {
            self.bpq.remove_bucket(items.peek().unwrap().priority);
        }

        items.pop()
    }

    pub fn update_priority(&self, item: schema::Item<T>, new_priority: u64) {
        let _guard: std::sync::MutexGuard<()> = self.mutex.lock().unwrap();

        // Replace the item with the new priority
        self.items.write().unwrap().retain(|x| x != &item);
        let mut new_item: schema::Item<T> = item;
        new_item.priority = new_priority;
        self.items.write().unwrap().push(new_item);
    }

    pub fn remove(&self, item: schema::Item<T>) {
        let _guard: std::sync::MutexGuard<()> = self.mutex.lock().unwrap();

        self.items.write().unwrap().retain(|x| x != &item);
        if self.items.write().unwrap().len() == 0 {
            self.bpq.remove_bucket(item.priority);
        }
    }
}
