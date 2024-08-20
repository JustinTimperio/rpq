use std::cmp::Ordering;
use std::time::{Duration, Instant};
use std::{collections::BinaryHeap, sync::RwLock};

pub struct PriorityQueue<T: Ord + Clone> {
    items: RwLock<BinaryHeap<Item<T>>>,
}

impl<T: Ord + Clone> PriorityQueue<T> {
    pub fn new() -> PriorityQueue<T> {
        PriorityQueue {
            items: RwLock::new(BinaryHeap::new()),
        }
    }

    pub fn len(&self) -> u64 {
        self.items.read().unwrap().len() as u64
    }

    pub fn peek(&self) -> Option<Item<T>> {
        self.items.read().unwrap().peek().cloned()
    }

    pub fn enqueue(&self, item: Item<T>) {
        self.items.write().unwrap().push(item);
    }

    pub fn dequeue(&self) -> Option<Item<T>> {
        self.items.write().unwrap().pop()
    }

    pub fn update_priority(&self, item: Item<T>, new_priority: u64) {
        // Replace the item with the new priority
        self.items.write().unwrap().retain(|x| x != &item);
        let mut new_item: Item<T> = item;
        new_item.priority = new_priority;
        self.items.write().unwrap().push(new_item);
    }

    pub fn remove(&self, item: Item<T>) {
        self.items.write().unwrap().retain(|x| x != &item);
    }
}

// Item is a struct that holds the data and metadata for an item in the queue
#[derive(Clone)]
pub struct Item<T: Clone> {
    // User
    pub priority: u64,
    pub data: T,
    pub disk_uuid: Option<String>,
    pub should_escalate: bool,
    pub escalation_rate: Option<Duration>,
    pub can_timeout: bool,
    pub timeout: Option<Duration>,

    // Internal
    pub submitted_at: Instant,
    pub last_escalation: Option<Instant>,
    pub index: u64,
    pub batch_id: u64,
    pub was_restored: bool,
}

// Implement the Ord, PartialOrd, PartialEq, and Eq traits for Item
// Why do we need to implement these traits?
impl<T: Ord + Clone> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl<T: Ord + Clone> PartialOrd for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialEq + Clone> PartialEq for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<T: PartialEq + Clone> Eq for Item<T> {}
