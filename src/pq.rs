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
        let mut item = item;

        // Set the internal fields
        item.submitted_at = Instant::now();
        item.last_escalation = None;
        item.was_restored = false;

        // Add the item to the queue
        self.items.write().unwrap().push(item);
    }

    pub fn dequeue(&self) -> Option<Item<T>> {
        self.items.write().unwrap().pop()
    }

    pub fn update_priority(&self, item: &Item<T>, new_priority: u64) {
        // Create a clone of the item with the new priority
        let mut item_clone = item.clone();
        item_clone.priority = new_priority;
        item_clone.last_escalation = Some(Instant::now());

        // Remove the old item and insert the new item
        let mut items = self.items.write().unwrap();
        items.retain(|x| x != item);
        items.push(item_clone);
    }

    pub fn prioritize(&self) {
        let items = self.items.write().unwrap();
        for item in items.iter() {
            // Timeout items that have been in the queue for too long
            if item.can_timeout {
                if item.timeout.unwrap().as_secs() > item.submitted_at.elapsed().as_secs() {
                    self.remove(item);
                    continue;
                }
            }

            // Escalate items that have been in the queue for too long
            if item.should_escalate {
                // Check if we have ever escalated this item
                if item.last_escalation.is_none() {
                    if item.escalation_rate.unwrap().as_secs()
                        > item.submitted_at.elapsed().as_secs()
                    {
                        self.update_priority(item, item.priority + 1);
                        continue;
                    }
                } else {
                    // Check if we need to escalate this item again
                    if item.escalation_rate.unwrap().as_secs()
                        > item.last_escalation.unwrap().elapsed().as_secs()
                    {
                        self.update_priority(item, item.priority + 1);
                        continue;
                    }
                }
            }
        }
    }

    pub fn remove(&self, item: &Item<T>) {
        self.items.write().unwrap().retain(|x| x != item);
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
    internal_priority: u64,
    submitted_at: Instant,
    last_escalation: Option<Instant>,
    batch_id: u64,
    was_restored: bool,
}

// Implement the Ord, PartialOrd, PartialEq, and Eq traits for Item
impl<T: Ord + Clone> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_priority.cmp(&other.internal_priority)
    }
}

impl<T: Ord + Clone> PartialOrd for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialEq + Clone> PartialEq for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        self.internal_priority == other.internal_priority
    }
}

impl<T: PartialEq + Clone> Eq for Item<T> {}

impl<T: Clone> Item<T> {
    // Constructor to initialize the struct
    pub fn new(
        priority: u64,
        data: T,
        disk_uuid: Option<String>,
        should_escalate: bool,
        escalation_rate: Option<Duration>,
        can_timeout: bool,
        timeout: Option<Duration>,
    ) -> Self {
        Item {
            // User-provided fields
            priority,
            data,
            disk_uuid,
            should_escalate,
            escalation_rate,
            can_timeout,
            timeout,

            // Internal fields
            submitted_at: Instant::now(),
            last_escalation: None,
            batch_id: 0,
            was_restored: false,
            internal_priority: 0,
        }
    }
}
