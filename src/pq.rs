use std::error::Error;
use std::time::Duration;
use std::{collections::VecDeque, sync::RwLock};

use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

mod ftime;

// PriorityQueue is a struct that holds methods for inserting, removing, and prioritizing items
// in a queue. Items are stored in a VecDeque and are prioritized based on the priority field.
// Items can be escalated or timed out based on the should_escalate and can_timeout fields.
pub struct PriorityQueue<T: Ord + Clone + Send> {
    items: RwLock<VecDeque<Item<T>>>,
    ftime: ftime::CachedTime,
}

impl<T: Ord + Clone + Send> PriorityQueue<T> {
    pub fn new() -> PriorityQueue<T> {
        PriorityQueue {
            items: RwLock::new(VecDeque::new()),
            ftime: ftime::CachedTime::new(Duration::from_millis(50)),
        }
    }

    pub fn len(&self) -> usize {
        self.items.read().unwrap().len()
    }

    pub fn enqueue(&self, item: Item<T>) {
        let mut item = item;

        // Set the internal fields
        item.submitted_at = self.ftime.get_time().into();
        item.last_escalation = None;

        // Add the item to the queue
        self.items.write().unwrap().push_back(item);
    }

    pub fn dequeue(&self) -> Option<Item<T>> {
        self.items.write().unwrap().pop_front()
    }

    pub fn prioritize(&self) -> Result<(usize, usize), Box<dyn Error>> {
        let mut items = self.items.write().unwrap();
        let mut to_remove = Vec::new();
        let mut to_swap = Vec::new();
        let mut was_error = false;

        for (index, item) in items.iter_mut().enumerate() {
            // Timeout items that have been in the queue for too long
            if item.can_timeout {
                if let (Some(timeout), Some(submitted_at)) = (item.timeout, item.submitted_at) {
                    let current_time_millis = self.ftime.get_time().timestamp_millis();
                    let submitted_time_millis = submitted_at.timestamp_millis();
                    let elapsed_time = current_time_millis - submitted_time_millis;

                    // Downcast u128 to i64 to compare with the timeout
                    if timeout.as_millis() <= i64::MAX as u128 {
                        if elapsed_time >= timeout.as_millis() as i64 {
                            to_remove.push(index);
                            continue;
                        }
                    } else {
                        was_error = true;
                    }
                }
            }

            // Escalate items that have been in the queue for too long
            if item.should_escalate {
                let current_time_millis = self.ftime.get_time().timestamp_millis();
                let submitted_time_millis = item.submitted_at.unwrap().timestamp_millis();
                let escalation_rate_millis = item.escalation_rate.unwrap().as_millis();

                // Downcast u128 to i64 to compare with the timeout
                if !item.timeout.unwrap().as_millis() <= i64::MAX as u128 {
                    // Check if we have ever escalated this item
                    if item.last_escalation.is_none() {
                        let elapsed_time = current_time_millis - submitted_time_millis;

                        if elapsed_time > escalation_rate_millis as i64 {
                            item.last_escalation = Some(self.ftime.get_time());
                            if index > 0 {
                                to_swap.push(index);
                            }
                        }
                    } else {
                        let last_escalation_time_millis =
                            item.last_escalation.unwrap().timestamp_millis();
                        let time_since_last_escalation =
                            current_time_millis - last_escalation_time_millis;

                        // Check if we need to escalate this item again
                        if time_since_last_escalation >= escalation_rate_millis as i64 {
                            item.last_escalation = Some(self.ftime.get_time());
                            if index > 0 {
                                to_swap.push(index);
                            }
                        }
                    }
                } else {
                    was_error = true;
                }
            }
        }

        let removed = to_remove.len();
        let swapped = to_swap.len();

        // Perform removals and swaps
        for index in to_remove.iter().rev() {
            items.remove(*index);
        }
        for index in to_swap {
            items.swap(index, index - 1);
        }

        if was_error {
            return Err(Box::<dyn std::error::Error>::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Timeout or escalation rate is too large",
            )));
        }

        Ok((removed, swapped))
    }
}

// Item is a struct that holds the data and metadata for an item in the queue
#[derive(Serialize, Deserialize, Clone)]
pub struct Item<T: Clone + Send> {
    // User
    pub priority: usize,
    pub data: T,
    pub should_escalate: bool,
    pub escalation_rate: Option<Duration>,
    pub can_timeout: bool,
    pub timeout: Option<Duration>,

    // Internal
    disk_uuid: Option<String>,
    submitted_at: Option<DateTime<Utc>>,
    last_escalation: Option<DateTime<Utc>>,
    batch_id: usize,
    was_restored: bool,
}

impl<T: Clone + Send> Item<T> {
    // Constructor to initialize the struct
    pub fn new(
        priority: usize,
        data: T,
        should_escalate: bool,
        escalation_rate: Option<Duration>,
        can_timeout: bool,
        timeout: Option<Duration>,
    ) -> Self {
        Item {
            // User-provided fields
            priority,
            data,
            should_escalate,
            escalation_rate,
            can_timeout,
            timeout,

            // Private with fn access
            batch_id: 0,
            was_restored: false,
            disk_uuid: None,

            // Internal fields
            submitted_at: None,
            last_escalation: None,
        }
    }

    pub fn set_disk_uuid(&mut self) {
        self.disk_uuid = Some(uuid::Uuid::new_v4().to_string());
    }

    pub fn get_disk_uuid(&self) -> Option<String> {
        self.disk_uuid.clone()
    }

    pub fn set_batch_id(&mut self, batch_id: usize) {
        self.batch_id = batch_id;
    }

    pub fn get_batch_id(&self) -> usize {
        self.batch_id
    }

    pub fn set_restored(&mut self) {
        self.was_restored = true;
    }

    pub fn was_restored(&self) -> bool {
        self.was_restored
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Item<T>, Box<dyn Error>>
    where
        T: Serialize + DeserializeOwned,
    {
        let b = bytes.to_vec();
        if b.is_empty() {
            return Err(Box::<dyn std::error::Error>::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Empty byte array",
            )));
        }
        Ok(deserialize(&b).unwrap())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Box<dyn Error>>
    where
        T: Serialize + DeserializeOwned,
    {
        let b = serialize(&self).unwrap();
        if b.is_empty() {
            return Err(Box::<dyn std::error::Error>::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Empty byte array",
            )));
        }
        Ok(b)
    }
}
