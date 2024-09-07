use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;

use bincode::{deserialize, serialize};
use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds};

/// RPQOptions is the configuration for the RPQ
pub struct RPQOptions {
    /// Holds the number of priorities(buckets) that this RPQ will accept for this queue.
    pub max_priority: usize,
    /// Enables or disables the disk cache using redb as the backend to store items
    pub disk_cache_enabled: bool,
    /// Holds the path to where the disk cache database will be persisted
    pub database_path: String,
    /// Enables or disables lazy disk writes and deletes. The speed can be quite variable depending
    /// on the disk itself and how often you are emptying the queue in combination with the write delay
    pub lazy_disk_cache: bool,
    /// Sets the delay between lazy disk writes. This delays items from being commit'ed to the disk cache.
    /// If you are pulling items off the queue faster than this delay, many times can be skip the write to disk,
    /// massively increasing the throughput of the queue.
    pub lazy_disk_write_delay: Duration,
    /// Sets the number of items that will be written to the disk cache in a single batch. This can be used to
    /// tune the performance of the disk cache depending on your specific workload.
    pub lazy_disk_cache_batch_size: usize,
}

/// Item holds the data that you want to store along with the metadata needed to manage the item.
/// The priority field is used to determine the order in which items are dequeued. The lower the
/// value, the higher the priority. Items will NOT escalate to a new priority level but instead
/// items will be escalated up or down within there same priority level. AKA, items are not promoted
/// to a higher priority level no matter how long they are in the queue.
#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct Item<T: Clone + Send> {
    // User-provided fields
    /// The priority of the item. Lower values are higher priority.
    /// Be sure that this value does not exceed the max_priority value set when creating the queue.
    pub priority: usize,
    /// The data associated with the item.
    pub data: T,
    /// Whether the item should be escalated over time.
    pub should_escalate: bool,
    /// The rate at which the item should be escalated.
    #[serde_as(as = "Option<DurationMilliSeconds<i64>>")]
    pub escalation_rate: Option<Duration>,
    /// Whether the item should be timed out.
    pub can_timeout: bool,
    /// The timeout duration for the item.
    #[serde_as(as = "Option<DurationMilliSeconds<i64>>")]
    pub timeout: Option<Duration>,

    // Internal
    /// INTERNAL USE ONLY: The UUID of the item when it is stored on disk.
    pub disk_uuid: Option<String>,
    /// INTERNAL USE ONLY: The time the item was submitted to the queue.
    pub submitted_at: Option<DateTime<Utc>>,
    /// INTERNAL USE ONLY: The last time the item was escalated.
    pub last_escalation: Option<DateTime<Utc>>,
    /// INTERNAL USE ONLY: The batch_id of the item if committed to disk in a batch.
    pub batch_id: usize,
    /// INTERNAL USE ONLY: Whether the item was restored from disk.
    pub was_restored: bool,
}

impl<T: Clone + Send> Item<T> {
    /// This function creates a new Item with the provided fields.
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

    // This function is for internal use only. It sets the disk_uuid field to a random UUID.
    pub fn set_disk_uuid(&mut self) {
        self.disk_uuid = Some(uuid::Uuid::new_v4().to_string());
    }

    // This function is for internal use only. It returns the disk_uuid field.
    pub fn get_disk_uuid(&self) -> Option<String> {
        self.disk_uuid.clone()
    }

    /// This function is for internal use only. It sets the batch_id field.
    pub fn set_batch_id(&mut self, batch_id: usize) {
        self.batch_id = batch_id;
    }

    /// This function is for internal use only. It returns the batch_id field.
    pub fn get_batch_id(&self) -> usize {
        self.batch_id
    }

    /// This function is for internal use only. It sets the was_restored field to true.
    pub fn set_restored(&mut self) {
        self.was_restored = true;
    }

    /// This function is for internal use only. It returns the was_restored field.
    pub fn was_restored(&self) -> bool {
        self.was_restored
    }

    /// This function is for internal use only. It returns creates a new Item from a serialized byte array.
    pub fn from_bytes(bytes: &[u8]) -> Result<Item<T>, Box<dyn Error>>
    where
        T: Serialize + DeserializeOwned,
    {
        let b = bytes.to_vec();
        if b.is_empty() {
            return Err(Box::<dyn Error>::from(IoError::new(
                IoErrorKind::InvalidInput,
                "Empty byte array",
            )));
        }

        let d = deserialize(&b);
        if let Err(e) = d {
            return Err(Box::<dyn Error>::from(IoError::new(
                IoErrorKind::InvalidInput,
                format!("Failed to deserialize item: {}", e),
            )));
        }

        Ok(d.unwrap())
    }

    /// This function is for internal use only. It returns a serialized byte array from an Item.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Box<dyn Error>>
    where
        T: Serialize + DeserializeOwned,
    {
        let b = serialize(&self);
        if b.is_err() {
            return Err(Box::<dyn Error>::from(IoError::new(
                IoErrorKind::InvalidInput,
                "Failed to serialize item",
            )));
        }
        let b = b.unwrap();
        if b.is_empty() {
            return Err(Box::<dyn Error>::from(IoError::new(
                IoErrorKind::InvalidInput,
                "Output empty byte array",
            )));
        }

        Ok(b)
    }
}
