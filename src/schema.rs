use std::cmp::Ordering;
use std::time::{Duration, Instant};

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
