use std::time::{Instant, Duration};
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::cmp::Ordering;
use std::error::Error;
use std::thread;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
use rand::Rng;


struct RPQ<T> {
    // total_len is the number of items across all queues
    total_len: u64,
    // buckets is a map of priorities to a binary heap of items
    buckets: HashMap<u64, BinaryHeap<Item<T>>>,
    // bucket_mutex_map is a map of priorities to a mutex
    bucket_mutex_map: HashMap<u64, std::sync::Mutex<()>>,
    // bucket_len_map is a map of priorities to the number of items in the queue
    bucket_len_map: HashMap<u64, u64>,
    // non_empty_buckets is a binary heap of priorities
    non_empty_buckets: BinaryHeap<u64>,
}

// Item is a struct that holds the data and metadata for an item in the queue
struct Item<T> {
    priority: u64,
    data: T,
    should_escalate: bool,
    escalation_rate: Duration,
    can_timeout: bool,
    timeout: Duration,
    submitted: Instant,
    last_escalation: Instant,
}

// Implement the Ord, PartialOrd, PartialEq, and Eq traits for Item
// Why do we need to implement these traits?
impl<T: Ord> Ord for Item<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl<T: Ord> PartialOrd for Item<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialEq> PartialEq for Item<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<T: PartialEq> Eq for Item<T> {}


impl <T: Ord> RPQ<T> {
    fn new(num_buckets: u64) -> RPQ<T> {
        let mut rpq = RPQ {
            total_len: 0,
            buckets: HashMap::new(),
            bucket_mutex_map: HashMap::new(),
            bucket_len_map: HashMap::new(),
            non_empty_buckets: BinaryHeap::new(),
        };

        // Initialize the buckets, bucket_mutex_map, and bucket_len_map
        for i in 0..num_buckets {
            rpq.buckets.insert(i, BinaryHeap::new());
            rpq.bucket_mutex_map.insert(i, std::sync::Mutex::new(()));
            rpq.bucket_len_map.insert(i, 0);
        }

        rpq
    }

    fn enqueue(&mut self, priority: u64, data: T, escalate : bool, escalation_rate: Duration, can_timeout: bool, timeout: Duration) {

        // Create the item
        let object: Item<T> = Item {
            priority: priority,
            data: data,
            should_escalate: escalate,
            escalation_rate: escalation_rate,
            can_timeout: can_timeout,
            timeout: timeout,
            submitted: Instant::now(),
            last_escalation: Instant::now(),
        };

        // Lock the bucket mutex
        let bucket_mutex: &Mutex<()> = self.bucket_mutex_map.get(&priority).expect("No bucket mutex");
        let _lock: std::sync::MutexGuard<'_, ()> = bucket_mutex.lock().unwrap();

        // Update the bucket length
        let bucket_len: &mut u64 = self.bucket_len_map.entry(priority).or_insert(0);

        // Insert the object into the bucket
        self.buckets.get_mut(&priority).unwrap().push(object);

        // Update the bucket length
        *bucket_len += 1;
        self.total_len += 1;

        // Add the priority to the non_empty_buckets if it is not already there
        if !self.non_empty_buckets.iter().any(|&p| p == priority) {
            self.non_empty_buckets.push(priority);
        }

    }

    fn dequeue(&mut self) -> Result<T, Box<dyn Error>> {
        // Loop until we find a non-empty bucket
        loop {
            // Get the priority of the non-empty bucket with the highest priority
            let bucket = match self.non_empty_buckets.peek() {
                Some(bucket) => *bucket,
                None => return Err("No items in the queue".into()),
            };

            // Lock the bucket mutex
            let bucket_mutex: &Mutex<()>  = self.bucket_mutex_map.get(&bucket).expect( "No bucket mutex");
            let _lock: std::sync::MutexGuard<'_, ()> = bucket_mutex.lock().unwrap();


            // Get the length of the bucket
            let item: Option<Item<T>> = match self.buckets.get_mut(&bucket) {
                Some(bucket) => bucket.pop(),
                None => return Err("No items in the bucket".into()),
            };

            // Get the length of the bucket
            let bucket_len: &mut u64 = self.bucket_len_map.get_mut(&bucket).expect("No bucket length");

            let item = match item {
                Some(item) => item,
                None => {
                    if *bucket_len == 0 {
                        self.non_empty_buckets.pop();
                    }
                    continue; // Continue the loop to find a non-empty bucket
                }
            };

            *bucket_len -= 1;
            self.total_len -= 1;
            if *bucket_len == 0 {
                self.non_empty_buckets.pop();
            }
            return Ok(item.data);
        }
    }


}

fn main() {
    let threads: i32 = 10;

    let start: Instant = Instant::now();
    let sent: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let received: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let rpq: Arc<Mutex<RPQ<i32>>> = Arc::new(Mutex::new(RPQ::new(100)));

    let mut handles: Vec<thread::JoinHandle<()>> = vec![];

    for _ in 0..threads {
        let rpq_clone: Arc<Mutex<RPQ<i32>>> = Arc::clone(&rpq);
        let sent_clone: Arc<AtomicU64> = Arc::clone(&sent);
        let handle: thread::JoinHandle<()> = thread::spawn(move || {
            for i in 0..10_000_000 / threads {
                let priority: u64 = rand::thread_rng().gen_range(0..100);
                let escalate: bool = false;
                let escalation_rate: Duration = Duration::from_secs(1);
                let can_timeout: bool = false;
                let timeout: Duration = Duration::from_secs(10);

                let mut rpq: std::sync::MutexGuard<'_, RPQ<i32>> = rpq_clone.lock().unwrap();
                rpq.enqueue(priority, i, escalate, escalation_rate, can_timeout, timeout);
                sent_clone.fetch_add(1, AtomicOrdering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap(); // Wait for each thread to finish
    }

    while let Ok(_top) = rpq.lock().unwrap().dequeue() {
        received.fetch_add(1, AtomicOrdering::SeqCst);
    }

    let end: Instant = Instant::now();

    println!("Sent: {} Received: {}", sent.load(AtomicOrdering::SeqCst), received.load(AtomicOrdering::SeqCst));
    println!("Time to send and remove 10 million integers: {:?}", end.duration_since(start));
}

