use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
pub struct CachedTime {
    time: Arc<RwLock<DateTime<Utc>>>,
}

impl CachedTime {
    pub fn new(update_interval: Duration) -> Arc<Self> {
        let cached_time = CachedTime {
            time: Arc::new(RwLock::new(Utc::now())),
        };
        let arc = Arc::new(cached_time);
        let arc_clone = arc.clone();

        tokio::spawn(async move {
            loop {
                sleep(update_interval).await;
                arc_clone.update_time();
            }
        });

        arc
    }

    pub fn update_time(&self) {
        let mut time = self.time.write().unwrap();
        let new_time = Utc::now();
        *time = new_time;
    }

    pub fn get_time(&self) -> DateTime<Utc> {
        *self.time.read().unwrap()
    }
}
