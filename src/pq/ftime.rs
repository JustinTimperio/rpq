use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
pub struct CachedTime {
    time: Arc<DateTime<Utc>>,
}

impl CachedTime {
    pub fn new(update_interval: Duration) -> Self {
        let cached_time = CachedTime {
            time: Arc::new(Utc::now()),
        };

        let mut clone = cached_time.clone();
        clone.time = Arc::new(Utc::now());
        tokio::spawn(async move {
            loop {
                sleep(update_interval).await;
                clone.time = Arc::new(Utc::now());
            }
        });

        cached_time
    }

    pub fn get_time(&self) -> DateTime<Utc> {
        self.time.as_ref().clone().into()
    }
}
