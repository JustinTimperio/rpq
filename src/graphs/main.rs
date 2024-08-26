use core::time;
use std::fs::File;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rpq::pq::Item;
use rpq::{RPQOptions, RPQ};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let disk_cache = false;
    let lazy_disk_cache = false;

    iter(true, disk_cache, lazy_disk_cache).await;
    iter(false, disk_cache, lazy_disk_cache).await;
}

async fn iter(prioritize: bool, disk_cache: bool, lazy_disk_cache: bool) {
    let mut name = "bench-report-no-repro.csv";
    if prioritize {
        name = "bench-report-repro.csv";
    }

    // Create the CSV file and add a header
    let file = File::create(name).unwrap();
    let mut writer = csv::Writer::from_writer(file);
    let header = vec![
        "Total Items",
        "Buckets",
        "Removed",
        "Escalated",
        "Time Elapsed",
        "Time to Send",
        "Time to Receive",
    ];
    writer.write_record(&header).unwrap();

    for total in (1_000_000..=10_000_000).step_by(1_000_000) {
        for buckets in (5..=100).step_by(5) {
            println!(
                "Starting test for {} entries and {} buckets",
                total, buckets
            );

            let (total_elapsed, send_elapsed, receive_elapsed, removed, escalated) =
                bench(total, buckets, prioritize, disk_cache, lazy_disk_cache).await;

            let stats = vec![
                total.to_string(),
                buckets.to_string(),
                removed.to_string(),
                escalated.to_string(),
                total_elapsed.to_string(),
                send_elapsed.to_string(),
                receive_elapsed.to_string(),
            ];

            writer.write_record(&stats).unwrap();
        }
    }

    writer.flush().unwrap();
}

async fn bench(
    message_count: u64,
    bucket_count: u64,
    prioritize: bool,
    lazy_disk_cache: bool,
    disk_cache_enabled: bool,
) -> (f64, f64, f64, u64, u64) {
    let options = RPQOptions {
        bucket_count: bucket_count,
        disk_cache_enabled: disk_cache_enabled,
        database_path: "/tmp/rpq.redb".to_string(),
        lazy_disk_cache: lazy_disk_cache,
        lazy_disk_max_delay: std::time::Duration::from_secs(5),
        lazy_disk_cache_batch_size: 5000,
        buffer_size: 1_000_000,
    };

    let r = Arc::new(RPQ::new(options).await);
    let rpq = Arc::clone(&r.0);

    let removed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let escalated = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (shutdown_sender, mut shutdown_receiver) = tokio::sync::watch::channel(false);

    let rpq_clone = Arc::clone(&rpq);
    let removed_clone = Arc::clone(&removed);
    let escalated_clone = Arc::clone(&escalated);
    if prioritize {
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_receiver.changed() => {
                    return;
                },
                _ = async {
                    loop {
                        tokio::time::sleep(time::Duration::from_secs(1)).await;
                        let results = rpq_clone.prioritize().await;

                        if !results.is_none() {
                            let (r, e) = results.unwrap();
                            removed_clone.fetch_add(r, Ordering::SeqCst);
                            escalated_clone.fetch_add(e, Ordering::SeqCst);
                        }
                    }
                } => {}
            }
        });
    }

    let timer = std::time::Instant::now();
    let send_timer = std::time::Instant::now();
    for i in 0..message_count {
        let item = Item::new(
            i % bucket_count,
            i,
            true,
            Some(std::time::Duration::from_secs(1)),
            true,
            Some(std::time::Duration::from_secs(2)),
        );
        rpq.enqueue(item).await;
    }

    let send_elapsed = send_timer.elapsed().as_secs_f64();

    let receive_timer = std::time::Instant::now();
    for _i in 0..message_count {
        rpq.dequeue().await;
    }
    let receive_elapsed = receive_timer.elapsed().as_secs_f64();
    shutdown_sender.send(true).unwrap();
    let total_elapsed = timer.elapsed().as_secs_f64();

    return (
        total_elapsed,
        send_elapsed,
        receive_elapsed,
        removed.load(Ordering::SeqCst),
        escalated.load(Ordering::SeqCst),
    );
}
