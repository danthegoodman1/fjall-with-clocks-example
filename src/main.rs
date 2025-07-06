use fjall::{Config, Keyspace, PartitionCreateOptions, PersistMode};
use std::sync::OnceLock;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Epoch offset calculation - captures the relationship between Instant and SystemTime at startup
static EPOCH_OFFSET: OnceLock<(Instant, u128)> = OnceLock::new();

/// Get the epoch offset, initializing it on first call
fn get_epoch_offset() -> (Instant, u128) {
    *EPOCH_OFFSET.get_or_init(|| {
        let instant_now = Instant::now();
        let system_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before Unix epoch")
            .as_nanos();
        (instant_now, system_now)
    })
}

/// Returns the current time in nanoseconds since Unix epoch.
/// This uses Instant for monotonicity but converts to epoch time,
/// ensuring both monotonic behavior and epoch-based timestamps.
fn current_monotonic_ns() -> u64 {
    let (start_instant, start_epoch_ns) = get_epoch_offset();
    let elapsed_ns = start_instant.elapsed().as_nanos();
    (start_epoch_ns + elapsed_ns) as u64
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize an in-memory (temporary) keyspace
    let keyspace = Config::new("data").temporary(true).open()?;

    // Each partition is its own physical LSM-tree
    let items = keyspace.open_partition("my_items", PartitionCreateOptions::default())?;

    // 1. Insert first record
    items.insert("a", "first")?;

    // 2. Wait a little to ensure the underlying monotonic clock advances
    sleep(Duration::from_millis(1));

    // 3. Capture a timestamp (sequence number) **after** the first insert
    //    This marks the snapshot moment we want to view later.
    let snapshot_ts = current_monotonic_ns();
    println!("Snapshot timestamp: {}", snapshot_ts);

    // 4. Wait a bit more (purely for demonstration – not strictly required)
    sleep(Duration::from_millis(1));

    // 5. Insert second record – this will have a higher sequence number
    items.insert("b", "second")?;

    // 6. List all keys – we should see both records
    println!("Keys currently in the partition:");
    for key in items.keys().flatten() {
        println!("  {}", String::from_utf8_lossy(&key));
    }

    // 7. Create a snapshot at the previously captured timestamp
    let snapshot = items.snapshot_at(snapshot_ts);

    // 8. List keys visible in that snapshot – the second insert should be invisible
    println!("\nKeys visible in snapshot taken at ts={snapshot_ts}:");
    for key in snapshot.keys().flatten() {
        println!("  {}", String::from_utf8_lossy(&key));
    }

    Ok(())
}
