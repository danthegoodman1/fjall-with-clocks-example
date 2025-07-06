// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::SeqNo;
use std::sync::OnceLock;
use std::sync::{
    atomic::{
        AtomicU64,
        Ordering::{Acquire, Release},
    },
    Arc,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

/// Thread-safe sequence number generator
///
/// # Examples
///
/// ```
/// # use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
/// #
/// # let path = tempfile::tempdir()?;
/// let tree = Config::new(path).open()?;
///
/// let seqno = SequenceNumberCounter::default();
///
/// // Do some inserts...
/// tree.insert("a".as_bytes(), "abc", seqno.next());
/// tree.insert("b".as_bytes(), "abc", seqno.next());
/// tree.insert("c".as_bytes(), "abc", seqno.next());
///
/// // Maybe create a snapshot
/// let snapshot = tree.snapshot(seqno.get());
///
/// // Create a batch
/// let batch_seqno = seqno.next();
/// tree.remove("a".as_bytes(), batch_seqno);
/// tree.remove("b".as_bytes(), batch_seqno);
/// tree.remove("c".as_bytes(), batch_seqno);
/// #
/// # assert!(tree.is_empty(None, None)?);
/// # Ok::<(), lsm_tree::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct SequenceNumberCounter(Arc<AtomicU64>);

impl std::ops::Deref for SequenceNumberCounter {
    type Target = Arc<AtomicU64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SequenceNumberCounter {
    /// Creates a new counter, setting it to some previous value
    #[must_use]
    pub fn new(prev: SeqNo) -> Self {
        Self(Arc::new(AtomicU64::new(prev)))
    }

    /// Gets the next sequence number, without incrementing the counter.
    ///
    /// This should only be used when creating a snapshot.
    #[must_use]
    pub fn get(&self) -> SeqNo {
        self.load(Acquire)
    }

    /// Gets the next sequence number.
    ///
    /// This returns the current time in nanoseconds since Unix epoch,
    /// while guaranteeing that the returned value is **strictly
    /// monotonically increasing** even if multiple calls happen within the
    /// same nanosecond. If the current time would not advance the counter,
    /// the value is incremented by one.
    ///
    /// This uses Instant for monotonicity but converts to epoch time,
    /// ensuring both monotonic behavior and epoch-based timestamps.
    #[must_use]
    pub fn next(&self) -> SeqNo {
        loop {
            let now = current_monotonic_ns();
            let last = self.load(Acquire);
            let candidate = if now > last { now } else { last + 1 };

            if self
                .compare_exchange(last, candidate, Release, Acquire)
                .is_ok()
            {
                return candidate;
            }
        }
    }
}

impl Default for SequenceNumberCounter {
    fn default() -> Self {
        Self::new(current_monotonic_ns())
    }
}

/// Returns the current time in nanoseconds since Unix epoch.
/// This uses Instant for monotonicity but converts to epoch time,
/// ensuring both monotonic behavior and epoch-based timestamps.
fn current_monotonic_ns() -> u64 {
    let (start_instant, start_epoch_ns) = get_epoch_offset();
    let elapsed_ns = start_instant.elapsed().as_nanos();
    (start_epoch_ns + elapsed_ns) as u64
}
