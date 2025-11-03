use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pingora_load_balancing::Backend;

/// Round-robin backend pool using atomic counter for thread-safe selection
pub struct SimpleBackendPool {
    backends: Vec<Backend>,
    counter: AtomicUsize,
}

impl SimpleBackendPool {
    /// Creates a new pool wrapped in Arc for shared access
    pub fn new(backends: Vec<Backend>) -> Arc<Self> {
        Arc::new(Self {
            backends,
            counter: AtomicUsize::new(0),
        })
    }

    /// Returns the next backend using round-robin
    pub fn select(&self) -> Option<&Backend> {
        if self.backends.is_empty() {
            return None;
        }

        let index = self.counter.fetch_add(1, Ordering::Relaxed) % self.backends.len();
        self.backends.get(index)
    }

    /// Returns the number of backends
    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// Returns true if no backends are configured
    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}
