use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;

use super::{AHTrieInner, CUT_OFF};


struct AccessStatistics {
    reads: usize,
    // history: u8,
    // last_epoch: usize,
}

enum Encoding {
    Fst,
    Art,
}

pub struct AHTrie<T> {
    skip_length: usize,
    epoch: usize,
    sampling_stat: DashMap<String, AccessStatistics>,

    /// Inner Adaptive Hybrid Trie
    inner: Box<AHTrieInner<T>>,

    /// Run-time tracing
    skip_counter: AtomicUsize,
}

impl<T> AHTrie<T> {
    pub fn new(keys: Vec<String>, values: Vec<T>, skip_length: usize) -> Self {
        Self {
            skip_length,
            epoch: 0,
            sampling_stat: DashMap::new(),
            inner: Box::new(AHTrieInner::new(keys, values, CUT_OFF)),
            skip_counter: AtomicUsize::new(skip_length),
        }
    }

    #[inline]
    fn is_sample(&self) -> bool {
        self.skip_counter.fetch_sub(1, Ordering::SeqCst);
        match self.skip_counter.compare_exchange(0, self.skip_length, Ordering::SeqCst, Ordering::SeqCst)  {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        if self.is_sample() {
            self.trace(key);
        }
        self.inner.get(key)
    }

    fn trace(&self, key: &str) {
        self.sampling_stat
            .entry(key.to_string())
            .and_modify(|v| v.reads += 1)
            .or_insert(AccessStatistics { reads: 0 });
    }

    fn convert_encoding(&self, key: &str, encoding: Encoding) {
        
    }
}