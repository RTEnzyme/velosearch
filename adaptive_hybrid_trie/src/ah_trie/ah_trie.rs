use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use fst_rs::FST;
use parking_lot::RwLock;

use super::{AHTrieInner, CUT_OFF, ChildNode};


struct AccessStatistics {
    reads: usize,
    // history: u8,
    // last_epoch: usize,
}

pub enum Encoding {
    Fst(usize),
    Art,
}

pub struct AHTrie<T: Clone> {
    skip_length: usize,
    epoch: usize,
    sampling_stat: DashMap<String, AccessStatistics>,

    /// Inner Adaptive Hybrid Trie
    inner: RwLock<AHTrieInner<T>>,

    /// Run-time tracing
    skip_counter: AtomicUsize,
}

impl<T: Clone> AHTrie<T> {
    pub fn new(keys: Vec<String>, values: Vec<T>, skip_length: usize) -> Self {
        Self {
            skip_length,
            epoch: 0,
            sampling_stat: DashMap::new(),
            inner: RwLock::new(AHTrieInner::new(keys, values, CUT_OFF)),
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

    pub fn get(&self, key: &str) -> Option<T> {
        if self.is_sample() {
            self.trace(key);
        }
        self.inner.read().get(key).cloned()
    }

    fn trace(&self, key: &str) {
        self.sampling_stat
            .entry(key.to_string())
            .and_modify(|v| v.reads += 1)
            .or_insert(AccessStatistics { reads: 0 });
    }

    fn convert_encoding(&self, key: &str, encoding: Encoding) {
        let cur_encoding = self.inner.read().encoding(&key);
        match (cur_encoding, encoding) {
            (Encoding::Art, Encoding::Fst(_)) => {
                let inner_read_guard = self.inner.read();
                let kvs = inner_read_guard.prefix_keys(&key[..CUT_OFF]);
                let mut keys = Vec::new();
                let mut values = Vec::new();
                kvs.into_iter()
                    .for_each(|(k, v)| {
                        keys.push(k.as_bytes().clone());
                        values.push(if let ChildNode::Offset(off) = v {*off} else {unreachable!()});
                    });
                let fst = FST::new_with_bytes(&keys, values);
                // Must drop RwLockReadGuard before acquiring RwLockWriteGuard
                drop(inner_read_guard);

                let mut inner = self.inner.write();
                for key in keys {
                    inner.remove_bytes(&key);
                }
                inner.insert(&key[..CUT_OFF], ChildNode::Fst(fst));
            }
            (Encoding::Fst(matched_length), Encoding::Art) => {
                let inner_read_guard = self.inner.read();
                let fst = inner_read_guard.get_fst(&key[..matched_length]).unwrap().clone();
                // Must drop RwLockReadGuard before acquiring RwLockWriteGuard
                drop(inner_read_guard);

                let mut inner_write_guard = self.inner.write();
                inner_write_guard.remove(&key[..matched_length]);
                inner_write_guard.insert(&key[..CUT_OFF], ChildNode::Fst(fst));
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AHTrie;

    macro_rules! ahtrie_test {
        ($KEYS:expr) => {
            let keys = $KEYS;
            let trie = AHTrie::new(keys.clone(), (0..keys.len()).collect::<Vec<usize>>(), 20);
            for (i, key) in keys.into_iter().enumerate() {
                assert_eq!(trie.get(&key), Some(i));
            }
            
        };
    }
    
    #[test]
    fn ahtrie_simple_test() {
        let keys = vec![
            "f".to_string(), "far".to_string(), "fas".to_string(), "fast".to_string(), "fat".to_string(),
            "s".to_string(), "top".to_string(), "toy".to_string(), "trie".to_string(), "trip".to_string(),
            "try".to_string(),
        ];
        ahtrie_test!(keys);
    }

    #[test]
    fn ahtrie_hard_test() {
        let keys = vec![
            "f".to_string(), "fastest".to_string(), "gta".to_string(), "hardest".to_string()
        ];
        ahtrie_test!(keys);
    }
}