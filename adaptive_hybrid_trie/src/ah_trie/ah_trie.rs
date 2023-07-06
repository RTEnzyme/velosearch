use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use fst_rs::FST;
use tokio::{runtime::{Runtime, Handle}, sync::RwLock};
use std::sync::Arc;
use tracing::debug;

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

pub struct AHTrie<T: Clone+Send+Sync> {
    /// A tokio runtime for executing synchronous operation
    // rt: Runtime,
    skip_length: usize,
    epoch: usize,
    sampling_stat: Arc<DashMap<String, AccessStatistics>>,

    /// Inner Adaptive Hybrid Trie
    inner: Arc<RwLock<AHTrieInner<T>>>,

    /// Run-time tracing
    skip_counter: AtomicUsize,
}

impl<T: Clone+Send+Sync> AHTrie<T> {
    pub fn new(keys: Vec<String>, values: Vec<T>, skip_length: usize) -> Self {
        Self::new_with_rt(
            keys,
            values,
            skip_length,
            // tokio::runtime::Builder::new_multi_thread().build().unwrap(),
        )
    }

    pub fn new_with_rt(keys: Vec<String>, values: Vec<T>, skip_length: usize) -> Self {
        Self {
            // rt,
            skip_length,
            epoch: 0,
            sampling_stat: Arc::new(DashMap::new()),
            inner: Arc::new(RwLock::new(AHTrieInner::new(keys, values, CUT_OFF))),
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
        debug!("get key: {:}", key);
        futures::executor::block_on(async {
            self.inner.as_ref().read().await.get(key).cloned()
        })
    }

    pub async fn get_async(&self, key: &str) -> Option<T> {
        if self.is_sample() {
            self.trace(key);
        }
        self.inner.as_ref().read().await.get(key).cloned()
    }

    pub fn get_mut<F>(&self, key: &str, f: F)
    where F: Fn(&mut T),
    {
        let mut block = self.inner.blocking_write();
        match block.get_offset(key) {
            Some(v) => f(block.get_mut(v)),
            None => unreachable!(),
        }
    }

    #[inline]
    fn trace(&self, key: &str) {
        debug!("start tracing");
        let map = Arc::clone(&self.sampling_stat);
        let k = key.to_string();
        // use runtime to trace the statistics asynchronously.
        tokio::task::spawn(async move {
            map
                .entry(k)
                .and_modify(|v| v.reads += 1)
                .or_insert(AccessStatistics { reads: 0 });
        });
        debug!("end tracing");
    }

    async fn convert_encoding(&self, key: &str, encoding: Encoding) {
        let cur_encoding = self.inner.read().await.encoding(&key);
        match (cur_encoding, encoding) {
            (Encoding::Art, Encoding::Fst(_)) => {
                let inner_read_guard = self.inner.read().await;
                let kvs = inner_read_guard.prefix_keys(&key[..CUT_OFF]);
                let mut keys = Vec::new();
                let mut values = Vec::new();
                kvs.into_iter()
                    .for_each(|(k, v)| {
                        println!("FST k: {:?}", k.as_bytes());
                        keys.push(k.as_bytes()[CUT_OFF..].to_vec());
                        values.push(if let ChildNode::Offset(off) = v {*off} else {unreachable!()});
                    });
                let fst = FST::new_with_bytes(&keys, values);
                // Safety
                // Must drop RwLockReadGuard before acquiring RwLockWriteGuard
                drop(inner_read_guard);

                let mut inner = self.inner.write().await;
                for key in keys {
                    inner.remove_bytes(&key);
                }
                inner.insert(&key[..CUT_OFF], ChildNode::Fst(fst));
            }
            (Encoding::Fst(matched_length), Encoding::Art) => {
                let inner_read_guard = self.inner.read().await;
                let fst = inner_read_guard.get_fst(&key[..matched_length]).unwrap().clone();
                // Safety:
                // Must drop RwLockReadGuard before acquiring RwLockWriteGuard
                drop(inner_read_guard);

                let mut inner_write_guard = self.inner.write().await;
                inner_write_guard.remove(&key[..matched_length]);
                fst.iter()
                    .for_each(|(k, v)| {
                        println!("insert k: {:?}", k);
                        inner_write_guard.insert_bytes(&[key[..matched_length].as_bytes(), &k].concat(), ChildNode::Offset(v as usize));
                    })
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AHTrie, Encoding};

    macro_rules! ahtrie_test {
        ($KEYS:expr) => {
            
            let keys = $KEYS;
            let trie = AHTrie::new(keys.clone(), (0..keys.len()).collect::<Vec<usize>>(), 20);
            for (i, key) in keys.into_iter().enumerate() {
                assert_eq!(trie.get(&key), Some(i), "key: {:?}", key);
            }
            std::mem::forget(trie);
        };
    }
    
    #[tokio::test(flavor ="multi_thread")]
    async fn ahtrie_simple_test() {
        let keys = vec![
            "f".to_string(), "far".to_string(), "fas".to_string(), "fast".to_string(), "fat".to_string(),
            "s".to_string(), "top".to_string(), "toy".to_string(), "trie".to_string(), "trip".to_string(),
            "try".to_string(),
        ];
        ahtrie_test!(keys);
    }

    #[test]
    fn ahtrie_raw_test() {
        let keys = vec![
            "f".to_string(), "far".to_string(), "fas".to_string(), "fast".to_string(), "fat".to_string(),
            "s".to_string(), "top".to_string(), "toy".to_string(), "trie".to_string(), "trip".to_string(),
            "try".to_string(),
        ];
        let trie = AHTrie::new(keys.clone(), (0..keys.len()).collect::<Vec<usize>>(), 20);
        for (i, key) in keys.into_iter().enumerate() {
            assert_eq!(trie.get(&key), Some(i), "key: {:?}", key);
        }
    }

    #[tokio::test(flavor ="multi_thread")]
    async fn ahtrie_hard_test() {
        let keys = vec![
            "f".to_string(), "fastest".to_string(), "gta".to_string(), "hardest".to_string(),
        ];
        ahtrie_test!(keys);
    }

    #[tokio::test(flavor ="multi_thread")]
    async fn ahtrie_convert_fst_to_art() {
        let keys = vec![
            "fast123".to_string(), "fast124".to_string(), "fast125".to_string(), "fast1255".to_string(),
        ];
        let trie = AHTrie::new(keys.clone(), (0..keys.len()).collect::<Vec<usize>>(), 20);
        trie.convert_encoding("fast", Encoding::Art).await;
        
        // Should get the value correctly
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(trie.get(k.as_str()), Some(i));
        }
        std::mem::forget(trie);
    }

    #[tokio::test(flavor ="multi_thread")]
    async fn ahtrie_convert_art_to_fst() {
        let keys = vec![
            "fast123".to_string(), "fast124".to_string(), "fast125".to_string(), "last1255".to_string(),
        ];
        let trie = AHTrie::new(keys.clone(), (0..keys.len()).collect::<Vec<usize>>(), 20);
        // Convert fst to art
        trie.convert_encoding("fast125", Encoding::Art).await;
        // Should get the value correctly
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(trie.get(k.as_str()), Some(i));
        }

        // And then, convert art to fst
        trie.convert_encoding("fast125", Encoding::Fst(0)).await;

        // Should get the value correctly
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(trie.get(k.as_str()), Some(i));
        }
        std::mem::forget(trie);
    }
}