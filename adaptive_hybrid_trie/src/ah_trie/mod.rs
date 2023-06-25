mod ah_trie;
use std::cmp::min;

use art_tree::{Art, ByteString};
use fst_rs::FST;
use parking_lot::RwLock;

const CUT_OFF: usize = 4;
pub enum ChildNode {
    Fst(FST),
    Offset(usize),
}

pub struct AHTrieInner<T> {
    pub root: RwLock<Art<ByteString, ChildNode>>,
    values: Vec<T>
}

impl<T> AHTrieInner<T> {
    pub fn new(keys: Vec<String>, values: Vec<T>, cut_off: usize) -> Self {
        assert_eq!(keys.len(), values.len(), "The length of keys and values should be same.");
        let mut key_prefix = prefix(&keys[0].clone());
        let mut tmp_keys = Vec::new();
        let mut tmp_off = Vec::new();
        let mut art = Art::new();
        let last_key_pre = prefix(&keys.last().unwrap().clone());
        for (off, key) in keys.into_iter().enumerate() {
            let pre = prefix(&key);
            if key.len() <= cut_off {
                art.insert(ByteString::new(&pre), ChildNode::Offset(off));
                continue;
            }
            if key_prefix == pre {
                tmp_keys.push(key.as_bytes()[cut_off..].to_vec());
                tmp_off.push(off);
                continue;
            } else {
                if tmp_keys.len() > 0 {
                    let fst = FST::new_with_bytes(&tmp_keys, tmp_off.clone());
                    tmp_keys.clear();
                    tmp_off.clear();
                    art.insert(ByteString::new(&key_prefix), ChildNode::Fst(fst));
                }
                key_prefix = pre;
                tmp_keys.push(key.as_bytes()[cut_off..].to_vec());
                tmp_off.push(off);
            }
        }
        if tmp_keys.len() > 0 {
            println!("keys: {:?}, off: {:?}", tmp_keys, tmp_off);
            let fst = FST::new_with_bytes(&tmp_keys, tmp_off);
            art.insert(ByteString::new(&last_key_pre), ChildNode::Fst(fst));
        }
        Self {
            root: RwLock::new(art),
            values: values,
        }
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        let key_bytes = prefix(key);
        match self.root.read().get_with_length(&ByteString::new(&key_bytes)) {
            Some((child, length)) => {
                match child {
                    ChildNode::Fst(fst) => fst
                        .get(&key.as_bytes()[length..])
                        .map(|v| &self.values[v as usize]),
                    ChildNode::Offset(off) => Some(&self.values[*off]),
                }
            }
            None => None
        }
    }
}

fn prefix(key: &str) -> [u8; CUT_OFF] {
    let mut pre: [u8; CUT_OFF] = [0xFF; CUT_OFF];
    for i in 0..min(key.len(), CUT_OFF) {
        pre[i] = key.as_bytes()[i].clone();
    }
    pre
}

#[cfg(test)]
mod tests {
    use crate::ah_trie::AHTrieInner;

    #[test]
    fn aht_simple_test() {
        let trie = AHTrieInner::new(
            vec!["123".to_string(), "1234".to_string(), "1235".to_string(), "12567".to_string()],
            vec![0, 1, 2, 3], 
            4);
        assert_eq!(trie.get("123"), Some(&0));
        assert_eq!(trie.get("12"), None);
        assert_eq!(trie.get("143"), None);
        assert_eq!(trie.get("1234"), Some(&1)); 
        assert_eq!(trie.get("1235"), Some(&2));
        assert_eq!(trie.get("12567"), Some(&3));
    }

    #[test]
    fn aht_under_cutoff() {
        let trie = AHTrieInner::new(
            vec!["12".to_string(), "123".to_string(), "223".to_string(), "34".to_string()],
            vec![0, 1, 2, 3],
            4,
        );
        assert_eq!(trie.get("12"), Some(&0));
        assert_eq!(trie.get("123"), Some(&1));
        assert_eq!(trie.get("12345"), None);
        assert_eq!(trie.get("34"), Some(&3))
    }

    #[test]
    fn aht_longer_test() {
        let trie = AHTrieInner::new(
            vec!["123".to_string(), "12567896".to_string(), "12567899".to_string()],
            vec![0, 1, 2],
            4, 
        );
        assert_eq!(trie.get("123"), Some(&0));
        assert_eq!(trie.get("12567896"), Some(&1));
        assert_eq!(trie.get("12567899"), Some(&2));
    }
}