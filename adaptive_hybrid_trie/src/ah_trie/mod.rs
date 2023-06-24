mod adaptation_manager;
use adaptation_manager::AdaptationManager;
use art_tree::{Art, ByteString};
use fst_rs::FST;

const CUT_OFF: usize = 4;
pub enum ChildNode {
    Fst(FST),
    Offset(usize),
}

pub struct AHTrie<'a, T> {
    manager: AdaptationManager<'a>,
    art_cutoff: usize,
    root: Art<ByteString, ChildNode>,
    values: Vec<T>
}

impl<T> AHTrie<'_, T> {
    // pub fn new() -> Self {
    //     Self {
    //         manager: AdaptationManager::new(20),
    //         art_cutoff: 4,
    //         root: Art::new(),
    //         values: Vec::new(),
    //     }
    // }

    pub fn new(keys: Vec<String>, values: Vec<T>, cut_off: usize) -> Self {
        assert_eq!(!keys.len(), values.len(), "The length of keys and values should be same.");
        let mut key_prefix = prefix(&keys[0].clone(), cut_off);
        let mut tmp_keys = Vec::new();
        let mut tmp_off = Vec::new();
        let mut art = Art::new();
        for (off, key) in keys.into_iter().enumerate() {
            let pre;
            if key.len() < cut_off {
                pre = prefix(&key, cut_off);
                art.insert(ByteString::new(&pre), ChildNode::Offset(off));
                continue;
            } else {
                pre = prefix(&key, cut_off)
            }
            if key_prefix == pre {
                tmp_keys.push(key.as_bytes()[cut_off..].to_vec());
                tmp_off.push(off);
                continue;
            } else {
                let fst = FST::new_with_bytes(&tmp_keys, tmp_off.clone());
                tmp_keys.clear();
                tmp_off.clear();
                art.insert(ByteString::new(&pre), ChildNode::Fst(fst));
                key_prefix = pre;
            }
        }
        unimplemented!()
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        let key_bytes = key.as_bytes();
        match self.root.get_with_length(&ByteString::new(&key_bytes[0..self.art_cutoff])) {
            Some((child, length)) => {
                match child {
                    ChildNode::Fst(fst) => fst
                        .get(&key_bytes[length..])
                        .map(|v| &self.values[v as usize]),
                    ChildNode::Offset(off) => Some(&self.values[*off]),
                }
            }
            None => None
        }
    }
}

fn prefix(key: &String, cut_off: usize) -> [u8; CUT_OFF] {
    let mut pre: [u8; CUT_OFF] = [0xFF; CUT_OFF];
    for i in 0..key.len() {
        pre[i] = key.as_bytes()[i].clone();
    }
    pre
}