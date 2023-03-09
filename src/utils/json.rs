use std::{path::Path, fs::File, io::{BufReader, BufRead}, collections::{HashMap, HashSet}};

use serde::{Deserialize, Serialize};
use serde_json::Result;

#[derive(Serialize, Deserialize)]
pub struct WikiItem {
    pub id: String,
    pub text: String,
    pub title: String,
}

pub fn parse_wiki_file(path: &Path) -> Result<Vec<WikiItem>> {
    let file = File::open(path).unwrap();
    let buf_reader = BufReader::new(file);
    Ok(
    buf_reader.lines()
    .into_iter()
    .map(|l| {
        let s = l.unwrap();
        serde_json::from_str::<WikiItem>(&s).unwrap()
    }).collect())
}

pub fn to_hashmap(ids: &Vec<u32>, words: &Vec<String>) -> HashMap<String, Vec<u8>> {
    let mut res = HashMap::new();
    let start = ids[0];
    let end = ids[ids.len()-1];
    ids.iter().zip(words.iter())
    .for_each(|(id, w)| {
        res.entry(w.clone()).or_insert(Vec::new()).push(*id);
    });
    res.iter_mut()
    .map(|(k, v)| {
        let set: HashSet<u32> = HashSet::from_iter(v.iter().cloned());
        let v = (start..=end).into_iter()
        .map(|i| {
            if set.contains(&i) {
                0x1
            } else {
                0x0 
            }
        }).collect();
        (k.clone(), v)
    }).collect()
}