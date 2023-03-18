use std::{path::Path, fs::{File, self}, io::{BufReader, BufRead}, collections::{HashMap, HashSet}};

use serde::{Deserialize, Serialize};
use tracing::info;
use crate::utils::Result;

#[derive(Serialize, Deserialize)]
pub struct WikiItem {
    pub id: String,
    pub text: String,
    pub title: String,
}

pub fn parse_wiki_file(path: &Path) -> Result<Vec<WikiItem>> {
    info!("Start parsing {:?}", path);
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

pub fn to_hashmap(ids: &Vec<u32>, words: &Vec<String>, length: u32, partition_nums: usize) -> HashMap<String, Vec<Vec<i8>>> {
    info!("start to_hashmap(), ids len: {}, words len: {}", ids.len(), words.len());
    let mut res = HashMap::new();

    ids.iter().zip(words.iter())
    .for_each(|(id, w)| {
        res.entry(w.clone()).or_insert(Vec::new()).push(*id);
    });
    let range_num = (length as f64/ partition_nums as f64).floor() as usize;
    res.iter_mut()
    .map(|(k, v)| {
        let set: HashSet<u32> = HashSet::from_iter(v.iter().cloned());
        let v = (0..partition_nums).into_iter()
        .map(|i| {
            ((i*range_num)..(i*range_num+range_num)).into_iter()
            .map(|j| {
                if set.contains(&(j as u32)) {
                    1
                } else {
                    0
                } 
            }).collect::<Vec<i8>>()
        }).collect::<Vec<Vec<i8>>>();
        (k.clone(), v)
    }).collect()
}

pub fn parse_wiki_dir(path: &str) -> Result<Vec<WikiItem>> {
    let mut wiki_items = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let meta = fs::metadata(entry_path.clone())?;
        if meta.is_dir() {
            match parse_wiki_dir(entry_path.clone().to_str().unwrap()) {
                Ok(mut items) => wiki_items.append(&mut items),
                Err(e) => return Err(e)
            }
        } else if meta.is_file() {
            wiki_items.append(&mut parse_wiki_file(&entry_path)?);
        }
    }
    Ok(wiki_items)
}

#[cfg(test)]
mod test {
    use super::parse_wiki_dir;

    #[test]
    fn test_parse_wiki_dir() {
        assert_eq!(parse_wiki_dir("~/repo/docker/spark/share/wikipedia/corpus/AA").is_ok(), true);
    }
}