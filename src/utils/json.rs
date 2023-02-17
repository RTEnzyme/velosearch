use std::{path::Path, fs::File, io::{BufReader, BufRead}};

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