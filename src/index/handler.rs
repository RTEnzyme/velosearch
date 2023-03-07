use std::{collections::{HashMap, HashSet}, path::PathBuf, str::FromStr, sync::Arc};

use datafusion::{arrow::{datatypes::{DataType, Field, Schema}, array::{ArrayRef, UInt32Array, UInt8Array}, record_batch::RecordBatch}};

use crate::{utils::{parse_wiki_file, json::WikiItem, FastErr}};

pub trait HandlerT {
   fn to_recordbatch(&mut self) -> Result<RecordBatch, FastErr>;

   fn get_words(&self, num: u32) -> Vec<String>; 
}


pub struct BaseHandler {
    ids: Vec<u32>,
    pub words: Vec<String>
}

impl BaseHandler {
    pub fn new(path: &str) -> Self {
        let items = parse_wiki_file(&PathBuf::from_str(path).unwrap()).unwrap();
        let mut ids: Vec<u32> = Vec::new();
        let mut words: Vec<String> = Vec::new();

        items.into_iter()
        .for_each(|e| {
            let WikiItem{id: i, text: w, title: _} = e;
            let id = u32::from_str(&i).unwrap();
            w.split(" ")
            .into_iter()
            .for_each(|e| {
                ids.push(id);
                words.push(e.to_string());
            })
        });

        Self { ids, words }
    }

    fn to_hashmap(&mut self) -> HashMap<String, Vec<Option<u8>>> {
        let mut res = HashMap::new();
        let start = self.ids[0];
        let end = self.ids[self.ids.len()-1];
        self.ids.iter().zip(self.words.iter())
        .for_each(|(id, w)| {
            res.entry(w.clone()).or_insert(Vec::new()).push(*id);
        });
        res.iter_mut()
        .map(|(k, v)| {
            let set: HashSet<u32> = HashSet::from_iter(v.iter().cloned());
            let v = (start..=end).into_iter()
            .map(|i| {
                if set.contains(&i) {
                    Some(1 as u8)
                } else {
                    None
                }
            }).collect();
            (k.clone(), v)
        }).collect()
    }
}

impl HandlerT for BaseHandler {
    fn to_recordbatch(&mut self) -> Result<RecordBatch, FastErr> {

        let nums = self.ids[self.ids.len()-1] - self.ids[0] + 1;
        let res = self.to_hashmap();
        // let res: HashMap<String, Vec<Option<u8>>> = self.to_hashmap().into_iter().take(100).collect();
        // self.words = res.keys().cloned().collect();
        let mut field_list = Vec::new();
        field_list.push(Field::new("__id__", DataType::UInt32, false));
        res.keys().for_each(|k| {
            field_list.push(Field::new(format!("{k}"), DataType::UInt8, true));
        });

        let schema = Arc::new(Schema::new(field_list));
        println!("The term dict length: {:}", res.len());
        let mut column_list: Vec<ArrayRef> = Vec::new();
        column_list.push(Arc::new(UInt32Array::from_iter((0..(nums as u32)).into_iter())));
        res.values().for_each(|v| {
            column_list.push(Arc::new(v.into_iter().collect::<UInt8Array>()))
        });

        Ok(RecordBatch::try_new(
            schema,
            column_list
        )?)
    }

    fn get_words(&self, num: u32) -> Vec<String> {
        self.words.iter().take(num as usize).cloned().collect()
    }
}


#[cfg(test)]
mod test {
    // use super::BaseHandler;

    // #[test]
    // fn test_json_bashhandler() {
        // let ids = vec![1, 2, 3, 4, 4, 5];
        // let words = vec!["a".to_string(), "b".to_string(), "c".to_string(), "a".to_string(),
        // "a".to_string(), "c".to_string()];
        // let res = BaseHandler::to_hashmap(ids, words);
        // assert_eq!(res.get("a"), Some(&vec![1,0,0,1,0]));
        // assert_eq!(res.get("b"), Some(&vec![0,1,0,0,0]));
        // assert_eq!(res.get("c"), Some(&vec![0,0,1,0,1]));
//     }
}