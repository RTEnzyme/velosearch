use std::{path::PathBuf, str::FromStr, sync::Arc};
use async_trait::async_trait;
use datafusion::{arrow::{datatypes::{DataType, Field, Schema}, array::{ArrayRef, UInt32Array, UInt8Array}, record_batch::RecordBatch}, from_slice::FromSlice};
use tokio::time::Instant;
use rand::prelude::*;
use datafusion::prelude::*;
use tracing::{span, info, Level};

use crate::{utils::{parse_wiki_file, json::WikiItem, Result, to_hashmap}};

#[async_trait]
pub trait HandlerT {
   fn get_words(&self, num: u32) -> Vec<String>; 

   async fn execute(&mut self) -> Result<()>;
}


pub struct BaseHandler {
    doc_len: u32,
    ids: Vec<u32>,
    pub words: Vec<String>
}

impl BaseHandler {
    pub fn new(path: &str) -> Self {
        let items = parse_wiki_file(&PathBuf::from_str(path).unwrap()).unwrap();
        let doc_len = items.len() as u32;
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

        Self { doc_len, ids, words }
    }

    fn to_recordbatch(&mut self) -> Result<RecordBatch> {
        let _span = span!(Level::INFO, "BaseHandler to_recordbatch").entered();

        let nums = self.ids[self.ids.len()-1] - self.ids[0] + 1;
        let res = to_hashmap(&self.ids, &self.words, self.doc_len);
        // let res: HashMap<String, Vec<Option<u8>>> = self.to_hashmap().into_iter().take(100).collect();
        // self.words = res.keys().cloned().collect();
        let mut field_list = Vec::new();
        field_list.push(Field::new("__id__", DataType::UInt32, false));
        res.keys().for_each(|k| {
            field_list.push(Field::new(format!("{k}"), DataType::UInt8, true));
        });

        let schema = Arc::new(Schema::new(field_list));
        info!("The term dict length: {:}", res.len());
        let mut column_list: Vec<ArrayRef> = Vec::new();
        column_list.push(Arc::new(UInt32Array::from_iter((0..(nums as u32)).into_iter())));
        res.values().for_each(|v| {
            column_list.push(Arc::new(UInt8Array::from_slice(v.as_slice())))
        });

        Ok(RecordBatch::try_new(
            schema,
            column_list
        )?)
    }
    
}

#[async_trait]
impl HandlerT for BaseHandler {


    fn get_words(&self, num: u32) -> Vec<String> {
        self.words.iter().take(num as usize).cloned().collect()
    }

    async fn execute(&mut self) -> Result<()> {

        let batch = self.to_recordbatch()?;
        // declare a new context.
        let ctx = SessionContext::new();

        ctx.register_batch("t", batch)?;
        let df = ctx.table("t").await?.cache().await?;
        

        let mut test_keys: Vec<String> = self.get_words(100);
        test_keys.shuffle(&mut rand::thread_rng());
        let test_keys = test_keys[..2].to_vec();
        let i = &test_keys[0];
        let j = &test_keys[1];
        let time = Instant::now();
        df.clone().explain(false, true)?.show().await?;
        info!("bare select time: {}", time.elapsed().as_millis());
        let time = Instant::now();
        df
            .filter(col(i).is_not_null()
            .and(col(j).is_not_null()))?
            .select_columns(&["__id__", i, j])? 
            .explain(false, true)?
            // .collect().await?
            .show().await?;
        // println!("{}", format!("select {i}, {j} from t where {i} is not null and {j} is not null"));
        // ctx.sql(&format!("select `{i}`, `{j}` from t where `{i}` is not null and `{j}` is not null")).await?;
        let query_time = time.elapsed().as_millis();
        info!("query time: {}", query_time);
        Ok(())
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