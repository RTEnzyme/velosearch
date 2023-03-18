use std::sync::Arc;
use async_trait::async_trait;
use datafusion::{arrow::{datatypes::{DataType, Field, Schema}, array::{ArrayRef, UInt32Array, Int8Array}, record_batch::RecordBatch}, from_slice::FromSlice, logical_expr::Operator};
use tokio::time::Instant;
use rand::prelude::*;
use datafusion::prelude::*;
use tracing::{span, info, Level};

use crate::{utils::{json::{WikiItem, parse_wiki_dir}, Result, to_hashmap}};

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
        let items = parse_wiki_dir(path).unwrap();
        let doc_len = items.len() as u32;
        let mut ids: Vec<u32> = Vec::new();
        let mut words: Vec<String> = Vec::new();
        let mut cnt = 0;

        items.into_iter()
        .for_each(|e| {
            let WikiItem{id: _, text: w, title: _} = e;
            w.split([' ', ',', '.', ';', '-'])
            .into_iter()
            .filter(|w| {w.len() > 2}) 
            .for_each(|e| {
                ids.push(cnt);
                words.push(e.to_uppercase().to_string());
                cnt+=1;
            })});

        Self { doc_len, ids, words }
    }

    fn to_recordbatch(&mut self) -> Result<RecordBatch> {
        let _span = span!(Level::INFO, "BaseHandler to_recordbatch").entered();

        let nums = self.doc_len as usize;
        let res = to_hashmap(&self.ids, &self.words, self.doc_len, 1);
        let mut field_list = Vec::new();
        field_list.push(Field::new("__id__", DataType::UInt32, false));
        res.keys().for_each(|k| {
            field_list.push(Field::new(format!("{k}"), DataType::Int8, true));
        });

        let schema = Arc::new(Schema::new(field_list));
        info!("The term dict length: {:}", res.len());
        let mut column_list: Vec<ArrayRef> = Vec::new();
        column_list.push(Arc::new(UInt32Array::from_iter((0..(nums as u32)).into_iter())));
        res.values().for_each(|v| {
            assert_eq!(v[0].len(), nums);
            column_list.push(Arc::new(Int8Array::from_slice(v[0].as_slice())))
        });
        info!("start try new RecordBatch");
        Ok(RecordBatch::try_new(
            schema,
            column_list
        )?)
    }
    
}

#[async_trait]
impl HandlerT for BaseHandler {


    fn get_words(&self, num: u32) -> Vec<String> {
        let mut rng = thread_rng();
        self.words.iter()
        .choose_multiple(&mut rng, num as usize)
        .into_iter()
        .map(|e| e.to_string()).collect()
    }

    async fn execute(&mut self) -> Result<()> {

        info!("start BaseHandler executing");
        let batch = self.to_recordbatch()?;
        info!("End BaseHandler.to_recordbatch()");
        // declare a new context.
        let ctx = SessionContext::new();

        ctx.register_batch("t", batch)?;
        info!("End register batch");
        let df = ctx.table("t").await?;
        info!("End cache table t");

        let test_keys: Vec<String> = self.get_words(100);
        let mut test_iter = test_keys.into_iter();

        let mut handlers = Vec::with_capacity(50);
        let time = Instant::now();
        let mut cnt = 0;
        for x in 0..50 {
            let keys = test_iter.by_ref().take(2).collect::<Vec<String>>();
            if keys[1] == keys[0] {
                continue
            }
            cnt += 1;
            let df = df.clone();
            handlers.push(tokio::spawn(async move {
                df
                    .select_columns(&["__id__", &keys[0], &keys[1]]).unwrap()
                    .with_column("cache", bitwise_and(col(&keys[0]), col(&keys[1]))).unwrap()
                    .filter(col("cache").eq(lit(1 as i8))).unwrap()
                    .explain(false, true).unwrap()
                    .show().await.unwrap();
                println!("{} complete", x);
            }));
        }
        let query_time = time.elapsed().as_micros();
        for handle in handlers {
            handle.await.unwrap();
        }
        info!("query time: {}", query_time/cnt);
        Ok(())
    }
}

fn bitwise_and(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::BitwiseAnd, right)
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