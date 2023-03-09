use std::collections::HashMap;
use std::sync::Arc;
use std::{path::PathBuf, str::FromStr};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, UInt32Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::from_slice::FromSlice;
use datafusion::prelude::*;
use datafusion::{arrow::datatypes::Field};
use datafusion::arrow::record_batch::RecordBatch;
use rand::seq::SliceRandom;
use tokio::time::Instant;
use tracing::{span, Level, info};

use super::HandlerT;
use crate::utils::{Result, json::WikiItem, parse_wiki_file, to_hashmap};


pub struct SplitHandler {
    ids: Vec<u32>,
    words: Vec<String>,
    partition_map: HashMap<String, usize>
}

impl SplitHandler {
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
        Self { ids, words, partition_map: HashMap::new() }
    }

    fn to_recordbatch(&mut self) -> Result<Vec<RecordBatch>> {
        let _span = span!(Level::INFO, "BaseHandler to_recordbatch").entered();

        let nums = self.ids[self.ids.len()-1] - self.ids[0] + 1;
        let res = to_hashmap(&self.ids, &self.words);
        let column_nums = res.len();
        let iter_nums = (column_nums + 99) / 100 as usize;
        let mut column_iter = res.into_iter();
        let mut schema_list = Vec::with_capacity(iter_nums);
        let mut column_list = Vec::with_capacity(iter_nums);

        for i in 0..iter_nums {
            let mut fields= vec![Field::new("__id__", DataType::UInt32, false)];
            let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt32Array::from_iter((0..(nums as u32)).into_iter()))];
            column_iter.by_ref().take(100).for_each(|(field, column)| {
                self.partition_map.insert(field.clone(), i);
                fields.push(Field::new(format!("{field}"), DataType::UInt8, true));
                columns.push(Arc::new(UInt8Array::from_slice(column.as_slice())));
            });

            schema_list.push(fields);
            column_list.push(columns);
        }

        Ok(schema_list.into_iter().zip(column_list.into_iter()).map(|(s, c)| {
            RecordBatch::try_new(Arc::new(Schema::new(s)), c).unwrap()
        }).collect())
    }
}

#[async_trait]
impl HandlerT for SplitHandler {


    fn get_words(&self, num: u32) -> Vec<String> {
        self.words.iter().take(num as usize).cloned().collect()
    }
 
    async fn execute(&mut self) -> Result<()> {
        let batches = self.to_recordbatch()?;
        // declare a new context.
        let ctx = SessionContext::new();

        batches.into_iter().enumerate().for_each(|(i, b)| {
            ctx.register_batch(format!("_table_{i}").as_str(), b).unwrap();
        });
        

        let mut test_keys: Vec<String> = self.get_words(100);
        test_keys.shuffle(&mut rand::thread_rng());
        let test_keys = test_keys[..2].to_vec();
        let i = &test_keys[0];
        let j = &test_keys[1];
        let ip = self.partition_map[i];
        let jp = self.partition_map[j];
        let time = Instant::now();
        let i_df = ctx.table(format!("_table_{ip}").as_str()).await?.select_columns(&[format!("{i}").as_str(), "__id__"])?;
        let j_df = ctx.table(format!("_table_{jp}").as_str()).await?.select_columns(&[format!("{j}").as_str(), "__id__"])?;
    
        i_df.join_on(j_df, JoinType::Inner, [col(i).eq(lit(1)), col(j).eq(lit(1)), col(format!("_table_{ip}.__id__")).eq(col(format!("_table_{jp}.__id__")))])?.explain(false, true)?.show().await?;
        let query_time = time.elapsed().as_millis();
        info!("query time: {}", query_time);
        Ok(())
    }
}