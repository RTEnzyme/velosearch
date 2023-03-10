use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::{str::FromStr};
use std::io::Write;

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
use crate::utils::json::parse_wiki_dir;
use crate::utils::{Result, json::WikiItem, to_hashmap};


pub struct SplitHandler {
    doc_len: u32,
    ids: Vec<u32>,
    words: Vec<String>,
    partition_map: HashMap<String, usize>
}

impl SplitHandler {
    pub fn new(path: &str) -> Self {
        let items = parse_wiki_dir(path).unwrap();
        let mut ids: Vec<u32> = Vec::new();
        let mut words: Vec<String> = Vec::new();
        let doc_len = items.len() as u32;

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
        Self { doc_len, ids, words, partition_map: HashMap::new() }
    }

    fn to_recordbatch(&mut self) -> Result<Vec<RecordBatch>> {
        let _span = span!(Level::INFO, "BaseHandler to_recordbatch").entered();

        let nums = self.doc_len as usize;
        let res = to_hashmap(&self.ids, &self.words, self.doc_len);
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

pub struct SplitO1 {
    base: SplitHandler,
    encode: HashMap<String, u32>,
    idx: u32
}

impl SplitO1 {
    pub fn new(path: &str) -> Self {
        Self { 
            base: SplitHandler::new(path),
            encode: HashMap::new(),
            idx: 0
        }
    }

    // use encode u32 inplace variant String
    fn recode(&mut self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut new_batch = Vec::with_capacity(batches.len());
        for batch in batches {
            let schema = batch.schema();
            let fields = schema.all_fields();
            let fields = fields.into_iter().skip(1)
            .map(|field| {
                self.encode.insert(field.name().to_string(), self.idx);
                self.idx += 1;
                Field::new(format!("{}", self.idx-1), DataType::UInt8, false)
            });
            let mut fields_id = vec![Field::new("__id__", DataType::UInt32, false)];
            fields_id.extend(fields);
            new_batch.push(RecordBatch::try_new(Arc::new(Schema::new(fields_id)), batch.columns().to_vec())?);
        }
        Ok(new_batch)
    }
}

#[async_trait]
impl HandlerT for SplitO1 {
    fn get_words(&self, num: u32) -> Vec<String> {
        self.base.get_words(num)
    }

    async fn execute(&mut self) ->Result<()> {
        let batches = self.base.to_recordbatch()?;
        let batches = self.recode(batches)?;
        // declare a new context.
        let ctx = SessionContext::new();

        batches.into_iter().enumerate().for_each(|(i, b)| {
            ctx.register_batch(format!("_table_{i}").as_str(), b).unwrap();
        });
        
        
        let mut test_keys: Vec<String> = self.get_words(100);
        test_keys.shuffle(&mut rand::thread_rng());
        let time = Instant::now();
        let mut handlers = Vec::with_capacity(50);
        for x in 0..50 {
            let test_keys = test_keys[2*x..2*x+2].to_vec();
            let ctx = ctx.clone();
            let i = &test_keys[0];
            let j = &test_keys[1];
            let ri = self.encode[i];
            let rj = self.encode[j];
            let ip = self.base.partition_map[i];
            let jp = self.base.partition_map[j];
                
            handlers.push(tokio::spawn(async move {
                if ri == rj {
                    return;
                }
                let query = if ip != jp {
                    let i_df = ctx.table(format!("_table_{ip}").as_str()).await.unwrap().select_columns(&[format!("{ri}").as_str(), "__id__"]).unwrap();
                    let j_df = ctx.table(format!("_table_{jp}").as_str()).await.unwrap().select_columns(&[format!("{rj}").as_str(), "__id__"]).unwrap();
                    i_df
                    .join_on(j_df, JoinType::Inner, [col(ri.to_string()).eq(lit(1)), col(rj.to_string()).eq(lit(1)), col(format!("_table_{ip}.__id__")).eq(col(format!("_table_{jp}.__id__")))]).unwrap()
                } else {
                    let i_df = ctx.table(format!("_table_{ip}").as_str()).await.unwrap().select_columns(&[format!("{rj}").as_str(), format!("{ri}").as_str(), "__id__"]).unwrap();
                    i_df.filter(col(ri.to_string()).eq(lit(1))).unwrap().filter(col(rj.to_string()).eq(lit(1))).unwrap()
                };
                
                query.collect().await.unwrap();
                println!("{} complete!", x);
            }));
        }
        // ctx.sql(sql)s
        for handle in handlers {
            handle.await.unwrap();
        }
        let query_time = time.elapsed().as_micros() / 50;
        info!("o1 query time: {} micors seconds", query_time);
        Ok(())    
    }
}

pub struct SplitConstruct {
    doc_len: u32,
    ids: Vec<u32>,
    words: Vec<String>,
    idx: u32,
    encode: HashMap<String, u32>,
    partition: HashMap<String, u32>
}

impl SplitConstruct {
    pub fn new(path: &str) -> Self {
        let items = parse_wiki_dir(path).unwrap();
        let doc_len = items.len() as u32;
        info!("WikiItem len: {}", items.len());

        let mut ids: Vec<u32> = Vec::new();
        let mut words: Vec<String> = Vec::new();
        let mut cnt = 0;
        items.into_iter()
        .for_each(|e| {
            let WikiItem{id: _, text: w, title: _} = e;
            w.split([' ', ',', '.', 'l'])
            .into_iter()
            .for_each(|e| {
                ids.push(cnt);
                words.push(e.to_uppercase().to_string());
                cnt+=1;
            })
        });
        Self { doc_len, ids, words, idx: 0, encode: HashMap::new(), partition: HashMap::new() } 
    }

    pub async fn split(&mut self, _: &str) -> Result<()> {
        let _span = span!(Level::INFO, "SplitConstruct to_recordbatch").entered();

        let nums = self.doc_len;
        let res = to_hashmap(&self.ids, &self.words, self.doc_len);
        info!("hashmap len: {}", res.len());
        let column_nums = res.len();
        let iter_nums = (column_nums + 99) / 100 as usize;
        let mut column_iter = res.into_iter();

        let ctx = SessionContext::new();
        for i in 0..iter_nums {
            info!("table {} creating", i);
            let mut fields= vec![Field::new("__id__", DataType::UInt32, false)];
            let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt32Array::from_iter((0..(nums as u32)).into_iter()))];
            column_iter.by_ref().take(100).for_each(|(field, column)| {
                self.partition.insert(field.clone(), i as u32);
                self.encode.insert(field.clone(), self.idx);
                self.idx += 1;
                fields.push(Field::new(format!("{}", self.idx-1), DataType::UInt8, true));
                assert_eq!(nums, column.len() as u32, "the {}th", i);
                columns.push(Arc::new(UInt8Array::from_slice(column.as_slice())));
            });

            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            ctx.read_batch(batch)?.write_parquet(format!("./data/tables/table_{i}.parquet").as_str(), None).await?;
        }


        let mut encode_f = File::create("./data/encode.json")?;
        let mut partition_f = File::create("./data/partition.json")?;
        encode_f.write_all(serde_json::to_string( &self.encode)?.as_bytes())?;
        partition_f.write_all(serde_json::to_string(&self.partition)?.as_bytes())?;

        Ok(())
    }
}

