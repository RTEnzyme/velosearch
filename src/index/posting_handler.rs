use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use datafusion::{arrow::{datatypes::{Schema, Field, DataType}}, sql::TableReference, prelude::col};
use learned_term_idx::TermIdx;
use rand::{thread_rng, seq::IteratorRandom};
use tantivy::tokenizer::{TextAnalyzer, SimpleTokenizer, RemoveLongFilter, LowerCaser, Stemmer};
use tokio::time::Instant;
use tracing::{info, span, Level, debug};

use crate::{utils::json::{parse_wiki_dir, WikiItem}, Result, batch::{PostingBatchBuilder, BatchRange, TermMetaBuilder}, datasources::posting_table::PostingTable, BooleanContext, query::boolean_query::BooleanPredicateBuilder};

use super::HandlerT;

pub struct PostingHandler {
    doc_len: usize,
    ids: Option<Vec<u32>>,
    words: Option<Vec<String>>,
    test_case: Vec<String>,
    partition_nums: usize,
}

impl PostingHandler {
    pub fn new(path: &str, partition_nums: usize) -> Self {
        let items = parse_wiki_dir(path).unwrap();
        let doc_len = items.len();
        let mut ids: Vec<u32> = Vec::new();
        let mut words: Vec<String> = Vec::new();
        let mut cnt = 0;
        let tokenizer = TextAnalyzer::from(SimpleTokenizer)
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(Stemmer::default());
        
        items
        .into_iter()
        .for_each(|e| {
            let WikiItem {id: _, text: w, title: _} = e;

            let mut stream = tokenizer.token_stream(w.as_str());
            while let Some(token) = stream.next() {
                ids.push(cnt);
                words.push(token.text.clone());
            }
            cnt += 1;
        });

        let mut rng = thread_rng();
        let test_case = words.iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .choose_multiple(&mut rng, 100)
            .into_iter()
            .map(|e| e.to_string())
            .collect();
        info!("self.doc_len = {}", doc_len);
        Self { doc_len, ids: Some(ids), words: Some(words), test_case, partition_nums }
    }

}

#[async_trait]
impl HandlerT for PostingHandler {
    fn get_words(&self, _num:u32) -> Vec<String>  {
        self.test_case.clone()
    }

    async fn execute(&mut self) ->  Result<()> {

        let partition_nums = self.partition_nums;
        let posting_table = to_batch(self.ids.take().unwrap(), self.words.take().unwrap(), self.doc_len, partition_nums);
        let ctx = BooleanContext::new();
        ctx.register_index(TableReference::Bare { table: "__table__".into() }, Arc::new(posting_table))?;
        let table = ctx.index("__table__").await?;
        let mut test_iter = self.test_case.clone().into_iter();

        // let mut handlers = Vec::with_capacity(20);
        debug!("======================start!===========================");
        let mut cnt = 0;
        // for _ in 0..1 {
            let keys = test_iter.by_ref().take(5).collect::<Vec<String>>();
            cnt += 1;
            let table = table.clone();
            // let predicate = BooleanPredicateBuilder::should(&[&keys[0], &keys[1]]).unwrap();
            // let predicate1 = BooleanPredicateBuilder::must(&[&keys[2], &keys[3], &keys[4]]).unwrap();
            let predicate = BooleanPredicateBuilder::should(&["and", "the"]).unwrap();
            // let predicate = predicate.with_must(predicate1).unwrap();
            let predicate = predicate.build();
            let predicate = predicate.boolean_and(col("me"));
            let time = Instant::now();
            // handlers.push(tokio::spawn(async move {
                debug!("start construct query");
                ctx.boolean("__table__", predicate).await.unwrap()
                    // .explain(false, true).unwrap()
                    // .show().await.unwrap();
                    .collect().await.unwrap();
                // table.boolean_predicate(predicate).unwrap()
                //     .collect().await.unwrap();
                    // .explain(false, true).unwrap()
                    // .show().await.unwrap();
            // }))
        // }

        // for handle in handlers {
        //     handle.await.unwrap();
        // }
        let query_time = time.elapsed().as_micros();
        info!("Total time: {} us", query_time / cnt);
        Ok(())
    }
}

const BATCH_SIZE: usize = 512;

fn to_batch(ids: Vec<u32>, words: Vec<String>, length: usize, partition_nums: usize) -> PostingTable {
    let _span = span!(Level::INFO, "PostingHanlder to_batch").entered();
    let num_512 = length / BATCH_SIZE;
    let num_512_partition = num_512 / partition_nums;

    let schema = Schema::new(
        words.iter().collect::<HashSet<_>>().into_iter().chain([&"__id__".to_string()].into_iter()).map(|v| Field::new(v.to_string(), DataType::Boolean, false)).collect()
    );
    info!("The lenght of schema: {}", schema.fields().len());
    info!("num_512: {}, num_512_partition: {}", num_512, num_512_partition);
    let mut partition_batch = Vec::new();
    let mut term_idx: Vec<TermIdx<TermMetaBuilder>> = Vec::new();
    for i in 0..partition_nums {
        let mut batches = Vec::new();
        for j in 0..num_512_partition {
            batches.push(PostingBatchBuilder::new((i * BATCH_SIZE * num_512_partition + j * BATCH_SIZE) as u32));
        }
        partition_batch.push(batches);
        term_idx.push(TermIdx::new());
    }
    let mut current = (0, 0);
    let mut thredhold = BATCH_SIZE;
    assert!(ids.is_sorted(), "ids must be sorted");
    words
        .into_iter()
        .zip(ids.into_iter())
        .for_each(|(word, id)| {
            let entry = term_idx[current.0].term_map.entry(word.clone()).or_insert(TermMetaBuilder::new(num_512_partition));
            if id == thredhold as u32 {
                debug!("id: {}", id);
                if id % (BATCH_SIZE * num_512_partition) as u32 == 0 {
                    current.0 += 1;
                    current.1 = 0;
                    info!("Start build ({}, {}) batch, current thredhold: {}", current.0, current.1, thredhold);
                } else if id % BATCH_SIZE as u32 == 0{
                    info!("Thredhold: {}, current: {:?}", thredhold, current);
                    current.1 += 1;
                }
                thredhold += BATCH_SIZE;
            }
            partition_batch[current.0][current.1].push_term(word, id).expect("Shoud push term correctly");
            entry.set_true(current.1);
        });
    let partition_batch = partition_batch
        .into_iter()
        .zip(term_idx.iter_mut())
        .map(|(b, t)| Arc::new(
            b
            .into_iter()
            .enumerate()
            .map(|(i, b)| b.build_with_idx(t, i as u16).unwrap()).collect()))
        .collect();

    let term_idx = term_idx
        .into_iter()
        .map(|m| {
            let map = m.term_map.into_iter().map(|(k, v)| (k, v.build())).collect();
            Arc::new(TermIdx { term_map: map })
        })
        .collect();
    PostingTable::new(
        Arc::new(schema),
        term_idx,
        partition_batch,
        &BatchRange::new(0, (num_512_partition * BATCH_SIZE) as u32),
    )
}

