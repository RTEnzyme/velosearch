use std::{collections::HashSet, sync::Arc, mem::size_of_val};

use async_trait::async_trait;
use datafusion::{arrow::{datatypes::{Schema, Field, DataType}}, sql::TableReference, prelude::col};
use adaptive_hybrid_trie::TermIdx;
use rand::{thread_rng, seq::IteratorRandom};
use tantivy::tokenizer::{TextAnalyzer, SimpleTokenizer, RemoveLongFilter, LowerCaser, Stemmer};
use tokio::time::Instant;
use tracing::{info, span, Level, debug};

use crate::{utils::json::{parse_wiki_dir, WikiItem}, Result, batch::{PostingBatchBuilder, BatchRange, TermMetaBuilder, self}, datasources::posting_table::PostingTable, BooleanContext, query::boolean_query::BooleanPredicateBuilder};

use super::HandlerT;

pub struct PostingHandler {
    doc_len: usize,
    ids: Option<Vec<u32>>,
    words: Option<Vec<String>>,
    test_case: Vec<String>,
    partition_nums: usize,
    batch_size: u32,
}

impl PostingHandler {
    pub fn new(base: String, path: Vec<String>, partition_nums: usize, batch_size: u32) -> Self {
        let items: Vec<WikiItem> = path
            .into_iter()
            .map(|p| parse_wiki_dir(&(base.clone() + &p)).unwrap())
            .flatten()
            .collect();
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
        Self { doc_len, ids: Some(ids), words: Some(words), test_case, partition_nums, batch_size }
    }

}

#[async_trait]
impl HandlerT for PostingHandler {
    fn get_words(&self, _num:u32) -> Vec<String>  {
        self.test_case.clone()
    }

    async fn execute(&mut self) ->  Result<u128> {

        let partition_nums = self.partition_nums;
        let posting_table = to_batch(self.ids.take().unwrap(), self.words.take().unwrap(), self.doc_len, partition_nums, self.batch_size);
        let ctx = BooleanContext::new();
        let space = posting_table.space_usage();
        ctx.register_index(TableReference::Bare { table: "__table__".into() }, Arc::new(posting_table))?;
        let table = ctx.index("__table__").await?;
        let mut test_iter = self.test_case.clone().into_iter();

        // let mut handlers = Vec::with_capacity(20);
        debug!("======================start!===========================");
        let mut cnt = 0;
        // for _ in 0..1 {
            let keys = test_iter.by_ref().take(100).collect::<Vec<String>>();
            cnt += 1;
            // let table = table.clone();
            // let predicate = BooleanPredicateBuilder::should(&[&keys[0], &keys[1]]).unwrap();
            // let predicate1 = BooleanPredicateBuilder::must(&[&keys[2], &keys[3], &keys[4]]).unwrap();
            // let predicate = BooleanPredicateBuilder::should(&["and", "the"]).unwrap();
            // let predicate = predicate.with_must(predicate1).unwrap();
            // let predicate = predicate.build();
            // let predicate = predicate.boolean_and(col("me"));
            let mut time_sum = 0;
            // handlers.push(tokio::spawn(async move {
                debug!("start construct query");
            let mut time_distri = Vec::new();
            let round = 20;
            for i in 0..round {
                let idx = i * 5;
                let predicate = BooleanPredicateBuilder::should(&[&keys[idx], &keys[idx + 1]]).unwrap();
                let predicate1 = BooleanPredicateBuilder::must(&[&keys[idx + 2], &keys[idx + 3], &keys[idx + 4]]).unwrap();
                // let predicate = BooleanPredicateBuilder::should(&["and", "the"]).unwrap();
                let predicate = predicate.with_must(predicate1).unwrap();
                let predicate = predicate.build();
                info!("Predicate{:}: {:?}", i, predicate);
                let index = ctx.boolean("__table__", predicate).await.unwrap();
                let timer = Instant::now();
                    index
                    // .explain(false, true).unwrap()
                    // .show().await.unwrap();
                    .collect().await.unwrap();
                let time = timer.elapsed().as_micros();
                time_distri.push(time);
                time_sum += time;
            }
            info!("Time distribution: {:?}", time_distri);
                // table.boolean_predicate(predicate).unwrap()
                //     .collect().await.unwrap();
                    // .explain(false, true).unwrap()
                    // .show().await.unwrap();
            // }))
        // }

        // for handle in handlers {
        //     handle.await.unwrap();
        // }
        // info!("Total time: {} us", query_time / 5);
        // info!("Total memory: {} MB", space / 1000_000);
        Ok(time_sum / round as u128)
    }
}


fn to_batch(ids: Vec<u32>, words: Vec<String>, length: usize, partition_nums: usize, batch_size: u32) -> PostingTable {
    let _span = span!(Level::INFO, "PostingHanlder to_batch").entered();
    let num_512 = (length as u32 + batch_size - 1) / batch_size;
    let num_512_partition = (num_512 + partition_nums as u32 - 1) / (partition_nums as u32);

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
            batches.push(PostingBatchBuilder::new((i as u32 * batch_size * num_512_partition + j as u32 * batch_size) as u32));
        }
        partition_batch.push(batches);
        term_idx.push(TermIdx::new());
    }
    let mut current = (0, 0);
    let mut thredhold = batch_size;
    assert!(ids.is_sorted(), "ids must be sorted");
    assert_eq!(ids.len(), words.len(), "The length of ids and words must be same");
    words
        .into_iter()
        .zip(ids.into_iter())
        .for_each(|(word, id)| {
            let entry = term_idx[current.0].term_map.entry(word.clone()).or_insert(TermMetaBuilder::new(num_512_partition as usize));
            if id >= thredhold as u32 {
                debug!("id: {}", id);
                if id >= (batch_size * num_512_partition * (current.0 as u32 + 1)) {
                    current.0 += 1;
                    current.1 = 0;
                    info!("Start build ({}, {}) batch, current thredhold: {}", current.0, current.1, thredhold);
                } else {
                    info!("Thredhold: {}, current: {:?}", thredhold, current);
                    current.1 += 1;
                }
                thredhold += batch_size;
            }
            partition_batch[current.0][current.1].push_term(word, id).expect("Shoud push term correctly");
            entry.set_true(current.1);
        });
    for (i, p) in partition_batch.iter().enumerate() {
        for (j, pp) in p.into_iter().enumerate() {
            debug!("The ({}, {}) batch len: {}", i, j, pp.doc_len())
        }
    }
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
        &BatchRange::new(0, (num_512_partition * batch_size) as u32),
    )
}

