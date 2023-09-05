use std::{collections::{HashSet, BTreeMap}, sync::Arc, fs::File, io::{BufWriter, BufReader}, path::PathBuf};

use async_trait::async_trait;
use datafusion::{sql::TableReference, arrow::datatypes::{Schema, DataType, Field}};
use adaptive_hybrid_trie::TermIdx;
use rand::{thread_rng, seq::IteratorRandom};
use tantivy::tokenizer::{TextAnalyzer, SimpleTokenizer, RemoveLongFilter, LowerCaser, Stemmer};
use tokio::time::Instant;
use tracing::{info, span, Level, debug};

use crate::{utils::json::{parse_wiki_dir, WikiItem}, Result, batch::{PostingBatchBuilder, BatchRange, TermMetaBuilder}, datasources::posting_table::PostingTable, BooleanContext, query::boolean_query::BooleanPredicateBuilder, jit::AOT_PRIMITIVES};

use super::HandlerT;

pub struct PostingHandler {
    test_case: Vec<String>,
    partition_nums: usize,
    batch_size: u32,
    posting_table: Option<PostingTable>,
}

impl PostingHandler {
    pub fn new(base: String, path: Vec<String>, partition_nums: usize, batch_size: u32, dump_path: Option<String>) -> Self {
        let posting_table = if let Some(p) = dump_path {
            if let Some(t) = deserialize_posting_table(p) {
                return Self {
                    test_case: vec![],
                    partition_nums,
                    batch_size,
                    posting_table: Some(t),
                };
            }
        };
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
        let posting_table = to_batch(ids, words, doc_len, partition_nums, batch_size, false);
    
        Self { 
            test_case,
            partition_nums,
            batch_size,
            posting_table: Some(posting_table),
        }
    }

}

#[async_trait]
impl HandlerT for PostingHandler {
    fn get_words(&self, _num:u32) -> Vec<String>  {
        self.test_case.clone()
    }

    async fn execute(&mut self) ->  Result<u128> {
        rayon::ThreadPoolBuilder::new().num_threads(22).build_global().unwrap();
        let partition_nums = self.partition_nums;
        let ctx = BooleanContext::new();
        let space = self.posting_table.as_ref().unwrap().space_usage();
        ctx.register_index(TableReference::Bare { table: "__table__".into() }, Arc::new(self.posting_table.take().unwrap()))?;
        let table = ctx.index("__table__").await?;
        let mut test_iter = self.test_case.clone().into_iter();
        let _ = AOT_PRIMITIVES.len();
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
            let round = 1;
            for i in 0..round {
                let idx = i * 5;
                // let predicate = BooleanPredicateBuilder::should(&[&keys[idx], &keys[idx + 1]]).unwrap();
                // let predicate1 = BooleanPredicateBuilder::must(&[&keys[idx + 2], &keys[idx + 3], &keys[idx + 4]]).unwrap();
                let predicate = BooleanPredicateBuilder::should(&["hello", "the"]).unwrap();
                let predicate1 = BooleanPredicateBuilder::must(&["it", "must", "to"]).unwrap();
                // let predicate = BooleanPredicateBuilder::should(&["and", "the"]).unwrap();
                let predicate = predicate.with_must(predicate1).unwrap();
                let predicate = predicate.build();
                info!("Predicate{:}: {:?}", i, predicate);
                let index = ctx.boolean("__table__", predicate, false).await.unwrap();
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


fn to_batch(ids: Vec<u32>, words: Vec<String>, length: usize, partition_nums: usize, batch_size: u32, is_serialize: bool) -> PostingTable {
    let _span = span!(Level::INFO, "PostingHanlder to_batch").entered();
    let num_512 = (length as u32 + batch_size - 1) / batch_size;
    let num_512_partition = (num_512 + partition_nums as u32 - 1) / (partition_nums as u32);

    info!("num_512: {}, num_512_partition: {}", num_512, num_512_partition);
    let mut partition_batch = Vec::new();
    let mut term_idx: BTreeMap<String, TermMetaBuilder> = BTreeMap::new();
    for i in 0..partition_nums {
        let mut batches = Vec::new();
        for j in 0..num_512_partition {
            batches.push(PostingBatchBuilder::new((i as u32 * batch_size * num_512_partition + j as u32 * batch_size) as u32));
        }
        partition_batch.push(batches);
    }
    let mut current = (0, 0);
    let mut thredhold = batch_size;
    assert!(ids.is_sorted(), "ids must be sorted");
    assert_eq!(ids.len(), words.len(), "The length of ids and words must be same");
    words
        .into_iter()
        .zip(ids.into_iter())
        .for_each(|(word, id)| {
            let entry = term_idx.entry(word.clone()).or_insert(TermMetaBuilder::new(num_512_partition as usize, partition_nums));
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
            entry.set_true(current.1, current.0, id);
        });
    for (i, p) in partition_batch.iter().enumerate() {
        for (j, pp) in p.into_iter().enumerate() {
            debug!("The ({}, {}) batch len: {}", i, j, pp.doc_len())
        }
    }

    if is_serialize {
        let f = File::create("posting_batch.bin").unwrap();
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &partition_batch).unwrap();

        let f = File::create("term_index.bin").unwrap();
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &term_idx).unwrap();
    }

    let partition_batch = partition_batch
        .into_iter()
        .enumerate()
        .map(|(n, b )| Arc::new(
            b
            .into_iter()
            .enumerate()
            .map(|(i, b)| b.build_with_idx(&mut term_idx, i as u16, n).unwrap() ).collect()))
        .collect();

    let mut keys = Vec::new();
    let mut values = Vec::new();

    term_idx
        .into_iter()
        .for_each(|m| {
            keys.push(m.0); 
            values.push(m.1.build());
        });

    let schema = Schema::new(
        keys.iter().chain([&"__id__".to_string()].into_iter()).map(|v| Field::new(v.to_string(), DataType::Boolean, false)).collect()
    );

    if is_serialize {
        // Serialize Batch Ranges
        let f = File::create("batch_ranges.bin").unwrap();
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &BatchRange::new(0, (num_512_partition * batch_size) as u32)).unwrap();
    }
    #[cfg(feature = "hash_idx")]
    let term_idx = Arc::new(TermIdx { term_map: map });

    #[cfg(all(feature = "trie_idx", not(feature = "hash_idx")))]
    let term_idx = Arc::new(TermIdx::new(keys, values, 20));

    PostingTable::new(
        Arc::new(schema),
        term_idx,
        partition_batch,
        &BatchRange::new(0, (num_512_partition * batch_size) as u32),
    )
}

fn deserialize_posting_table(dump_path: String) -> Option<PostingTable> {
    info!("Deserialize data from {:}", dump_path);
    let path = PathBuf::from(dump_path);
    let posting_batch: Vec<Vec<PostingBatchBuilder>>;
    let batch_range: BatchRange;
    let term_idx: BTreeMap<String, TermMetaBuilder>;
    // posting_batch.bin
    if let Ok(f) = File::open(path.join(PathBuf::from("posting_batch.bin"))) {
        let reader = BufReader::new(f);
        posting_batch = bincode::deserialize_from(reader).unwrap();
    } else {
        return None;
    }
    // batch_range.bin
    if let Ok(f) = File::open(path.join(PathBuf::from("batch_ranges.bin"))) {
        let reader = BufReader::new(f);
        batch_range = bincode::deserialize_from(reader).unwrap();
    } else {
        return None;
    }
    // term_keys.bin
    if let Ok(f) = File::open(path.join(PathBuf::from("term_index.bin"))) {
        let reader = BufReader::new(f);
        term_idx = bincode::deserialize_from(reader).unwrap();
    } else {
        return None;
    }

    let schema = Schema::new(
        term_idx.keys().chain([&"__id__".to_string()].into_iter()).map(|v| Field::new(v.to_string(), DataType::Boolean, false)).collect()
    );
    let partition_batch = posting_batch
        .into_iter()
        .map(|b| Arc::new(
            b
            .into_iter()
            .map(|b| b.build_single().unwrap() ).collect()))
        .collect();

    let mut keys = Vec::new();
    let mut values = Vec::new();

    term_idx
        .into_iter()
        .for_each(|m| {
            keys.push(m.0); 
            values.push(m.1.build());
        });

    let term_idx = Arc::new(TermIdx::new(keys, values, 20));

    Some(PostingTable::new(
        Arc::new(schema),
        term_idx,
        partition_batch,
        &batch_range,
    ))
}