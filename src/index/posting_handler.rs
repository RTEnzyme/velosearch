use std::{collections::{HashSet, BTreeMap, HashMap}, sync::Arc, fs::File, io::BufWriter, path::PathBuf, cell::RefCell};

use async_trait::async_trait;
use datafusion::{sql::TableReference, arrow::datatypes::{Schema, DataType, Field}, datasource::provider_as_source, common::cast::as_int64_array};
use adaptive_hybrid_trie::TermIdx;
use rand::{thread_rng, seq::IteratorRandom};
use tantivy::tokenizer::{TextAnalyzer, SimpleTokenizer, RemoveLongFilter, LowerCaser, Stemmer};
use tokio::time::Instant;
use tracing::{info, span, Level, debug};

use crate::{utils::{json::{parse_wiki_dir, WikiItem}, builder::{deserialize_posting_table, serialize_term_meta}}, Result, batch::{PostingBatchBuilder, BatchRange, TermMetaBuilder, PostingBatch}, datasources::posting_table::PostingTable, BooleanContext, query::boolean_query::BooleanPredicateBuilder, jit::AOT_PRIMITIVES};

use super::HandlerT;

pub struct PostingHandler {
    test_case: Vec<String>,
    partition_nums: usize,
    posting_table: Option<PostingTable>,
    tokenizer: TextAnalyzer,
}

impl PostingHandler {
    pub fn new(base: String, path: Vec<String>, partition_nums: usize, batch_size: u32, dump_path: Option<String>) -> Self {
        let tokenizer = TextAnalyzer::from(SimpleTokenizer)
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser);
        if let Some(p) = dump_path.clone() {
            if let Some(t) = deserialize_posting_table(p) {
                return Self {
                    test_case: vec![],
                    partition_nums,
                    posting_table: Some(t),
                    tokenizer,
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

        items
        .into_iter()
        .for_each(|e| {
            let WikiItem {id: _, text: w} = e;

            let mut stream = tokenizer.token_stream(w.as_str());
            stream.process(&mut |token| {
                ids.push(cnt);
                words.push(token.text.clone());
            });
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
        let posting_table = to_batch(ids, words, doc_len, partition_nums, batch_size, dump_path);
    
        Self { 
            test_case,
            partition_nums,
            posting_table: Some(posting_table),
            tokenizer,
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
            let round = 15;
            let provider = ctx.index_provider("__table__").await?;
            let schema = &provider.schema();
            let table_source = provider_as_source(Arc::clone(&provider));
            for i in 0..round {
                let idx = i * 5;
                // let predicate = BooleanPredicateBuilder::should(&[&keys[idx], &keys[idx + 1]]).unwrap();
                // let predicate1 = BooleanPredicateBuilder::must(&[&keys[idx + 2], &keys[idx + 3], &keys[idx + 4]]).unwrap();
                // let predicate = BooleanPredicateBuilder::should(&["hello", "the"]).unwrap();
                // let predicate = BooleanPredicateBuilder::must(&["hot", "spring", "south", "dakota"]).unwrap();
                // let predicate = BooleanPredicateBuilder::must(&["civil", "war", "battlefield"]).unwrap();
                let timer = Instant::now();
                let predicates = ["ace", "frehley"];
                let predicate: Vec<String> = predicates
                    .into_iter()
                    .map(|w| {
                        self.tokenizer.token_stream(w).next().unwrap().text.clone()
                    })
                    .collect();
                let predicate = BooleanPredicateBuilder::should(&predicate.iter().map(|w| w.as_str()).collect::<Vec<_>>()).unwrap();
                // let predicate = BooleanPredicateBuilder::should(&["and", "the"]).unwrap();
                // let predicate = predicate.with_must(predicate1).unwrap();
                let predicate = predicate.build();
                info!("Predicate{:}: {:?}", i, predicate);
                let index = ctx.boolean_with_provider(table_source.clone(), &schema, predicate, false).await.unwrap();
                let res = index
                    // .explain(false, true).unwrap()
                    // .show().await.unwrap();
                    .count_agg().unwrap()
                    .collect().await.unwrap();
                let time = timer.elapsed().as_micros();
                info!("res: {:?}", as_int64_array(res[0].column(0)).unwrap().value(0));
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


fn to_batch(ids: Vec<u32>, words: Vec<String>, length: usize, partition_nums: usize, batch_size: u32, dump_path: Option<String>) -> PostingTable {
    let _span = span!(Level::INFO, "PostingHanlder to_batch").entered();
    let num_512 = (length as u32 + batch_size - 1) / batch_size;
    let num_512_partition = (num_512 + partition_nums as u32 - 1) / (partition_nums as u32);

    info!("num_512: {}, num_512_partition: {}", num_512, num_512_partition);
    let mut partition_batch = Vec::new();
    let mut term_idx: BTreeMap<String, TermMetaBuilder> = BTreeMap::new();
    for _ in 0..partition_nums {
        partition_batch.push(PostingBatchBuilder::new());
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
            partition_batch[current.0].push_term(word, id).expect("Shoud push term correctly");
            entry.set_true(current.1, current.0, id);
        });

    for (i, p) in partition_batch.iter().enumerate() {
        debug!("The partition {} batch len: {}", i, p.doc_len())
    }

    if let Some(ref p) = dump_path {
        let path = PathBuf::from(p);
        let f = File::create(path.join(PathBuf::from("posting_batch.bin"))).unwrap();
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &partition_batch).unwrap();
    }

    let term_idx = RefCell::new(term_idx);

    let partition_batch: Vec<Arc<PostingBatch>> = partition_batch
        .into_iter()
        .enumerate()
        .map(|(n, b )| {
            Arc::new(b.build_with_idx(Some(&term_idx), n).unwrap())
        })
        .collect();
    let term_idx = term_idx.into_inner();

    let mut keys = Vec::new();
    let mut values = Vec::new();

    term_idx
        .into_iter()
        .for_each(|m| {
            keys.push(m.0); 
            values.push(m.1.build());
        });
    let (fields_index, fields) = keys.iter()
        .chain([&"__id__".to_string()].into_iter())
        .enumerate()
        .map(|(i, v)| {
            let idx = (v.to_string(), i);
            let field = Field::new(v.to_string(), DataType::Boolean, false);
            (idx, field)
        })
        .unzip();
    let schema = Schema {
        fields,
        metadata: HashMap::new(),
        fields_index: Some(fields_index),
    };

    if let Some(ref p) = dump_path {
        // Serialize Batch Ranges
        let path = PathBuf::from(p);

        let f = File::create(path.join(PathBuf::from("batch_ranges.bin"))).unwrap();
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &BatchRange::new(0, (num_512_partition * batch_size) as u32)).unwrap();

        let f = File::create(path.join(PathBuf::from("term_keys.bin"))).unwrap();
        let writer = BufWriter::new(f);
        bincode::serialize_into(writer, &keys).unwrap();

        serialize_term_meta(&values, p.to_string());
    }
    assert_eq!(keys.len(), values.len());
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

