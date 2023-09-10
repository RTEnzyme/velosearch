use std::{path::PathBuf, collections::{BTreeMap, HashMap}, fs::File, io::BufReader, sync::Arc};

use adaptive_hybrid_trie::TermIdx;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use tracing::info;

use crate::{datasources::posting_table::PostingTable, batch::{PostingBatchBuilder, BatchRange, TermMetaBuilder}};


pub fn deserialize_posting_table(dump_path: String) -> Option<PostingTable> {
    info!("Deserialize data from {:}", dump_path);
    let path = PathBuf::from(dump_path);
    let posting_batch: Vec<PostingBatchBuilder>;
    let batch_range: BatchRange;
    let mut term_idx: BTreeMap<String, TermMetaBuilder>;
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

    let (fields_index, fields) = term_idx.keys()
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

    let partition_batch = posting_batch
        .into_iter()
        .enumerate()
        .map(|(i, b)| Arc::new(
            b.build_with_idx(&mut term_idx, i).unwrap()
        ))
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