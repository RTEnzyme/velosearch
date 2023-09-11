use std::{path::PathBuf, collections::{BTreeMap, HashMap}, fs::File, io::{BufReader, BufWriter}, sync::Arc};

use adaptive_hybrid_trie::TermIdx;
use datafusion::{arrow::{datatypes::{Schema, Field, DataType}, array::BooleanArray}, common::TermMeta};
use tracing::info;

use crate::{datasources::posting_table::PostingTable, batch::{PostingBatchBuilder, BatchRange, TermMetaBuilder}, utils::array::build_boolean_array};

#[derive(serde::Serialize, serde::Deserialize)]
struct TermMetaTemp {
    /// Which horizantal partition batches has this Term
    pub distribution: Vec<Vec<u8>>,
    /// Witch Batch has this Term
    pub index: Arc<Vec<Option<u32>>>,
    /// The number of this Term
    pub nums: Vec<u32>,
    /// Selectivity
    pub selectivity: f64,
}

pub fn serialize_term_meta(term_meta: &Vec<TermMeta>, dump_path: String) {
    let path = PathBuf::from(dump_path);
    let f = File::create(path.join(PathBuf::from("term_values.bin"))).unwrap();
    let writer = BufWriter::new(f);
    let term_metas = term_meta
        .iter()
        .map(|v| {
            let distribution: Vec<Vec<u8>> = v.distribution
                .as_ref()
                .iter()
                .map(|v| {
                    v.values().to_vec()
                })
                .collect();
            TermMetaTemp {
                distribution,
                index: v.index.clone(),
                nums: v.nums.clone(),
                selectivity: v.selectivity.clone(),
            }
        })
        .collect();
    bincode::serialize_into::<_, Vec<TermMetaTemp>>(writer, &term_metas).unwrap();
}

pub fn deserialize_posting_table(dump_path: String) -> Option<PostingTable> {
    info!("Deserialize data from {:}", dump_path);
    let path = PathBuf::from(dump_path);
    let posting_batch: Vec<PostingBatchBuilder>;
    let batch_range: BatchRange;
    let keys: Vec<String>;
    let values: Vec<TermMetaTemp>;
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
    if let Ok(f) = File::open(path.join(PathBuf::from("term_keys.bin"))) {
        let reader = BufReader::new(f);
        keys = bincode::deserialize_from(reader).unwrap();
    } else {
        return None;
    }
    // term_values.bin
    if let Ok(f) = File::open(path.join(PathBuf::from("term_values.bin"))) {
        let reader = BufReader::new(f);
        values = bincode::deserialize_from(reader).unwrap();
    } else {
        return None;
    }

    let (fields_index, fields) = keys
        .iter()
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
        .map(|b| Arc::new(
            b.build().unwrap()
        ))
        .collect();

    let values = values
        .into_iter()
        .map(|v| {
            let distris = v.distribution
                .into_iter()
                .map(|v| {
                    let array_len = v.len() * 8;
                    Arc::new(build_boolean_array(v, array_len))
                })
                .collect();
            TermMeta {
                distribution: Arc::new(distris),
                index: v.index,
                nums: v.nums,
                selectivity: v.selectivity,
            }
        })
        .collect();

    let term_idx = Arc::new(TermIdx::new(keys, values, 20));

    Some(PostingTable::new(
        Arc::new(schema),
        term_idx,
        partition_batch,
        &batch_range,
    ))
}