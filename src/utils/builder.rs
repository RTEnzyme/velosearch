use std::{path::PathBuf, collections::HashMap, fs::File, io::{BufReader, BufWriter}, sync::Arc};

use adaptive_hybrid_trie::TermIdx;
use datafusion::{arrow::{datatypes::{Schema, Field, DataType, Int64Type, Int16Type}, array::{UInt64Array, PrimitiveRunBuilder, Int64RunArray, Array, Int16RunArray}}, common::TermMeta};
use tracing::{info, debug};

use crate::{datasources::posting_table::PostingTable, batch::{PostingBatchBuilder, BatchRange}};

#[derive(serde::Serialize, serde::Deserialize)]
struct TermMetaTemp {
    /// Which horizantal partition batches has this Term
    pub distribution: Vec<Vec<u64>>,
    /// Witch Batch has this Term
    pub index: Arc<Vec<Option<u32>>>,
    /// The number of this Term
    pub nums: Vec<u32>,
    /// Selectivity
    pub selectivity: f64,
}

impl TermMetaTemp {
    pub fn rle_usage(&self) -> usize {
        let mut real = 1;
        let mut run = 1;
        let mut cur = 0;
        for &i in &self.distribution[0] {
            if i != cur {
                real += 1;
                run += 1;
                cur = i;
            }
        }
        run * 2 + real * 8
    }
}

pub fn serialize_term_meta(term_meta: &Vec<TermMeta>, dump_path: String) {
    let path = PathBuf::from(dump_path);
    let f = File::create(path.join(PathBuf::from("term_values.bin"))).unwrap();
    let writer = BufWriter::new(f);
    let term_metas: Vec<TermMetaTemp> = term_meta
        .iter()
        .map(|v| {
            let distribution: Vec<Vec<u64>> = v.distribution
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
    let consumption: usize = term_metas.iter().map(|v| v.rle_usage()).sum();
    info!("terms len: {:}", term_metas.len());
    info!("Compressed index consumption: {:}", consumption);
    bincode::serialize_into::<_, Vec<TermMetaTemp>>(writer, &term_metas).unwrap();
}

pub fn deserialize_posting_table(dump_path: String, partitions_num: usize) -> Option<PostingTable> {
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

    let mut memory_consume = 0;
    let mut compressed_consume = 0;
    let values: Vec<TermMeta> = values
        .into_iter()
        .map(|v| {
            compressed_consume += v.rle_usage();
            let distris = v.distribution
                .into_iter()
                .map(|v| {
                    Arc::new(UInt64Array::from(v))
                })
                .collect();
            let termmeta = TermMeta {
                distribution: Arc::new(distris),
                index: v.index,
                nums: v.nums,
                selectivity: v.selectivity,
            };
            memory_consume += termmeta.memory_consumption();
            termmeta
        })
        .collect();
    info!("term len: {:}", values.len());
    info!("term index: {:}", memory_consume);
    info!("compreed index: {:}", compressed_consume);
    let term_idx = Arc::new(TermIdx::new(keys, values, 20));

    Some(PostingTable::new(
        Arc::new(schema),
        term_idx,
        partition_batch,
        &batch_range,
        partitions_num,
    ))
}