use std::{sync::{Arc, RwLock}, ops::Index, collections::{HashMap, BTreeMap}, cmp::max, mem::size_of_val, ptr::NonNull};

use datafusion::{arrow::{datatypes::{SchemaRef, Field, DataType, Schema, UInt8Type}, array::{UInt32Array, UInt16Array, ArrayRef, BooleanArray, Array, ArrayData, GenericListArray}, record_batch::RecordBatch, buffer::Buffer}, from_slice::FromSlice, common::TermMeta};
use crate::utils::{Result, FastErr};

/// The doc_id range [start, end) Batch range determines the  of relevant batch.
/// nums32 = (end - start) / 32
#[derive(Clone, Debug, PartialEq)]
pub struct BatchRange {
    start: u32, 
    end: u32,
    nums32: u32,
}

impl BatchRange {
    /// new a BatchRange
    pub fn new(start: u32, end: u32) -> Self {
        Self {
            start,
            end,
            nums32: (end - start + 31) / 32
        }
    }
    
    /// get the `end` of BatchRange
    pub fn end(&self) -> u32 {
        self.end
    }

    /// get the `len` of BatchRange
    pub fn len(&self) -> u32 {
        self.end - self.start
    }
}

pub type PostingList = ArrayRef;
pub type TermSchemaRef = SchemaRef;
pub type BatchFreqs = Vec<Arc<GenericListArray<i32>>>;

/// A batch of Postinglist which contain serveral terms,
/// which is in range[start, end)
#[derive(Clone, Debug, PartialEq)]
pub struct PostingBatch {
    schema: TermSchemaRef,
    postings: Vec<PostingList>,
    term_freqs: Option<BatchFreqs>,
    range: Arc<BatchRange>
}

impl PostingBatch {
    pub fn try_new(
        schema: TermSchemaRef,
        postings: Vec<PostingList>,
        range: Arc<BatchRange>,
    ) -> Result<Self> {
        Self::try_new_impl(schema, postings, None, range)
    }

    pub fn try_new_with_freqs(
        schema: TermSchemaRef,
        postings: Vec<PostingList>,
        term_freqs: BatchFreqs,
        range: Arc<BatchRange>,
    ) -> Result<Self> {
        Self::try_new_impl(schema, postings, Some(term_freqs), range)
    }

    pub fn try_new_impl(
        schema: TermSchemaRef,
        postings: Vec<PostingList>,
        term_freqs: Option<BatchFreqs>,
        range: Arc<BatchRange>,
    ) -> Result<Self> {
        if schema.fields().len() != postings.len() {
            return Err(FastErr::InternalErr(format!(
                "number of columns({}) must match number of fields({}) in schema",
                postings.len(),
                schema.fields().len(),
            )));
        }
        Ok(Self {
            schema, 
            postings,
            term_freqs,
            range
        })  
    }

    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let projected_schema = self.schema.project(indices)?;
        let projected_postings = indices
            .iter()
            .map(|f| {
                self.postings.get(*f).cloned().ok_or_else(|| {
                    FastErr::InternalErr(format!(
                        "project index {} out of bounds, max field {}",
                        f,
                        self.postings.len()
                    ))
                })
            }).collect::<Result<Vec<_>>>()?;
        PostingBatch::try_new_impl(
            SchemaRef::new(projected_schema),
            projected_postings, 
            None,
            self.range.clone()
        )
    }

    pub fn project_adapt(&self, indices: &[Option<usize>], projected_schema: SchemaRef) -> Result<RecordBatch> {
        let mut batches: Vec<ArrayRef> = indices
            .into_iter()
            .map(|v| match v {
                Some(v) => self.postings[*v].clone() as ArrayRef,
                None => Arc::new(UInt16Array::from_slice(&[])),
        })
            .collect();
        batches.insert(projected_schema.index_of("__id__").expect("Should have __id__ field"), Arc::new(UInt32Array::from_slice(&[])));
        
        Ok(RecordBatch::try_new(
            projected_schema,
            batches,
        )?)
    }

    pub fn project_fold(&self, indices: &[Option<usize>], projected_schema: SchemaRef) -> Result<RecordBatch> {
        let bitmask_size: usize = self.range.len() as usize;
        let mut batches: Vec<ArrayRef> = Vec::with_capacity(indices.len());
        for idx in indices {
            // To be optimized, we can convert bitvec to BooleanArray
            let mut bitmap = vec![false; bitmask_size];
            if idx.is_none() {
                batches.push(Arc::new(BooleanArray::from(bitmap)));
                continue;
            }

            // Safety: If idx is none, this loop will continue before this unwrap_unchecked()
            let idx = unsafe{ idx.unwrap_unchecked() };
            let posting = self.postings.get(idx).cloned().ok_or_else(|| {
                FastErr::InternalErr(format!(
                    "project index {} out of bounds, max field {}",
                    idx,
                    self.postings.len()
                ))
            })?;
            match posting.data_type() {
                DataType::Boolean => batches.push(posting.clone()),
                DataType::UInt16 => {
                    posting.as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap()
                    .iter()
                    .for_each(|v| {
                        bitmap[v.unwrap() as usize] = true
                    });
                    batches.push(Arc::new(BooleanArray::from(bitmap)));
                }
                _ => {}
            }
        }
        batches.insert(projected_schema.index_of("__id__").expect("Should have __id__ field"), Arc::new(UInt32Array::from_iter_values((self.range.start).. (self.range.start + 32 * self.range.nums32))));
        Ok(RecordBatch::try_new(projected_schema, batches)?)
    }

    pub fn project_fold_with_freqs(&self, indices: &[Option<usize>], projected_schema: SchemaRef) -> Result<RecordBatch> {
        // Add freqs fields
        let mut fields = projected_schema.fields().clone();
        let mut freqs = fields
            .iter()
            .filter(|v| v.name() != "__id__")
            .map(|v| Field::new(format!("{}_freq", v.name()), DataType::UInt8, true)).collect();
        fields.append(&mut freqs);
        let mut freqs: Vec<ArrayRef> = Vec::with_capacity(indices.len());
        let bitmask_size: usize = self.range.len() as usize;
        let mut batches: Vec<ArrayRef> = Vec::new();
        for idx in indices {
            // Use customized construction for performance
            let mut bitmap = vec![0; 8];
            if idx.is_none() {
                batches.push(build_boolean_array(bitmap, bitmask_size));
                freqs.push(Arc::new(GenericListArray::<i32>::from_iter_primitive::<UInt8Type, _, _>(vec![None as Option<Vec<Option<u8>>>, None, None, None])));
                continue;
            }

            // Safety: If idx is none, this loop will continue before this unwrap_unchecked()
            let idx = unsafe { idx.unwrap_unchecked() };
            let posting = self.postings.get(idx).cloned();
            if let Some(posting) = posting {
                match posting.data_type() {
                    DataType::Boolean => batches.push(posting.clone()),
                    DataType::UInt16 => {
                        posting.as_any()
                        .downcast_ref::<UInt16Array>()
                        .unwrap()
                        .iter()
                        .for_each(|v| {
                            let v = v.unwrap();
                            bitmap[v as usize >> 6] |= 1 << (v % 64) as usize;
                        });
                        batches.push(build_boolean_array(bitmap, bitmask_size));
                    }
                    _ => {}
                }
                // add freqs array
                freqs.push(self.term_freqs.as_ref().unwrap().get(idx).cloned().ok_or_else(|| {
                    FastErr::InternalErr(format!(
                        "freqs index {} out of bounds, term_freq is none: {}",
                        idx,
                        self.term_freqs.is_none(),
                    ))
                })?);
            } else {
                batches.push(build_boolean_array(bitmap, bitmask_size));
                freqs.push(Arc::new(GenericListArray::<i32>::from_iter_primitive::<UInt8Type, _, _>(vec![None as Option<Vec<Option<u8>>>, None, None, None])));
            }
        }
        batches.insert(projected_schema.index_of("__id__").expect("Should have __id__ field"), Arc::new(UInt32Array::from_slice([])));
        batches.extend(freqs.into_iter());
        // Update: Don't add array for __id__
        let projected_schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(
            projected_schema,
            batches,
        )?)
    }

    pub fn space_usage(&self) -> usize {
        let mut space = 0;
        space += size_of_val(self.schema.as_ref());
        space += self.postings
            .iter()
            .map(|v| size_of_val(v.data().buffers()[0].as_slice()))
            .sum::<usize>();
        space
    }

    pub fn schema(&self) -> TermSchemaRef {
        self.schema.clone()
    }

    pub fn range(&self) -> &BatchRange {
        &self.range
    }

    pub fn posting(&self, index: usize) -> &PostingList {
        &self.postings[index]
    }

    pub fn postings(&self) -> &[PostingList] {
        &self.postings[..]
    }

    pub fn posting_by_term(&self, name: &str) -> Option<&PostingList> {
        self.schema()
            .column_with_name(name)
            .map(|(index, _)| &self.postings[index])
    }
}

impl Index<&str> for PostingBatch {
    type Output = PostingList;

    /// Get a reference to a term's posting by name
    /// 
    /// # Panics
    /// 
    /// Panics if the name is not int the schema
    fn index(&self, name: &str) -> &Self::Output {
        self.posting_by_term(name).unwrap()
    }
}

pub struct PostingBatchBuilder {
    start: u32,
    current: u32,
    term_dict: RwLock<HashMap<String, Vec<(u16, u8)>>>,
    term_num: usize,
}

impl PostingBatchBuilder {
    pub fn new(start: u32) -> Self {
        Self { 
            start,
            current: start,
            term_dict: RwLock::new(HashMap::new()),
            term_num: 0,
        }
    }

    pub fn doc_len(&self) -> u32 {
        self.current - self.start
    }

    pub fn push_term(&mut self, term: String, doc_id: u32) -> Result<()> {
        let off = (doc_id - self.start) as u16;
        let entry = self.term_dict
            .get_mut()
            .map_err(|_| FastErr::InternalErr("Can't acquire the RwLock correctly".to_string()))?
            .entry(term)
            .or_insert(vec![(off, 0)]);
        if entry.last().unwrap().0 ==  off {
            unsafe { entry.last_mut().unwrap_unchecked().1 += 1 }
        } else {
            entry.push((off, 1));
        }
        self.current = doc_id;
        self.term_num += 1;
        Ok(())
    }

    pub fn push_term_posting(&mut self, term: String, doc_ids: Vec<(u32, u8)>) -> Result<()> {
        let current = max(self.current, doc_ids[doc_ids.len() - 1].0);
        self.term_dict
            .get_mut()
            .map_err(|_| FastErr::InternalErr(format!("Can't acquire the RwLock correctly")))?
            .entry(term)
            .or_insert(Vec::new())
            .extend(doc_ids.into_iter().map(|v| ((v.0 - self.start) as u16, v.1)));
        self.current = current;
        Ok(())
    }

    pub fn build_single(self) -> Result<PostingBatch> {
        let term_dict = self.term_dict
            .into_inner()
            .expect("Can't acquire the RwLock correctly");
        let mut schema_list = Vec::new();
        let mut postings: Vec<ArrayRef> = Vec::new();
        let mut freqs = Vec::new();
        term_dict
            .into_iter()
            .for_each(|(k, v)| {
                let mut freq: Vec<Option<Vec<Option<u8>>>> = vec![None; 4];
                let mut posting = Vec::with_capacity(v.len());
                let v_len = v.len();
                v.into_iter()
                .for_each(|(p, f)| {
                    posting.push(p);
                    let idx=  f / 64 % 8;
                    match freq[idx as usize] {
                        Some(ref mut v) => {
                            v.push(Some(f));
                        }
                        None => {
                            freq[idx as usize] = Some(vec![Some(f)]);
                        }
                    }
                });
                freqs.push(Arc::new(GenericListArray::<i32>::from_iter_primitive::<UInt8Type, _, _>(freq)));
                if v_len > 32 {
                    let mut bitmap = vec![false; 512];
                    for i in posting {
                        bitmap[i as usize] = true;
                    }
                    schema_list.push(Field::new(k, DataType::Boolean, false));
                    postings.push(Arc::new(BooleanArray::from(bitmap)));
                } else {
                    schema_list.push(Field::new(k, DataType::UInt32, false));
                    postings.push(Arc::new(UInt16Array::from(posting)));
                }
            });
        schema_list.push(Field::new("__id__", DataType::UInt32, false));
        postings.push(Arc::new(UInt16Array::from_slice(&[])));
       PostingBatch::try_new_with_freqs(
            Arc::new(Schema::new(schema_list)),
            postings,
            freqs,
            Arc::new(BatchRange::new(self.start, self.current + 1)))
    }

    pub fn build_with_idx(self, idx: &mut BTreeMap<String, TermMetaBuilder>, batch_idx: u16, partition_num: usize) -> Result<PostingBatch> {
        let term_dict = self.term_dict
            .into_inner()
            .expect("Can't acquire the RwLock correctly");
        let mut schema_list = Vec::new();
        let mut postings: Vec<ArrayRef> = Vec::new();
        let mut freqs = Vec::new();
        term_dict
            .into_iter()
            .enumerate()
            .for_each(|(i, (k, v))| {
                idx.get_mut(&k).unwrap().add_idx((batch_idx, i as u16), partition_num);
                let mut freq: Vec<Option<Vec<Option<u8>>>> = vec![None; 4];
                let mut posting = Vec::with_capacity(v.len());
                let v_len = v.len();
                v.into_iter()
                .for_each(|(p, f)| {
                    posting.push(p);
                    let idx=  f / 64 % 8;
                    match freq[idx as usize] {
                        Some(ref mut v) => {
                            v.push(Some(f));
                        }
                        None => {
                            freq[idx as usize] = Some(vec![Some(f)]);
                        }
                    }
                });
                freqs.push(Arc::new(GenericListArray::<i32>::from_iter_primitive::<UInt8Type, _, _>(freq)));
                if v_len > 32 {
                    let mut bitmap = vec![false; 512];
                    for i in posting {
                        bitmap[i as usize] = true;
                    }
                    schema_list.push(Field::new(k, DataType::Boolean, false));
                    postings.push(Arc::new(BooleanArray::from(bitmap)));
                } else {
                    schema_list.push(Field::new(k, DataType::UInt32, false));
                    postings.push(Arc::new(UInt16Array::from(posting)));
                }
            });
        schema_list.push(Field::new("__id__", DataType::UInt32, false));
        postings.push(Arc::new(UInt16Array::from_slice(&[])));
        PostingBatch::try_new_with_freqs(
            Arc::new(Schema::new(schema_list)),
            postings,
            freqs,
            Arc::new(BatchRange::new(self.start, self.current + 1))
        )
    }
}

#[derive(Clone)]
pub struct TermMetaBuilder {
    distribution: Vec<Vec<bool>>,
    nums: Vec<u32>,
    idx: Vec<Vec<Option<u16>>>,
    partition_num: usize,
}

impl TermMetaBuilder {
    pub fn new(batch_num: usize, partition_num: usize) -> Self {
        Self {
            distribution: vec![vec![false; batch_num]; partition_num],
            nums: vec![0; partition_num],
            idx: vec![vec![None; batch_num]; partition_num],
            partition_num,
        }
    }

    pub fn set_true(&mut self, i: usize, partition_num: usize) {
        if self.distribution[partition_num][i] {
            return;
        }
        self.nums[partition_num] += 1;
        self.distribution[partition_num][i] = true;
    }

    pub fn add_idx(&mut self, idx: (u16, u16), partition_num: usize) {
        self.idx[partition_num][idx.0 as usize] = Some(idx.1);
    }


    pub fn build(self) -> TermMeta {
        let (distribution, index): (Vec<Option<Arc<BooleanArray>>>, _)= (0..self.partition_num)
            .into_iter()
            .map(|v| {
                if self.nums[v] == 0 {
                    (None, None)
                } else {
                    let distribution = Arc::new(BooleanArray::from_slice(&self.distribution[v]));
                    let index = Arc::new(UInt16Array::from(self.idx[v].clone()));
                    (Some(distribution), Some(index))
                }
            })
            .unzip();
        let valid_batch_num: usize = distribution.iter()
            .map(|d| match d {
                Some(v) => v.true_count(),
                None => 0,
            })
            .sum();
        let sel = self.nums.iter().map(|v| *v).sum::<u32>() as f64 / (512. * valid_batch_num as f64);
        TermMeta {
            distribution: Arc::new(distribution),
            index: Arc::new(index),
            nums: self.nums,
            selectivity: sel,
        }
    }
}

fn build_boolean_array(mut data: Vec<u64>, batch_len: usize) -> ArrayRef {
    let value_buffer = unsafe {
        let buf = Buffer::from_raw_parts(NonNull::new_unchecked(data.as_mut_ptr() as *mut u8), batch_len, batch_len);
        std::mem::forget(data);
        buf
    };
    let builder = ArrayData::builder(DataType::Boolean)
        .len(batch_len)
        .add_buffer(value_buffer);

    let array_data = unsafe { builder.build_unchecked() };
    Arc::new(BooleanArray::from(array_data))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{arrow::{datatypes::{Schema, Field, DataType, UInt8Type}, array::{UInt16Array, BooleanArray, GenericListArray, ArrayRef}}, from_slice::FromSlice};

    use super::{BatchRange, PostingBatch};

    fn build_batch() -> PostingBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test1", DataType::Boolean, true),
            Field::new("test2", DataType::Boolean, true),
            Field::new("test3", DataType::Boolean, true),
            Field::new("test4", DataType::Boolean, true),
            Field::new("__id__", DataType::UInt32, false),
        ]));
        let range = Arc::new(BatchRange {
            start: 0,
            end: 64,
            nums32: 1
        });
        let postings: Vec<ArrayRef> = vec![
           Arc::new(UInt16Array::from_slice([1, 6, 9])),
           Arc::new(UInt16Array::from_slice([0, 4, 16])),
           Arc::new(UInt16Array::from_slice([4, 6, 8])),
           Arc::new(UInt16Array::from_slice([6, 16, 31])),
           Arc::new(UInt16Array::from_slice([])),
        ];
        PostingBatch::try_new(schema, postings, range).unwrap()
    }

    fn build_batch_with_freqs() -> PostingBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test1", DataType::Boolean, true),
            Field::new("test2", DataType::Boolean, true),
            Field::new("test3", DataType::Boolean, true),
            Field::new("test4", DataType::Boolean, true),
            Field::new("__id__", DataType::UInt32, false),
        ]));
        let range = Arc::new(BatchRange {
            start: 0,
            end: 64,
            nums32: 1
        });
        let postings: Vec<ArrayRef> = vec![
           Arc::new(UInt16Array::from_slice([1, 6, 9])),
           Arc::new(UInt16Array::from_slice([0, 4, 16])),
           Arc::new(UInt16Array::from_slice([4, 6, 8])),
           Arc::new(UInt16Array::from_slice([6, 16, 31])),
           Arc::new(UInt16Array::from_slice([])),
        ];
        let freqs = vec![
            Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                Some(vec![Some(1 as u8), Some(2), Some(3)]),
                None,
                None,
                Some(vec![Some(22), Some(2), Some(2)]),
            ])),
            Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                Some(vec![Some(2), Some(1)]),
                None,
                Some(vec![Some(1), Some(2)]),
                None,
            ])),
            Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                Some(vec![Some(3), Some(1), Some(1)]),
                None,
                None,
                None,
            ])),
            Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                None,
                Some(vec![Some(1), Some(2), Some(2)]),
                None,
                None,
            ])),
            Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
                None as Option<Vec<Option<u8>>>,
                None,
                None,
                None,
            ])),
        ];
        PostingBatch::try_new_with_freqs(schema, postings, freqs, range).unwrap()
    }

    #[test]
    fn postingbatch_project_fold() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test1", DataType::Boolean, false),
            Field::new("test2", DataType::Boolean, false),
            Field::new("test3", DataType::Boolean, false),
            Field::new("test4", DataType::Boolean, false),
            Field::new("__id__", DataType::UInt32, false),
        ]));
        let batch = build_batch();
        let res = batch.project_fold(&[Some(1), Some(2)], Arc::new(schema.clone().project(&[1, 2, 4]).unwrap())).unwrap();
        let mut exptected1 = vec![false; 64];
        exptected1[0] = true;
        exptected1[4] = true;
        exptected1[16] = true;
        let exptected1 = BooleanArray::from(exptected1);
        let mut exptected2 = vec![false; 64];
        exptected2[4] = true;
        exptected2[6] = true;
        exptected2[8] = true;
        let exptected2 = BooleanArray::from(exptected2);

        assert_eq!(res.column(0).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected1);
        assert_eq!(res.column(1).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected2);
    }

    #[test]
    fn postingbatch_project_fold_with_freqs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test1", DataType::Boolean, false),
            Field::new("test2", DataType::Boolean, false),
            Field::new("test3", DataType::Boolean, false),
            Field::new("test4", DataType::Boolean, false),
            Field::new("__id__", DataType::UInt32, false),
        ]));
        let batch = build_batch_with_freqs();
        let res = batch.project_fold_with_freqs(&[Some(1), Some(2)], Arc::new(schema.clone().project(&[1, 2, 4]).unwrap())).unwrap();
        let mut exptected1 = vec![false; 64];
        exptected1[0] = true;
        exptected1[4] = true;
        exptected1[16] = true;
        let exptected1 = BooleanArray::from(exptected1);
        let mut exptected2 = vec![false; 64];
        exptected2[4] = true;
        exptected2[6] = true;
        exptected2[8] = true;
        let exptected2 = BooleanArray::from(exptected2);
        println!("buffer: {:?}", res.column(0).data().buffers()[0].as_slice());
        println!("res: {:?}", res);
        assert_eq!(res.column(0).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected1);
        assert_eq!(res.column(1).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected2);
    }
}