use std::{sync::Arc, ops::Index, collections::{HashMap, BTreeMap}, cmp::max, mem::size_of_val, ptr::NonNull, cell::RefCell, arch::x86_64::{_pext_u64, _blsr_u64}};

use datafusion::{arrow::{datatypes::{SchemaRef, Field, DataType, Schema, UInt8Type}, array::{UInt32Array, UInt16Array, ArrayRef, BooleanArray, Array, ArrayData, GenericListArray, GenericBinaryArray, GenericBinaryBuilder}, record_batch::{RecordBatch, RecordBatchOptions}, buffer::Buffer}, from_slice::FromSlice, common::TermMeta};
use serde::{Serialize, Deserialize};
use tracing::debug;
use crate::utils::{Result, FastErr};


/// The doc_id range [start, end) Batch range determines the  of relevant batch.
/// nums32 = (end - start) / 32
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

pub type PostingList = GenericBinaryArray<i32>;
pub type TermSchemaRef = SchemaRef;
pub type BatchFreqs = Vec<Arc<GenericListArray<i32>>>;

/// A batch of Postinglist which contain serveral terms,
/// which is in range[start, end)
#[derive(Clone, Debug, PartialEq)]
pub struct PostingBatch {
    schema: TermSchemaRef,
    postings: Vec<Arc<PostingList>>,
    term_freqs: Option<BatchFreqs>,
    boundary: Vec<Vec<u16>>,
    range: Arc<BatchRange>
}

impl PostingBatch {
    pub fn try_new(
        schema: TermSchemaRef,
        postings: Vec<Arc<PostingList>>,
        boundary: Vec<Vec<u16>>,
        range: Arc<BatchRange>,
    ) -> Result<Self> {
        Self::try_new_impl(schema, postings, None, boundary, range)
    }

    pub fn try_new_with_freqs(
        schema: TermSchemaRef,
        postings: Vec<Arc<PostingList>>,
        term_freqs: BatchFreqs,
        boundary: Vec<Vec<u16>>,
        range: Arc<BatchRange>,
    ) -> Result<Self> {
        Self::try_new_impl(schema, postings, Some(term_freqs), boundary, range)
    }

    pub fn try_new_impl(
        schema: TermSchemaRef,
        postings: Vec<Arc<PostingList>>,
        term_freqs: Option<BatchFreqs>,
        boundary: Vec<Vec<u16>>,
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
            boundary,
            range
        })  
    }

    /// Return the length of this partition
    pub fn batch_len(&self) -> usize {
        self.boundary.len()
    }

    pub fn project_fold(
        &self,
        indices: &[Option<u32>],
        projected_schema: SchemaRef,
        distris: &[Option<u64>],
        boundary_idx: usize,
        min_range: u64,
    ) -> Result<RecordBatch> {
        debug!("project_fold");
        const CHUNK_SIZE: usize = 64;
        let valid_batch_num = min_range.count_ones() as usize;
        let mut batches: Vec<ArrayRef> = Vec::with_capacity(indices.len());
        for (idx, distri) in indices.into_iter().zip(distris.into_iter()) {
            // To be optimized, we can convert bitvec to BooleanArray
            if idx.is_none() {
                batches.push(Arc::new(BooleanArray::from(vec![] as Vec<bool>)));
                continue;
            }

            // Safety: If idx is none, this loop will continue before this unwrap_unchecked()
            let idx = unsafe{ idx.unwrap_unchecked() as usize };
            let distri = unsafe { distri.unwrap_unchecked() };

            let posting = self.postings.get(idx).cloned().ok_or_else(|| {
                FastErr::InternalErr(format!(
                    "project index {} out of bounds, max field {}",
                    idx,
                    self.postings.len()
                ))
            })?;

            let boundary = &self.boundary[idx];

            let boundary_len = if boundary_idx < boundary.len() - 1 {
                (boundary[boundary_idx + 1] - boundary[boundary_idx] + 1) as usize
            } else if  boundary_idx == boundary.len() - 1 {
                posting.len() - boundary[boundary_idx] as usize - 1
            } else {
                batches.push(Arc::new(BooleanArray::from(vec![] as Vec<bool>)));
                continue;
            };

            let mut valid_batch: Vec<[u8; 64]> =vec![[0; 64]; valid_batch_num];
            let mut write_mask = unsafe { _pext_u64(distri, min_range) };

            let mut cnt = 0;
            let mut write_pos = write_mask.trailing_zeros() as usize;
            write_mask = clear_lowest_set_bit(write_mask);
            for i in 0..boundary_len.min(64) {
                if distri & 1 << i == 0 {
                    continue
                }
                let batch = posting.value(cnt);


                if batch.len() >= 16 {
                    valid_batch[write_pos] = unsafe {*(batch.as_ptr() as *const [u8; 64])};
                } else {
                    // means this's Integer list
                    let batch = unsafe {posting.value(cnt).align_to::<u16>().1 };
                    //  means this's bitmap
                    let mut bitmap = [0; CHUNK_SIZE];
                    for off in batch {
                        bitmap[(*off >> 8) as usize] |= 1 << (*off % (1 << 8));
                    }
                    valid_batch[write_pos] = bitmap;
                }
                cnt += 1;
                write_pos = write_mask.trailing_ones() as usize;
                write_mask = clear_lowest_set_bit(write_mask);
            }
            let batch = build_boolean_array_u8(valid_batch.into_flattened(), 512 * valid_batch_num);
            batches.push(batch);
        }
        batches.insert(projected_schema.index_of("__id__").expect("Should have __id__ field"), Arc::new(UInt32Array::from_iter_values((self.range.start).. (self.range.start + 32 * self.range.nums32))));
        debug!("end of project fold");
        let option = RecordBatchOptions::new().with_row_count(Some(64 * 512));
        Ok(RecordBatch::try_new_with_options(projected_schema, batches, &option)?)
    }

    pub fn project_fold_with_freqs(
        &self,
        indices: &[Option<usize>],
        projected_schema: SchemaRef,
        distris: &[Option<u64>],
        boundary_idx: usize,
        min_range: u64,
    ) -> Result<RecordBatch> {
        debug!("project_fold_with_freqs");
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

    pub fn posting_by_term(&self, name: &str) -> Option<&PostingList> {
        self.schema()
            .column_with_name(name)
            .map(|(index, _)| self.postings[index].as_ref())
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

#[derive(Serialize, Deserialize)]
pub struct PostingBatchBuilder {
    current: u32,
    term_dict: RefCell<HashMap<String, Vec<(u32, u8)>>>,
    term_num: usize,
}

impl PostingBatchBuilder {
    pub fn new() -> Self {
        Self { 
            current: 0,
            term_dict: RefCell::new(HashMap::new()),
            term_num: 0,
        }
    }

    pub fn doc_len(&self) -> usize {
        self.current as usize
    }

    pub fn push_term(&mut self, term: String, doc_id: u32) -> Result<()> {
        let off = doc_id;
        let entry = self.term_dict
            .get_mut()
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

    pub fn build_with_idx(self, idx: &mut BTreeMap<String, TermMetaBuilder>, partition_num: usize) -> Result<PostingBatch> {
        let term_dict = self.term_dict
            .into_inner();
        let mut schema_list = Vec::new();
        let mut postings: Vec<Arc<GenericBinaryArray<i32>>> = Vec::new();
        let mut freqs = Vec::new();
        let mut boundarys = Vec::new();
        term_dict
            .into_iter()
            .enumerate()
            .for_each(|(i, (k, v))| {
                let mut boundary = vec![0];
                let mut cnter = 0;
                let mut posting_builder = GenericBinaryBuilder::new();
                let mut builder_len = 0;
                let mut batch_num = 0;

                idx.get_mut(&k).unwrap().add_idx(i as u32, partition_num);
                let mut freq: Vec<Option<Vec<Option<u8>>>> = vec![None; 4];
                let mut buffer = Vec::new();
                v.into_iter()
                .for_each(|(p, f)| {
                    if p - cnter < 512 {
                        buffer.push(p);
                    } else {
                        let base = cnter;
                        let skip_num = (p - cnter + 1) / 512;
                        cnter += skip_num * 512;
                        batch_num += skip_num;
                        if batch_num % 64 == 0 {
                            boundary.push(builder_len);
                        }
                        
                        if buffer.len() > 16 {
                            let mut bitmap = vec![0; 64];
                            for i in &buffer {
                                let off = *i - base;
                                bitmap[(off >> 3) as usize] |= 1 << (off % (1 << 8));
                            }
                            posting_builder.append_value(bitmap);
                        } else {
                            posting_builder.append_value(unsafe {buffer.as_slice().align_to::<u8>().1 });
                        }
                        builder_len += 1;
                        buffer.clear();
                    }

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
                schema_list.push(Field::new(k.clone(), DataType::UInt32, false));
                postings.push(Arc::new(posting_builder.finish()));
                boundarys.push(boundary);
            });
        schema_list.push(Field::new("__id__", DataType::UInt32, false));
        postings.push(Arc::new(GenericBinaryArray::<i32>::from(vec![] as Vec<&[u8]>)));
        PostingBatch::try_new_with_freqs(
            Arc::new(Schema::new(schema_list)),
            postings,
            freqs,
            boundarys,
            Arc::new(BatchRange::new(0, 512))
        )
    }
}

#[inline]
fn clear_lowest_set_bit(v: u64) -> u64 {
    unsafe { _blsr_u64(v) }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TermMetaBuilder {
    distribution: Vec<Vec<bool>>,
    nums: Vec<u32>,
    idx: Vec<Option<u32>>,
    partition_num: usize,
    bounder: Option<u32>,
}

impl TermMetaBuilder {
    pub fn new(batch_num: usize, partition_num: usize) -> Self {
        Self {
            distribution: vec![vec![false; batch_num]; partition_num],
            nums: vec![0; partition_num],
            idx: vec![None; partition_num],
            partition_num,
            bounder: None,
        }
    }

    pub fn set_true(&mut self, i: usize, partition_num: usize, id: u32) {
        if self.bounder.is_none() || id > self.bounder.unwrap() {
            self.nums[partition_num] += 1;
            self.bounder = Some(id);
        }
        if self.distribution[partition_num][i] {
            return;
        }
        self.distribution[partition_num][i] = true;
    }

    pub fn add_idx(&mut self, idx: u32, partition_num: usize) {
        self.idx[partition_num] = Some(idx);
    }


    pub fn build(self) -> TermMeta {
        let (distribution, index): (Vec<Arc<BooleanArray>>, _)= (0..self.partition_num)
            .into_iter()
            .map(|v| {
                if self.nums[v] == 0 {
                    (Arc::new(BooleanArray::from(vec![false; self.distribution[0].len()])), None)
                } else {
                    let distribution = Arc::new(BooleanArray::from_slice(&self.distribution[v]));
                    let index = self.idx[v].clone();
                    (distribution, index)
                }
            })
            .unzip();
        let valid_batch_num: usize = distribution.iter()
            .map(|d| d.true_count() )
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

fn build_boolean_array_u8(mut data: Vec<u8>, batch_len: usize) -> ArrayRef {
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
    // use std::sync::Arc;

    // use datafusion::{arrow::{datatypes::{Schema, Field, DataType, UInt8Type}, array::{UInt16Array, BooleanArray, GenericListArray, ArrayRef}}, from_slice::FromSlice};

    // use super::{BatchRange, PostingBatch};

    // fn build_batch() -> PostingBatch {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("test1", DataType::Boolean, true),
    //         Field::new("test2", DataType::Boolean, true),
    //         Field::new("test3", DataType::Boolean, true),
    //         Field::new("test4", DataType::Boolean, true),
    //         Field::new("__id__", DataType::UInt32, false),
    //     ]));
    //     let range = Arc::new(BatchRange {
    //         start: 0,
    //         end: 64,
    //         nums32: 1
    //     });
    //     let postings: Vec<ArrayRef> = vec![
    //        Arc::new(UInt16Array::from_slice([1, 6, 9])),
    //        Arc::new(UInt16Array::from_slice([0, 4, 16])),
    //        Arc::new(UInt16Array::from_slice([4, 6, 8])),
    //        Arc::new(UInt16Array::from_slice([6, 16, 31])),
    //        Arc::new(UInt16Array::from_slice([])),
    //     ];
    //     PostingBatch::try_new(schema, postings, range).unwrap()
    // }

    // fn build_batch_with_freqs() -> PostingBatch {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("test1", DataType::Boolean, true),
    //         Field::new("test2", DataType::Boolean, true),
    //         Field::new("test3", DataType::Boolean, true),
    //         Field::new("test4", DataType::Boolean, true),
    //         Field::new("__id__", DataType::UInt32, false),
    //     ]));
    //     let range = Arc::new(BatchRange {
    //         start: 0,
    //         end: 64,
    //         nums32: 1
    //     });
    //     let postings: Vec<ArrayRef> = vec![
    //        Arc::new(UInt16Array::from_slice([1, 6, 9])),
    //        Arc::new(UInt16Array::from_slice([0, 4, 16])),
    //        Arc::new(UInt16Array::from_slice([4, 6, 8])),
    //        Arc::new(UInt16Array::from_slice([6, 16, 31])),
    //        Arc::new(UInt16Array::from_slice([])),
    //     ];
    //     let freqs = vec![
    //         Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
    //             Some(vec![Some(1 as u8), Some(2), Some(3)]),
    //             None,
    //             None,
    //             Some(vec![Some(22), Some(2), Some(2)]),
    //         ])),
    //         Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
    //             Some(vec![Some(2), Some(1)]),
    //             None,
    //             Some(vec![Some(1), Some(2)]),
    //             None,
    //         ])),
    //         Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
    //             Some(vec![Some(3), Some(1), Some(1)]),
    //             None,
    //             None,
    //             None,
    //         ])),
    //         Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
    //             None,
    //             Some(vec![Some(1), Some(2), Some(2)]),
    //             None,
    //             None,
    //         ])),
    //         Arc::new(GenericListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
    //             None as Option<Vec<Option<u8>>>,
    //             None,
    //             None,
    //             None,
    //         ])),
    //     ];
    //     PostingBatch::try_new_with_freqs(schema, postings, freqs, range).unwrap()
    // }

    // #[test]
    // fn postingbatch_project_fold() {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("test1", DataType::Boolean, false),
    //         Field::new("test2", DataType::Boolean, false),
    //         Field::new("test3", DataType::Boolean, false),
    //         Field::new("test4", DataType::Boolean, false),
    //         Field::new("__id__", DataType::UInt32, false),
    //     ]));
    //     let batch = build_batch();
    //     let res = batch.project_fold(&[Some(1), Some(2)], Arc::new(schema.clone().project(&[1, 2, 4]).unwrap())).unwrap();
    //     let mut exptected1 = vec![false; 64];
    //     exptected1[0] = true;
    //     exptected1[4] = true;
    //     exptected1[16] = true;
    //     let exptected1 = BooleanArray::from(exptected1);
    //     let mut exptected2 = vec![false; 64];
    //     exptected2[4] = true;
    //     exptected2[6] = true;
    //     exptected2[8] = true;
    //     let exptected2 = BooleanArray::from(exptected2);

    //     assert_eq!(res.column(0).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected1);
    //     assert_eq!(res.column(1).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected2);
    // }

    // #[test]
    // fn postingbatch_project_fold_with_freqs() {
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("test1", DataType::Boolean, false),
    //         Field::new("test2", DataType::Boolean, false),
    //         Field::new("test3", DataType::Boolean, false),
    //         Field::new("test4", DataType::Boolean, false),
    //         Field::new("__id__", DataType::UInt32, false),
    //     ]));
    //     let batch = build_batch_with_freqs();
    //     let res = batch.project_fold_with_freqs(&[Some(1), Some(2)], Arc::new(schema.clone().project(&[1, 2, 4]).unwrap())).unwrap();
    //     let mut exptected1 = vec![false; 64];
    //     exptected1[0] = true;
    //     exptected1[4] = true;
    //     exptected1[16] = true;
    //     let exptected1 = BooleanArray::from(exptected1);
    //     let mut exptected2 = vec![false; 64];
    //     exptected2[4] = true;
    //     exptected2[6] = true;
    //     exptected2[8] = true;
    //     let exptected2 = BooleanArray::from(exptected2);
    //     println!("buffer: {:?}", res.column(0).data().buffers()[0].as_slice());
    //     println!("res: {:?}", res);
    //     assert_eq!(res.column(0).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected1);
    //     assert_eq!(res.column(1).as_any().downcast_ref::<BooleanArray>().unwrap(), &exptected2);
    // }
}