use std::{sync::Arc, ops::Index, collections::BTreeMap, mem::size_of_val, ptr::NonNull, cell::RefCell, arch::x86_64::{_pext_u64, _blsr_u64, _mm512_loadu_epi64, _mm512_popcnt_epi64, _mm512_reduce_add_epi64, _mm512_setzero_si512, _mm512_add_epi64, _mm512_loadu_epi8, _mm512_mask_compressstoreu_epi8}, slice::from_raw_parts};

use datafusion::{arrow::{datatypes::{SchemaRef, Field, DataType, Schema, UInt8Type, ToByteSlice}, array::{UInt32Array, UInt16Array, ArrayRef, BooleanArray, Array, ArrayData, GenericListArray, GenericBinaryArray, GenericBinaryBuilder, UInt64Array}, record_batch::{RecordBatch, RecordBatchOptions}, buffer::Buffer}, from_slice::FromSlice, common::TermMeta};
use serde::{Serialize, Deserialize};
use tracing::{debug, info};
use crate::{utils::{Result, FastErr, vec::{store_advance_aligned, set_vec_len_by_ptr}}, physical_expr::{BooleanEvalExpr, boolean_eval::{Chunk, TempChunk}}};


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

const COMPRESS_INDEX: [u8; 64] =  [
        0, 1, 2, 3, 4, 5, 6, 7,
        8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23,
        24, 25, 26, 27, 28, 29, 30, 31,
        32, 33, 34, 35, 36, 37, 38, 39,
        40, 41, 42, 43, 44, 45, 46, 47,
        48, 49, 50, 51, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 62, 63,
    ];


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
        const CHUNK_SIZE: usize = 8;
        let valid_batch_num = min_range.count_ones() as usize;
        let mut batches: Vec<ArrayRef> = Vec::with_capacity(indices.len());
        for (idx, distri) in indices.into_iter().zip(distris.into_iter()) {
            // To be optimized, we can convert bitvec to BooleanArray
            if idx.is_none() {
                batches.push(Arc::new(UInt64Array::from(vec![] as Vec<u64>)));
                continue;
            }

            // Safety: If idx is none, this loop will continue before this unwrap_unchecked()
            let idx = unsafe{ idx.unwrap_unchecked() as usize };
            let distri = unsafe { distri.unwrap_unchecked() };

            let posting = if idx != usize::MAX {
                self.postings.get(idx).cloned().ok_or_else(|| {
                    FastErr::InternalErr(format!(
                        "project index {} out of bounds, max field {}",
                        idx,
                        self.postings.len()
                    ))
                })?
            } else {
                batches.push(Arc::new(UInt64Array::from(vec![] as Vec<u64>)));
                continue;
            };

            let boundary = &self.boundary[idx];
            if boundary_idx >= boundary.len() {
                batches.push(Arc::new(UInt64Array::from(vec![] as Vec<u64>)));
                continue;
            }
            let start_idx = boundary[boundary_idx] as usize;

            let boundary_len = if boundary_idx < boundary.len() - 1 {
                (boundary[boundary_idx + 1] - boundary[boundary_idx]) as usize
            } else if  boundary_idx == boundary.len() - 1 {
                posting.len() - boundary[boundary_idx] as usize
            } else {
                batches.push(Arc::new(UInt64Array::from(vec![] as Vec<u64>)));
                continue;
            };

            let mut valid_batch: Vec<u64> = vec![0; valid_batch_num * 8];
            let valid_mask = unsafe { _pext_u64(min_range, distri) };
            let mut write_mask = unsafe { _pext_u64(distri, min_range) };

            let mut write_pos = write_mask.trailing_zeros() as usize;
            write_mask = clear_lowest_set_bit(write_mask);
            for i in 0..boundary_len.min(64) {
                if valid_mask & (1 << i) == 0 {
                    continue
                }
                let batch = posting.value(start_idx + i);
                // if batch.len() == 64 {
                //     let batch = unsafe { from_raw_parts(batch.as_ptr() as *const u64, 8) };
                //     valid_batch[(write_pos * 8)..(write_pos * 8 + 8)].copy_from_slice(batch);
                // } else {
                //     // means this's Integer list
                //     let batch = unsafe { from_raw_parts(batch.as_ptr() as *const u16, batch.len() / 2)};
                //     //  means this's bitmap
                //     let mut bitmap = [0; CHUNK_SIZE];
                //     for off in batch {
                //         bitmap[(*off >> 6) as usize] |= 1 << (*off % (1 << 6));
                //     }
                //     valid_batch[(write_pos * 8)..(write_pos * 8 + 8)].copy_from_slice(&bitmap);
                // }
                valid_batch[write_pos] = batch.len() as u64;
                write_pos = write_mask.trailing_zeros() as usize;
                write_mask = clear_lowest_set_bit(write_mask);
            }
            let batch = UInt64Array::from(valid_batch);
            // info!("fetch true count: {:}", as_boolean_array(&batch).true_count());
            batches.push(Arc::new(batch));
        }
        batches.insert(projected_schema.index_of("__id__").expect("Should have __id__ field"), Arc::new(UInt32Array::from(vec![] as Vec<u32>)));
        
        debug!("end of project fold");
        let option = RecordBatchOptions::new().with_row_count(Some(valid_batch_num * 8));
        Ok(RecordBatch::try_new_with_options(projected_schema, batches, &option)?)
    }

    pub fn project_with_predicate(
        &self,
        indices: &[Option<u32>],
        _projected_schema: SchemaRef,
        distris: &[Option<u64>],
        boundary_idx: usize,
        min_range: u64,
        predicate: &BooleanEvalExpr,
        _is_encoding: &Vec<bool>,
    ) -> Result<usize> {
        let predicate = predicate.predicate.as_ref().unwrap().get();
        let predicate_ref = unsafe {predicate.as_ref().unwrap() };
        let valid_num = min_range.count_ones() as usize;
        
        let mut batches: Vec<Option<Vec<Chunk>>> = vec![None; indices.len()];
        debug!("start select valid batch");
        const EMPTY: [u64; 8] = [0; 8];
        for (j, index) in indices.iter().enumerate() {
            let distri = unsafe { distris.get_unchecked(j).unwrap_unchecked() };
            let mut write_mask = unsafe { _pext_u64(min_range, distri )};
            let valid_mask = unsafe { _pext_u64(distri, min_range) };
            let idx = unsafe { index.unwrap_unchecked() as usize };
            let bucket = if idx != usize::MAX {
                unsafe {
                self.postings.get(idx).cloned().ok_or_else(|| {
                    FastErr::InternalErr(format!(
                        "project index {} out of bounds, max fields {}",
                        idx,
                        self.postings.len(),
                    ))
                })?
                }
            } else {
                continue;
            };
            let boundary = &self.boundary[idx];
            if boundary_idx >= boundary.len() {
                continue;
            }
            let start_idx = unsafe { *boundary.get_unchecked(boundary_idx) as usize };
            let mut posting = Vec::with_capacity(valid_num);
            let mut posting_ptr = posting.as_mut_ptr();

            for i in 0..valid_num {
                if valid_mask & (1 << i) != 0 {
                    let pos = write_mask.trailing_zeros() as usize;
                    write_mask = clear_lowest_set_bit(write_mask);
                    let batch = bucket.value(start_idx + pos);
                    if batch.len() == 64 {
                        let batch = unsafe { from_raw_parts(batch.as_ptr() as *const u64, 8) };
                        unsafe { store_advance_aligned(Chunk::Bitmap(batch), &mut posting_ptr) };
                    } else {
                        let batch = unsafe { from_raw_parts(batch.as_ptr() as *const u16, batch.len() / 2) };
                        // if !encoding {
                            unsafe { store_advance_aligned(Chunk::IDs(batch), &mut posting_ptr) };
                        // } else {
                        //     let mut bitmap: [u64; 8] = [0; 8];
                        //     for off in batch {
                        //         unsafe {
                        //             *bitmap.get_unchecked_mut((*off >> 6) as usize) |= 1 << (*off % (1 << 6));
                        //         }
                        //     }
                        //     // leak_pool.push(bitmap);
                        //     // let bitmap = Arc::new(bitmap);
                        //     std::mem::forget(bitmap);
                        //     unsafe { store_advance_aligned(Chunk::Bitmap(
                        //         from_raw_parts(bitmap.as_ptr() as *const u64, 8)
                        //         // from_raw_parts(leak_pool.last().unwrap().as_ptr() as *const u64, 8)
                        //     ), &mut posting_ptr) };
                        // }
                    }
                } else {
                    unsafe {
                        store_advance_aligned(Chunk::Bitmap(&EMPTY), &mut posting_ptr);  
                    }
                }
            }
            unsafe { set_vec_len_by_ptr(&mut posting, posting_ptr)}
            batches[j] = Some(posting);
        }
        
        debug!("start eval");
        let eval = predicate_ref.eval_avx512(&batches, None, true)?;
        debug!("end eval");
        // batches.clear();
        if let Some(e) = eval {
            let mut accumulator = unsafe { _mm512_setzero_si512() };
            let mut id_acc = 0;
            e.into_iter()
            .for_each(|v| unsafe {
                match  v {
                    TempChunk::Bitmap(b) => {
                        let popcnt = _mm512_popcnt_epi64(b);
                        accumulator = _mm512_add_epi64(accumulator, popcnt);
                    }
                    TempChunk::IDs(i) => {
                        id_acc += i.len();
                    }
                    TempChunk::N0NE => {},
                }
            });
            // let sum = unsafe { _mm512_reduce_add_epi64(accumulator) } as usize + id_acc;
            let sum = id_acc;
            debug!("sum: {:}", sum);
            // let sum = 0;
            Ok(sum)
        } else {
            Ok(0)
        }
    }

    // #[inline]
    // fn process_dense_bucket(&self, indices: &[Option<u32>], distris: &[Option<u64>], min_range: u64, boundary_idx: usize, valid_num: usize) -> Result<Vec<Option<Vec<Chunk>>>> {
    //     let mut batches: Vec<Option<Vec<Chunk>>> = vec![None; indices.len()];
    //     let compress_index = unsafe {
    //         _mm512_loadu_epi8(COMPRESS_INDEX.as_ptr() as *const i8)
    //     };
    //     for (j, index) in indices.iter().enumerate() {
    //         let distri = unsafe { distris.get_unchecked(j).unwrap_unchecked() };
    //         let write_mask = unsafe { _pext_u64(min_range, distri ) };
    //         let valid_mask = unsafe { _pext_u64(distri, min_range) };

    //         let idx = unsafe { index.unwrap_unchecked() as usize };

    //         let bucket = if idx != usize::MAX {
    //             self.postings.get(idx).cloned().ok_or_else(|| {
    //                 FastErr::InternalErr(format!(
    //                     "project index {} out of bounds, max field {}",
    //                     idx,
    //                     self.postings.len()
    //                 ))
    //             })?
    //         } else {
    //             continue;
    //         };

    //         let boundary = &self.boundary[idx];
    //         if boundary_idx >= boundary.len() {
    //             continue;
    //         }
    //         let start_idx = boundary[boundary_idx] as usize;
    //         let mut posting = vec![Chunk::N0NE; valid_num];


    //         let mut write_index: Vec<u8> = vec![0; valid_num];
    //         let mut posting_index: Vec<u8> = vec![0; valid_num];
    //         unsafe {
    //             _mm512_mask_compressstoreu_epi8(write_index.as_mut_ptr() as *mut u8, valid_mask, compress_index);
    //             _mm512_mask_compressstoreu_epi8(posting_index.as_mut_ptr() as *mut u8, write_mask, compress_index);
    //         }

    //         for (w, p) in write_index.into_iter().zip(posting_index.into_iter()){
    //             let batch = bucket.value(start_idx + p as usize);
    //             if batch.len() == 64 {
    //                 posting[w as usize] = Chunk::Bitmap(batch);
    //             } else {
    //                 // means this's Integer list
    //                 let batch = unsafe {batch.align_to::<u16>().1};
    //                 posting[w as usize] = Chunk::IDs(batch);
    //             }
    //         }
    //         batches[j] = Some(posting);
    //     }
    //     Ok(batches)
    // }

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
    pub term_dict: RefCell<BTreeMap<String, Vec<(u32, u8)>>>,
    term_num: usize,
}

impl PostingBatchBuilder {
    pub fn new() -> Self {
        Self { 
            current: 0,
            term_dict: RefCell::new(BTreeMap::new()),
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

    pub fn build(self) -> Result<PostingBatch> {
        self.build_with_idx(None, 0)
    }

    pub fn build_with_idx(self, idx: Option<&RefCell<BTreeMap<String, TermMetaBuilder>>>, partition_num: usize) -> Result<PostingBatch> {
        let term_dict = self.term_dict
            .into_inner();
        let mut schema_list = Vec::new();
        let mut postings: Vec<Arc<GenericBinaryArray<i32>>> = Vec::new();
        let mut freqs = Vec::new();
        let mut boundarys = Vec::new();
        
        for (i, (k, v)) in term_dict.into_iter().enumerate() {
                let mut boundary = vec![0];
                let mut cnter = 0;
                let mut posting_builder = GenericBinaryBuilder::new();
                let mut builder_len = 0;
                let mut batch_num = 0;
                if idx.is_some() {
                    idx.as_ref().unwrap().borrow_mut().get_mut(&k).unwrap().add_idx(i as u32, partition_num);
                }
                let mut freq: Vec<Option<Vec<Option<u8>>>> = vec![None; 4];
                let mut buffer: Vec<u16> = Vec::new();
                v.into_iter()
                .for_each(|(p, f)| {
                    if p - cnter < 512 {
                        buffer.push((p - cnter) as u16);
                    } else {
                        let skip_num = (p - cnter) / 512;
                        cnter += skip_num * 512;

                        let skip_boundary = (batch_num + skip_num) / 64 - batch_num / 64;
                        for _ in 0..skip_boundary{
                            boundary.push(builder_len);
                        }
                        batch_num += skip_num;
                        
                        if buffer.len() > 16 {
                            let mut bitmap: Vec<u64> = vec![0; 8];
                            for i in &buffer {
                                let off = *i;
                                bitmap[(off >> 6) as usize] |= 1 << (off % (1 << 6));
                            }
                            posting_builder.append_value(bitmap.to_byte_slice());
                            builder_len += 1;
                        } else if buffer.len() > 0 {
                            posting_builder.append_value(unsafe {
                                let value = buffer.as_slice().align_to::<u8>();
                                assert!(value.0.len() == 0 && value.2.len() == 0);
                                value.1
                            });
                            builder_len += 1;
                        }
                        buffer.clear();
                        buffer.push((p - cnter) as u16);
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

                if buffer.len() > 0 {
                    if buffer.len() > 16 {
                        let mut bitmap: Vec<u64> = vec![0; 8];
                        for i in &buffer {
                            let off = *i;
                            bitmap[(off >> 6) as usize] |= 1 << (off % (1 << 6));
                        }
                        posting_builder.append_value(bitmap.to_byte_slice());
                    } else {
                        posting_builder.append_value(unsafe {
                            let value = buffer.as_slice().align_to::<u8>();
                            assert!(value.0.len() == 0 && value.2.len() == 0);
                            value.1
                        });
                    }
                }
                freqs.push(Arc::new(GenericListArray::<i32>::from_iter_primitive::<UInt8Type, _, _>(freq)));
                schema_list.push(Field::new(k.clone(), DataType::UInt32, false));
                postings.push(Arc::new(posting_builder.finish()));
                boundarys.push(boundary);
        }
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
    pub distribution: Vec<Vec<u64>>,
    pub nums: Vec<u32>,
    idx: Vec<Option<u32>>,
    partition_num: usize,
    bounder: Option<u32>,
}

impl TermMetaBuilder {
    pub fn new(batch_num: usize, partition_num: usize) -> Self {
        Self {
            distribution: vec![vec![0; (batch_num + 63) / 64]; partition_num],
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
        if self.distribution[partition_num][i / 64] & (1 << (i % 64)) != 0 {
            return;
        }
        self.distribution[partition_num][i / 64] |= 1 << (i % 64);
    }

    pub fn add_idx(&mut self, idx: u32, partition_num: usize) {
        self.idx[partition_num] = Some(idx);
    }


    pub fn build(self) -> TermMeta {
        let (distribution, index): (Vec<Arc<UInt64Array>>, _)= (0..self.partition_num)
            .into_iter()
            .map(|v| {
                if self.nums[v] == 0 {
                    (Arc::new(UInt64Array::from(vec![0; self.distribution[0].len()])), None)
                } else {
                    let distribution = Arc::new(UInt64Array::from_slice(&self.distribution[v]));
                    let index = self.idx[v].clone();
                    (distribution, index)
                }
            })
            .unzip();
        let valid_batch_num: usize = distribution.iter()
            .map(|d| d.iter().map(|v| unsafe { v.unwrap_unchecked().count_ones() }).sum::<u32>() as usize)
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

    let array_data = builder.build().unwrap();
    Arc::new(BooleanArray::from(array_data))
}

fn build_boolean_array_u8(mut data: Vec<u8>, batch_len: usize) -> ArrayRef {
    let value_buffer = unsafe {
        let buf = Buffer::from_raw_parts(NonNull::new_unchecked(data.as_mut_ptr() as *mut u8), batch_len / 8, batch_len / 8);
        std::mem::forget(data);
        buf
    };
    let builder = ArrayData::builder(DataType::Boolean)
        .len(batch_len)
        .add_buffer(value_buffer);

    let array_data = builder.build().unwrap();
    Arc::new(BooleanArray::from(array_data))
}


#[cfg(test)]
mod test {
    // use std::sync::Arc;

    // use datafusion::{arrow::{datatypes::{Schema, Field, DataType, UInt8Type}, array::{UInt16Array, BooleanArray, GenericListArray, ArrayRef}}, from_slice::FromSlice};

    // use super::{BatchRange, PostingBatch};

    use std::{ptr::{self, from_raw_parts}, arch::x86_64::{_mm512_mask_compressstoreu_epi16, __mmask32, _mm512_loadu_epi16, __mmask64, _mm512_loadu_epi8, _mm512_mask_compressstoreu_epi8}};

    use crate::batch::posting_batch::COMPRESS_INDEX;

    #[test]
    fn test_ptr() {
        // let val: [u64; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        // println!("{:?}", ptr::metadata(val.as_slic8e() as *const [u64]));
        println!("{:}", std::mem::size_of::<Option<&[u8]>>())
    }

    #[test]
    fn test_compress() {
        let data = unsafe {
            _mm512_loadu_epi8(COMPRESS_INDEX.as_ptr() as *const i8)
        };
        println!("{:?}", data);
        let mut res: Vec<u8> = vec![0; 64];
        unsafe {
            _mm512_mask_compressstoreu_epi8(res.as_mut_ptr() as *mut u8, u64::MAX as __mmask64, data);
        }
        println!("{:?}", res);
    }

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