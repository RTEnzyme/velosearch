use std::{sync::{Arc, RwLock}, ops::Index, collections::HashMap};

use datafusion::{arrow::{datatypes::{SchemaRef, Field, DataType, Schema}, array::{UInt32Array, UInt16Array, ArrayRef}, record_batch::RecordBatch}};
use crate::utils::{Result, FastErr};

/// 
#[derive(Clone, Debug, PartialEq)]
pub struct BatchRange {
    start: u32, 
    end: u32,
    nums32: u32,
}

impl BatchRange {
    pub fn new(start: u32, end: u32) -> Self {
        Self {
            start,
            end,
            nums32: (end - start + 31) / 32
        }
    }
}

pub type PostingList = Arc<UInt16Array>;
pub type PostingMask = Arc<UInt32Array>;
pub type TermSchemaRef = SchemaRef;

/// A batch of Postinglist which contain serveral terms,
/// which is in range[start, end)
#[derive(Clone, Debug, PartialEq)]
pub struct PostingBatch {
    schema: TermSchemaRef,
    postings: Vec<PostingList>,

    range: Arc<BatchRange>
}

impl PostingBatch {
    pub fn try_new(
        schema: TermSchemaRef,
        postings: Vec<PostingList>,
        range: Arc<BatchRange>
    ) -> Result<Self> {
        Self::try_new_impl(schema, postings, range)
    }

    pub fn try_new_impl(
        schema: TermSchemaRef,
        postings: Vec<PostingList>,
        range: Arc<BatchRange>
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
            self.range.clone()
        )
    }

    pub fn project_fold(&self, indices: &[usize]) -> Result<RecordBatch> {
        let projected_schema = self.schema.project(indices)?;
        let bitmask_size = self.range.nums32;
        let mut batches: Vec<ArrayRef> = Vec::with_capacity(indices.len());
        for idx in indices {
            let mut bitmask = vec![0 as u32; bitmask_size as usize];
            let posting = self.postings.get(*idx).cloned().ok_or_else(|| {
                FastErr::InternalErr(format!(
                    "project index {} out of bounds, max field {}",
                    *idx,
                    self.postings.len()
                ))
            })?;
            posting.values()
            .iter()
            .for_each(|v| {
                let offset = (*v >> 6) as usize;
                bitmask[offset] |= 1 << (31 - *v %32)
            });
            batches.push(Arc::new(UInt32Array::from(bitmask)));
        }
        Ok(RecordBatch::try_new(Arc::new(projected_schema), batches)?)
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

pub struct PostringBatchBuilder {
    start: u32,
    current: u32,
    term_dict: RwLock<HashMap<String, Vec<u16>>>,
}

impl PostringBatchBuilder {
    pub fn new(start: u32) -> Self {
        Self { 
            start,
            current: start,
            term_dict: RwLock::new(HashMap::new())
        }
    }

    pub fn push_term(&mut self, term: String, doc_id: u32) -> Result<()> {
        self.term_dict
            .get_mut()
            .map_err(|_| FastErr::InternalErr("Can't acquire the RwLock correctly".to_string()))?
            .entry(term)
            .or_insert(Vec::new())
            .push((doc_id - self.start) as u16);
        self.current = doc_id;
        Ok(())
    }

    pub fn push_terms(&mut self, terms: Vec<String>, doc_id: u32) -> Result<()> {
        let term_dict = self.term_dict.get_mut().map_err(|e| {
            FastErr::InternalErr("Can't acquire the RwLock correctly".to_string())
        })?;
        let doc_id = (doc_id - self.start) as u16;
        terms.into_iter()
        .for_each(|v| term_dict.entry(v).or_insert(Vec::new()).push(doc_id));
        Ok(())
    }

    pub fn build_single(self) -> Result<PostingBatch> {
        let term_dict = self.term_dict
            .into_inner()
            .expect("Can't acquire the RwLock correctly");
        let mut schema_list = Vec::new();
        let mut postings = Vec::new();
        term_dict
            .into_iter()
            .for_each(|(k, v)| {
                schema_list.push(Field::new(k, DataType::UInt32, false));
                postings.push(Arc::new(UInt16Array::from(v)));
            });
       PostingBatch::try_new(
            Arc::new(Schema::new(schema_list)),
            postings,
            Arc::new(BatchRange::new(self.start, self.current + 1)))
    }
}

#[cfg(test)]
mod test {
    use std::{arch::x86_64::{ __m512i, _mm512_sllv_epi32}, simd::Simd, sync::Arc};

    use datafusion::{arrow::{datatypes::{Schema, Field, DataType}, array::{UInt16Array}}, from_slice::FromSlice, common::cast::as_uint32_array};

    use super::{BatchRange, PostingBatch};

    unsafe fn test_simd() -> __m512i {
        // let position: __m512i = __m512i::from(Simd::from([1, 3,5,7, 12, 16, 20, 22, 27, 29, 30, 33, 39, 44, 49, 66] as [u32;16]));
        let position: __m512i = __m512i::from(Simd::from([1,3,5,0,0,0,0,0,0,0,0,0,0,0,0,0] as [u32;16]));
        let ones8: __m512i = __m512i::from(Simd::from([u32::MAX; 16]));
        let res = _mm512_sllv_epi32(ones8, position);
        return res;
    }

    #[test]
    fn test_print() {
        let res: Simd<u64,8> = unsafe {
            test_simd()
        }.into();
        let res = res.as_array();
        for i in res {
            print!("{:#b} ", *i);
        }
        panic!("");
    }

    fn build_batch() -> PostingBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test1", DataType::UInt32, true),
            Field::new("test2", DataType::UInt32, true),
            Field::new("test3", DataType::UInt32, true),
            Field::new("test4", DataType::UInt32, true)
        ]));
        let range = Arc::new(BatchRange {
            start: 0,
            end: 64,
            nums32: 1
        });
        let postings = vec![
           Arc::new(UInt16Array::from_slice([1, 6, 9])),
           Arc::new(UInt16Array::from_slice([0, 4, 16])),
           Arc::new(UInt16Array::from_slice([4, 6, 8])),
           Arc::new(UInt16Array::from_slice([6, 16, 31])),
        ];
        PostingBatch::try_new(schema, postings, range).unwrap()
    }

    #[test]
    fn postingbatch_project_fold() {
        let batch = build_batch();
        let res = batch.project_fold(&[1, 2]).unwrap();
        let expected = vec![0x88008000 as u32, 0x0a800000 as u32];
        res.columns()
        .into_iter()
        .enumerate()
        .for_each(|(i, v)| {
            let arr = as_uint32_array(v).expect("can't downcast to UInt32Array");
            assert_eq!(arr.value(0), expected[i], "Round{}: real: {:#b}, expected: {:#b}", i, arr.value(0), expected[i])
        });
    }
}