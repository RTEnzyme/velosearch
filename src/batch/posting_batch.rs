use std::{sync::{Arc, RwLock}, ops::Index, collections::HashMap, cmp::max};

use datafusion::{arrow::{datatypes::{SchemaRef, Field, DataType, Schema}, array::{UInt32Array, UInt16Array, ArrayRef, BooleanArray}, record_batch::RecordBatch}, from_slice::FromSlice};
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
    
    pub fn end(&self) -> u32 {
        self.end
    }

    pub fn len(&self) -> u32 {
        self.end - self.start
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
            let idx = idx.unwrap();
            let posting = self.postings.get(idx).cloned().ok_or_else(|| {
                FastErr::InternalErr(format!(
                    "project index {} out of bounds, max field {}",
                    idx,
                    self.postings.len()
                ))
            })?;
            posting.values()
            .iter()
            .for_each(|v| {
                bitmap[*v as usize] = true
            });
            batches.push(Arc::new(BooleanArray::from(bitmap)));
        }
        batches.insert(projected_schema.index_of("__id__").expect("Should have __id__ field"), Arc::new(UInt32Array::from_iter_values((self.range.start).. (self.range.start + 32 * self.range.nums32))));
        Ok(RecordBatch::try_new(projected_schema, batches)?)
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
    term_dict: RwLock<HashMap<String, Vec<u16>>>,
}

impl PostingBatchBuilder {
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

    pub fn push_term_posting(&mut self, term: String, doc_ids: Vec<u32>) -> Result<()> {
        let current = max(self.current, doc_ids[doc_ids.len() - 1]);
        self.term_dict
            .get_mut()
            .map_err(|_| FastErr::InternalErr(format!("Can't acquire the RwLock correctly")))?
            .entry(term)
            .or_insert(Vec::new())
            .extend(doc_ids.into_iter().map(|v| (v - self.start) as u16));
        self.current = current;
        Ok(())
    }

    pub fn push_terms(&mut self, terms: Vec<String>, doc_id: u32) -> Result<()> {
        let term_dict = self.term_dict.get_mut().map_err(|_| {
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
        schema_list.push(Field::new("__id__", DataType::UInt32, false));
        postings.push(Arc::new(UInt16Array::from_slice(&[])));
       PostingBatch::try_new(
            Arc::new(Schema::new(schema_list)),
            postings,
            Arc::new(BatchRange::new(self.start, self.current + 1)))
    }
}

#[cfg(test)]
mod test {
    use std::{arch::x86_64::{ __m512i, _mm512_sllv_epi32}, simd::Simd, sync::Arc};

    use datafusion::{arrow::{datatypes::{Schema, Field, DataType}, array::{UInt16Array, BooleanArray}}, from_slice::FromSlice};

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
        let postings = vec![
           Arc::new(UInt16Array::from_slice([1, 6, 9])),
           Arc::new(UInt16Array::from_slice([0, 4, 16])),
           Arc::new(UInt16Array::from_slice([4, 6, 8])),
           Arc::new(UInt16Array::from_slice([6, 16, 31])),
           Arc::new(UInt16Array::from_slice([])),
        ];
        PostingBatch::try_new(schema, postings, range).unwrap()
    }

    #[test]
    fn postingbatch_project_fold() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test1", DataType::Boolean, true),
            Field::new("test2", DataType::Boolean, true),
            Field::new("test3", DataType::Boolean, true),
            Field::new("test4", DataType::Boolean, true),
            Field::new("__id__", DataType::UInt32, false),
        ]));
        let batch = build_batch();
        let res = batch.project_fold(&[Some(1), Some(2)], schema.clone()).unwrap();
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
}