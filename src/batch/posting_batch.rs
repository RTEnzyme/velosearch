use std::{sync::Arc, ops::Index};

use datafusion::{arrow::{datatypes::SchemaRef, array::{UInt32Array, UInt16Array}}, from_slice::FromSlice};
use crate::utils::{Result, FastErr};

/// 
#[derive(Clone, Debug, PartialEq)]
pub struct BatchRange {
    start: usize, 
    end: usize,
    nums32: usize,
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

    pub fn project_fold(&self, indices: &[usize]) -> Result<Vec<PostingMask>> {
        // let projected_schema = self.schema.project(indices)?;
        let bitmask_size = self.range.nums32;
        indices
            .iter()
            .map(|f| {
                let mut bitmask = vec![0 as u32; bitmask_size];
                self.postings.get(*f).cloned().ok_or_else(|| {
                    FastErr::InternalErr(format!(
                        "project index {} out of bounds, max field {}",
                        f,
                        self.postings.len()
                    ))
                }).and_then(|f| {
                    let posting = f.values();
                    posting.into_iter()
                    .for_each(|v| {
                        let offset = (*v >> 6) as usize;
                        bitmask[offset] |= 1 << (31 - *v % 32)
                    });
                    Ok(Arc::new(UInt32Array::from_slice(bitmask.as_slice())))
                })
            }).collect()
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

#[cfg(test)]
mod test {
    use std::{arch::x86_64::{ __m512i, _mm512_sllv_epi32}, simd::Simd, sync::Arc};

    use datafusion::{arrow::{datatypes::{Schema, Field, DataType}, array::UInt16Array}, from_slice::FromSlice};

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
            Field::new("test1", DataType::UInt16, true),
            Field::new("test2", DataType::UInt16, true),
            Field::new("test3", DataType::UInt16, true),
            Field::new("test4", DataType::UInt16, true)
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
        res.into_iter()
        .enumerate()
        .for_each(|(i, v)| {
            assert_eq!(v.value(0), expected[i], "Round{}: real: {:#b}, expected: {:#b}", i, v.value(0), expected[i])
        });
    }
}