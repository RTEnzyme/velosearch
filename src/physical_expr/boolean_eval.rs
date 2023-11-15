use std::{sync::Arc, any::Any, cell::SyncUnsafeCell, arch::x86_64::{__m512i, _mm512_and_epi64, _mm512_or_epi64, _mm512_loadu_epi64}, slice::from_raw_parts};

use dashmap::DashSet;
use datafusion::{physical_plan::{expressions::{BinaryExpr, Column}, PhysicalExpr, ColumnarValue}, arrow::{datatypes::{Schema, DataType}, record_batch::RecordBatch, array::UInt64Array}, error::DataFusionError, common::{Result, cast::as_uint64_array}};
use sorted_iter::{assume::AssumeSortedByItemExt, SortedIterator};
use tracing::{debug, info};

use crate::{ShortCircuit, utils::avx512::{avx512_bitwise_and, avx512_bitwise_or, U64x8}};

#[derive(Clone, Debug)]
pub enum Primitives {
    BitwisePrimitive(BinaryExpr),
    ShortCircuitPrimitive(ShortCircuit),
    ColumnPrimitive(Column),
}

#[derive(Clone, Debug)]
pub enum Chunk<'a> {
    Bitmap(&'a [u64]),
    IDs(&'a [u16]),
    N0NE,
}

#[derive(Clone, Debug)]
pub enum TempChunk {
    Bitmap(__m512i),
    IDs(Vec<u16>),
    N0NE,
}

impl Primitives {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self {
            Primitives::BitwisePrimitive(b) => b.evaluate(batch),
            Primitives::ShortCircuitPrimitive(s) => s.evaluate(batch),
            Primitives::ColumnPrimitive(c) => c.evaluate(batch),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SubPredicate {
    /// The children nodes of physical predicate
    pub sub_predicate: PhysicalPredicate,
    /// Node num
    pub node_num: usize,
    /// Leaf num
    pub leaf_num: usize,
    /// The selectivity of this node
    pub selectivity: f64,
    /// The rank of this node, calculate by $$\frac{sel - 1}{leaf_num}$$
    pub rank: f64,
    /// The cumulative selectivity
    pub cs: f64,
}

impl SubPredicate {
    pub fn new(
        sub_predicate: PhysicalPredicate,
        node_num: usize,
        leaf_num: usize,
        selectivity: f64,
        rank: f64,
        cs: f64,
    ) -> Self {
        Self {
            sub_predicate,
            node_num,
            leaf_num,
            selectivity,
            rank,
            cs,
        }
    }

    pub fn new_with_predicate(
        sub_predicate: PhysicalPredicate
    ) -> Self {
        Self::new(sub_predicate, 0, 0, 0., 0., 0.)
    }

    pub fn node_num(&self) -> usize {
        self.node_num
    }

    pub fn leaf_num(&self) -> usize {
        self.leaf_num
    }

    pub fn sel(&self) -> f64 {
        self.selectivity
    }

    pub fn rank(&self) -> f64 {
        self.rank
    }
}

#[derive(Clone, Debug)]
pub enum PhysicalPredicate {
    /// `And` Level
    And { args: Vec<SubPredicate> },
    /// `Or` Level
    Or { args: Vec<SubPredicate> },
    /// Leaf
    Leaf { primitive: Primitives },
}

impl PhysicalPredicate {
    pub fn eval_avx512(&self, batch: &Vec<Option<Vec<Chunk>>>, init_v: Option<Vec<TempChunk>>, is_and: bool, valid_num: usize) -> Result<Option<Vec<TempChunk>>> {
        let mut init_v = init_v;
        match self {
            PhysicalPredicate::And { args } => {
                for predicate in args {
                    init_v = predicate.sub_predicate.eval_avx512(&batch, init_v, true, valid_num)?;
                }
                Ok(init_v)
            }
            PhysicalPredicate::Or { args } => {
                for predicate in args {
                    init_v = predicate.sub_predicate.eval_avx512(&batch, init_v, false, valid_num)?;
                }
                Ok(init_v)
            }
            PhysicalPredicate::Leaf { primitive } => {
                match primitive {
                    Primitives::BitwisePrimitive(_b) => {
                        todo!()
                    }
                    Primitives::ShortCircuitPrimitive(s) => {
                        if is_and {
                            Ok(Some(s.eval_avx512(init_v, &batch, valid_num)))
                        } else {
                            let eval = s.eval_avx512(None, &batch, valid_num);
                            match init_v {
                                Some(mut e) => {
                                    e.iter_mut()
                                    .zip(eval.iter())
                                    .for_each(|(a, b)| { 
                                        match (&a, b) {
                                            (TempChunk::Bitmap(a_bm), TempChunk::Bitmap(b)) => {
                                                *a = TempChunk::Bitmap(unsafe {
                                                    _mm512_or_epi64(*a_bm, *b)
                                                })
                                            }
                                            (TempChunk::IDs(ids), TempChunk::Bitmap(b)) => {
                                                let mut chunk = unsafe {U64x8{vector: *b}.vals};
                                                for id in ids {
                                                    chunk[(*id as usize) >> 8] |= 1 << ((*id as usize) % 64);
                                                }
                                                *a = TempChunk::Bitmap(unsafe {
                                                    U64x8{ vals: chunk }.vector
                                                })
                                            }
                                            (_, _) => unreachable!(),
                                        }
                                    });
                                }
                                None => {}
                            }
                            Ok(Some(eval))
                        }
                    }
                    Primitives::ColumnPrimitive(c) => {
                        if is_and {
                            let eval_array = &batch[c.index()];
                            match (&mut init_v, eval_array) {
                                (Some(e), Some(i)) => {
                                    for (a, b) in e.iter_mut().zip(i.into_iter()) {
                                        match (&a, b) {
                                            (TempChunk::Bitmap(a_bm), Chunk::Bitmap(b_bm)) => {
                                                let b_bm = unsafe { _mm512_loadu_epi64((*b_bm).as_ptr() as *const i64)};
                                                *a = TempChunk::Bitmap(unsafe { _mm512_and_epi64(*a_bm, b_bm) })
                                            }
                                            (TempChunk::Bitmap(a_bm), Chunk::IDs(b)) => {
                                                let mut a_id = unsafe { U64x8{ vector: *a_bm }.vals };
                                                for id in *b {
                                                    a_id[(*id as usize) >> 8] |= 1 << ((*id as usize) % 64);
                                                }
                                                *a = TempChunk::Bitmap(unsafe { U64x8 { vals: a_id }.vector })
                                            }
                                            (TempChunk::IDs(a_id), Chunk::Bitmap(b_bm)) => {
                                                *a = TempChunk::IDs(
                                                    a_id.into_iter()
                                                    .filter(|v| {
                                                        b_bm[(**v as usize) >> 8] & (1 << (**v as usize % 64)) != 0
                                                    })
                                                    .map(|v| *v)
                                                    .collect()
                                                )
                                            }
                                            (TempChunk::IDs(a_id), Chunk::IDs(b_id)) => {
                                                let a_id = a_id.into_iter().map(|v|*v).assume_sorted_by_item();
                                                let b_id = b_id.into_iter().map(|&v| v).assume_sorted_by_item();
                                                *a = TempChunk::IDs(a_id.intersection(b_id).collect())
                                            }
                                            (_, Chunk::N0NE) => {
                                                *a = TempChunk::N0NE
                                            }
                                            _ => unreachable!(),
                                        }
                                    }
                                }
                                (_, Some(i)) => {
                                    init_v = Some(
                                        i.into_iter()
                                        .map(|a| match a {
                                                Chunk::Bitmap(b) => {
                                                    TempChunk::Bitmap(unsafe { _mm512_loadu_epi64((*b).as_ptr() as *const i64)})
                                                }
                                                Chunk::IDs(ids) => {
                                                    TempChunk::IDs(ids.to_vec())
                                                }
                                                Chunk::N0NE => TempChunk::N0NE
                                        })
                                        .collect()
                                );},
                                (_, _) => {}
                            }
                        } else {
                            let eval_array = &batch[c.index()];
                            match (eval_array, &mut init_v) {
                                (Some(e), Some(i)) => {
                                    for (a, b) in i.iter_mut().zip(e.into_iter()) {
                                        match (b, &a) {
                                            (Chunk::Bitmap(a_bm), TempChunk::Bitmap(b_bm)) => {
                                                let a_bm = load_u64_slice(a_bm);
                                                *a = TempChunk::Bitmap(unsafe { _mm512_or_epi64(a_bm, *b_bm) })
                                            }
                                            (Chunk::Bitmap(a_bm), TempChunk::IDs(b_id)) => {
                                                let mut bitmap = (*a_bm).to_owned();
                                                for id in b_id {
                                                    bitmap[(*id as usize) >> 8] |= 1 << ((*id as usize) % 64);
                                                }
                                                *a = TempChunk::Bitmap(unsafe { _mm512_loadu_epi64(bitmap.as_ptr() as *const i64)})
                                            }
                                            (Chunk::IDs(a_id), TempChunk::Bitmap(b_bm)) => {
                                                let mut b = unsafe { U64x8{ vector: *b_bm }.vals };
                                                for &id in *a_id {
                                                    b[(id as usize) >> 8] |= 1 << ((id as usize) % 64);
                                                }
                                                *a = TempChunk::Bitmap(unsafe { U64x8 { vals: b }.vector })
                                            }
                                            (Chunk::IDs(a_id), TempChunk::IDs(b_id)) => {
                                                let a_id = a_id.into_iter().cloned().assume_sorted_by_item();
                                                let b_id = b_id.into_iter().cloned().assume_sorted_by_item();
                                                *a = TempChunk::IDs(b_id.intersection(a_id).collect())
                                            }
                                            (Chunk::Bitmap(a_bm), TempChunk::N0NE) => {
                                                *a = TempChunk::Bitmap(load_u64_slice(a_bm))
                                            }
                                            (Chunk::IDs(a_ids), TempChunk::N0NE) => {
                                                *a = TempChunk::IDs(a_ids.to_vec())
                                            }
                                            _ => *a = TempChunk::N0NE,
                                        }
                                    }
                                }
                                (Some(eval), None) => {
                                    let mut init: Vec<TempChunk> = Vec::with_capacity(eval.len());
                                    for e in eval {
                                        match e {
                                            Chunk::Bitmap(b) => {
                                                init.push(TempChunk::Bitmap(load_u64_slice(b)));
                                            }
                                            Chunk::IDs(ids) => {
                                                init.push(TempChunk::IDs(ids.to_vec()));
                                            }
                                            Chunk::N0NE => {
                                                init.push(TempChunk::N0NE);
                                            }
                                        }
                                    }
                                    init_v = Some(init);
                                }
                                (_, _) => {}
                            }
                        }
                        Ok(init_v)
                    }
                }
            }
        }

    }
    fn eval(&self, batch: &RecordBatch, init_v: Vec<u64>, is_and: bool) -> Result<Vec<u64>> {
        let mut init_v = init_v;
        match self {
            PhysicalPredicate::And { args } => {
                for predicate in args {
                    init_v = predicate.sub_predicate.eval(batch, init_v, true)?;
                }
                Ok(init_v)
            }
            PhysicalPredicate::Or { args } => {
                for predicate in args {
                    init_v = predicate.sub_predicate.eval(batch, init_v, false)?;
                }
                Ok(init_v)
            }
            PhysicalPredicate::Leaf { primitive } => {
                match  primitive {
                    Primitives::BitwisePrimitive(b) => {
                        let tmp = b.evaluate(batch)?.into_array(0);
                        let res = as_uint64_array(&tmp).unwrap();
                        if is_and {
                            if res.len() == 0 {
                                init_v.fill(0);
                                Ok(init_v)
                            } else {
                                Ok(init_v.into_iter()
                                .zip(res.into_iter())
                                .map(|(i, j)| i & unsafe { j.unwrap_unchecked() })
                                .collect())
                            }
                        } else {
                            if res.len() == 0 {
                                Ok(init_v)
                            } else {
                                Ok(init_v.into_iter()
                                .zip(res.into_iter())
                                .map(|(i, j)| i | unsafe { j.unwrap_unchecked() })
                                .collect())
                            }
                        }
                    }
                    Primitives::ShortCircuitPrimitive(s) => {
                        if is_and {
                            unsafe { s.eval(init_v.align_to_mut().1, batch).align_to::<u64>() };
                            Ok(init_v)
                        } else {
                            let mut init_v_or = vec![u8::MAX; init_v.len() * 8];
                            s.eval(&mut init_v_or, batch);
                            let init_v_or = unsafe { init_v_or.align_to::<u64>().1 };
                            for i in 0..init_v.len(){
                                init_v[i] |= init_v_or[i];
                            }
                            Ok(init_v)
                        }
                    }
                    Primitives::ColumnPrimitive(c) => {
                        if is_and {
                            let eval_array = batch.column(c.index());
                            let eval_res = as_uint64_array(&eval_array).unwrap();
                            if eval_res.len() == 0 {
                                init_v.fill(0);
                                Ok(init_v)
                            } else {
                                assert_eq!(init_v.len(), eval_res.len(), "evalue res: {:?}", eval_res);
                                avx512_bitwise_and(&mut init_v, eval_res.values());
                                Ok(init_v)
                            }
                        } else {
                            let eval_array = batch.column(c.index());
                            let eval_res = as_uint64_array(&eval_array).unwrap();
                            if eval_res.len() == 0 {
                                Ok(init_v)
                            } else {
                                avx512_bitwise_or(&mut init_v, eval_res.values());
                                Ok(init_v)
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Combined Primitives Expression
#[derive(Debug, Clone)]
pub struct BooleanEvalExpr {
    pub predicate: Option<Arc<SyncUnsafeCell<PhysicalPredicate>>>,
    pub valid_idx: Arc<DashSet<usize>>,
}

impl BooleanEvalExpr {
    pub fn new(predicate: Option<PhysicalPredicate>) -> Self {
        match predicate {
            Some(p) => {
                Self {
                    predicate: Some(Arc::new(SyncUnsafeCell::new(p))),
                    valid_idx: Arc::new(DashSet::new()),
                }
            }
            None => {
                Self {
                    predicate: None,
                    valid_idx: Arc::new(DashSet::new()),
                }
            }
        }
    }
}

impl std::fmt::Display for BooleanEvalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref predicate) = self.predicate {
            write!(f, "{:?}", unsafe{ &*(predicate.as_ref().get()) as &PhysicalPredicate })
        } else {
            write!(f, "None")
        }
    }
}

impl PhysicalExpr for BooleanEvalExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    
    fn data_type(&self, _input_schema: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::common::Result<bool> {
        Ok(false)
    }
    
    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        if let Some(ref predicate) = self.predicate {
            debug!("evalute batch len: {:}", batch.num_rows());
            let batch_len = batch.num_rows();
            let predicate = predicate.as_ref().get();
            let predicate_ref = unsafe {predicate.as_ref().unwrap() };
            match predicate_ref {
                PhysicalPredicate::And { .. } => {
                    let res = predicate_ref.eval(batch, vec![u64::MAX; batch_len], true)?;
                    debug!("res len: {:}", res.len());
                    let array = Arc::new(UInt64Array::from(res));
                    // debug!("evalute true count: {:}", &array.true_count());
                    Ok(ColumnarValue::Array(array))
                }
                PhysicalPredicate::Or { .. } => {
                    let res = predicate_ref.eval(batch, vec![0; batch_len], false)?;
                    let array = Arc::new(UInt64Array::from(res));
                    Ok(ColumnarValue::Array(array))
                }
                PhysicalPredicate::Leaf { primitive } => {
                    primitive.evaluate(batch)
                }
            }
        } else {
            let batch_len = batch.num_rows();
            Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(vec![0; batch_len]))))
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Internal(format!(
            "Don't support with_new_children in BooleanQueryExpr"
        )))
    }
}

impl PartialEq<dyn Any> for BooleanEvalExpr {
    fn eq(&self, _other: &dyn Any) -> bool {
        false
    }
}
#[inline(always)]
fn load_u64_slice(bitmap: &&[u64]) -> __m512i {
    unsafe { _mm512_loadu_epi64((*bitmap).as_ptr() as *const i64) }
}

#[cfg(test)]
mod tests {
    use std::{ptr::NonNull, sync::Arc};

    use datafusion::{arrow::{record_batch::RecordBatch, array::{BooleanArray, ArrayData}, buffer::Buffer, datatypes::{DataType, Schema, Field}}, physical_plan::{expressions::{col, BinaryExpr}, PhysicalExpr}, logical_expr::Operator};

    use crate::{ShortCircuit, jit::ast::Predicate, physical_expr::boolean_eval::SubPredicate};

    use super::{PhysicalPredicate, Primitives, BooleanEvalExpr};

    #[test]
    fn test_boolean_eval_simple() {
        let predicate = Predicate::And {
            args: vec![
                Predicate::Or { 
                    args: vec![
                        Predicate::Leaf { idx: 2 },
                        Predicate::Leaf { idx: 3 },
                    ]
                },
                Predicate::Leaf { idx: 4 },
            ]
        };
        let primitive = ShortCircuit::try_new(
            vec![1, 2, 3],
            predicate,
            4,
            3,
            2,
        ).unwrap();
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("test1", DataType::Boolean, false),
                Field::new("test2", DataType::Boolean, false),
                Field::new("test3", DataType::Boolean, false),
                Field::new("test4", DataType::Boolean, false),
                Field::new("test5", DataType::Boolean, false),
                Field::new("__temp__", DataType::Boolean, false),
            ])
        );
        let t1 = vec![0b1010_1110, 0b0011_1010];
        let t2 = vec![0b1100_0100, 0b1100_1011];
        let t3 = vec![0b0110_0100, 0b1100_0011];
        let t4 = vec![0b1011_0000, 0b0111_0110];
        let t5 = vec![0b1001_0011, 0b1100_0101];
        let t6 = vec![0b1111_1111, 0b1111_1111];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(u8_2_boolean(t1.clone())),
                Arc::new(u8_2_boolean(t2.clone())),
                Arc::new(u8_2_boolean(t3.clone())),
                Arc::new(u8_2_boolean(t4.clone())),
                Arc::new(u8_2_boolean(t5.clone())),
                Arc::new(u8_2_boolean(t6.clone())),
            ],
        ).unwrap();

        let op = |(v1, v2, v3, v4, v5): (u8, u8, u8, u8, u8)| v1 & (v2 | v3) & v4 & v5;
        let expect: Vec<u8> = (0..2).into_iter()
            .map(|v| (t1[v], t2[v], t3[v], t4[v], t5[v]))
            .map(op)
            .collect();
        let binary = BinaryExpr::new(col("test1", &schema).unwrap(), Operator::And, col("test5", &schema).unwrap());
        let sub_predicate1 = SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive: Primitives::BitwisePrimitive(binary) });
        let sub_predicate2 = SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive: Primitives::ShortCircuitPrimitive(primitive) });
        let physical_predicate = PhysicalPredicate::And {
            args: vec![
                sub_predicate1,
                sub_predicate2,
            ]
        };
        let res = BooleanEvalExpr::new(Some(physical_predicate)).evaluate(&batch).unwrap().into_array(0);
        let res = unsafe { res.data().buffers()[0].align_to::<u8>().1 };
        println!("left: {:b}, right: {:b}", res[0], expect[0]);
        assert_eq!(res, &expect);
    }

    fn u8_2_boolean(mut u8s: Vec<u8>) -> BooleanArray {
        let batch_len = u8s.len() * 8;
        let value_buffer = unsafe {
            let buf = Buffer::from_raw_parts(NonNull::new_unchecked(u8s.as_mut_ptr()), u8s.len(), u8s.capacity());
            std::mem::forget(u8s);
            buf
        };
        let builder = ArrayData::builder(DataType::Boolean)
            .len(batch_len)
            .add_buffer(value_buffer);
        let array_data = builder.build().unwrap();
        BooleanArray::from(array_data)
    }
}