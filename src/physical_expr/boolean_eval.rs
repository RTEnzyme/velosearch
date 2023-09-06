use std::{sync::Arc, any::Any, cell::SyncUnsafeCell};

use datafusion::{physical_plan::{expressions::{BinaryExpr, Column}, PhysicalExpr, ColumnarValue}, arrow::{datatypes::{Schema, DataType}, record_batch::RecordBatch}, error::DataFusionError, common::Result};
use tracing::debug;

use crate::ShortCircuit;
use crate::utils::array::build_boolean_array;

#[derive(Clone, Debug)]
pub enum Primitives {
    BitwisePrimitive(BinaryExpr),
    ShortCircuitPrimitive(ShortCircuit),
    ColumnPrimitive(Column),
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
    fn eval(&self, batch: &RecordBatch, init_v: Vec<u8>, is_and: bool) -> Result<Vec<u8>> {
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
                        let tmp = b.evaluate(batch)?
                        .into_array(0);
                        let res = unsafe { 
                            tmp.data()
                            .buffers()[0]
                            .align_to::<u8>().1
                        };
                        if is_and {
                            Ok(init_v.into_iter()
                            .zip(res.into_iter())
                            .map(|(i, j)| i & j)
                            .collect())
                        } else {
                            Ok(init_v.into_iter()
                            .zip(res.into_iter())
                            .map(|(i, j)| i | j)
                            .collect())
                        }
                    }
                    Primitives::ShortCircuitPrimitive(s) => {
                        if is_and {
                            Ok(s.eval(&mut init_v, batch))
                        } else {
                            let mut init_v_or = vec![u8::MAX; init_v.len()];
                            s.eval(&mut init_v_or, batch);
                            for i in 0..init_v.len(){
                                init_v[i] |= init_v_or[i];
                            }
                            Ok(init_v)
                        }
                    }
                    Primitives::ColumnPrimitive(c) => {
                        let eval_res = c.evaluate(batch)?.into_array(0).data().buffers()[0].to_vec();
                        if is_and {
                            Ok(eval_res
                                .into_iter()
                                .zip(init_v.into_iter())
                                .map(|(i, j)| i & j)
                                .collect()
                            )
                        } else {
                            Ok(eval_res
                                .into_iter()
                                .zip(init_v.into_iter())
                                .map(|(i, j)| i | j)
                                .collect()
                            )
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
    pub predicate: Arc<SyncUnsafeCell<PhysicalPredicate>>,
}

impl BooleanEvalExpr {
    pub fn new(predicate: PhysicalPredicate) -> Self {
        Self {
            predicate: Arc::new(SyncUnsafeCell::new(predicate)),
        }
    }
}

impl std::fmt::Display for BooleanEvalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", unsafe{ &*(self.predicate.as_ref().get()) as &PhysicalPredicate })
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
        let batch_len = (batch.num_rows() + 7) / 8;
        let predicate = self.predicate.as_ref().get();
        let predicate_ref = unsafe {predicate.as_ref().unwrap() };
        match predicate_ref {
            PhysicalPredicate::And { .. } => {
                let res = predicate_ref.eval(batch, vec![u8::MAX; batch_len], true)?;
                let array = Arc::new(build_boolean_array(res, batch.num_rows()));
                Ok(ColumnarValue::Array(array))
            }
            PhysicalPredicate::Or { .. } => {
                let res = predicate_ref.eval(batch, vec![0; batch_len], false)?;
                let array = Arc::new(build_boolean_array(res, batch.num_rows()));
                Ok(ColumnarValue::Array(array))
            }
            PhysicalPredicate::Leaf { primitive } => {
                primitive.evaluate(batch)
            }
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
        let res = BooleanEvalExpr::new(physical_predicate).evaluate(&batch).unwrap().into_array(0);
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