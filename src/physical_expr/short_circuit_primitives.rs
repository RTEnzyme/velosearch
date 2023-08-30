use std::{any::Any, ptr::NonNull, sync::Arc};

use datafusion::{physical_plan::{PhysicalExpr, ColumnarValue}, arrow::{datatypes::DataType, record_batch::RecordBatch, array::{BooleanArray, ArrayData}, buffer::Buffer}, error::DataFusionError};

use crate::{jit::{ast::{Predicate, Boolean}, jit_short_circuit}, JIT_MAX_NODES};
use crate::utils::Result;

use super::{boolean_eval::PhysicalPredicate, Primitives};

#[derive(Debug, Clone)]
pub struct ShortCircuit {
    batch_idx: Vec<usize>,
    primitive: fn(*const *const u8, *const u8, *mut u8, i64) -> (),
}

impl ShortCircuit {
    pub fn try_new(
        batch_idx: Vec<usize>,
        boolean_op_tree: Predicate,
        _node_num: usize,
        leaf_num: usize,
        start_idx: usize,
    ) -> Result<Self> {
        // let primitive = if node_num <= JIT_MAX_NODES {
            // Seek the AOT compilation
        //     unimplemented!()
        // } else {
        //     // JIT compilation the BooleanOpTree
        //     jit_short_circuit(Boolean { predicate: boolean_op_tree }, leaf_num)?
        // };
        // JIT compilation the BooleanOpTree
        let primitive = jit_short_circuit(Boolean { predicate: boolean_op_tree, start_idx}, leaf_num)?;
        Ok(Self {
            batch_idx,
            primitive,
        })
    }

    pub fn new(
        cnf: &Vec<&PhysicalPredicate>,
        node_num: usize,
        leaf_num: usize,
    ) -> Self {
        if node_num <= JIT_MAX_NODES {
            let mut builder = LoudsBuilder::new(node_num);
            predicate_2_louds(cnf[0], &mut builder);
            let louds = builder.build();
            unimplemented!()
        }
        let (predicate, batch_idx) = convert_predicate(&cnf);
        Self::try_new(batch_idx, predicate, node_num + 1, leaf_num, 0).unwrap()
    }

    pub fn eval(&self, init_v: &mut Vec<u8>, batch: &RecordBatch) -> Vec<u8> {
        let batch_len = init_v.len();
        let batch: Vec<*const u8> = self.batch_idx
            .iter()
            .map(|v| {
                unsafe {
                    batch.column(*v).data().buffers()[0].align_to::<u8>().1.as_ptr()
                }
            })
            .collect();
        let mut res: Vec<u8> = vec![0; batch_len];
        (self.primitive)(batch.as_ptr(), init_v.as_ptr(), res.as_mut_ptr(), batch_len as i64);
        res
    }
}

impl PhysicalExpr for ShortCircuit {
    fn as_any(&self) -> &dyn std::any::Any {
       self 
    }

    fn data_type(&self, _input_schema: &datafusion::arrow::datatypes::Schema) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &datafusion::arrow::datatypes::Schema) -> datafusion::error::Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::error::Result<ColumnarValue> {
        let init_v = batch.column_by_name("__temp__").ok_or(DataFusionError::Internal(format!("Should have `init_v` as the input param in batch")))?;
        let batch_len = init_v.len() / 8;
        let batch: Vec<*const u8> = self.batch_idx
            .iter()
            .map(|v| {
                unsafe {
                    batch.column(*v).data().buffers()[0].align_to::<u8>().1.as_ptr()
                }
            })
            .collect();
        let init_v_ptr = unsafe { init_v.data().buffers()[0].align_to::<u8>().1.as_ptr() };
        let mut res: Vec<u8> = vec![0; batch_len];
        (self.primitive)(batch.as_ptr(), init_v_ptr, res.as_mut_ptr(), batch_len as i64);
        let res = build_boolean_array(res, init_v.len());
        Ok(ColumnarValue::Array(Arc::new(res)))
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::Internal(format!(
            "Don't support with_new_children in ShortCircuit"
        )))
    }
}

impl std::fmt::Display for ShortCircuit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Use short-circuit primitive")
    }
}

impl PartialEq<dyn Any> for ShortCircuit {
    fn eq(&self, _other: &dyn Any) -> bool {
        false
    }
}

#[inline]
fn build_boolean_array(mut res: Vec<u8>, array_len: usize) -> BooleanArray {
    let value_buffer = unsafe {
        let buf = Buffer::from_raw_parts(NonNull::new_unchecked(res.as_mut_ptr()), res.len(), res.capacity());
        std::mem::forget(res);
        buf
    };
    let builder = ArrayData::builder(DataType::Boolean)
        .len(array_len)
        .add_buffer(value_buffer);

    let array_data = unsafe { builder.build_unchecked() };
    BooleanArray::from(array_data)
}

fn convert_predicate(cnf: &Vec<&PhysicalPredicate>) -> (Predicate, Vec<usize>) {
    let mut batch_idx = Vec::new();
    let mut predicates = Vec::new();
    let mut start_idx = 0;
    for forumla in cnf.into_iter().rev() {
        predicates.push(physical_2_logical(&forumla, &mut batch_idx, &mut start_idx));
    }
    let predicate = Predicate::And { args: predicates };
    (predicate, batch_idx)
}

fn physical_2_logical(
    physical_predicate: &PhysicalPredicate,
    batch_idx: &mut Vec<usize>,
    start_idx: &mut usize,
) -> Predicate {
    match physical_predicate {
        PhysicalPredicate::And { args } => {
            let mut predicates = Vec::new();
            for arg in args {
                predicates.push(physical_2_logical(&arg.sub_predicate, batch_idx, start_idx));
                *start_idx += 1;
            }
            Predicate::And { args: predicates }
        }
        PhysicalPredicate::Or { args } => {
            let mut predicates = Vec::new();
            for arg in args {
                predicates.push(physical_2_logical(&arg.sub_predicate, batch_idx, start_idx));
                *start_idx += 1;
            }
            Predicate::Or { args: predicates }
        }
        PhysicalPredicate::Leaf { primitive } => {
            let idx = if let Primitives::ColumnPrimitive(c) = primitive {
                c.index()
            } else {
                unreachable!("The subtree of a short-circuit primitive must have not other exprs");
            };
            batch_idx.push(idx);
            Predicate::Leaf { idx: *start_idx }
        }
    }
}

struct LoudsBuilder {
    node_num: usize,
    pos: usize,
    has_child: u16,
    louds: u16,
}

impl LoudsBuilder {
    fn new(node_num: usize)  -> Self {
        assert!(node_num < 14, "Only supports building LOUDS with less 14 nodes");
        Self {
            node_num,
            pos: 0,
            has_child: 0,
            louds: 0,
        }
    }

    fn set_haschild(&mut self, has_child: bool) {
        self.has_child |= 1 << self.pos;
    }

    fn set_louds(&mut self, louds: bool) {
        self.louds |= 1 << self.pos;
    }

    fn next(&mut self) {
        self.pos += 1;
    }

    fn build(self) -> u32 {
        let mut louds = (self.node_num as u32) << 28;
        louds |= (self.louds as u32) << 14;
        louds |= self.has_child as u32;
        louds
    }
}

fn predicate_2_louds(predicate: &PhysicalPredicate, builder: &mut LoudsBuilder) {
    match predicate {
        PhysicalPredicate::And { args } => {
            // This node is `AND` node, so it has children.
            builder.set_haschild(true);
            // The LOUDS of first child node is Set bit.
            builder.next();
            builder.set_louds(true);
            for child in &args[1..] {
                builder.next();
                builder.set_louds(false);
                predicate_2_louds(&child.sub_predicate, builder);
            }
        }
        PhysicalPredicate::Or { args } => {
            // This node is `OR` node, so it has children.
            builder.set_haschild(true);
            // The LOUDS of first child node is Set.
            builder.next();
            builder.set_louds(true);
            for child in &args[1..] {
                builder.next();
                builder.set_louds(false);
                predicate_2_louds(&child.sub_predicate, builder);
            }
        }
        PhysicalPredicate::Leaf { .. } => {
            builder.set_haschild(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, ptr::NonNull};

    use datafusion::{physical_plan::PhysicalExpr, arrow::{datatypes::{Schema, Field, DataType}, record_batch::RecordBatch, array::{BooleanArray, ArrayData}, buffer::Buffer}};

    use crate::{jit::ast::Predicate, ShortCircuit};

    #[test]
    fn test_short_circuit() {
        let predicate = Predicate::And {
            args: vec![
                Predicate::Leaf { idx: 0 },
                Predicate::Or { 
                    args: vec![
                        Predicate::Leaf { idx: 1 },
                        Predicate::Leaf { idx: 2 },
                    ]
                },
                Predicate::Leaf { idx: 3 },
            ]
        };
        let primitive = ShortCircuit::try_new(
            vec![0, 1, 2, 3],
            predicate,
            5,
            4,
            0,
        ).unwrap();
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("test1", DataType::Boolean, false),
                Field::new("test2", DataType::Boolean, false),
                Field::new("test3", DataType::Boolean, false),
                Field::new("test4", DataType::Boolean, false),
                Field::new("__temp__", DataType::Boolean, false),
            ])
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(u8_2_boolean(vec![0b1010_1110, 0b0011_1010])),
                Arc::new(u8_2_boolean(vec![0b1100_0100, 0b1100_1011])),
                Arc::new(u8_2_boolean(vec![0b0110_0100, 0b1100_0011])),
                Arc::new(u8_2_boolean(vec![0b1011_0000, 0b0111_0110])),
                Arc::new(u8_2_boolean(vec![0b1111_1111, 0b1111_1111])),
            ],
        ).unwrap();
        let res = primitive.evaluate(&batch).unwrap().into_array(0);
        let res = unsafe {res.data().buffers()[0].align_to::<u8>().1 };
        assert_eq!(res, &[0b1010_0000, 0b0000_0010])
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