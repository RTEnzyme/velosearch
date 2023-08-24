use std::{any::Any, ptr::NonNull, sync::Arc};

use datafusion::{physical_plan::{PhysicalExpr, ColumnarValue}, arrow::{datatypes::DataType, record_batch::RecordBatch, array::{BooleanArray, ArrayData}, buffer::Buffer}, error::DataFusionError};

use crate::jit::{ast::{Predicate, Boolean}, jit_short_circuit};
use crate::utils::Result;

#[derive(Debug, Clone)]
pub struct ShortCircuit {
    batch_idx: Vec<usize>,
    primitive: fn(*const *const u8, *const u8, *const u8, i64) -> (),
}

impl ShortCircuit {
    pub fn try_new(
        batch_idx: Vec<usize>,
        boolean_op_tree: Predicate,
        _node_num: usize,
        leaf_num: usize
    ) -> Result<Self> {
        // let primitive = if node_num <= JIT_MAX_NODES {
        //     // Seek the AOT compilation
        //     unimplemented!()
        // } else {
        //     // JIT compilation the BooleanOpTree
        //     jit_short_circuit(Boolean { predicate: boolean_op_tree }, leaf_num)?
        // };
        // JIT compilation the BooleanOpTree
        let primitive = jit_short_circuit(Boolean { predicate: boolean_op_tree }, leaf_num)?;
        Ok(Self {
            batch_idx,
            primitive,
        })
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
        (self.primitive)(batch.as_ptr(), init_v_ptr, res.as_ptr(), batch_len as i64);
        let value_buffer = unsafe {
            let buf = Buffer::from_raw_parts(NonNull::new_unchecked(res.as_mut_ptr()), res.len(), res.capacity());
            std::mem::forget(res);
            buf
        };
        let builder = ArrayData::builder(DataType::Boolean)
            .len(init_v.len())
            .add_buffer(value_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(array_data))))
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