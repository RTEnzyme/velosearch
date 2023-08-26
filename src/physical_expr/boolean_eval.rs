use std::{sync::Arc, any::Any};

use datafusion::{physical_plan::{expressions::BinaryExpr, PhysicalExpr, ColumnarValue}, arrow::{datatypes::{Schema, DataType}, record_batch::RecordBatch, array::BooleanArray}, error::DataFusionError};

use crate::ShortCircuit;
use crate::utils::Result;

#[derive(Clone, Debug)]
pub enum Primitives {
    BitwisePrimitive(BinaryExpr),
    ShortCircuitPrimitive(ShortCircuit)
}

#[derive(Clone, Debug)]
pub enum PhysicalPredicate {
    /// `And` Level
    And { args: Vec<PhysicalPredicate> },
    /// `Or` Level
    Or { args: Vec<PhysicalPredicate> },
    /// Leaf
    Leaf { primitive: Primitives },
}

impl PhysicalPredicate {
    fn eval(&self, batch: &RecordBatch, init_v: Vec<u8>, is_and: bool) -> Result<Vec<u8>> {
        let mut init_v = init_v;
        match self {
            PhysicalPredicate::And { args } => {
                for predicate in args {
                    init_v = predicate.eval(batch, init_v, true)?;
                }
                Ok(init_v)
            }
            PhysicalPredicate::Or { args } => {
                for predicate in args {
                    init_v = predicate.eval(batch, init_v, false)?;
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
                            s.eval(&mut init_v, batch);
                            Ok(init_v)
                        } else {
                            let mut init_v_or = vec![u8::MAX; init_v.len()];
                            s.eval(&mut init_v_or, batch);
                            for i in 0..init_v.len(){
                                init_v[i] |= init_v_or[i];
                            }
                            Ok(init_v)
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
    pub predicate: Arc<PhysicalPredicate>,
}

impl BooleanEvalExpr {
    pub fn new(predicate: Arc<PhysicalPredicate>) -> Self {
        Self { predicate }
    }
}

impl std::fmt::Display for BooleanEvalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.predicate)
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
        unimplemented!()
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
