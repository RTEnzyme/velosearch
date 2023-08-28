//! PrimitivesCombination optimizer that combining the bitwise primitves
//! and short-circuit primitive according the cost per operation (cpo).

use std::sync::Arc;

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{ExecutionPlan, boolean::BooleanExec}};

use crate::physical_expr::BooleanEvalExpr;

/// PrimitivesCombination optimizer that optimizes the combination of 
/// bitwise primitives and short-circuit primitive.
#[derive(Default)]
pub struct PrimitivesCombination {}

impl PrimitivesCombination {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for PrimitivesCombination {
    fn optimize(
        &self,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Some(boolean) = plan.as_any().downcast_ref::<BooleanExec>() {
            let predicate = boolean.predicate[&0].clone();
            let predicate = predicate.as_any().downcast_ref::<BooleanEvalExpr>();
            match predicate {
                Some(p) => {
                    
                    Ok(plan)
                }
                None => Ok(plan)
            }
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "PrimitivesCombination"
    }

    fn schema_check(&self) -> bool {
        false
    }
}