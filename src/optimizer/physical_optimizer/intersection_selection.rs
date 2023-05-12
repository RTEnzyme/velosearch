//! IntersectionSelection optimizer that choose the better algorithm
//! from `PIA` and `VIA`

use std::sync::Arc;

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{rewrite::{TreeNodeRewriter, RewriteRecursion, TreeNodeRewritable}, ExecutionPlan, boolean::BooleanExec}};
use datafusion::common::Result;
/// Optimizer rule that choose intersection algorithm using selectivity
#[derive(Default)]
pub struct IntersectionSelection {}

impl IntersectionSelection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for IntersectionSelection {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            if let Some(boolean) = plan.as_any().downcast_ref::<BooleanExec>() {
                unimplemented!()
            } else {
                Ok(None)
            }
        })
    }

    fn name(&self) -> &str {
        "IntersectionSelection"
    }

    fn schema_check(&self) -> bool {
        false
    }
}