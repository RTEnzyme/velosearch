//! IntersectionSelection optimizer that choose the better algorithm
//! from `PIA` and `VIA`

use datafusion::physical_optimizer::PhysicalOptimizerRule;

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
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        todo!()
    }

    fn name(&self) -> &str {
        "IntersectionSelection"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
