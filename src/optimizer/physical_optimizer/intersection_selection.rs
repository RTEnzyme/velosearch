//! IntersectionSelection optimizer that choose the better algorithm
//! from `PIA` and `VIA`

use std::{sync::Arc, unimplemented};

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{rewrite::{TreeNodeRewriter, RewriteRecursion, TreeNodeRewritable}, ExecutionPlan, boolean::BooleanExec}, physical_expr::BooleanQueryExpr};
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
        if let Some(boolean) = plan.as_any().downcast_ref::<BooleanExec>() {
            let is_via = boolean
                .predicate
                .values()
                .map(|p| {
                    if let Some(boolean) = p.as_any().downcast_ref::<BooleanQueryExpr>() {
                        if boolean.cnf_predicates[0].selectivity() < 0.05 {
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                })
                .collect();
            Ok(Arc::new(
                BooleanExec::try_new(
                    boolean.predicate.to_owned(),
                    boolean.input.clone(),
                    Some(is_via),
                    boolean.terms_stats.clone(),
                )?
            ))
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "IntersectionSelection"
    }

    fn schema_check(&self) -> bool {
        false
    }
}