//! IntersectionSelection optimizer that choose the better algorithm
//! from `PIA` and `VIA`

use std::sync::Arc;

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{ExecutionPlan, boolean::BooleanExec, rewrite::TreeNodeRewritable}, physical_expr::BooleanQueryExpr};
use datafusion::common::Result;

use crate::datasources::posting_table::PostingExec;
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
            let is_via: Vec<bool> = boolean
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
            plan.transform_up(&|p| {
                if let Some(posting) = p.as_any().downcast_ref::<PostingExec>() {
                    let mut input = (*posting).clone();
                    input.is_via = Some(is_via.clone());
                    Ok(Some(Arc::new(
                        PostingExec::try_new(
                            input.partitions.to_owned(),
                            input.term_idx,
                            input.schema,
                            input.projection,
                            input.partition_min_range,
                            Some(is_via.clone()),
                        )?
                    )))
                } else if let Some(boolean) = p.as_any().downcast_ref::<BooleanExec>() {
                    Ok(Some(Arc::new(
                        BooleanExec::try_new(
                            boolean.predicate.clone(),
                            boolean.input.clone(),
                            Some(is_via.clone()),
                            boolean.terms_stats.clone(),
                        )?
                    )))
                } else {
                    Ok(None)
                }
            })
               
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