//! IntersectionSelection optimizer that choose the better algorithm
//! from `PIA` and `VIA`

use std::{sync::Arc, collections::HashMap};

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{ExecutionPlan, boolean::BooleanExec, rewrite::TreeNodeRewritable, PhysicalExpr}, physical_expr::BooleanQueryExpr};
use datafusion::common::Result;

use crate::{datasources::posting_table::PostingExec, jit::create_boolean_query_fn};
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
        if let Some(_) = plan.as_any().downcast_ref::<BooleanExec>() {
            plan.transform_up(&|p| {
                if let Some(_) = p.as_any().downcast_ref::<PostingExec>() {
                    Ok(None)
                } else if let Some(boolean) = p.as_any().downcast_ref::<BooleanExec>() {
                    let predicates: HashMap<usize, Arc<dyn PhysicalExpr>> = boolean
                    .predicate
                    .iter()
                    .map(|(i, p)| {
                        if let Some(expr) = p.as_any().downcast_ref::<BooleanQueryExpr>() {
                            if let Some(ref cnf) = expr.cnf_predicates {
                                if cnf[0].selectivity() < 2.05  {
                                    let gen_fn = create_boolean_query_fn(
                                        cnf,
                                    );
                                    return (*i, Arc::new(BooleanQueryExpr::new_with_fn(expr.predicate_tree.clone(), gen_fn)) as Arc<dyn PhysicalExpr>);
                                }
                            }
                        }
                        (*i, p.clone())
                    })
                    .collect();
                    Ok(Some(Arc::new(
                        BooleanExec::try_new(
                            predicates,
                            boolean.input.clone(),
                            boolean.terms_stats.clone(),
                            boolean.is_score,
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