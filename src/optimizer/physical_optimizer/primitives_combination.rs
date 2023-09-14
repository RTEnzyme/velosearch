//! PrimitivesCombination optimizer that combining the bitwise primitves
//! and short-circuit primitive according the cost per operation (cpo).

use std::sync::Arc;

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{ExecutionPlan, boolean::BooleanExec, rewrite::TreeNodeRewritable}};
use tracing::debug;

use crate::{physical_expr::{BooleanEvalExpr, boolean_eval::{PhysicalPredicate, SubPredicate}, Primitives}, JIT_MAX_NODES, ShortCircuit, datasources::posting_table::PostingExec};

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
        plan.transform_down(&|plan| {
            if let Some(boolean) = plan.as_any().downcast_ref::<PostingExec>() {
                // let boolean_eval = boolean.predicate[&0].clone();
                // let boolean_eval = boolean_eval.as_any().downcast_ref::<BooleanEvalExpr>();
                match &boolean.predicate {
                    Some(p) => {
                        if let Some(ref predicate) = p.predicate {
                            let predicate = predicate.get();
                            optimize_predicate_inner(unsafe{predicate.as_mut()}.unwrap());
                            Ok(Some(plan))
                        } else {
                            Ok(Some(plan))
                        }
                    }
                    None => Ok(Some(plan))
                }
            } else {
                Ok(None)
            }
        })
    }

    fn name(&self) -> &str {
        "PrimitivesCombination"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

fn optimize_predicate_inner(predicate: &mut PhysicalPredicate) {
    match predicate {
        PhysicalPredicate::And { args } => {
            // The first level is `AND`.
            let mut node_num = 0;
            let mut leaf_num = 0;
            let mut cum_instructions: f64 = 0.;
            let mut cnf = Vec::new();
            let mut idx = args.len() - 1;
            let mut optimized_args = Vec::new();
            for node in args.iter_mut().rev() {
                if node.node_num() >= JIT_MAX_NODES {
                    // If this node oversize the JIT_MAX_NDOES, skip this node
                    optimize_predicate_inner(&mut node.sub_predicate);
                    optimized_args.push(SubPredicate::new_with_predicate(node.sub_predicate.to_owned()));
                    idx -= 1;
                    continue;
                }
                if node_num + node.node_num() > JIT_MAX_NODES {
                    // The number of cumulative node is larger than AOT node num.
                    // So it should compact to short-circuit primitive
                    let primitive = Primitives::ShortCircuitPrimitive(ShortCircuit::new(&cnf, node_num, leaf_num));
                    optimized_args.push(SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive }));
                    cnf.clear();
                    node_num = 0;
                    leaf_num = 0;
                    cum_instructions = 0.;
                    idx -= 1;
                    continue;
                }
                cum_instructions += node.cs * node.leaf_num as f64 ;
                // If cpo > threshold, end this optimization stage
                if (cum_instructions + node.cs) / (leaf_num as f64 - 1. + node.leaf_num as f64) > 0.5 {
                    if node_num < 2 {
                        break;
                    }
                    idx -= 1;
                    let primitive = Primitives::ShortCircuitPrimitive(ShortCircuit::new(&cnf, node_num, leaf_num));
                    optimized_args.push(SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive }));
                    break;
                }
                cnf.push(&node.sub_predicate);
                node_num += node.node_num();
                leaf_num += node.leaf_num();
                idx -= 1;
            }
            args.truncate(idx + 1);
            args.append(&mut optimized_args)
        }
        PhysicalPredicate::Or { args } => {
            for arg in args {
                optimize_predicate_inner(&mut arg.sub_predicate);
            }
        }
        PhysicalPredicate::Leaf { .. } => {
            // The first level is only one node.
            debug!("Skip optimize predicate because the first level is only one node.");
        }
    }
}