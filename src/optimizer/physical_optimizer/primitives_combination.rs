//! PrimitivesCombination optimizer that combining the bitwise primitves
//! and short-circuit primitive according the cost per operation (cpo).

use std::{sync::Arc, collections::HashSet};

use dashmap::DashSet;
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
                            debug!("optimize posting_exec predicate");
                            let predicate = predicate.get();
                            let valid_idx = optimize_predicate_inner(unsafe{predicate.as_mut()}.unwrap());
                            for i in valid_idx {
                                p.valid_idx.insert(i);
                            }
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

fn optimize_predicate_inner(predicate: &mut PhysicalPredicate) -> DashSet<usize> {
    let mut valid_batch_idx: DashSet<usize> = DashSet::new();
    match predicate {
        PhysicalPredicate::And { args } => {
            // The first level is `AND`.
            let mut node_num = 0;
            let mut leaf_num = 0;
            // let mut cum_instructions: f64 = 0.;
            let mut cnf = Vec::new();
            let mut optimized_args = Vec::new();
            let mut combine_num = 0;
            for node in args.iter_mut().rev() {
                if node.node_num() >= JIT_MAX_NODES {
                    // If this node oversize the JIT_MAX_NDOES, skip this node
                    valid_batch_idx.extend(optimize_predicate_inner(&mut node.sub_predicate).into_iter());
                    optimized_args.push(SubPredicate::new_with_predicate(node.sub_predicate.to_owned()));
                    combine_num += 1;

                    continue;
                }
                if node_num + node.node_num() > JIT_MAX_NODES {
                    // The number of cumulative node is larger than AOT node num.
                    // So it should compact to short-circuit primitive
                    combine_num += cnf.len();
                    let short_circuit = ShortCircuit::new(&cnf, node_num, leaf_num);
                    valid_batch_idx.extend(short_circuit.batch_idx.iter().cloned());
                    let primitive = Primitives::ShortCircuitPrimitive(short_circuit);
                    optimized_args.push(SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive }));
                    cnf.clear();
                    node_num = 0;
                    leaf_num = 0;
                    // cum_instructions = 0.;
                    continue;
                }
                if node.rank < -1.99 {
                    if node_num < 2 {
                        break;
                    }
                    combine_num += cnf.len();
                    let short_circuit = ShortCircuit::new(&cnf, node_num, leaf_num);
                    valid_batch_idx.extend(short_circuit.batch_idx.iter().cloned());
                    let primitive = Primitives::ShortCircuitPrimitive(short_circuit);
                    optimized_args.push(SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive }));
                    cnf.clear();
                    node_num = 0;
                    leaf_num = 0;
                    break;
                }
                cnf.push(&node.sub_predicate);
                node_num += node.node_num();
                leaf_num += node.leaf_num();
            }
            if cnf.len() >= 2 {
                combine_num += cnf.len();
                let short_circuit = ShortCircuit::new(&cnf, node_num, leaf_num);
                valid_batch_idx.extend(short_circuit.batch_idx.iter().cloned());
                let primitive = Primitives::ShortCircuitPrimitive(short_circuit);
                optimized_args.push(SubPredicate::new_with_predicate(PhysicalPredicate::Leaf { primitive }));
            }
            args.truncate(args.len() - combine_num);
            args.append(&mut optimized_args);
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
    valid_batch_idx
}