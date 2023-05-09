//! IntersectionSelection optimizer that obtain the minimal valid range
//! of CNF predicate

use std::sync::Arc;

use datafusion::{physical_optimizer::PhysicalOptimizerRule, physical_plan::{rewrite::{TreeNodeRewriter, RewriteRecursion, TreeNodeRewritable}, ExecutionPlan, boolean::{BooleanExec, TermMeta}}, error::DataFusionError};
use datafusion::common::Result;

use crate::datasources::posting_table::PostingExec;

/// Optimizer rule that get the minimal valid range of CNF predicate
#[derive(Default)]
pub struct MinOperationRange {}

impl MinOperationRange {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for MinOperationRange {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        plan.transform_using(&mut GetMinRange::new())
    }

    fn name(&self) -> &str {
        "MinOperationRange"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[derive(Clone)]
struct GetMinRange {
    partition_stats: Option<Vec<Vec<Option<TermMeta>>>>,
}

impl GetMinRange {
    fn new() -> Self {
        Self {
            partition_stats: None
        }
    }
}

impl TreeNodeRewriter<Arc<dyn ExecutionPlan>> for GetMinRange {
    /// Invoked before (Preorder) any children of `node` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(&mut self, node: &Arc<dyn ExecutionPlan>) -> Result<RewriteRecursion> {
        let any_node = node.as_any();
        if any_node.downcast_ref::<BooleanExec>().is_some() {
            Ok(RewriteRecursion::Continue)
        } else if let Some(posting) = any_node.downcast_ref::<PostingExec>(){
            let projected_schema = posting.schema();
            let project_terms: Vec<&str> = projected_schema.fields().into_iter().map(|f| f.name().as_str()).collect();
            let partition_num = posting.output_partitioning().partition_count();
            self.partition_stats = Some(
                (0..partition_num).into_iter()
                .map(|p| {
                    posting.term_metas_of(&project_terms, p)
                })
                .collect()
            );
            Ok(RewriteRecursion::Stop)
        } else {
            Ok(RewriteRecursion::Continue)
        }
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Some(boolean) = node.as_any().downcast_ref::<BooleanExec>() {
            let term_stats = self.partition_stats.take().expect("Should get the terms stats");
            
            Ok(Arc::new(BooleanExec::try_new(
                boolean.predicate.to_owned(),
                boolean.input().clone(),
                boolean.is_via.to_owned(),
                Some(term_stats),
            )?))
        } else {
            Err(DataFusionError::Internal(format!(
                "Should get terms statistics from PostingExec."
            )))
        }
    }
}