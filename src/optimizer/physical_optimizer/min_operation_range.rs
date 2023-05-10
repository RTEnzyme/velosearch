//! IntersectionSelection optimizer that obtain the minimal valid range
//! of CNF predicate

use std::sync::Arc;

use datafusion::{
    physical_optimizer::PhysicalOptimizerRule, 
    physical_plan::{rewrite::{TreeNodeRewriter, RewriteRecursion, TreeNodeRewritable}, 
    ExecutionPlan, boolean::{BooleanExec, TermMeta}}, error::DataFusionError, 
    physical_expr::BooleanQueryExpr, 
    arrow::{datatypes::Schema, array::{BooleanArray, ArrayRef}, record_batch::RecordBatch}
};
use datafusion::common::Result;
use rayon::prelude::*;

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
    partition_stats: Option<Vec<Vec<TermMeta>>>,
    partition_schema: Option<Arc<Schema>>,
    predicate: Option<Arc<BooleanQueryExpr>>,
    min_range: Option<Vec<BooleanArray>>,
}

impl GetMinRange {
    fn new() -> Self {
        Self {
            partition_stats: None,
            partition_schema: None,
            predicate: None,
            min_range: None,
        }
    }
}

impl TreeNodeRewriter<Arc<dyn ExecutionPlan>> for GetMinRange {
    /// Invoked before (Preorder) any children of `node` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(&mut self, node: &Arc<dyn ExecutionPlan>) -> Result<RewriteRecursion> {
        let any_node = node.as_any();
        if let Some(boolean) = any_node.downcast_ref::<BooleanExec>() {
            self.predicate = Some(Arc::new(boolean.predicate().clone()));
            Ok(RewriteRecursion::Mutate)
        } else if let Some(posting) = any_node.downcast_ref::<PostingExec>(){
            let projected_schema = posting.schema();
            self.partition_schema = Some(projected_schema.clone());
            let project_terms: Vec<&str> = projected_schema.fields().into_iter().map(|f| f.name().as_str()).collect();
            let partition_num = posting.output_partitioning().partition_count();
            self.partition_stats = Some(
                (0..partition_num).into_iter()
                .map(|p| {
                    posting.term_metas_of(&project_terms, p)
                })
                .collect()
            );
            Ok(RewriteRecursion::Mutate)
        } else {
            Ok(RewriteRecursion::Continue)
        }
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Some(boolean) = node.as_any().downcast_ref::<BooleanExec>() {
            let term_stats = self.partition_stats.take().expect("Should get the terms stats");
            let global_predicate = boolean.predicate.get(&0).expect("Should get the global predicate");
            let global_predicate = match global_predicate.as_any().downcast_ref::<BooleanQueryExpr>() {
                Some(predicate) => Ok(predicate),
                None => Err(DataFusionError::Internal(format!("Can't downcast to BooleanQueryExpr"))),
            }?;
            let partition_schema = self.partition_schema.take().expect("Should get the schema").clone();
            let partition_range: Vec<BooleanArray> = (0..term_stats.len())
                .into_par_iter()
                .map(|p| {
                    let mut range: Vec<ArrayRef> = Vec::new();
                    for t in &term_stats[p] {
                        range.push(t.distribution());
                    }
                    let batch = RecordBatch::try_new(
                        partition_schema.clone(),
                        range,
                    ).unwrap();
                    global_predicate.eval(batch).unwrap()
                })
                .collect();
            self.min_range = Some(partition_range);
            Ok(Arc::new(BooleanExec::try_new(
                boolean.predicate.to_owned(),
                boolean.input().clone(),
                boolean.is_via.to_owned(),
                Some(term_stats),
            )?))
        } else if let Some(posting) = node.as_any().downcast_ref::<PostingExec>() {
            let min_range = self.min_range.take().expect("Min range shouldn't be null");
            Ok(Arc::new(PostingExec::try_new(
                posting.partitions.to_owned(),
                posting.term_idx.to_owned(),
                posting.schema.to_owned(),
                posting.projection.to_owned(),
                Some(min_range),
            )?))
        }else {
            Err(DataFusionError::Internal(format!(
                "Should get terms statistics from PostingExec."
            )))
        }
    }
}