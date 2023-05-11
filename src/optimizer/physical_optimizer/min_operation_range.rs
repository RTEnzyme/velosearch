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
use tracing::debug;

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
            debug!("Pre_visit BooleanExec");
            self.predicate = Some(Arc::new(boolean.predicate().clone()));
            Ok(RewriteRecursion::Continue)
        } else if let Some(posting) = any_node.downcast_ref::<PostingExec>(){
            debug!("Pre_visit PostingExec");
            let projected_schema = posting.schema();
            self.partition_schema = Some(projected_schema.clone());
            let project_terms: Vec<&str> = projected_schema.fields().into_iter().map(|f| f.name().as_str()).collect();
            let partition_num = posting.output_partitioning().partition_count();
            let term_stats: Vec<Vec<TermMeta>> = 
                (0..partition_num).into_iter()
                .map(|p| {
                    posting.term_metas_of(&project_terms, p)
                })
                .collect();
            let partition_range: Vec<BooleanArray> = (0..term_stats.len())
                .into_par_iter()
                .map(|p| {
                    let mut range: Vec<ArrayRef> = Vec::new();
                    for t in &term_stats[p] {
                        range.push(t.distribution());
                    }
                    let batch = RecordBatch::try_new(
                        projected_schema.clone(),
                        range,
                    ).unwrap();
                    self.predicate.as_ref().unwrap().eval(batch).unwrap()
                })
                .collect();
            self.min_range = Some(partition_range);
            self.partition_stats = Some(term_stats);
            Ok(RewriteRecursion::Continue)
        } else {
            Ok(RewriteRecursion::Continue)
        }
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Some(boolean) = node.as_any().downcast_ref::<BooleanExec>() {
            debug!("Mutate BooleanExec");
            let term_stats = match self.partition_stats.take() {
                Some(s) => s,
                None => return Err(DataFusionError::Internal(format!("Term_stats shouldn't be null"))),
            };
            
            Ok(Arc::new(BooleanExec::try_new(
                boolean.predicate.to_owned(),
                boolean.input().clone(),
                boolean.is_via.to_owned(),
                Some(term_stats),
            )?))
        } else if let Some(posting) = node.as_any().downcast_ref::<PostingExec>() {
            debug!("Mutate PostingExec");
            let min_range = self.min_range.take();
            Ok(Arc::new(PostingExec::try_new(
                posting.partitions.to_owned(),
                posting.term_idx.to_owned(),
                posting.schema.to_owned(),
                posting.projection.to_owned(),
                min_range,
            )?))
        }else {
            Err(DataFusionError::Internal(format!(
                "Should get terms statistics from PostingExec."
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, collections::HashMap};

    use datafusion::{arrow::{datatypes::{SchemaRef, Field, Schema, DataType}, record_batch::RecordBatch, array::{UInt16Array, BooleanArray}}, from_slice::FromSlice, physical_plan::{boolean::{TermMeta, BooleanExec}, expressions::{col, lit}, ExecutionPlan}, physical_expr::{BooleanQueryExpr, boolean_query}, logical_expr::Operator, physical_optimizer::PhysicalOptimizerRule, config::ConfigOptions};
    use learned_term_idx::TermIdx;
    use tracing::Level;

    use crate::{datasources::posting_table::PostingExec, batch::{PostingBatch, BatchRange}, MinOperationRange};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("c", DataType::Boolean, true),
            Field::new("d", DataType::Boolean, true),
        ]))
    }

    macro_rules! array {
        ($slice:expr) => {
            Arc::new(UInt16Array::from_slice($slice))
        };
    }

    fn partition_batches() -> Vec<Vec<PostingBatch>> {
        let schema = schema();
        let range1 = Arc::new(BatchRange::new(0, 10));
        let range2 = Arc::new(BatchRange::new(10, 20));
        let range3 = Arc::new(BatchRange::new(20, 30));
        let range4 = Arc::new(BatchRange::new(30, 40));
        vec![
            vec![
                PostingBatch::try_new(
                    schema.clone(),
                    vec![
                        array!(&[0, 2, 4, 7]),
                        array!(&[1, 6, 7]),
                        array!(&[2, 6, 8]),
                        array!(&[3, 7, 9]),
                    ],
                    range1.clone(),
                ).unwrap(),
                PostingBatch::try_new(
                    Arc::new(schema.clone().project(&[1, 3]).unwrap()),
                    vec![
                        array!(&[11, 16, 17]),
                        array!(&[12, 14, 18]),
                    ],
                    range2.clone(),
                ).unwrap(),
            ],
            vec![
                PostingBatch::try_new(
                    schema.clone(),
                    vec![
                        array!(&[21, 26, 29]),
                        array!(&[22, 25]),
                        array!(&[22, 26]),
                        array!(&[23, 24, 27]),
                    ],
                    range3.clone(),
                ).unwrap(),
                PostingBatch::try_new(
                    Arc::new(schema.clone().project(&[1, 2, 3]).unwrap()),
                    vec![
                        array!(&[30, 31, 36]),
                        array!(&[31, 32, 37]),
                        array!(&[32, 33, 36]),
                    ],
                    range4.clone()
                ).unwrap(),
            ]
        ]
    }

    fn posting_exec() -> Arc<PostingExec> {
        let term_idx1: HashMap<String, TermMeta> = vec![
            ("a".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, false])), index: vec![], nums:4 }),
            ("b".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: vec![], nums: 6}),
            ("c".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, false])), index: vec![], nums: 3}),
            ("d".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: vec![], nums: 6}),
        ].into_iter().collect();
        let term_idx2: HashMap<String, TermMeta> = vec![
            ("a".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, false])), index: vec![], nums: 3}),
            ("b".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: vec![], nums: 5}),
            ("c".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: vec![], nums: 5}),
            ("d".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: vec![], nums: 5}),
        ].into_iter().collect();
        let term_idx = vec![Arc::new(TermIdx {term_map: term_idx1}), Arc::new(TermIdx {term_map: term_idx2})];
        Arc::new(PostingExec::try_new(
            partition_batches(),
            term_idx,
            schema(), 
            Some(vec![0, 1, 2]),
            None,
        ).unwrap())
    }

    fn boolean_exec() -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let predicate = boolean_query(
            boolean_query(
                col("a", &schema).unwrap(),
                Operator::BitwiseOr,
                col("b", &schema).unwrap(), 
                &schema
            ).unwrap(),
            Operator::BitwiseAnd,
            col("c", &schema).unwrap(),
            &schema,
        ).unwrap();
        let predicate = boolean_query(
            predicate,
            Operator::BitwiseAnd,
            lit(1 as u32),
            &schema,
        ).unwrap();
        let predicate = (0..2)
            .into_iter()
            .map(|v| {
                (v, predicate.clone())
            })
            .collect();
        Arc::new(
            BooleanExec::try_new(
                predicate,
                posting_exec(),
                None,
                None,
            ).unwrap()
        )
    }

    #[test]
    fn min_operation_range_simple() {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
        let optimizer = MinOperationRange::new();
        let optimized = optimizer.optimize(boolean_exec(), &ConfigOptions::new()).unwrap();
        let optimized_boolean = optimized.as_any().downcast_ref::<BooleanExec>().unwrap();
        assert_eq!(format!("{}", optimized_boolean.predicate()).as_str(), "a@0 | b@1 & c@2 & 1");
        assert!(optimized_boolean.terms_stats.is_some());
        let posting = optimized_boolean.input.as_any().downcast_ref::<PostingExec>().unwrap();
        let expected = vec![
            BooleanArray::from_slice(&[true, false]),
            BooleanArray::from_slice(&[true, true]),
        ];
        posting.partition_min_range.as_ref().unwrap()
            .into_iter()
            .zip(expected.iter())
            .for_each(|(l, r)| {
                assert_eq!(l, r)
            });
    }
}