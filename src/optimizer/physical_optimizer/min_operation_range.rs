//! IntersectionSelection optimizer that obtain the minimal valid range
//! of CNF predicate

use std::sync::Arc;

use datafusion::{
    physical_optimizer::PhysicalOptimizerRule, 
    physical_plan::{rewrite::{TreeNodeRewriter, RewriteRecursion, TreeNodeRewritable}, 
    ExecutionPlan, boolean::BooleanExec}, error::DataFusionError, 
    physical_expr::BooleanQueryExpr, 
    arrow::{datatypes::Schema, array::{BooleanArray, ArrayRef}, record_batch::RecordBatch}, common::TermMeta, from_slice::FromSlice
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
    partition_stats: Option<Vec<Vec<Option<TermMeta>>>>,
    partition_schema: Option<Arc<Schema>>,
    predicate: Option<Arc<BooleanQueryExpr>>,
    min_range: Option<Vec<Arc<BooleanArray>>>,
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
            self.partition_schema = Some(boolean.input.schema().clone());
            self.predicate = Some(Arc::new(boolean.predicate().clone()));
            Ok(RewriteRecursion::Continue)
        } else if let Some(posting) = any_node.downcast_ref::<PostingExec>(){
            debug!("Pre_visit PostingExec");
            let projected_schema = self.partition_schema.as_ref().unwrap().clone();
            let project_terms: Vec<&str> = projected_schema.fields().into_iter().map(|f| f.name().as_str()).collect();
            let partition_num = posting.output_partitioning().partition_count();
            let term_stats: Vec<Vec<Option<TermMeta>>> = 
                (0..partition_num).into_iter()
                .map(|p| {
                    posting.term_metas_of(&project_terms, p)
                })
                .collect();
            let partition_range: Vec<Arc<BooleanArray>> = (0..term_stats.len())
                .into_par_iter()
                .map(|p| {
                    let mut range: Vec<ArrayRef> = Vec::new();
                    let mut length = None;
                    for term in &term_stats[p] {
                        if let Some(t) = term {
                            length = Some(t.distribution().len());
                            break;
                        }
                    }
                    if let Some(length) = length {
                        for t in &term_stats[p] {
                            range.push(
                                match t {
                                    Some(t) => t.distribution().clone(),
                                    None => Arc::new(BooleanArray::from(vec![false; length])),
                                }
                            )
                        }
                        let batch = RecordBatch::try_new(
                            projected_schema.clone(),
                            range,
                        ).unwrap();
                        let eval = self.predicate.as_ref().unwrap().eval(batch).unwrap();
                        eval
                    } else {
                        Arc::new(BooleanArray::from_slice(&[]))
                    }
                })
                .collect();
            let term_stats: Vec<Vec<Option<TermMeta>>> = term_stats
                .into_iter()
                .zip(partition_range.iter())
                .enumerate()
                .map(|(i, (s, d))| {
                    let batch_size = posting.partitions[i][0].range().len();
                    let selectivity = if d.len() == 0 { 0. } else {(d.true_count() / d.len()) as f64};
                    s.into_iter()
                    .map(|s| {
                        match s {
                            Some(mut s) => {
                                s.selectivity = (s.nums as f64 * selectivity) /  batch_size as f64;
                                Some(s)
                            }
                            None => None
                        }
                    })
                    .collect()
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
                posting.is_via.to_owned(),
            )?))
        }else {
            Ok(node)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, collections::HashMap};

    use datafusion::{arrow::{datatypes::{SchemaRef, Field, Schema, DataType}, array::{UInt16Array, BooleanArray}}, from_slice::FromSlice, physical_plan::{boolean::BooleanExec, expressions::col, ExecutionPlan}, physical_expr::boolean_query, physical_optimizer::PhysicalOptimizerRule, config::ConfigOptions, common::TermMeta};
    use learned_term_idx::TermIdx;
    use tracing::{Level, debug};

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
            ("a".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, false])), index: Arc::new(UInt16Array::from(vec![Some(0), None])), nums:4 , selectivity: 0.}),
            ("b".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: Arc::new(UInt16Array::from(vec![Some(1), Some(1)])), nums: 6, selectivity: 0.}),
            ("c".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, false])), index: Arc::new(UInt16Array::from(vec![Some(2), None])), nums: 3, selectivity: 0.}),
            ("d".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: Arc::new(UInt16Array::from(vec![Some(3), Some(3)])), nums: 6, selectivity: 0.}),
        ].into_iter().collect();
        let term_idx2: HashMap<String, TermMeta> = vec![
            ("a".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, false])), index: Arc::new(UInt16Array::from(vec![Some(0), None])), nums: 3, selectivity: 0.}),
            ("b".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: Arc::new(UInt16Array::from(vec![Some(1), Some(1)])), nums: 5, selectivity: 0.}),
            ("c".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: Arc::new(UInt16Array::from(vec![Some(2), Some(2)])), nums: 5, selectivity: 0.}),
            ("d".to_string(), TermMeta{distribution: Arc::new(BooleanArray::from_slice(&[true, true])), index: Arc::new(UInt16Array::from(vec![Some(3), Some(3)])), nums: 5, selectivity: 0.}),
        ].into_iter().collect();
        let term_idx = vec![Arc::new(TermIdx {term_map: term_idx1}), Arc::new(TermIdx {term_map: term_idx2})];
        Arc::new(PostingExec::try_new(
            partition_batches(),
            term_idx,
            schema(), 
            Some(vec![0, 1, 2]),
            None,
            None,
        ).unwrap())
    }

    fn boolean_exec() -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let predicate = boolean_query(
            vec![
                vec![col("a", &schema).unwrap(), col("b", &schema).unwrap()],
                vec![col("c", &schema).unwrap()],
            ], &schema).unwrap();
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
                assert_eq!(l.as_ref(), r)
            });
        debug!("Final ExecutionPlan: {:?}", optimized_boolean);
    }
}