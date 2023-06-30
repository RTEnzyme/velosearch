use std::{any::Any, sync::Arc, task::Poll, mem::size_of_val};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::{SchemaRef, Schema, Field, DataType}, record_batch::RecordBatch, array::{BooleanArray, UInt16Array}, compute::filter}, 
    datasource::TableProvider, 
    logical_expr::TableType, execution::context::SessionState, prelude::Expr, error::{Result, DataFusionError}, 
    physical_plan::{ExecutionPlan, Partitioning, DisplayFormatType, project_schema, RecordBatchStream, metrics::{ExecutionPlanMetricsSet, BaselineMetrics, MetricsSet}}, common::TermMeta};
use futures::Stream;
use adaptive_hybrid_trie::TermIdx;
use tracing::debug;

use crate::batch::{PostingBatch, BatchRange, PostingBatchBuilder};


pub struct PostingTable {
    schema: SchemaRef,
    term_idx: Vec<Arc<TermIdx<TermMeta>>>,
    postings: Vec<Arc<Vec<PostingBatch>>>,
    batch_builder: PostingBatchBuilder,
}

impl PostingTable {
    pub fn new(
        schema: SchemaRef,
        term_idx: Vec<Arc<TermIdx<TermMeta>>>,
        batches: Vec<Arc<Vec<PostingBatch>>>,
        range: &BatchRange,
    ) -> Self {
        // construct field map to index the position of the fields in schema
        Self {
            schema,
            term_idx,
            postings: batches,
            batch_builder: PostingBatchBuilder::new(range.end()),
        }
    }

    #[inline]
    pub fn stat_of(&self, term_name: &str, partition: usize) -> Option<TermMeta> {
        self.term_idx[partition].get(term_name)
    }

    pub fn stats_of(&self, term_names: &[&str], partition: usize) -> Vec<Option<TermMeta>> {
        term_names
            .into_iter()
            .map(|v| self.stat_of(v, partition))
            .collect()
    }

    pub fn space_usage(&self) -> usize {
        let mut space: usize = 0;
        space += self.term_idx
            .iter()
            .map(|v| size_of_val(&v))
            .sum::<usize>();
        space += self.postings
            .iter()
            .map(|v| v.iter().map(|v| v.space_usage()).sum::<usize>())
            .sum::<usize>();
        space
    }
}

#[async_trait]
impl TableProvider for PostingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // @TODO how to search field index efficiently
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
       TableType::Base 
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("PostingTable scan");
        Ok(Arc::new(PostingExec::try_new(
            self.postings.clone(), 
            self.term_idx.clone(),
            self.schema(), 
            projection.cloned(),
            None,
            None
        )?))
    }
}

#[derive(Clone)]
pub struct PostingExec {
    pub partitions: Vec<Arc<Vec<PostingBatch>>>,
    pub schema: SchemaRef,
    pub term_idx: Vec<Arc<TermIdx<TermMeta>>>,
    pub projected_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub partition_min_range: Option<Vec<Arc<BooleanArray>>>,
    pub is_via: Option<Vec<bool>>,
    metric: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for PostingExec {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
       write!(f, "partitions: [...]")?;
       write!(f, "schema: {:?}", self.projected_schema)?;
       write!(f, "projection: {:?}", self.projection)
   } 
}

impl ExecutionPlan for PostingExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::context::TaskContext>,
        ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        debug!("Start PostingExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        Ok(Box::pin(PostingStream::try_new(
            self.partitions[partition].clone(),
            self.projected_schema.clone(),
            self.projection.clone(),
            self.is_via.as_ref().map_or(false, |v| v[partition]),
            self.partition_min_range.as_ref().unwrap()[partition].clone(),
            self.term_idx[partition].clone(),
            BaselineMetrics::new(&self.metric, partition),
        )?))
    }

    fn fmt_as(
        &self, 
        t: datafusion::physical_plan::DisplayFormatType, 
        f: &mut std::fmt::Formatter
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let partitions: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();
                write!(f,
                    "PostingExec: partitions={}, partition_size={:?}",
                    partitions.len(),
                    partitions
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }

    // We recompute the statistics dynamically from the arrow metadata as it is pretty cheap to do so
    // fn statistics(&self) -> datafusion::physical_plan::Statistics {
    //     common::compute_record_batch_statistics(
    //         &self.partitions, &self.schema, self.projection.clone())
    // }
}

impl PostingExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` shuold not have the projection applied.
    pub fn try_new(
        partitions: Vec<Arc<Vec<PostingBatch>>>,
        term_idx: Vec<Arc<TermIdx<TermMeta>>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        partition_min_range: Option<Vec<Arc<BooleanArray>>>,
        is_via: Option<Vec<bool>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions,
            term_idx,
            schema,
            projected_schema,
            projection,
            partition_min_range,
            is_via,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Get TermMeta From &[&str]
    pub fn term_metas_of(&self, terms: &[&str], partition: usize) -> Vec<Option<TermMeta>> {
        let term_idx = self.term_idx[partition].clone();
        terms
            .into_iter()
            .map(|&t| {
                term_idx.get(t)
            })
            .collect()
    }
}

pub struct PostingStream {
    /// Vector of recorcd batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// If use via
    is_via: bool,
    /// TermIdx
    term_idx: Arc<TermIdx<TermMeta>>,
    /// metric
    metric: BaselineMetrics,
}

impl PostingStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Arc<Vec<PostingBatch>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        is_via: bool,
        min_range: Arc<BooleanArray>,
        term_idx: Arc<TermIdx<TermMeta>>,
        metric: BaselineMetrics,
    ) -> Result<Self> {
        debug!("Try new a PostingStream");
        let valid_cnt = min_range.true_count();
        let distr: Vec<UInt16Array> = schema.fields().into_iter()
            .map(|f| f.name())
            .filter(|f| *f != "__id__")
            .map(|f| match term_idx.get(f) {
                Some(v) => filter(
                    v.index.as_ref(),
                    &min_range,
                ).unwrap().as_any().downcast_ref::<UInt16Array>().unwrap().to_owned(),
                None => UInt16Array::from(vec![None; valid_cnt]),
            })
            .collect();
        let mut project_idx = vec![vec![]; distr[0].len()];
        debug!("Obtain the project idx");
        for terms in distr {
            for (i, term) in terms.into_iter().enumerate() {
                project_idx[i].push(term.map(|v| v as usize))
            }
        }
        // project_fold传入&[Option<usize>]和projected_schema
        let valid_data: Vec<RecordBatch> = data.as_ref().into_iter()
            .zip(project_idx.into_iter())
            .zip(min_range.into_iter())
            .filter(|(_, v)| v.unwrap())
            .map(|((d, p), _)| if is_via {
                d.project_fold(&p, schema.clone()).map_err(|e| DataFusionError::Execution(e.to_string()))
            } else {
                d.project_adapt(&p, schema.clone()).map_err(|e| DataFusionError::Execution(e.to_string()))
            })
            .collect::<Result<_>>()?;
        debug!("Obtain the valid distri");
        
        debug!("Finish Trying new a PostingStream");
        Ok(Self {
            data: valid_data,
            schema,
            projection,
            is_via,
            term_idx,
            metric,
        })
    }
}

impl Stream for PostingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let poll = Poll::Ready(if self.data.len() > 0 {
            let batch =self.data.remove(0);
            // return just the columns requested
            Some(Ok(batch))
        } else {
            None
        });
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for PostingStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub fn make_posting_schema(fields: Vec<&str>) -> Schema {
    Schema::new(
        [fields.into_iter()
        .map(|f| Field::new(f, DataType::Boolean, false))
        .collect(), vec![Field::new("__id__", DataType::UInt32, false)]].concat()
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::{
        prelude::SessionContext, 
        arrow::array::{UInt16Array, UInt32Array}, 
        from_slice::FromSlice, common::cast::as_uint32_array, 
        physical_plan::{expressions::col, PhysicalExpr, common::collect, boolean::BooleanExec}
    };
    use futures::StreamExt;
    use datafusion::physical_expr::expressions::boolean_query;
    use tracing::{Level, debug};

    use super::*;

    fn create_posting_table() -> (Arc<Schema>, Arc<BatchRange>, PostingTable) {
        let schema = Arc::new(make_posting_schema(vec!["a", "b", "c", "d"]));
        let range = Arc::new(BatchRange::new(0, 20));

        let batch = PostingBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt16Array::from_slice([1, 2, 6, 8, 15])),
                Arc::new(UInt16Array::from_slice([0, 4, 9, 13, 17])),
                Arc::new(UInt16Array::from_slice([3, 7, 11, 17, 19])),
                Arc::new(UInt16Array::from_slice([6, 7, 9, 14, 18])),
                Arc::new(UInt16Array::from_slice([])),
            ],
            range.clone()
        ).expect("Can't try new a PostingBatch");

        let term_idx: Vec<Arc<TermIdx<TermMeta>>> = vec![];
        let provider = PostingTable::new(
                schema.clone(),
                term_idx,
                vec![Arc::new(vec![batch])],
                &range
            );
        return (schema, range, provider)
    }

    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (_, _, provider) = create_posting_table();

        let exec = provider
            .scan(&session_ctx.state(), Some(&vec![1, 2]), &[], None)
            .await?;

        let mut it = exec.execute(0, task_ctx)?;
        let batch2 = it.next().await.unwrap()?;
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("b", batch2.schema().field(0).name());
        assert_eq!("c", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());
        Ok(())
    }

    #[tokio::test]
    async fn test_exec_fold() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (_, _, provider) = create_posting_table();

        let exec = provider
            .scan(&session_ctx.state(), Some(&vec![0, 3]), &[], None)
            .await?;

        let mut it = exec.execute(0, task_ctx)?;
        let res_batch: RecordBatch = it.next().await.unwrap()?;
        assert_eq!(2, res_batch.schema().fields().len());

        let target_res = vec![
            // 1, 2, 6, 8, 15
            UInt32Array::from_slice([0x62810000 as u32]),
            // 6, 7, 9, 14, 18]
            UInt32Array::from_slice([0x03422000 as u32]),
        ];
        res_batch.columns()
        .into_iter()
        .enumerate()
        .for_each(|(i, v)| {
            assert_eq!(&target_res[i], as_uint32_array(v).expect("Can't cast to UIn32Array"));
        });

        Ok(())
    }

    #[tokio::test]
    async fn simple_boolean_query_without_optimizer() -> Result<()> {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
        let schema = Arc::new(make_posting_schema(vec!["a", "b", "c", "d"]));
        let range = Arc::new(BatchRange::new(0, 20));

        let batch = PostingBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt16Array::from_slice([1, 2, 6, 8, 15])),
                Arc::new(UInt16Array::from_slice([0, 4, 9, 13, 17])),
                Arc::new(UInt16Array::from_slice([3, 7, 11, 17, 19])),
                Arc::new(UInt16Array::from_slice([6, 7, 9, 14, 18])),
                Arc::new(UInt16Array::from_slice([])),
            ],
            range.clone()
        ).expect("Can't try new a PostingBatch");

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let input = Arc::new(PostingExec::try_new(
            vec![Arc::new(vec![batch])], 
            vec![], 
            schema.clone(), 
            Some(vec![1, 2, 4]),
            None,
            None,
        ).unwrap());
        
        let predicate: Arc<dyn PhysicalExpr> = boolean_query(
            vec![vec![col("a", &schema.clone())?, col("b", &schema)?]],
            &schema,
        )?;
        let predicates = HashMap::from([(0, predicate)]);
        let filter: Arc<dyn ExecutionPlan> = 
            Arc::new(BooleanExec::try_new(predicates, input, None, None).unwrap());
        
        let stream = filter.execute(0, task_ctx).unwrap();
        debug!("{:?}", collect(stream).await.unwrap()[0]);
        Ok(())
    }
}