use std::{any::Any, sync::Arc, task::Poll, mem::size_of_val, ops::Range};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::{SchemaRef, Schema, Field, DataType}, record_batch::RecordBatch, array::UInt64Array, compute::and}, 
    datasource::TableProvider, 
    logical_expr::TableType, execution::context::SessionState, prelude::Expr, error::{Result, DataFusionError}, 
    physical_plan::{ExecutionPlan, Partitioning, DisplayFormatType, project_schema, RecordBatchStream, metrics::{ExecutionPlanMetricsSet, MetricsSet}, EmptyRecordBatchStream}, common::TermMeta};
use futures::Stream;
use adaptive_hybrid_trie::TermIdx;
use serde::{Serialize, ser::SerializeStruct};
use tracing::{debug, info};

use crate::{batch::{PostingBatch, BatchRange}, physical_expr::BooleanEvalExpr};

pub struct PostingTable {
    schema: SchemaRef,
    term_idx: Arc<TermIdx<TermMeta>>,
    postings: Vec<Arc<PostingBatch>>,
    partitions_num: usize,
}

impl PostingTable {
    pub fn new(
        schema: SchemaRef,
        term_idx: Arc<TermIdx<TermMeta>>,
        batches: Vec<Arc<PostingBatch>>,
        _range: &BatchRange,
        partitions_num: usize,
    ) -> Self {
        // construct field map to index the position of the fields in schema
        Self {
            schema,
            term_idx,
            postings: batches,
            partitions_num,
        }
    }

    #[inline]
    pub fn stat_of(&self, term_name: &str, _partition: usize) -> Option<TermMeta> {
        self.term_idx.get(term_name)
    }

    pub fn stats_of(&self, term_names: &[&str], partition: usize) -> Vec<Option<TermMeta>> {
        term_names
            .into_iter()
            .map(|v| self.stat_of(v, partition))
            .collect()
    }

    pub fn memory_consumption(&self) -> usize {
        let mut postings: usize = 0;
        let mut offsets: usize = 0;
        let mut all: usize = 0;
        self.postings.iter()
            .for_each(|v| {
                let size = v.memory_consumption();
                all += size.0;
                postings += size.1;
                offsets += size.2;
            });
        info!("posting size: {:}", postings);
        info!("offsets size: {:}", offsets);
        all
    }

    // pub fn serialize(&self, path: &Path) -> Result<()> {
    //     if let Ok(f) = File::open(path) {
    //         let writer = std::io::BufWriter::new(f);
    //         bincode::serialize_into(writer, &self)
    //         .map_err(|e| FastErr::IoError(e.to_string()))
    //     } else {
    //         let file = File::create(path)?;
    //         let writer = std::io::BufWriter::new(file);
    //         bincode::serialize_into(writer, &self)
    //         .map_err(|e| FastErr::IoError(e.to_string()))
    //     }
    // }
}

impl Serialize for PostingTable {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        let mut state = serializer.serialize_struct("PostingTable", 4)?;
        state.serialize_field("schema", &self.schema)?;
        
        state.end()
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
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("PostingTable scan");
        Ok(Arc::new(PostingExec::try_new(
            self.postings.clone(), 
            self.term_idx.clone(),
            self.schema(), 
            projection.cloned(),
            None,
            vec![],
            self.partitions_num,
        )?))
    }
}

#[derive(Clone)]
pub struct PostingExec {
    pub partitions: Vec<Arc<PostingBatch>>,
    pub schema: SchemaRef,
    pub term_idx: Arc<TermIdx<TermMeta>>,
    pub projected_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub partition_min_range: Option<Arc<UInt64Array>>,
    pub offsets: Option<Vec<usize>>,
    pub is_score: bool,
    pub projected_term_meta: Vec<Option<TermMeta>>,
    pub predicate: Option<BooleanEvalExpr>,
    partitions_num: usize,
    metric: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for PostingExec {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
       write!(f, "partitions: [...]")?;
       write!(f, "schema: {:?}", self.projected_schema)?;
       write!(f, "projection: {:?}", self.projection)?;
       write!(f, "is_score: {:}", self.is_score)
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
        Partitioning::UnknownPartitioning(self.partitions_num)
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
        let task_len = self.partition_min_range.as_ref().unwrap().len();
        let batch_len = if self.partitions_num * 10 > task_len {
            if partition * 10 > task_len {
                return Ok(Box::pin(EmptyRecordBatchStream::new(self.projected_schema.clone())));
            }

            10
        } else {
            task_len / self.partitions_num
        };
        debug!("Start PostingExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let predicate_ref = self.predicate.as_ref().map(|v| v.valid_idx.as_ref());
        let (distris, (indices, is_encoding)) = self.projected_term_meta.iter()
            .enumerate()
            .map(|(n, v)| match v {
                Some(v) => (Some(v.distribution[0].clone()), (v.index[0].clone(), predicate_ref.map(|v| v.contains(&n)).unwrap_or(false))),
                None => (None, (None, false)),
            })
            .unzip();
        Ok(Box::pin(PostingStream::try_new(
            self.partitions[0].clone(),
            self.projected_schema.clone(),
            self.partition_min_range.as_ref().unwrap().clone(),
            distris,
            indices,
            self.predicate.clone(),
            is_encoding,
            self.is_score,
            (batch_len * partition)..(batch_len * partition + batch_len)
        )?))
    }

    fn fmt_as(
        &self, 
        t: datafusion::physical_plan::DisplayFormatType, 
        f: &mut std::fmt::Formatter
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f,
                    "PostingExec: partition_size={:?}, is_score: {:}, predicate: {:?}",
                    self.partitions.len(),
                    self.is_score,
                    self.predicate.as_ref().map(|v| v.predicate.as_ref().map(|v| unsafe { &(*v.get()) })),
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
        partitions: Vec<Arc<PostingBatch>>,
        term_idx: Arc<TermIdx<TermMeta>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        partition_min_range: Option<Arc<UInt64Array>>,
        projected_term_meta: Vec<Option<TermMeta>>,
        partitions_num: usize,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions,
            term_idx,
            schema,
            projected_schema,
            projection,
            partition_min_range,
            offsets: None,
            is_score: false,
            projected_term_meta,
            predicate: None,
            partitions_num,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Get TermMeta From &[&str]
    pub fn term_metas_of(&self, terms: &[&str]) -> Vec<Option<TermMeta>> {
        let term_idx = self.term_idx.clone();
        terms
            .into_iter()
            .map(|&t| {
                term_idx.get(t)
            })
            .collect()
    }

    /// Get TermMeta From &str
    pub fn term_meta_of(&self, term: &str) -> Option<TermMeta> {
        self.term_idx.get(term)
    }
}

pub struct PostingStream {
    /// Vector of recorcd batches
    posting_lists:  Arc<PostingBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// is_score
    is_score: bool,
    /// min_range
    min_range: Arc<UInt64Array>,
    /// distris
    distris: Vec<Option<Arc<UInt64Array>>>,
    /// indecis
    indices: Vec<Option<u32>>,
    /// index the bucket
    index: usize,
    /// 
    predicate: Option<BooleanEvalExpr>,
    ///
    is_encoding: Vec<bool>,
    /// task range
    task_range: Range<usize>,
}

impl PostingStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Arc<PostingBatch>,
        schema: SchemaRef,
        min_range: Arc<UInt64Array>,
        distris: Vec<Option<Arc<UInt64Array>>>,
        indices: Vec<Option<u32>>,
        predicate: Option<BooleanEvalExpr>,
        is_encoding: Vec<bool>,
        is_score: bool,
        task_range: Range<usize>,
    ) -> Result<Self> {
        debug!("Try new a PostingStream");
        Ok(Self {
            posting_lists: data,
            schema,
            min_range,
            distris,
            is_score,
            indices,
            predicate,
            index: task_range.start,
            is_encoding,
            task_range,
        })
    }
}

impl Stream for PostingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {

        loop {
            debug!("index: {:}, task_range: {:?}", self.index, self.task_range);
            if self.index >= self.min_range.len() || self.index >= self.task_range.end {
                return Poll::Ready(None);
            }
            if true {
                let min_range = self.min_range.values().get(self.index).unwrap().clone();
                debug!("min_range: {:b}", min_range);
                if min_range == 0 {
                    self.index += 1;
                    continue;
                }
                let distris: Vec<Option<u64>> = self.distris.iter()
                    .map(|v| { 
                        match v {
                            Some(v) => {
                                Some(v.values().get(self.index).unwrap().clone())
                            }
                            None => None
                        }
                    })
                    .collect();
                let batch = if self.is_score {
                    self.posting_lists.project_with_predicate_with_score(
                        &self.indices,
                        self.schema.clone(),
                        &distris,
                        self.index,
                        min_range,
                        self.predicate.as_ref().unwrap(),
                        &self.is_encoding,
                    ).unwrap()
                } else {
                    self.posting_lists.project_with_predicate(
                        &self.indices,
                        self.schema.clone(),
                        &distris,
                        self.index,
                        min_range,
                        self.predicate.as_ref().unwrap(),
                        &self.is_encoding,
                    ).unwrap()
                };
                self.index += 1;
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("mask", DataType::UInt64, false)])),
                    vec![
                        Arc::new(UInt64Array::from_value(batch as u64, 1)),
                    ]
                )?;

                return Poll::Ready(Some(Ok(batch)));
            } else {
                unreachable!()
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(0))
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

    // use datafusion::{
    //     prelude::SessionContext, 
    //     arrow::array::{UInt16Array, UInt32Array}, 
    //     from_slice::FromSlice, common::cast::as_uint32_array, 
    // };
    // use futures::StreamExt;

    // use super::*;

    // fn create_posting_table() -> (Arc<Schema>, Arc<BatchRange>, PostingTable) {
    //     let schema = Arc::new(make_posting_schema(vec!["a", "b", "c", "d"]));
    //     let range = Arc::new(BatchRange::new(0, 20));

    //     let batch = PostingBatch::try_new(
    //         schema.clone(),
    //         vec![
    //             Arc::new(UInt16Array::from_slice([1, 2, 6, 8, 15])),
    //             Arc::new(UInt16Array::from_slice([0, 4, 9, 13, 17])),
    //             Arc::new(UInt16Array::from_slice([3, 7, 11, 17, 19])),
    //             Arc::new(UInt16Array::from_slice([6, 7, 9, 14, 18])),
    //             Arc::new(UInt16Array::from_slice([])),
    //         ],
    //         range.clone()
    //     ).expect("Can't try new a PostingBatch");

    //     let term_idx: Arc<TermIdx<TermMeta>> = Arc::new(TermIdx::new(keys, values, skip_length));
    //     let provider = PostingTable::new(
    //             schema.clone(),
    //             term_idx,
    //             vec![Arc::new(vec![batch])],
    //             &range
    //         );
    //     return (schema, range, provider)
    // }

    // #[tokio::test]
    // async fn test_with_projection() -> Result<()> {
    //     let session_ctx = SessionContext::new();
    //     let task_ctx = session_ctx.task_ctx();
    //     let (_, _, provider) = create_posting_table();

    //     let exec = provider
    //         .scan(&session_ctx.state(), Some(&vec![1, 2]), &[], None)
    //         .await?;

    //     let mut it = exec.execute(0, task_ctx)?;
    //     let batch2 = it.next().await.unwrap()?;
    //     assert_eq!(2, batch2.schema().fields().len());
    //     assert_eq!("b", batch2.schema().field(0).name());
    //     assert_eq!("c", batch2.schema().field(1).name());
    //     assert_eq!(2, batch2.num_columns());
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn test_exec_fold() -> Result<()> {
    //     let session_ctx = SessionContext::new();
    //     let task_ctx = session_ctx.task_ctx();
    //     let (_, _, provider) = create_posting_table();

    //     let exec = provider
    //         .scan(&session_ctx.state(), Some(&vec![0, 3]), &[], None)
    //         .await?;

    //     let mut it = exec.execute(0, task_ctx)?;
    //     let res_batch: RecordBatch = it.next().await.unwrap()?;
    //     assert_eq!(2, res_batch.schema().fields().len());

    //     let target_res = vec![
    //         // 1, 2, 6, 8, 15
    //         UInt32Array::from_slice([0x62810000 as u32]),
    //         // 6, 7, 9, 14, 18]
    //         UInt32Array::from_slice([0x03422000 as u32]),
    //     ];
    //     res_batch.columns()
    //     .into_iter()
    //     .enumerate()
    //     .for_each(|(i, v)| {
    //         assert_eq!(&target_res[i], as_uint32_array(v).expect("Can't cast to UIn32Array"));
    //     });

    //     Ok(())
    // }

    // #[tokio::test]
    // async fn simple_boolean_query_without_optimizer() -> Result<()> {
    //     tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
    //     let schema = Arc::new(make_posting_schema(vec!["a", "b", "c", "d"]));
    //     let range = Arc::new(BatchRange::new(0, 20));

    //     let batch = PostingBatch::try_new(
    //         schema.clone(),
    //         vec![
    //             Arc::new(UInt16Array::from_slice([1, 2, 6, 8, 15])),
    //             Arc::new(UInt16Array::from_slice([0, 4, 9, 13, 17])),
    //             Arc::new(UInt16Array::from_slice([3, 7, 11, 17, 19])),
    //             Arc::new(UInt16Array::from_slice([6, 7, 9, 14, 18])),
    //             Arc::new(UInt16Array::from_slice([])),
    //         ],
    //         range.clone()
    //     ).expect("Can't try new a PostingBatch");

    //     let session_ctx = SessionContext::new();
    //     let task_ctx = session_ctx.task_ctx();
    //     let input = Arc::new(PostingExec::try_new(
    //         vec![Arc::new(vec![batch])], 
    //         vec![], 
    //         schema.clone(), 
    //         Some(vec![1, 2, 4]),
    //         None,
    //     ).unwrap());
        
    //     let predicate: Arc<dyn PhysicalExpr> = boolean_query(
    //         vec![vec![col("a", &schema.clone())?, col("b", &schema)?]],
    //         &schema,
    //     )?;
    //     let predicates = HashMap::from([(0, predicate)]);
    //     let filter: Arc<dyn ExecutionPlan> = 
    //         Arc::new(BooleanExec::try_new(predicates, input, None, None).unwrap());
        
    //     let stream = filter.execute(0, task_ctx).unwrap();
    //     debug!("{:?}", collect(stream).await.unwrap()[0]);
    //     Ok(())
    // }
}