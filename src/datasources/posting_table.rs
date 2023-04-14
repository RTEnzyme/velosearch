use std::{collections::HashMap, any::Any, sync::Arc, task::Poll};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch}, 
    datasource::TableProvider, 
    logical_expr::TableType, execution::context::SessionState, prelude::{Expr}, error::{Result, DataFusionError}, 
    physical_plan::{ExecutionPlan, Partitioning, DisplayFormatType, common, project_schema, RecordBatchStream}};
use futures::Stream;
use learned_term_idx::TermIdx;

use crate::batch::PostingBatch;

pub struct TermMeta {
    distribution: Vec<u16>,
}

pub struct PostingTable {
    schema: SchemaRef,
    term_idx: Arc<TermIdx<TermMeta>>,
    postings: Vec<Vec<PostingBatch>>,
}

impl PostingTable {
    pub fn new(schema: SchemaRef, term_idx: Arc<TermIdx<TermMeta>>, batches: Vec<Vec<PostingBatch>>) -> Self {
        // construct field map to index the position of the fields in schema
        Self {
            schema,
            term_idx,
            postings: batches,
        }
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
        Ok(Arc::new(PostingExec::try_new(
            &self.postings.clone(), 
            self.term_idx.clone(),
            self.schema(), 
            projection.cloned()
        )?))
    }
}

pub struct PostingExec {
    partitions: Vec<Vec<PostingBatch>>,
    schema: SchemaRef,
    term_idx: Arc<TermIdx<TermMeta>>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
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
            _context: Arc<datafusion::execution::context::TaskContext>,
        ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        Ok(Box::pin(PostingStream::try_new(
            self.partitions[partition].clone(),
            self.projected_schema.clone(),
            self.projection.clone(),
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
        partitions: &[Vec<PostingBatch>],
        term_idx: Arc<TermIdx<TermMeta>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions.to_vec(),
            term_idx,
            schema,
            projected_schema,
            projection
        })
    }
}

pub struct PostingStream {
    /// Vector of recorcd batches
    data: Vec<PostingBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl PostingStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<PostingBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0
        })
    }
}

impl Stream for PostingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
       Poll::Ready(if self.index < self.data.len() {
        self.index += 1;
        let batch = &self.data[self.index - 1];

        // return just the columns requested
        let batch = match self.projection.as_ref() {
            Some(columns) => batch.project_fold(columns).map_err(|e| DataFusionError::Execution(e.to_string()))?,
            None => unreachable!("must have projection")
        };
        Some(Ok(batch))
       } else {
        None
       }) 
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