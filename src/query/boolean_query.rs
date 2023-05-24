use std::sync::Arc;

use datafusion::{
    prelude::{col, lit, Expr, boolean_or, boolean_and}, 
    logical_expr::{Operator, LogicalPlan, LogicalPlanBuilder}, 
    execution::context::{SessionState, TaskContext}, 
    error::DataFusionError, 
    physical_plan::{ExecutionPlan, collect}, 
    arrow::{record_batch::RecordBatch, util::pretty}
};
use tracing::debug;

use crate::{Result, utils::FastErr};




/// A query that matches documents matching boolean combinations of other queries.
/// The bool query maps to `Lucene BooleanQuery` except `filter`. It's built using one or more
/// boolean clauses, each clause with a typed occurrence. The occurrence types are:
/// | Occur  | Description |
/// | :----: | :---------: |
/// | must | The clause must appear in matching documents |
/// | should | The clause should appear in the matching document |
/// | must_not | The clause must not appear in the matching documents |
/// 
/// 
pub struct BooleanPredicateBuilder {
   predicate: Expr,
}

impl BooleanPredicateBuilder {
    pub fn must(terms: &[&str]) -> Result<Self> {
        let mut terms = terms.into_iter();
        if terms.len() <= 1 {
            return Err(DataFusionError::Internal("The param of terms should at least two items".to_string()).into())
        }
        let mut predicate = boolean_and(col(*terms.next().unwrap()), col(*terms.next().unwrap()));
        for &expr in terms {
            predicate = boolean_and(predicate, col(expr));
        }
        Ok(BooleanPredicateBuilder {
            predicate
        })
    }

    pub fn with_must(self, right: BooleanPredicateBuilder) -> Result<Self> {
        Ok(BooleanPredicateBuilder { 
            predicate: boolean_and(self.predicate, right.predicate)
        })
    }

    pub fn should(terms: &[&str]) -> Result<Self> {
        let mut terms = terms.into_iter();
        if terms.len() <= 1 {
            return Err(DataFusionError::Internal("The param of terms should at least two items".to_string()).into())
        }
        let mut predicate = boolean_or(col(*terms.next().unwrap()), col(*terms.next().unwrap()));
        for &expr in terms {
            predicate = boolean_or(predicate, col(expr));
        }
        Ok(BooleanPredicateBuilder { 
            predicate: predicate
        })
    }

    pub fn with_should(self, right: BooleanPredicateBuilder) -> Result<Self> {
        Ok(BooleanPredicateBuilder {
            predicate: boolean_or(self.predicate, right.predicate)
        })
    }

    pub fn build(self) -> Expr {
        self.predicate
    }

    // pub fn with_must_not(self) -> result<BooleanPredicateBuilder> {
    //     unimplemented!()
    //     // Ok(BooleanPredicateBuilder {
    //     // })
    // }
}

/// BooleanQuery represents a full-text search query.
/// 
#[derive(Debug, Clone)]
pub struct BooleanQuery {
    plan: LogicalPlan,
    session_state: SessionState
}

impl BooleanQuery {
    /// Create a new BooleanQuery
    pub fn new(plan: LogicalPlan, session_state: SessionState) -> Self {
        Self {
            plan,
            session_state
        }
    }

    /// Create BooleanQuery based on a bitwise binary operation expression
    pub fn boolean_predicate(self, predicate: Expr) -> Result<Self> {
        let mut project_exprs = binary_expr_columns(&predicate);
        project_exprs.push(col("__id__"));
        match predicate {
            Expr::BooleanQuery(expr) => {
                let project_plan = LogicalPlanBuilder::from(self.plan).project(project_exprs)?.build()?;
                Ok(Self {
                plan: LogicalPlanBuilder::from(project_plan).boolean(Expr::BooleanQuery(expr))?.build()?,
                session_state: self.session_state 
            })
            },
            _ => Err(FastErr::UnimplementErr("Predicate expression must be the BinaryExpr".to_string()))
        }   
    } 

    /// Create a physical plan
    pub async fn create_physical_plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.session_state
            .create_physical_plan(&self.plan).await
            .map_err(|e| FastErr::DataFusionErr(e))
    }

    /// Return a BooleanQuery with the explanation of its plan so far
    /// 
    /// if `analyze` is specified, runs the plan and reports metrics
    /// 
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<BooleanQuery> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .explain(verbose, analyze)?
            .build()?;
        Ok(BooleanQuery::new(plan, self.session_state))
    }

    /// Print results
    /// 
    pub async fn show(self) -> Result<()> {
        let results = self.collect().await?;
        debug!("Show() collect result from self.collect()");
        Ok(pretty::print_batches(&results)?)
    }

    /// Convert the logical plan represented by this BooleanQuery into a physical plan and
    /// execute it, collect all resulting batches into memory
    /// Executes this DataFrame and collects all results into a vecotr of RecordBatch.
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let task_ctx = Arc::new(self.task_ctx());
        debug!("Create physical plan");
        let plan = self.create_physical_plan().await?;
        debug!("Finish physical plan");
        collect(plan, task_ctx).await.map_err(|e| FastErr::DataFusionErr(e))
    }

    fn task_ctx(&self) -> TaskContext {
        TaskContext::from(&self.session_state)
    }

}

fn binary_expr_columns(be: &Expr) -> Vec<Expr> {
    debug!("Binary expr columns: {:?}", be);
    match be {
        Expr::BooleanQuery(b) => {
            let mut left_columns = binary_expr_columns(&b.left);
            left_columns.extend(binary_expr_columns(&b.right));
            left_columns
        },
        Expr::Column(c) => {
            vec![Expr::Column(c.clone())]
        },
        Expr::Literal(_) => { Vec::new() },
        _ => unreachable!()
    }
}


#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use datafusion::arrow::array::{UInt16Array, BooleanArray};
    use datafusion::arrow::datatypes::{Schema, Field, DataType};
    use datafusion::common::TermMeta;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::col;
    use learned_term_idx::TermIdx;
    use tracing::Level;
    use crate::batch::{BatchRange, PostingBatch};
    use crate::{utils::Result, BooleanContext, datasources::posting_table::PostingTable};

    use super::{BooleanPredicateBuilder, binary_expr_columns};

    #[test]
    fn boolean_must_builder() -> Result<()> {
        let predicate = BooleanPredicateBuilder::must(&["a", "b", "c"])?;
        assert_eq!(format!("{}", predicate.build()).as_str(), "((a & b) & c) = Int8(1)");
        Ok(())
    }

    #[test]
    fn boolean_should() -> Result<()> {
        let predicate = BooleanPredicateBuilder::should(&["a", "b", "c"])?;
        assert_eq!(format!("{}", predicate.build()).as_str(), "((a | b) | c) = Int8(1)");
        Ok(())
    }

    #[test]
    fn binary_expr_children_test() -> Result<()> {
        let predicate = BooleanPredicateBuilder::should(&["a", "b", "c"])?;
        assert_eq!(binary_expr_columns(&predicate.build()), vec![col("a"), col("b"), col("c")]);
        Ok(())
    }

    pub fn make_posting_schema(fields: Vec<&str>) -> Schema {
        Schema::new(
            fields.into_iter()
            .map(|f| if f == "__id__" {
                    Field::new(f, DataType::UInt32, false)
                } else {
                    Field::new(f, DataType::Boolean, false)
                })
            .collect()
        )
    }

    #[tokio::test]
    async fn simple_query() -> Result<()> {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
        let schema = Arc::new(make_posting_schema(vec!["__id__", "a", "b", "c", "d"]));
        let range = Arc::new(BatchRange::new(0, 20));

        let batch = PostingBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt16Array::from_iter_values((0..32).into_iter())),
                Arc::new(UInt16Array::from_slice([0, 2, 6, 8, 15])),
                Arc::new(UInt16Array::from_slice([0, 4, 6, 13, 17])),
                Arc::new(UInt16Array::from_slice([3, 7, 11, 17, 19])),
                Arc::new(UInt16Array::from_slice([6, 7, 9, 14, 18]))
            ],
            range.clone()
        ).expect("Can't try new a PostingBatch");

        let mut term_idx = TermIdx::new();
        term_idx.insert("a".to_string(), TermMeta {
            distribution: Arc::new(BooleanArray::from_slice(&[true])),
            nums: 5,
            index: vec![(0, 1)],
            selectivity: 0.,
        });
        term_idx.insert("b".to_string(), TermMeta {
            distribution: Arc::new(BooleanArray::from_slice(&[true])),
            nums: 5,
            index: vec![(0, 2)],
            selectivity: 0.,
        });
        term_idx.insert("c".to_string(), TermMeta {
            distribution: Arc::new(BooleanArray::from_slice(&[true])),
            nums: 5,
            index: vec![(0, 3)],
            selectivity: 0.,
        });
        term_idx.insert("d".to_string(), TermMeta {
            distribution: Arc::new(BooleanArray::from_slice(&[true])),
            nums: 5,
            index: vec![(0, 4)],
            selectivity: 0.
        });
        term_idx.insert("__id__".to_string(), TermMeta {
            distribution: Arc::new(BooleanArray::from_slice(&[false])),
            nums: 0,
            index: vec![],
            selectivity: 0.,
        });

        let session_ctx = BooleanContext::new();
        session_ctx.register_index("t", Arc::new(PostingTable::new(
            schema.clone(),
            vec![Arc::new(term_idx)],
            vec![vec![batch]],
            &BatchRange::new(0, 20)
        ))).unwrap();

        let index = session_ctx.index("t").await.unwrap();
        let t = Instant::now();
        index.boolean_predicate(BooleanPredicateBuilder::must(&["a", "b"]).unwrap().build()).unwrap()
            .show().await.unwrap();
        println!("{}", t.elapsed().as_nanos());
        panic!("");
        Ok(())
    }
}