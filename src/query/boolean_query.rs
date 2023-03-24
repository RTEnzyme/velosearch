use std::sync::Arc;

use datafusion::{
    prelude::{binary_expr, col, lit, Expr}, 
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
        let mut predicate = bitwise_and(col(*terms.next().unwrap()), col(*terms.next().unwrap()));
        for &expr in terms {
            predicate = bitwise_and(predicate, col(expr));
        }
        Ok(BooleanPredicateBuilder {
            predicate
        })
    }

    pub fn with_must(self, right: BooleanPredicateBuilder) -> Result<Self> {
        Ok(BooleanPredicateBuilder { 
            predicate: bitwise_and(self.predicate, right.predicate)
        })
    }

    pub fn should(terms: &[&str]) -> Result<Self> {
        let mut terms = terms.into_iter();
        if terms.len() <= 1 {
            return Err(DataFusionError::Internal("The param of terms should at least two items".to_string()).into())
        }
        let mut predicate = bitwise_or(col(*terms.next().unwrap()), col(*terms.next().unwrap()));
        for &expr in terms {
            predicate = bitwise_or(predicate, col(expr));
        }
        Ok(BooleanPredicateBuilder { 
            predicate: predicate
        })
    }

    pub fn with_should(self, right: BooleanPredicateBuilder) -> Result<Self> {
        Ok(BooleanPredicateBuilder {
            predicate: bitwise_or(self.predicate, right.predicate)
        })
    }

    pub fn build(self) -> Expr {
        self.predicate.eq(lit(1 as i8))
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
        let project_exprs = binary_expr_columns(&predicate);
        match predicate {
            Expr::BinaryExpr(expr) => {
                let project_plan = LogicalPlanBuilder::from(self.plan).project(project_exprs)?.build()?;
                Ok(Self {
                plan: LogicalPlanBuilder::from(project_plan).filter(Expr::BinaryExpr(expr))?.build()?,
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
        Ok(pretty::print_batches(&results)?)
    }

    /// Convert the logical plan represented by this BooleanQuery into a physical plan and
    /// execute it, collect all resulting batches into memory
    /// Executes this DataFrame and collects all results into a vecotr of RecordBatch.
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        collect(plan, task_ctx).await.map_err(|e| FastErr::DataFusionErr(e))
    }

    fn task_ctx(&self) -> TaskContext {
        TaskContext::from(&self.session_state)
    }

}

fn bitwise_and(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::BitwiseAnd, right)
}

fn bitwise_or(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::BitwiseOr, right)
}

// fn bitwise_xor(left: Expr, right: Expr) -> Expr {
//     binary_expr(left, Operator::BitwiseXor, right)
// }

fn binary_expr_columns(be: &Expr) -> Vec<Expr> {
    debug!("Binary expr columns: {:?}", be);
    match be {
        Expr::BinaryExpr(b) => {
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
    use datafusion::prelude::col;

    use crate::utils::Result;

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
}