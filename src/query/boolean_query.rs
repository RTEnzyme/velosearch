use std::sync::Arc;

use datafusion::{prelude::{Expr, binary_expr, col, lit}, logical_expr::{Operator, LogicalPlan, LogicalPlanBuilder}, execution::context::{SessionState, TaskContext}, error::DataFusionError, physical_plan::{ExecutionPlan, collect}, arrow::record_batch::RecordBatch};

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
        match predicate {
            Expr::BinaryExpr(_) => Ok(Self {
                plan: LogicalPlanBuilder::from(self.plan).filter(predicate)?.build()?,
                session_state: self.session_state 
            }),
            _ => Err(FastErr::UnimplementErr("Predicate expression must be the BinaryExpr".to_string()))
        }   
    } 

    /// Create a physical plan
    pub async fn create_physical_plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.session_state
            .create_physical_plan(&self.plan).await
            .map_err(|e| FastErr::DataFusionErr(e))
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



#[cfg(test)]
pub mod tests {
    use crate::utils::Result;

    use super::BooleanPredicateBuilder;

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
}