use std::{sync::Arc, collections::HashMap};

use async_trait::async_trait;
use datafusion::{
    physical_plan::{
        PhysicalPlanner, ExecutionPlan, PhysicalExpr, 
        expressions::{Column, Literal, binary, self}, 
        explain::ExplainExec, projection::ProjectionExec, boolean::BooleanExec, displayable, analyze::AnalyzeExec}, 
        execution::context::SessionState, error::{Result, DataFusionError}, 
        logical_expr::{
            LogicalPlan, expr::BooleanQuery, BinaryExpr, PlanType, ToStringifiedPlan, Projection, TableScan, StringifiedPlan, Operator
        },
        physical_expr::boolean_query_with_cnf,
        common::DFSchema, arrow::datatypes::{Schema, SchemaRef}, 
        prelude::Expr, physical_expr::execution_props::ExecutionProps, datasource::source_as_provider, 
        physical_optimizer::PhysicalOptimizerRule
    };
use futures::{future::BoxFuture, FutureExt};
use tracing::{debug, trace};

/// Boolean physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
#[derive(Default)]
pub struct BooleanPhysicalPlanner { }

#[async_trait]
impl PhysicalPlanner for BooleanPhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.handle_explain(logical_plan, session_state).await? {
            Some(plan) => Ok(plan),
            None => {
                let plan = self
                    .create_boolean_plan(logical_plan, session_state)
                    .await?;
                self.optimize_internal(plan, session_state, |_, _| {})
            }
        }
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            session_state.execution_props(),
        )
    }
}

impl BooleanPhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn create_boolean_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        async move {
            let exec_plan: Result<Arc<dyn ExecutionPlan>> = match logical_plan {
                LogicalPlan::TableScan(TableScan {
                    source,
                    projection,
                    filters,
                    fetch,
                    ..
                }) => {
                    debug!("projection: {:?}", projection);
                    let source = source_as_provider(source)?;
                    debug!("source as provider");
                    // Remove all qualifiers from the scan as the provider
                    // doesn't know (nor should care) how the relation was
                    // referred to in the query
                    // let filters = unnormalize_cols(filters.iter().cloned());
                    // let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                    source.scan(session_state, projection.as_ref(), &filters, *fetch).await
                }
                LogicalPlan::Projection(Projection { input, expr, ..}) => {
                    let input_exec = self.create_boolean_plan(input, session_state).await?;
                    let input_dfschema = input.as_ref().schema();
                    let input_schema: Arc<Schema> = input_exec.schema();

                    let physical_exprs = expr
                        .iter()
                        .map(|e| {
                            let physical_name = if let Expr::Column(col) = e {
                                    match input_schema.index_of(&col.name) {
                                        Ok(idx) => {
                                            // index physical field using logical fields index
                                            Ok(input_exec.schema().field(idx).name().to_string())
                                        }
                                        // logical column is not a derived column, safe to pass along to
                                        // physical_name
                                        Err(_) => physical_name(e),
                                    }
                                } else {
                                    physical_name(e)
                                };

                                tuple_err((
                                    self.create_physical_expr(
                                        e,
                                        input_dfschema,
                                        &input_schema,
                                        session_state,
                                    ),
                                    physical_name,
                                ))
                            }).collect::<Result<Vec<_>>>()?;
                    
                    Ok(Arc::new(ProjectionExec::try_new(
                        physical_exprs,
                        input_exec,
                    )?))
                }
                LogicalPlan::Boolean(boolean) => {
                    debug!("Create boolean plan");
                    let physical_input = self.create_boolean_plan(&boolean.input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = boolean.input.schema();
                    debug!("Create boolean predicate");
                    let runtime_expr = if let Expr::BooleanQuery(ref predicate) = boolean.binary_expr {
                        let op = match predicate.op {
                            Operator::BitwiseAnd => Operator::And,
                            Operator::BitwiseOr => Operator::Or,
                            _ => unreachable!(),
                        };
                        let binary_expr = self.create_physical_expr(&Expr::BinaryExpr(BinaryExpr{
                            left: predicate.left.clone(),
                            op,
                            right: predicate.right.clone(),
                        }), input_dfschema, &input_schema, session_state)?;
                        // If the height of predicate is large than 5, choose Vectorized Boolean Query
                        debug!("Using code_gen");
                        let schema = boolean.input.schema();
                        let inputs: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
                        let term2idx: HashMap<&str, i64> = inputs
                            .into_iter()
                            .enumerate()
                            .map(|(i, s)| (s, i as i64))
                            .collect(); 
                        let mut cnf_predicates = CnfPredicate::new(
                            &predicate,
                            term2idx,
                        );
                        cnf_predicates.flatten_cnf_predicate();
                        let cnf_predicates = cnf_predicates.collect();
                        boolean_query_with_cnf(cnf_predicates, binary_expr, &input_schema)
                    } else {
                        unreachable!()
                    }?;
                    debug!("Optimize predicate on every partition");
                    // Should Optimize predicate on every partition.
                    let num_partition = physical_input.output_partitioning().partition_count();
                    let partition_predicate = (0..num_partition)
                        .map(|v| (v, runtime_expr.clone()))
                        .collect();
                    debug!("Finish creating boolean physical plan. Is_score: {}", boolean.is_score);
                    Ok(Arc::new(BooleanExec::try_new(partition_predicate, physical_input, None, boolean.is_score)?))
                }
                LogicalPlan::Analyze(a) => {
                    let input = self.create_boolean_plan(&a.input, session_state).await?;
                    let schema = SchemaRef::new((*a.schema).clone().into());
                    Ok(Arc::new(AnalyzeExec::new(a.verbose, input, schema)))
                }
                _ => unreachable!("Don't support LogicalPlan {:?} in BooleanPlanner", logical_plan),
            };
            exec_plan
        }.boxed()
    }

    /// Handles capturing the various plans for EXPLAIN queries
    /// 
    /// Returns
    /// Some(plan) if optimized, and None if logical_plan was not an
    /// explain (and thus needs to be optimized as normal)
    async fn handle_explain(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let LogicalPlan::Explain(e) = logical_plan {
            let mut stringified_plans = vec![];

            let config = &session_state.config_options().explain;

            if !config.physical_plan_only {
                stringified_plans = e.stringified_plans.clone();
                if e.logical_optimization_succeeded {
                    stringified_plans.push(e.plan.to_stringified(PlanType::FinalLogicalPlan));
                }
            }

            if !config.logical_plan_only && e.logical_optimization_succeeded {
                match self
                    .create_boolean_plan(e.plan.as_ref(), session_state)
                    .await {
                        Ok(input) => {
                            stringified_plans.push(
                                displayable(input.as_ref())
                                .to_stringified(PlanType::InitialPhysicalPlan),
                            );

                            match self.optimize_internal(
                                input,
                                session_state,
                                |plan, optimizer| {
                                    let optimizer_name = optimizer.name().to_string();
                                    let plan_type = PlanType::OptimizedPhysicalPlan { optimizer_name };
                                    stringified_plans
                                        .push(displayable(plan).to_stringified(plan_type));
                                },
                            ) {
                                Ok(input) => stringified_plans.push(
                                    displayable(input.as_ref())
                                        .to_stringified(PlanType::FinalPhysicalPlan),
                                ),
                                Err(DataFusionError::Context(optimizer_name, e)) => {
                                    let plan_type = PlanType::OptimizedPhysicalPlan { optimizer_name };
                                    stringified_plans
                                        .push(StringifiedPlan::new(plan_type, e.to_string()))
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(e) => stringified_plans
                            .push(StringifiedPlan::new(PlanType::InitialPhysicalPlan, e.to_string())),
                    }
            }

            Ok(Some(Arc::new(ExplainExec::new(
                SchemaRef::new(e.schema.as_ref().to_owned().into()),
                stringified_plans,
                e.verbose,
            ))))
        } else {
            Ok(None)
        }
    }

    /// Optimize a physical plan by applying each physical optimizer,
    /// calling observer(plan, optimizer after each one
    fn optimize_internal<F>(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
        mut observer: F,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        F: FnMut(&dyn ExecutionPlan, &dyn PhysicalOptimizerRule)
    {
        let optimizers = session_state.physical_optimizers();
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent()
        );
        trace!("Detailed input physical plan:\n{:?}", plan);

        let mut new_plan = plan;
        for optimizer in optimizers {
            let before_schema = new_plan.schema();
            new_plan = optimizer
                .optimize(new_plan, session_state.config_options())
                .map_err(|e| {
                    DataFusionError::Context(optimizer.name().to_string(), Box::new(e))
                })?;
            if optimizer.schema_check() && new_plan.schema() != before_schema {
                let e = DataFusionError::Internal(format!(
                    "PhysicalOptimizer rule '{}' failed, due to generate a different schema, original schema: {:?}, new schema: {:?}",
                    optimizer.name(),
                    before_schema,
                    new_plan.schema(),
                ));
                return Err(DataFusionError::Context(
                    optimizer.name().to_string(),
                    Box::new(e),
                ));
            }
            trace!(
                "Optimized physical plan by {}:\n{}\n",
                optimizer.name(),
                displayable(new_plan.as_ref()).indent()
            );
            observer(new_plan.as_ref(), optimizer.as_ref())
        }
        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent()
        );
        trace!("Detailed optimized physical plan:\n{:?}", new_plan);
        Ok(new_plan)
    }
}

fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    if input_schema.fields.len() != input_dfschema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "create_physical_expr expected same number of fields, got \
            Arrow schema with {} and DataFusion schema with {}",
            input_schema.fields.len(),
            input_dfschema.fields().len(),
        )));
    }
    match e {
        Expr::Alias(expr, ..) => Ok(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Column(c) => {
            let idx = input_schema.index_of(&c.name)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::BinaryExpr(BinaryExpr { left, op, right}) => {
            let lhs = create_physical_expr(
                left,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let rhs = create_physical_expr(
                right,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            binary(lhs, *op, rhs, input_schema)
        }
        Expr::BooleanQuery(BooleanQuery { left, op, right }) => {
            // let mut cnf_predicates = CnfPredicate::new(
            //     boolean,
            //     input_dfschema,
            //     input_schema,
            //     execution_props,
            // );
            // cnf_predicates.flatten_cnf_predicate();
            // let cnf_predicates = cnf_predicates.collect();
            // let binary_expr = create_physical_expr(&Expr::BinaryExpr(BinaryExpr{
            //     left: boolean.left,
            //     op: boolean.op,
            //     right: boolean.right,
            // }), input_dfschema, input_schema, execution_props)?;
            // boolean_query(binary_expr, input_schema)
            let lhs = create_physical_expr(
                left,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let rhs = create_physical_expr(
                right,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let op = match op {
                Operator::BitwiseAnd => Operator::And,
                Operator::BitwiseOr => Operator::Or,
                _ => unreachable!(),
            };
            binary(lhs, op, rhs, input_schema)
        }
        Expr::Not(expr) => expressions::not(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        _ => unreachable!("Don't support expr {} in BooleanPlanner", e),
    }
}

fn physical_name(e: &Expr) -> Result<String> {
    create_physical_name(e, true)
}

fn create_physical_name(e: &Expr, is_first_expr: bool) -> Result<String> {
    match e {
        Expr::Column(c) => {
            if is_first_expr {
                Ok(c.name.clone())
            } else {
                Ok(c.flat_name())
            }
        }
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{left} {op} {right}"))
        }
        Expr::BooleanQuery(BooleanQuery { left, op, right }) => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{left} {op} {right}"))
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {expr}"))
        }
        e => Err(DataFusionError::Internal(
            format!("Create physical name does not support {}", e)
        )),
    }
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

struct CnfPredicate<'a> {
    root: &'a BooleanQuery,
    predicates: Vec<Vec<i64>>,
    idx: usize,
    term2idx: HashMap<&'a str, i64>,
}

impl<'a> CnfPredicate<'a> {
    fn new(root: &'a BooleanQuery, term2idx: HashMap<&'a str, i64>) -> Self {
        Self {
            root,
            predicates: Vec::new(),
            idx: 0,
            term2idx,
        }
    }

    fn flatten_cnf_predicate(&mut self) {
        self.flatten_impl(&self.root.left);
        self.flatten_impl(&self.root.right);
    }

    fn flatten_impl(&mut self, expr: &Expr) {
        if let Expr::BooleanQuery(boolean) = expr {
            if boolean.op == Operator::BitwiseAnd {
                self.flatten_impl(&boolean.left);
                self.flatten_impl(&boolean.right);
            } else if boolean.op == Operator::BitwiseOr {
                self.predicates.push(vec![]);
                self.append_or_expr(&boolean.left);
                self.append_or_expr(&boolean.right);
                self.idx += 1;
            }
        } else {
            self.predicates.push(vec![self.term2idx[expr.to_string().as_str()]])
        }
    }

    fn append_or_expr(&mut self, e: &Expr) {
        match e {
            Expr::BooleanQuery(boolean) => {
                self.append_or_expr(&boolean.left);
                self.append_or_expr(&boolean.right);
            }
            _ => self.predicates[self.idx].push(self.term2idx[e.to_string().as_str()])
        }
    }

    fn collect(self) -> Vec<Vec<i64>> {
        self.predicates
    }
}

#[cfg(test)]
mod tests {
    // use std::{unreachable, println, assert_eq};

    // use datafusion::prelude::{col, boolean_or, boolean_and, Expr};

    // use super::CnfPredicate;

    // #[test]
    // fn cnf_predicates_convert() {
    //     let a = col("a");
    //     let b = col("b");
    //     let c = col("c");
    //     let a_b = boolean_or(a, b);
    //     let a_b_c = boolean_and(a_b, c);
    //     if let Expr::BooleanQuery(boolean) = a_b_c {
    //         let mut cnf = CnfPredicate::new(
    //             &boolean,
    //         );
    //         cnf.flatten_cnf_predicate();
    //         let cnf_list = cnf.collect();
    //         assert_eq!(2, cnf_list.len());
    //         // assert_eq!(&Column::new("a", 0), cnf_list[0][0].clone().as_any().downcast_ref::<Column>().unwrap());
    //         // assert_eq!(&Column::new("b", 1), cnf_list[0][1].clone().as_any().downcast_ref::<Column>().unwrap());
    //         // assert_eq!(&Column::new("c", 2), cnf_list[1][0].clone().as_any().downcast_ref::<Column>().unwrap());
    //         println!("{:?}", cnf_list[0][0]);
    //     } else {
    //         unreachable!()
    //     }
    // }
}