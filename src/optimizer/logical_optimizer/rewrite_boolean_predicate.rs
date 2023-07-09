// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::cmp::max;

use datafusion::{
    optimizer::{OptimizerRule, OptimizerConfig, optimizer::ApplyOrder},
    logical_expr::{LogicalPlan, logical_plan::Boolean, BooleanQuery, Operator},
    prelude::Expr, error::DataFusionError, common::Result,
};


/// Optimizer pass that rewrites predicates of the form
///
/// ```text
/// (A = B AND <expr1>) OR (A = B AND <expr2>) OR ... (A = B AND <exprN>)
/// ```
///
/// Into
/// ```text
/// (A = B) AND (<expr1> OR <expr2> OR ... <exprN> )
/// ```
///
/// Predicates connected by `OR` typically not able to be broken down
/// and distributed as well as those connected by `AND`.
///
/// The idea is to rewrite predicates into `good_predicate1 AND
/// good_predicate2 AND ...` where `good_predicate` means the
/// predicate has special support in the execution engine.
///
/// Equality join predicates (e.g. `col1 = col2`), or single column
/// expressions (e.g. `col = 5`) are examples of predicates with
/// special support.
///
/// # TPCH Q19
///
/// This optimization is admittedly somewhat of a niche usecase. It's
/// main use is that it appears in TPCH Q19 and is required to avoid a
/// CROSS JOIN.
///
/// Specifically, Q19 has a WHERE clause that looks like
///
/// ```sql
/// where
///   p_partkey = l_partkey
///   and l_shipmode in (‘AIR’, ‘AIR REG’)
///   and l_shipinstruct = ‘DELIVER IN PERSON’
///   and (
///     (
///       and p_brand = ‘[BRAND1]’
///       and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)
///       and l_quantity >= [QUANTITY1] and l_quantity <= [QUANTITY1] + 10
///       and p_size between 1 and 5
///     )
///     or
///     (
///       and p_brand = ‘[BRAND2]’
///       and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)
///       and l_quantity >= [QUANTITY2] and l_quantity <= [QUANTITY2] + 10
///       and p_size between 1 and 10
///     )
///     or
///     (
///       and p_brand = ‘[BRAND3]’
///       and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)
///       and l_quantity >= [QUANTITY3] and l_quantity <= [QUANTITY3] + 10
///       and p_size between 1 and 15
///     )
/// )
/// ```
///
/// Naively planning this query will result in a CROSS join with that
/// single large OR filter. However, rewriting it using the rewrite in
/// this pass results in a proper join predicate, `p_partkey = l_partkey`:
///
/// ```sql
/// where
///   p_partkey = l_partkey
///   and l_shipmode in (‘AIR’, ‘AIR REG’)
///   and l_shipinstruct = ‘DELIVER IN PERSON’
///   and (
///     (
///       and p_brand = ‘[BRAND1]’
///       and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)
///       and l_quantity >= [QUANTITY1] and l_quantity <= [QUANTITY1] + 10
///       and p_size between 1 and 5
///     )
///     or
///     (
///       and p_brand = ‘[BRAND2]’
///       and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)
///       and l_quantity >= [QUANTITY2] and l_quantity <= [QUANTITY2] + 10
///       and p_size between 1 and 10
///     )
///     or
///     (
///       and p_brand = ‘[BRAND3]’
///       and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)
///       and l_quantity >= [QUANTITY3] and l_quantity <= [QUANTITY3] + 10
///       and p_size between 1 and 15
///     )
/// )
/// ```
///
#[derive(Default)]
pub struct RewriteBooleanPredicate;

impl RewriteBooleanPredicate {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for RewriteBooleanPredicate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Boolean(boolean) => {
                let mut expr_cnt = 0;
                let mut op_cnt = 0;
                let predicate = predicate(&boolean.predicate, (&mut expr_cnt, &mut op_cnt))?;
                let rewritten_predicate = rewrite_predicate(predicate, 0);
                let rewritten_expr = normalize_predicate(rewritten_predicate.0);
                Ok(Some(LogicalPlan::Boolean(Boolean::try_new(
                    rewritten_expr,
                    rewritten_predicate.1,
                    expr_cnt,
                    op_cnt,
                    boolean.input.clone(),
                )?)))
            },
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "rewrite_boolean_predicate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[derive(Clone, PartialEq, Debug)]
enum Predicate {
    And { args: Vec<Predicate> },
    Or { args: Vec<Predicate> },
    Other { expr: Box<Expr> },
}

fn predicate(expr: &Expr, (expr_cnt, op_cnt): (&mut usize, &mut usize)) -> Result<Predicate> {
    match expr {
    Expr::BooleanQuery(BooleanQuery {left, op, right }) => match op {
            Operator::BitwiseAnd => {
                *op_cnt += 1;
                let args = vec![predicate(left, (expr_cnt, op_cnt))?, predicate(right, (expr_cnt, op_cnt))?];
                Ok(Predicate::And { args })
            }
            Operator::BitwiseOr => {
                *op_cnt += 1;
                let args = vec![predicate(left, (expr_cnt, op_cnt))?, predicate(right, (expr_cnt, op_cnt))?];
                Ok(Predicate::Or { args })
            }
            _ => Err(DataFusionError::Internal(format!("Don't support op: {:}", op))),
        },
        _ => {
            *expr_cnt += 1; 
            Ok(Predicate::Other {
                expr: Box::new(expr.clone()),
            })
        }
    }
}

fn normalize_predicate(predicate: Predicate) -> Expr {
    match predicate {
        Predicate::And { args } => {
            assert!(args.len() >= 2);
            args.into_iter()
                .map(normalize_predicate)
                .reduce(Expr::boolean_and)
                .expect("had more than one arg")
        }
        Predicate::Or { args } => {
            assert!(args.len() >= 2);
            args.into_iter()
                .map(normalize_predicate)
                .reduce(Expr::boolean_or)
                .expect("had more than one arg")
        }
        Predicate::Other { expr } => *expr,
    }
}

fn rewrite_predicate(predicate: Predicate, height: usize) -> (Predicate, usize) {
    match predicate {
        Predicate::And { args } => {
            let mut rewritten_args = Vec::with_capacity(args.len());
            let mut max_height = 0;
            for arg in args.iter() {
                let rewritten = rewrite_predicate(arg.clone(), height + 1);
                rewritten_args.push(rewritten.0);
                max_height = max(height, rewritten.1);
            }
            rewritten_args = flatten_and_predicates(rewritten_args);
            (Predicate::And {
                args: rewritten_args,
            }, max_height)
        }
        Predicate::Or { args } => {
            let mut rewritten_args = vec![];
            let mut max_height = 0;
            for arg in args.iter() {
                let rewriten = rewrite_predicate(arg.clone(), height + 1);
                rewritten_args.push(rewriten.0);
                max_height = max(max_height, rewriten.1);
            }
            rewritten_args = flatten_or_predicates(rewritten_args);
            (delete_duplicate_predicates(&rewritten_args), max_height)
        }
        Predicate::Other { expr } => (Predicate::Other {
            expr: Box::new(*expr),
        }, height),
    }
}

fn flatten_and_predicates(
    and_predicates: impl IntoIterator<Item = Predicate>,
) -> Vec<Predicate> {
    let mut flattened_predicates = vec![];
    for predicate in and_predicates {
        match predicate {
            Predicate::And { args } => {
                flattened_predicates
                    .extend_from_slice(flatten_and_predicates(args).as_slice());
            }
            _ => {
                flattened_predicates.push(predicate);
            }
        }
    }
    flattened_predicates
}

fn flatten_or_predicates(
    or_predicates: impl IntoIterator<Item = Predicate>,
) -> Vec<Predicate> {
    let mut flattened_predicates = vec![];
    for predicate in or_predicates {
        match predicate {
            Predicate::Or { args } => {
                flattened_predicates
                    .extend_from_slice(flatten_or_predicates(args).as_slice());
            }
            _ => {
                flattened_predicates.push(predicate);
            }
        }
    }
    flattened_predicates
}

fn delete_duplicate_predicates(or_predicates: &[Predicate]) -> Predicate {
    let mut shortest_exprs: Vec<Predicate> = vec![];
    let mut shortest_exprs_len = 0;
    // choose the shortest AND predicate
    for or_predicate in or_predicates.iter() {
        match or_predicate {
            Predicate::And { args } => {
                let args_num = args.len();
                if shortest_exprs.is_empty() || args_num < shortest_exprs_len {
                    shortest_exprs = (*args).clone();
                    shortest_exprs_len = args_num;
                }
            }
            _ => {
                // if there is no AND predicate, it must be the shortest expression.
                shortest_exprs = vec![or_predicate.clone()];
                break;
            }
        }
    }

    // dedup shortest_exprs
    shortest_exprs.dedup();

    // Check each element in shortest_exprs to see if it's in all the OR arguments.
    let mut exist_exprs: Vec<Predicate> = vec![];
    for expr in shortest_exprs.iter() {
        let found = or_predicates.iter().all(|or_predicate| match or_predicate {
            Predicate::And { args } => args.contains(expr),
            _ => or_predicate == expr,
        });
        if found {
            exist_exprs.push((*expr).clone());
        }
    }
    if exist_exprs.is_empty() {
        return Predicate::Or {
            args: or_predicates.to_vec(),
        };
    }

    // Rebuild the OR predicate.
    // (A AND B) OR A will be optimized to A.
    let mut new_or_predicates = vec![];
    for or_predicate in or_predicates.iter() {
        match or_predicate {
            Predicate::And { args } => {
                let mut new_args = (*args).clone();
                new_args.retain(|expr| !exist_exprs.contains(expr));
                if !new_args.is_empty() {
                    if new_args.len() == 1 {
                        new_or_predicates.push(new_args[0].clone());
                    } else {
                        new_or_predicates.push(Predicate::And { args: new_args });
                    }
                } else {
                    new_or_predicates.clear();
                    break;
                }
            }
            _ => {
                if exist_exprs.contains(or_predicate) {
                    new_or_predicates.clear();
                    break;
                }
            }
        }
    }
    if !new_or_predicates.is_empty() {
        if new_or_predicates.len() == 1 {
            exist_exprs.push(new_or_predicates[0].clone());
        } else {
            exist_exprs.push(Predicate::Or {
                args: flatten_or_predicates(new_or_predicates),
            });
        }
    }

    if exist_exprs.len() == 1 {
        exist_exprs[0].clone()
    } else {
        Predicate::And {
            args: flatten_and_predicates(exist_exprs),
        }
    }
}
