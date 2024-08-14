// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::{agg_kinds, AggKind, PbAggKind};
use risingwave_sqlparser::ast::{Function, FunctionArgExpr};

use crate::binder::Clause;
use crate::error::{ErrorCode, Result};
use crate::expr::{AggCall, ExprImpl, Literal, OrderBy};
use crate::utils::Condition;
use crate::Binder;

impl Binder {
    fn ensure_aggregate_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::Where
                | Clause::Values
                | Clause::From
                | Clause::GeneratedColumn
                | Clause::Insert
                | Clause::JoinOn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "aggregate functions are not allowed in {}",
                        clause
                    ))
                    .into())
                }
                Clause::Having | Clause::Filter | Clause::GroupBy => {}
            }
        }
        Ok(())
    }

    pub(super) fn bind_aggregate_function(
        &mut self,
        f: Function,
        kind: AggKind,
    ) -> Result<ExprImpl> {
        self.ensure_aggregate_allowed()?;

        let distinct = f.arg_list.distinct;
        let filter_expr = f.filter.clone();

        let (direct_args, args, order_by) = if matches!(kind, agg_kinds::ordered_set!()) {
            self.bind_ordered_set_agg(f, kind.clone())?
        } else {
            self.bind_normal_agg(f, kind.clone())?
        };

        let filter = match filter_expr {
            Some(filter) => {
                let mut clause = Some(Clause::Filter);
                std::mem::swap(&mut self.context.clause, &mut clause);
                let expr = self
                    .bind_expr_inner(*filter)
                    .and_then(|expr| expr.enforce_bool_clause("FILTER"))?;
                self.context.clause = clause;
                if expr.has_subquery() {
                    bail_not_implemented!("subquery in filter clause");
                }
                if expr.has_agg_call() {
                    bail_not_implemented!("aggregation function in filter clause");
                }
                if expr.has_table_function() {
                    bail_not_implemented!("table function in filter clause");
                }
                Condition::with_expr(expr)
            }
            None => Condition::true_cond(),
        };

        Ok(ExprImpl::AggCall(Box::new(AggCall::new(
            kind,
            args,
            distinct,
            order_by,
            filter,
            direct_args,
        )?)))
    }

    fn bind_ordered_set_agg(
        &mut self,
        f: Function,
        kind: AggKind,
    ) -> Result<(Vec<Literal>, Vec<ExprImpl>, OrderBy)> {
        // Syntax:
        // aggregate_name ( [ expression [ , ... ] ] ) WITHIN GROUP ( order_by_clause ) [ FILTER
        // ( WHERE filter_clause ) ]

        assert!(matches!(kind, agg_kinds::ordered_set!()));

        if !f.arg_list.order_by.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "ORDER BY is not allowed for ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }
        if f.arg_list.distinct {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "DISTINCT is not allowed for ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }

        let within_group = *f.within_group.ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(format!(
                "WITHIN GROUP is expected for ordered-set aggregation `{}`",
                kind
            ))
        })?;

        let mut direct_args: Vec<_> = f
            .arg_list
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        let mut args =
            self.bind_function_expr_arg(FunctionArgExpr::Expr(within_group.expr.clone()))?;
        let order_by = OrderBy::new(vec![self.bind_order_by_expr(within_group)?]);

        // check signature and do implicit cast
        match (&kind, direct_args.as_mut_slice(), args.as_mut_slice()) {
            (
                AggKind::Builtin(PbAggKind::PercentileCont | PbAggKind::PercentileDisc),
                [fraction],
                [arg],
            ) => {
                decimal_to_float64(fraction, &kind)?;
                if matches!(&kind, AggKind::Builtin(PbAggKind::PercentileCont)) {
                    arg.cast_implicit_mut(DataType::Float64).map_err(|_| {
                        ErrorCode::InvalidInputSyntax(format!(
                            "arg in `{}` must be castable to float64",
                            kind
                        ))
                    })?;
                }
            }
            (AggKind::Builtin(PbAggKind::Mode), [], [_arg]) => {}
            (
                AggKind::Builtin(PbAggKind::ApproxPercentile),
                [percentile, relative_error],
                [_percentile_col],
            ) => {
                decimal_to_float64(percentile, &kind)?;
                decimal_to_float64(relative_error, &kind)?;
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "invalid direct args or within group argument for `{}` aggregation",
                    kind
                ))
                .into())
            }
        }

        Ok((
            direct_args
                .into_iter()
                .map(|arg| *arg.into_literal().unwrap())
                .collect(),
            args,
            order_by,
        ))
    }

    fn bind_normal_agg(
        &mut self,
        f: Function,
        kind: AggKind,
    ) -> Result<(Vec<Literal>, Vec<ExprImpl>, OrderBy)> {
        // Syntax:
        // aggregate_name (expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name (ALL expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name (DISTINCT expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name ( * ) [ FILTER ( WHERE filter_clause ) ]

        assert!(!matches!(kind, agg_kinds::ordered_set!()));

        if f.within_group.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "WITHIN GROUP is not allowed for non-ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }

        let args: Vec<_> = f
            .arg_list
            .args
            .iter()
            .map(|arg| self.bind_function_arg(arg.clone()))
            .flatten_ok()
            .try_collect()?;
        let order_by = OrderBy::new(
            f.arg_list
                .order_by
                .into_iter()
                .map(|e| self.bind_order_by_expr(e))
                .try_collect()?,
        );

        if f.arg_list.distinct {
            if matches!(
                kind,
                AggKind::Builtin(PbAggKind::ApproxCountDistinct)
                    | AggKind::Builtin(PbAggKind::ApproxPercentile)
            ) {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT is not allowed for approximate aggregation `{}`",
                    kind
                ))
                .into());
            }

            if args.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT is not allowed for aggregate function `{}` without args",
                    kind
                ))
                .into());
            }

            // restrict arguments[1..] to be constant because we don't support multiple distinct key
            // indices for now
            if args.iter().skip(1).any(|arg| arg.as_literal().is_none()) {
                bail_not_implemented!("non-constant arguments other than the first one for DISTINCT aggregation is not supported now");
            }

            // restrict ORDER BY to align with PG, which says:
            // > If DISTINCT is specified in addition to an order_by_clause, then all the ORDER BY
            // > expressions must match regular arguments of the aggregate; that is, you cannot sort
            // > on an expression that is not included in the DISTINCT list.
            if !order_by.sort_exprs.iter().all(|e| args.contains(&e.expr)) {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "ORDER BY expressions must match regular arguments of the aggregate for `{}` when DISTINCT is provided",
                    kind
                ))
                .into());
            }
        }

        Ok((vec![], args, order_by))
    }
}

fn decimal_to_float64(decimal_expr: &mut ExprImpl, kind: &AggKind) -> Result<()> {
    if decimal_expr.cast_implicit_mut(DataType::Float64).is_err() {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "direct arg in `{}` must be castable to float64",
            kind
        ))
        .into());
    }

    let Some(Ok(fraction_datum)) = decimal_expr.try_fold_const() else {
        bail_not_implemented!(
            issue = 14079,
            "variable as direct argument of ordered-set aggregate",
        );
    };

    if let Some(ref fraction_value) = fraction_datum
        && !(0.0..=1.0).contains(&fraction_value.as_float64().0)
    {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "direct arg in `{}` must between 0.0 and 1.0",
            kind
        ))
        .into());
    }
    // note that the fraction can be NULL
    *decimal_expr = Literal::new(fraction_datum, DataType::Float64).into();
    Ok(())
}
