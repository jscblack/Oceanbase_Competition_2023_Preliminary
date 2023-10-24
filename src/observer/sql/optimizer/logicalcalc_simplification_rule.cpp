/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/12/26.
//

#include "sql/optimizer/logicalcalc_simplification_rule.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/optimizer/expression_rewriter.h"

RC try_to_get_bool_constant(std::unique_ptr<Expression> &expr, bool &constant_value)
{
  if (expr->type() == ExprType::VALUE && expr->value_type() == BOOLEANS) {
    auto value_expr = dynamic_cast<ValueExpr *>(expr.get());
    constant_value  = value_expr->get_value().get_boolean();
    return RC::SUCCESS;
  }
  return RC::INTERNAL;
}
RC LogicalCalcSimplificationRule::rewrite(std::unique_ptr<Expression> &expr, bool &change_made)
{
  // TODO 修复bug
  return RC::SUCCESS;

  RC rc = RC::SUCCESS;
  if (expr != nullptr && expr->type() != ExprType::LOGICALCALC) {
    return rc;
  }

  change_made           = false;
  auto logicalcalc_expr = dynamic_cast<LogicalCalcExpr *>(expr.get());
  // 检查左右两边是否有可以直接计算的表达式
  std::unique_ptr<Expression> &left_expr  = logicalcalc_expr->left();
  std::unique_ptr<Expression> &right_expr = logicalcalc_expr->right();
  ExpressionRewriter           rewriter;
  rc = rewriter.rewrite_expression(left_expr, change_made);

  if (rc != RC::SUCCESS) {
    return rc;
  }

  bool constant_value = false;
  rc                  = try_to_get_bool_constant(left_expr, constant_value);
  if (rc == RC::SUCCESS) {
    if (constant_value == false && logicalcalc_expr->logical_calc_type() == LogiOp::AND_ENUM) {
      // left always be false, the whole expression is false
      // this node will be deleted
      expr.reset(new ValueExpr(Value(false)));
    } else if (constant_value == true && logicalcalc_expr->logical_calc_type() == LogiOp::AND_ENUM) {
      // left always be true, this node will be useless
      // this node will be deleted
      expr = std::move(right_expr);
    } else if (constant_value == true && logicalcalc_expr->logical_calc_type() == LogiOp::OR_ENUM) {
      // left always be true, the whole expression is true
      // this node will be deleted
      expr.reset(new ValueExpr(Value(true)));
    } else if (constant_value == false && logicalcalc_expr->logical_calc_type() == LogiOp::OR_ENUM) {
      // left always be false, this node will be useless
      // this node will be deleted
      expr = std::move(right_expr);
    }
    change_made = true;
    return rc;
  }

  rc = rewriter.rewrite_expression(right_expr, change_made);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = try_to_get_bool_constant(right_expr, constant_value);

  if (rc == RC::SUCCESS) {
    if (constant_value == false && logicalcalc_expr->logical_calc_type() == LogiOp::AND_ENUM) {
      // right always be false, the whole expression is false
      // this node will be deleted
      expr.reset(new ValueExpr(Value(false)));
    } else if (constant_value == true && logicalcalc_expr->logical_calc_type() == LogiOp::AND_ENUM) {
      // right always be true, this node will be useless
      // this node will be deleted
      expr = std::move(left_expr);
    } else if (constant_value == true && logicalcalc_expr->logical_calc_type() == LogiOp::OR_ENUM) {
      // right always be true, the whole expression is true
      // this node will be deleted
      expr.reset(new ValueExpr(Value(true)));
    } else if (constant_value == false && logicalcalc_expr->logical_calc_type() == LogiOp::OR_ENUM) {
      // right always be false, this node will be useless
      // this node will be deleted
      expr = std::move(left_expr);
    }
    change_made = true;
    return rc;
  }
  return rc;
}
