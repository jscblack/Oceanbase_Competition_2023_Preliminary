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
// Created by tong1heng on 2023/10/03.
//

#pragma once

#include <memory>
#include <vector>

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/having_filter_stmt.h"
#include "storage/field/field.h"

/**
 * @brief aggregate 表示聚合运算
 * @ingroup LogicalOperator
 * @details 投影后，可能需要进行聚合。
 */
class AggregateLogicalOperator : public LogicalOperator
{
public:
  AggregateLogicalOperator(const std::vector<Expression *> &fields_expressions,
      const std::vector<Expression *>                      &group_by_fields_expressions);
  virtual ~AggregateLogicalOperator() = default;

  LogicalOperatorType              type() const override { return LogicalOperatorType::AGGREGATE; }
  const std::vector<Expression *> &fields_expressions() const { return fields_expressions_; }
  const std::vector<Expression *> &group_by_fields_expressions() const { return group_by_fields_expressions_; }

  void add_having_filters_expression(std::unique_ptr<Expression> having_filters_expression)
  {
    // NOTE: 分组筛选条件加入到基类的expressions_中
    expressions_.emplace_back(std::move(having_filters_expression));
  }

private:
  std::vector<Expression *> fields_expressions_;           //! select的字段 - 包含聚合的字段
  std::vector<Expression *> group_by_fields_expressions_;  //! 分组的字段
};
