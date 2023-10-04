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

#include <vector>
#include <memory>

#include "sql/operator/logical_operator.h"
#include "sql/expr/expression.h"
#include "storage/field/field.h"

/**
 * @brief aggregate 表示聚合运算
 * @ingroup LogicalOperator
 * @details 投影后，可能需要进行聚合。
 */
class AggregateLogicalOperator : public LogicalOperator
{
public:
  AggregateLogicalOperator(
      const std::vector<std::pair<std::string, Field>> &aggregations, const std::vector<Field> &fields);
  virtual ~AggregateLogicalOperator() = default;

  LogicalOperatorType                               type() const override { return LogicalOperatorType::AGGREGATE; }
  const std::vector<std::pair<std::string, Field>> &aggregations() const { return aggregations_; }
  const std::vector<Field>                         &fields() const { return fields_; }

private:
  std::vector<std::pair<std::string, Field>> aggregations_;  //! 聚合的字段 - 聚合类型
  std::vector<Field>                         fields_;        //! 投影映射的字段名称
};
