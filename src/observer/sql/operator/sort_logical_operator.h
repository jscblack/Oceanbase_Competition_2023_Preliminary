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
// Created by LuoYuanhui on 2023/10/09
//

#pragma once

#include <vector>
#include <memory>

#include "sql/operator/logical_operator.h"
#include "sql/expr/expression.h"
#include "storage/field/field.h"

/**
 * @brief sort 表示排序运算
 * @ingroup LogicalOperator
 * @details 从表中获取数据后，按对应属性列排序
 */
class SortLogicalOperator : public LogicalOperator
{
public:
  SortLogicalOperator(const std::vector<std::pair<Field, bool>> &orders);
  virtual ~SortLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::SORT; }

  const std::vector<std::pair<Field, bool>> &orders() const { return orders_; }

private:
  std::vector<std::pair<Field, bool>> orders_;
};
