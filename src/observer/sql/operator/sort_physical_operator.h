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
// Created by LuoYuanhui on 2023/10/10
//

#pragma once

#include <algorithm>

#include "sql/operator/physical_operator.h"
#include "sql/operator/sort_logical_operator.h"
#include "sql/parser/value.h"
#include "sql/expr/tuple.h"

/**
 * @brief 排序物理算子
 * @ingroup PhysicalOperator
 */
class SortPhysicalOperator : public PhysicalOperator
{
public:
  SortPhysicalOperator()  = default;
  ~SortPhysicalOperator() = default;
  SortPhysicalOperator(const std::vector<std::pair<Field, bool>> &orders) : orders_(orders) {}

  PhysicalOperatorType type() const override { return PhysicalOperatorType::SORT; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  int                  cur_idx_ = -1;   // current_tuple_idx
  std::vector<Tuple *> sorted_tuples_;  // results

  std::vector<std::pair<Field, bool>> orders_;
};