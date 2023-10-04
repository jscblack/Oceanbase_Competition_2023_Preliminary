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

#include "sql/operator/physical_operator.h"

/**
 * @brief 聚合物理算子
 * @ingroup PhysicalOperator
 */
class AggregatePhysicalOperator : public PhysicalOperator
{
public:
  AggregatePhysicalOperator(const std::vector<std::pair<std::string, Field>> &aggregations, const std::vector<Field> &fields)
    : aggregations_(aggregations), fields_(fields) {}

  virtual ~AggregatePhysicalOperator() = default;

//   void add_expressions(std::vector<std::unique_ptr<Expression>> &&expressions) {}

  PhysicalOperatorType type() const override { return PhysicalOperatorType::AGGREGATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  std::vector<std::pair<std::string, Field>> aggregations_;
  std::vector<Field> fields_;

  std::vector<std::vector<Value>> tuples_values_;   // 保存所有可能需要聚合的tuple，每行为tuple的field value
  std::vector<ValueListTuple> aggregate_results_;   // 保存所有聚合之后的结果，tuple类型统一为ValueListTuple
  int aggregate_results_idx = -1;

  void do_max_aggregate(Field& field);
  void do_min_aggregate(Field& field);
  void do_count_aggregate(Field& field);
  void do_avg_aggregate(Field& field);
  void do_sum_aggregate(Field& field);
};
