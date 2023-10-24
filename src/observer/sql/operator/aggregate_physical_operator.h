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
#include "sql/stmt/having_filter_stmt.h"

struct GroupByValues
{
  std::vector<Value> data;
  bool               operator<(const GroupByValues &that) const
  {
    for (int i = 0; i < data.size(); i++) {
      const Value &this_value = data[i];
      const Value &that_value = that.data[i];
      if (this_value.compare(that_value) != 0) {
        return this_value.compare(that_value) < 0;
      }
    }
    return false;
  }
};

/**
 * @brief 聚合物理算子
 * @ingroup PhysicalOperator
 */
class AggregatePhysicalOperator : public PhysicalOperator
{
public:
  AggregatePhysicalOperator(const std::vector<Expression *> &fields_expressions,
      const std::vector<Expression *>                       &group_by_fields_expressions,
      std::unique_ptr<Expression>                            having_filters_expression = nullptr)
      : fields_expressions_(fields_expressions),
        group_by_fields_expressions_(group_by_fields_expressions),
        having_filters_expression_(std::move(having_filters_expression))
  {}

  virtual ~AggregatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::AGGREGATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  std::vector<Expression *>   fields_expressions_;
  std::vector<Expression *>   group_by_fields_expressions_;
  std::unique_ptr<Expression> having_filters_expression_;

  std::vector<Tuple *>                          tuples_;        // 从project算子拿上来的tuples
  std::map<GroupByValues, std::vector<Tuple *>> group_tuples_;  // 分组之后的tuples

  std::vector<ValueListTuple> return_results_;  // 构造返回结果 ValueListTuple
  int                         return_results_idx = -1;
};
