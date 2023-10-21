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
    ASSERT(false, "GroupByValues::operator<(const GroupByValues &that):  Unreachable!!!!!!!!!!!");
    return false;
  }

  // bool operator == (GroupByValues &that) {
  //   for (int i = 0; i < data.size(); i++) {
  //     Value& this_value = data[i];
  //     Value& that_value = that.data[i];
  //     if (this_value.compare(that_value) != 0) {
  //       return false;
  //     }
  //   }
  //   return true;
  // }
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

  // 废弃代码*********************************************************BEGIN

  // AggregatePhysicalOperator(const std::vector<std::pair<std::string, Field>> &aggregations,
  //     const std::vector<Field> &fields, const std::vector<Expression *> &fields_expressions)
  //     : aggregations_(aggregations), fields_(fields), fields_expressions_(fields_expressions)
  // {}

  // // void add_expressions(std::vector<std::unique_ptr<Expression>> &&expressions) {}

  // void set_group_by_fields(const std::vector<Field> &group_by_fields) { group_by_fields_ = group_by_fields; }
  // void set_having_filters(std::unique_ptr<Expression> expression) { having_filters_ = std::move(expression); }
  // void set_having_filter_units(const std::vector<HavingFilterUnit *> &having_filter_units)
  // {
  //   having_filter_units_ = having_filter_units;
  // }

  // 废弃代码*********************************************************END

private:
  std::vector<Expression *>   fields_expressions_;
  std::vector<Expression *>   group_by_fields_expressions_;
  std::unique_ptr<Expression> having_filters_expression_;

  std::vector<Tuple *>                          tuples_;        // 从project算子拿上来的tuples
  std::map<GroupByValues, std::vector<Tuple *>> group_tuples_;  // 分组之后的tuples

  std::vector<ValueListTuple> return_results_;  // 构造返回结果 ValueListTuple
  int                         return_results_idx = -1;

  // 废弃代码*********************************************************BEGIN

  // std::vector<std::pair<std::string, Field>> aggregations_;
  // std::vector<Field>                         fields_;
  // std::vector<std::vector<Value>> tuples_values_;  // 保存所有可能需要聚合的tuple，每行为tuple的field value
  // std::vector<Value> aggregate_results_;  // 保存所有聚合之后的结果，tuple类型统一为ValueListTuple

  // void do_max_aggregate(Field &field);
  // void do_min_aggregate(Field &field);
  // void do_count_aggregate(Field &field);
  // void do_avg_aggregate(Field &field);
  // void do_sum_aggregate(Field &field);

  // std::vector<Field>                                       group_by_fields_;
  // std::vector<int>                                         group_by_fields_idx_;
  // std::vector<HavingFilterUnit *>                          having_filter_units_;
  // std::map<GroupByValues, std::vector<std::vector<Value>>> group_tuples_values_;

  // 废弃代码*********************************************************END
};
