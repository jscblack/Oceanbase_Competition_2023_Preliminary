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

#include "sql/operator/aggregate_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (RC::SUCCESS == (rc = children_[0]->next())) {  // 取出所有的(project)tuple
    Tuple *tuple = children_[0]->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get tuple from operator");
      return RC::INTERNAL;
    }

    // clone tuple的写法
    Tuple *new_tuple = nullptr;
    tuple->clone(new_tuple);
    tuples_.push_back(new_tuple);
  }
  if (rc != RC::RECORD_EOF) {
    return rc;
  }

  LOG_DEBUG("========== tuples_.size() = %d ========== log by tyh", tuples_.size());

  // need group by or not?
  if (group_by_fields_expressions_.empty()) {  // no group by
    // 构造聚合的结果
    std::vector<Value> result_value;
    // 聚合属性
    for (auto &expr : fields_expressions_) {
      if (expr->type() == ExprType::AGGREGATION) {
        Value v;
        expr->get_value(tuples_, v);
        result_value.emplace_back(v);
      } else if (expr->type() == ExprType::ARITHMETIC) {
        Value v;
        expr->get_value(tuples_, v);
        result_value.emplace_back(v);
      } else {
        ASSERT(false, "In AggregatePhysicalOperator::open(Trx *trx): non-group-by selection cannot have non-agg field");
      }
    }
    ValueListTuple vlt;
    vlt.set_cells(result_value);
    return_results_.emplace_back(vlt);
    return RC::SUCCESS;
  }

  // need group by
  // 1. 分组
  // 1.1 首先找到分组属性在每行tuple的位置
  std::vector<int> group_by_idx;
  for (auto &group_by_field_expression : group_by_fields_expressions_) {
    for (int idx = 0; idx < fields_expressions_.size(); idx++) {
      if (group_by_field_expression->alias(true) == fields_expressions_[idx]->alias(true)) {
        group_by_idx.emplace_back(idx);
        break;
      }
    }
  }
  // 1.2 根据分组属性将tuples_values填充到映射group_tuples_values中
  for (auto &tuple_ptr : tuples_) {
    GroupByValues group_by_values;
    Value         v;
    for (auto idx : group_by_idx) {
      tuple_ptr->cell_at(idx, v);
      group_by_values.data.emplace_back(v);
    }
    auto it = group_tuples_.find(group_by_values);
    if (it != group_tuples_.end()) {
      it->second.emplace_back(tuple_ptr);
    } else {
      std::vector<Tuple *> tmp;
      tmp.emplace_back(tuple_ptr);
      group_tuples_.insert({group_by_values, tmp});
    }
  }

  // 2. having分组筛选 TODO:
  // 可优化，因为having的聚合可能和分组的聚合是重复的，规范做法是先聚集再筛选，但目前先筛选会比较好实现 need having or
  // not?
  if (having_filters_expression_ != nullptr) {  // need having
    for (auto it = group_tuples_.begin(); it != group_tuples_.end();) {
      Value value;
      rc = having_filters_expression_->get_value(it->second, value);
      if (!value.get_boolean()) {
        group_tuples_.erase(it++);
      } else {
        it++;
      }
    }
  }

  // 3. 分组聚集
  // 3.1
  for (auto it = group_tuples_.begin(); it != group_tuples_.end(); it++) {
    std::vector<Value> result_value;
    Value              v;
    for (int i = 0; i < fields_expressions_.size(); i++) {
      if (fields_expressions_[i]->type() == ExprType::FIELD) {
        it->second.front()->cell_at(i, v);
        result_value.emplace_back(v);
      } else if (fields_expressions_[i]->type() == ExprType::AGGREGATION) {
        fields_expressions_[i]->get_value(it->second, v);
        result_value.emplace_back(v);
      } else if (fields_expressions_[i]->type() == ExprType::ARITHMETIC) {
        fields_expressions_[i]->get_value(it->second, v);
        result_value.emplace_back(v);
      } else {
        return RC::INTERNAL;
      }
    }
    ValueListTuple vlt;
    vlt.set_cells(result_value);
    return_results_.emplace_back(vlt);
  }

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  // 判断聚合后的结果是否都已经返回
  return_results_idx++;

  if (0 <= return_results_idx && return_results_idx < return_results_.size()) {
    return RC::SUCCESS;
  } else {
    return RC::RECORD_EOF;
  }
}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *AggregatePhysicalOperator::current_tuple() { return &return_results_[return_results_idx]; }