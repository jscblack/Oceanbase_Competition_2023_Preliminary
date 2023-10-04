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

#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
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

  Tuple* tuple = nullptr;
  while (RC::SUCCESS == (rc = children_[0]->next())) {  // 取出所有的(project)tuple
    tuple = children_[0]->current_tuple();
    const int cell_num = tuple->cell_num();
    std::vector<Value> values(cell_num);
    for (int i = 0; i < cell_num; i++) {
      tuple->cell_at(i, values[i]);
    }
    tuples_values_.emplace_back(values);
  }

  LOG_DEBUG("========== tuples_values_.size() = %d ==========", tuples_values_.size());
  LOG_DEBUG("========== tuples_values_[i].size() = %d ==========", tuples_values_[0].size());

  for (auto &agg : aggregations_) {
    if (agg.first == "MAX") {
      do_max_aggregate(agg.second);
    }
    else if (agg.first == "MIN") {
      do_min_aggregate(agg.second);
    }
    else if (agg.first == "COUNT") {
      do_count_aggregate(agg.second);
    }
    else if (agg.first == "AVG") {
      do_avg_aggregate(agg.second);
    }
    else if (agg.first == "SUM") {
      do_sum_aggregate(agg.second);
    }
    else {
      return RC::INVALID_ARGUMENT;
    }
  }

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  // 判断聚合后的结果是否都已经返回
  aggregate_results_idx ++;

  // LOG_DEBUG("========== aggregate_results_idx = %d ==========", aggregate_results_idx);

  if(0 <= aggregate_results_idx && aggregate_results_idx < aggregate_results_.size()) {
    return RC::SUCCESS;
  }
  else {
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

Tuple *AggregatePhysicalOperator::current_tuple()
{
  return &aggregate_results_[aggregate_results_idx];
}

void AggregatePhysicalOperator::do_max_aggregate(Field& field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_max_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx ++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 
        && strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);

  Value& max_value = tuples_values_[0][idx];

  for (auto t : tuples_values_) {
    Value& cur_value = t[idx];
    if (cur_value.compare(max_value) > 0) { // FIXME: 实现NULL之后修改为 cur_value != NULL && cur_value.compare(max_value) > 0
      max_value = cur_value;
    }
  }

  std::vector<Value> vec_value;
  vec_value.emplace_back(max_value);
  ValueListTuple vlt;
  vlt.set_cells(vec_value);
  aggregate_results_.emplace_back(vlt);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_min_aggregate(Field& field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_min_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx ++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 
        && strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);

  Value& min_value = tuples_values_[0][idx];

  for (auto t : tuples_values_) {
    Value& cur_value = t[idx];
    if (cur_value.compare(min_value) < 0) { // FIXME: 实现NULL之后修改为 cur_value != NULL && cur_value.compare(min_value) < 0
      min_value = cur_value;
    }
  }

  std::vector<Value> vec_value;
  vec_value.emplace_back(min_value);
  ValueListTuple vlt;
  vlt.set_cells(vec_value);
  aggregate_results_.emplace_back(vlt);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_count_aggregate(Field& field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_count_aggregate(Field& field) ==========");
  int count = 0;

  if (field.table() != nullptr && field.meta() == nullptr) {  // count(*)
    LOG_DEBUG("========== do_count(*) ==========");
    count = tuples_values_.size();
  }
  else {
    int idx;
    for (idx = 0; idx < fields_.size(); idx ++) {
      LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
      LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
      LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
      LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

      if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 
          && strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
        break;
      }
    }

    LOG_DEBUG("========== idx = %d ==========",idx);

    for (auto t : tuples_values_) {
      Value& cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      count ++;
    }
  }

  Value count_value;
  count_value.set_int(count);
  std::vector<Value> vec_value;
  vec_value.emplace_back(count_value);
  ValueListTuple vlt;
  vlt.set_cells(vec_value);
  aggregate_results_.emplace_back(vlt);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_avg_aggregate(Field& field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_avg_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx ++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 
        && strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);

  Value avg_value;
  int cnt = 0;
  AttrType attr_type = tuples_values_[0][idx].attr_type();
  if(attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples_values_) {
      Value& cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      sum += cur_value.get_int();
      cnt ++;
    }
    avg_value.set_int(sum / cnt);
  }
  else if(attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples_values_) {
      Value& cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      sum += cur_value.get_float();
      cnt ++;
    }
    avg_value.set_float(sum / cnt);
  }
  else if(attr_type == CHARS) {
    for (auto t : tuples_values_) {
      t[idx].str_to_number();
    }

    if (tuples_values_[0][idx].attr_type() == INTS) {
      int sum = 0;
      for (auto t : tuples_values_) {
        Value& cur_value = t[idx];
        /*
          FIXME: 实现NULL之后需要加入空值判断
          if (cur_value != NULL)
        */
        sum += cur_value.get_int();
        cnt ++;
      }
      avg_value.set_int(sum / cnt);
    }
    else {
      float sum = 0;
      for (auto t : tuples_values_) {
        Value& cur_value = t[idx];
        /*
          FIXME: 实现NULL之后需要加入空值判断
          if (cur_value != NULL)
        */
        sum += cur_value.get_float();
        cnt ++;
      }
      avg_value.set_float(sum / cnt);
    }    
  }
  else {  // 其余类型无法求和
    return ;
  }

  std::vector<Value> vec_value;
  vec_value.emplace_back(avg_value);
  ValueListTuple vlt;
  vlt.set_cells(vec_value);
  aggregate_results_.emplace_back(vlt);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_sum_aggregate(Field& field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_sum_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx ++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 
        && strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);

  Value sum_value;
  AttrType attr_type = tuples_values_[0][idx].attr_type();
  if(attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples_values_) {
      Value& cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      sum += cur_value.get_int();
    }
    sum_value.set_int(sum);
  }
  else if(attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples_values_) {
      Value& cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      sum += cur_value.get_float();
    }
    sum_value.set_float(sum);
  }
  else if(attr_type == CHARS) {
    for (auto t : tuples_values_) {
      t[idx].str_to_number();
    }

    if (tuples_values_[0][idx].attr_type() == INTS) {
      int sum = 0;
      for (auto t : tuples_values_) {
        Value& cur_value = t[idx];
        /*
          FIXME: 实现NULL之后需要加入空值判断
          if (cur_value != NULL)
        */
        sum += cur_value.get_int();
      }
      sum_value.set_int(sum);
    }
    else {
      float sum = 0;
      for (auto t : tuples_values_) {
        Value& cur_value = t[idx];
        /*
          FIXME: 实现NULL之后需要加入空值判断
          if (cur_value != NULL)
        */
        sum += cur_value.get_float();
      }
      sum_value.set_float(sum);
    }    
  }
  else {  // 其余类型无法求和
    return ;
  }

  std::vector<Value> vec_value;
  vec_value.emplace_back(sum_value);
  ValueListTuple vlt;
  vlt.set_cells(vec_value);
  aggregate_results_.emplace_back(vlt);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}
