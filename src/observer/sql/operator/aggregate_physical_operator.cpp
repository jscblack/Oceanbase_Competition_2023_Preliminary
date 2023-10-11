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

  Tuple *tuple = nullptr;
  while (RC::SUCCESS == (rc = children_[0]->next())) {  // 取出所有的(project)tuple
    tuple                       = children_[0]->current_tuple();
    const int          cell_num = tuple->cell_num();
    std::vector<Value> values(cell_num);
    for (int i = 0; i < cell_num; i++) {
      tuple->cell_at(i, values[i]);
    }
    tuples_values_.emplace_back(values);
  }

  // LOG_DEBUG("========== tuples_values_.size() = %d ==========", tuples_values_.size());
  // LOG_DEBUG("========== tuples_values_[i].size() = %d ==========", tuples_values_[0].size());

  // 分组
  // 1. 首先找到分组属性在每行tuple的位置
  for (auto group_by_field : group_by_fields_) {
    for (int idx = 0; idx < fields_.size(); idx++) {
      if (strcmp(group_by_field.table_name(), fields_[idx].table_name()) == 0 &&
          strcmp(group_by_field.field_name(), fields_[idx].field_name()) == 0) {
        group_by_fields_idx_.emplace_back(idx);
        break;
      }
    }
  }
  // 2. 根据分组属性将tuples_values填充到映射group_tuples_values中
  for (auto tuple_values : tuples_values_) {
    GroupByValues group_by_values;
    for (auto idx : group_by_fields_idx_) {
      group_by_values.data.emplace_back(tuple_values[idx]);
    }

    auto it = group_tuples_values_.find(group_by_values);
    if (it != group_tuples_values_.end()) {
      it->second.emplace_back(tuple_values);
    } else {
      std::vector<std::vector<Value>> tmp;
      tmp.emplace_back(tuple_values);
      group_tuples_values_.insert({group_by_values, tmp});
    }
  }


  // 聚集
  //
  // // 以下为feat-aggregation-func的实现
  // for (auto &agg : aggregations_) {
  //   if (agg.first == "MAX") {
  //     do_max_aggregate(agg.second);
  //   } else if (agg.first == "MIN") {
  //     do_min_aggregate(agg.second);
  //   } else if (agg.first == "COUNT") {
  //     do_count_aggregate(agg.second);
  //   } else if (agg.first == "AVG") {
  //     do_avg_aggregate(agg.second);
  //   } else if (agg.first == "SUM") {
  //     do_sum_aggregate(agg.second);
  //   } else {
  //     return RC::INVALID_ARGUMENT;
  //   }
  // }

  // // 构造返回的value list tuple
  // std::vector<Value> result_value;
  // for (int i = 0; i < aggregate_results_.size(); i++) {
  //   result_value.emplace_back(aggregate_results_[i]);
  // }
  // ValueListTuple vlt;
  // vlt.set_cells(result_value);
  // return_results_.emplace_back(vlt);

  // 以下为分组聚集的实现




  // 筛选
  // 1. 取出筛选having子句的属性---应该已经做过了，直接筛选即可

  // for (int i = 0; i < having_filter_units_.size(); i++) {
  //   const HavingFilterObj &filter_obj_left = having_filter_units_[i]->left();
  //   const HavingFilterObj &filter_obj_right = having_filter_units_[i]->right();
  //   if (filter_obj_left.is_attr) {

  //   }
    
  // }

  // 2. 做having子句中的聚集操作，并构建


  // 3. 通过expression判断是否条件满足



  // 以下代码被废弃，尽管使用了类型转换，但无法判断出comparison expression的left和right哪个是field expression哪个是value expression
  // ConjunctionExpr *having_filters_cast = dynamic_cast<ConjunctionExpr *>(having_filters_.get());
  // std::vector<std::unique_ptr<Expression>> &children_expressions = having_filters_cast->children();
  // for (int i = 0; i < children_expressions.size(); i++) {
  //   ComparisonExpr *child_comparison = dynamic_cast<ComparisonExpr *>(children_expressions[i].get());
  //   child_comparison->left();
  // }


  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  // 判断聚合后的结果是否都已经返回
  return_results_idx++;

  // LOG_DEBUG("========== return_results_idx = %d ==========", return_results_idx);

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

void AggregatePhysicalOperator::do_max_aggregate(Field &field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_max_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
        strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);
  // 检查是否为空
  if (tuples_values_.empty()) {
    Value empty_value;
    empty_value.set_type(field.attr_type());
    aggregate_results_.emplace_back(empty_value);
    return;
  }

  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples_values_) {
    Value &cur_value = t[idx];
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    Value null_value;
    null_value.set_type(AttrType::NONE);
    aggregate_results_.emplace_back(null_value);
    return;
  }

  Value max_value = tuples_values_[0][idx];

  for (auto t : tuples_values_) {
    Value &cur_value = t[idx];
    if (cur_value.compare(max_value) > 0) {
      // FIXME: 实现NULL之后修改为 cur_value != NULL && cur_value.compare(max_value) > 0
      // RESP: 这里不影响，因为任何东西都比null小
      max_value = cur_value;
    }
  }

  aggregate_results_.emplace_back(max_value);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_min_aggregate(Field &field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_min_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
        strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);
  // 检查是否为空
  if (tuples_values_.empty()) {
    Value empty_value;
    empty_value.set_type(field.attr_type());
    aggregate_results_.emplace_back(empty_value);
    return;
  }

  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples_values_) {
    Value &cur_value = t[idx];
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    Value null_value;
    null_value.set_type(AttrType::NONE);
    aggregate_results_.emplace_back(null_value);
    return;
  }

  Value min_value = tuples_values_[0][idx];

  for (auto t : tuples_values_) {
    Value &cur_value = t[idx];
    if (cur_value.compare(min_value) < 0) {
      // FIXME: 实现NULL之后修改为 cur_value != NULL && cur_value.compare(max_value) < 0
      min_value = cur_value;
    }
  }

  aggregate_results_.emplace_back(min_value);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_count_aggregate(Field &field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_count_aggregate(Field& field) ==========");
  int count = 0;

  if (field.table() != nullptr && field.meta() == nullptr) {  // count(*)
    LOG_DEBUG("========== do_count(*) ==========");
    if (tuples_values_.empty()) {
      count = 0;
    }
    count = tuples_values_.size();
  } else {
    int idx;
    for (idx = 0; idx < fields_.size(); idx++) {
      LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
      LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
      LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
      LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

      if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
          strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
        break;
      }
    }

    LOG_DEBUG("========== idx = %d ==========",idx);
    // 检查是否为空
    if (!tuples_values_.empty()) {
      for (auto t : tuples_values_) {
        Value &cur_value = t[idx];
        if (!cur_value.is_null()) {
          count++;
        }
      }
    }
  }

  Value count_value;
  count_value.set_int(count);
  aggregate_results_.emplace_back(count_value);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_avg_aggregate(Field &field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_avg_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
        strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);
  // 检查是否为空
  if (tuples_values_.empty()) {
    Value empty_value;
    empty_value.set_type(field.attr_type());
    aggregate_results_.emplace_back(empty_value);
    return;
  }
  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples_values_) {
    Value &cur_value = t[idx];
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    Value null_value;
    null_value.set_type(AttrType::NONE);
    aggregate_results_.emplace_back(null_value);
    return;
  }

  Value    avg_value;
  int      cnt       = 0;
  AttrType attr_type = tuples_values_[0][idx].attr_type();
  if (attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples_values_) {
      Value &cur_value = t[idx];
      if (!cur_value.is_null()) {
        sum += cur_value.get_int();
        cnt++;
      }
    }
    avg_value.set_int(sum / cnt);
  } else if (attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples_values_) {
      Value &cur_value = t[idx];
      if (!cur_value.is_null()) {
        sum += cur_value.get_float();
        cnt++;
      }
    }
    avg_value.set_float(sum / cnt);
  } else if (attr_type == CHARS) {
    for (auto t : tuples_values_) {
      float  sum       = 0;
      Value &cur_value = t[idx];
      if (!cur_value.is_null()) {
        cur_value.str_to_number();
        if (cur_value.attr_type() == INTS) {
          sum += cur_value.get_int();
          cnt++;
        } else {
          sum += cur_value.get_float();
          cnt++;
        }
      }
      avg_value.set_float(sum / cnt);
    }
    // if (tuples_values_[0][idx].attr_type() == INTS) {
    //   int sum = 0;
    //   for (auto t : tuples_values_) {
    //     Value &cur_value = t[idx];
    //     /*
    //       FIXME: 实现NULL之后需要加入空值判断
    //       if (cur_value != NULL)
    //     */
    //     sum += cur_value.get_int();
    //     cnt++;
    //   }
    //   avg_value.set_int(sum / cnt);
    // } else {
    //   float sum = 0;
    //   for (auto t : tuples_values_) {
    //     Value &cur_value = t[idx];
    //     /*
    //       FIXME: 实现NULL之后需要加入空值判断
    //       if (cur_value != NULL)
    //     */
    //     sum += cur_value.get_float();
    //     cnt++;
    //   }
    //   avg_value.set_float(sum / cnt);
    // }
  } else {  // 其余类型无法求和
    return;
  }

  aggregate_results_.emplace_back(avg_value);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}

void AggregatePhysicalOperator::do_sum_aggregate(Field &field)
{
  LOG_DEBUG("========== In AggregatePhysicalOperator::do_sum_aggregate(Field& field) ==========");
  int idx;
  for (idx = 0; idx < fields_.size(); idx++) {
    LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
    LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

    if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
        strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========",idx);
  // 检查是否为空
  if (tuples_values_.empty()) {
    Value empty_value;
    empty_value.set_type(field.attr_type());
    aggregate_results_.emplace_back(empty_value);
    return;
  }
  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples_values_) {
    Value &cur_value = t[idx];
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    Value null_value;
    null_value.set_type(AttrType::NONE);
    aggregate_results_.emplace_back(null_value);
    return;
  }

  Value    sum_value;
  AttrType attr_type = tuples_values_[0][idx].attr_type();
  if (attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples_values_) {
      Value &cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      if (!cur_value.is_null()) {
        sum += cur_value.get_int();
      }
    }
    sum_value.set_int(sum);
  } else if (attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples_values_) {
      Value &cur_value = t[idx];
      /*
        FIXME: 实现NULL之后需要加入空值判断
        if (cur_value != NULL)
      */
      if (!cur_value.is_null()) {
        sum += cur_value.get_float();
      }
    }
    sum_value.set_float(sum);
  } else if (attr_type == CHARS) {
    for (auto t : tuples_values_) {
      float  sum       = 0;
      Value &cur_value = t[idx];
      if (!cur_value.is_null()) {
        cur_value.str_to_number();
        if (cur_value.attr_type() == INTS) {
          sum += cur_value.get_int();
        } else {
          sum += cur_value.get_float();
        }
      }
      sum_value.set_float(sum);
      // // 处理对字符串求和的情况
      // for (auto t : tuples_values_) {
      //   // 这里有点疑问，CHARS不一定可以是个数
      //   if (!t[idx].is_null()) {
      //     t[idx].str_to_number();
      //   }
      // }
      // if (tuples_values_[0][idx].attr_type() == INTS) {
      //   int sum = 0;
      //   for (auto t : tuples_values_) {
      //     Value &cur_value = t[idx];
      //     /*
      //       FIXME: 实现NULL之后需要加入空值判断
      //       if (cur_value != NULL)
      //     */
      //     if (!cur_value.is_null()) {
      //       sum += cur_value.get_int();
      //     }
      //   }
      //   sum_value.set_int(sum);
      // } else {
      //   float sum = 0;
      //   for (auto t : tuples_values_) {
      //     Value &cur_value = t[idx];
      //     /*
      //       FIXME: 实现NULL之后需要加入空值判断
      //       if (cur_value != NULL)
      //     */
      //     if (!cur_value.is_null()) {
      //       sum += cur_value.get_float();
      //     }
      //   }
      //   sum_value.set_float(sum);
      // }
    }
  } else {  // 其余类型无法求和
    return;
  }

  aggregate_results_.emplace_back(sum_value);

  LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
}
