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

    // 废弃代码*********************************************************BEGIN
    // // 拆分成value的写法 TODO: 为了aggregation-func的兼容，需要重构
    // const int          cell_num = tuple->cell_num();
    // std::vector<Value> values(cell_num);
    // for (int i = 0; i < cell_num; i++) {
    //   tuple->cell_at(i, values[i]);
    // }
    // tuples_values_.emplace_back(values);
    // 废弃代码*********************************************************END

    // clone tuple的写法
    Tuple *new_tuple = nullptr;
    tuple->clone(new_tuple);
    tuples_.push_back(new_tuple);
  }
  if (rc != RC::RECORD_EOF) {
    return rc;
  }

  LOG_DEBUG("========== tuples_.size() = %d ========== log by tyh", tuples_.size());

  // 废弃代码*********************************************************BEGIN
  // LOG_DEBUG("========== tuples_values_.size() = %d ==========", tuples_values_.size());
  // LOG_DEBUG("========== tuples_values_[i].size() = %d ==========", tuples_values_[0].size());

  // if (group_by_fields_.empty()) {  // TODO: 为了aggregation-func的兼容，需要重构
  //   for (auto &agg : aggregations_) {
  //     if (agg.first == "MAX") {
  //       do_max_aggregate(agg.second);
  //     } else if (agg.first == "MIN") {
  //       do_min_aggregate(agg.second);
  //     } else if (agg.first == "COUNT") {
  //       do_count_aggregate(agg.second);
  //     } else if (agg.first == "AVG") {
  //       do_avg_aggregate(agg.second);
  //     } else if (agg.first == "SUM") {
  //       do_sum_aggregate(agg.second);
  //     } else {
  //       return RC::INVALID_ARGUMENT;
  //     }
  //   }
  //   // 构造返回的value list tuple
  //   std::vector<Value> result_value;
  //   for (int i = 0; i < aggregate_results_.size(); i++) {
  //     result_value.emplace_back(aggregate_results_[i]);
  //   }
  //   ValueListTuple vlt;
  //   vlt.set_cells(result_value);
  //   return_results_.emplace_back(vlt);
  //   return RC::SUCCESS;
  // }

  // 废弃代码*********************************************************END

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
      } else if(fields_expressions_[i]->type() == ExprType::ARITHMETIC) {
        fields_expressions_[i]->get_value(it->second, v);
        result_value.emplace_back(v);
      } else {
        ASSERT(false, "In AggregatePhysicalOperator::open(Trx *trx): non-group-by selection cannot have non-agg field");
      }
    }
    ValueListTuple vlt;
    vlt.set_cells(result_value);
    return_results_.emplace_back(vlt);
  }

  // 废弃代码*********************************************************BEGIN
  // // 分组
  // // 1. 首先找到分组属性在每行tuple的位置
  // for (auto group_by_field : group_by_fields_) {
  //   for (int idx = 0; idx < fields_.size(); idx++) {
  //     if (strcmp(group_by_field.table_name(), fields_[idx].table_name()) == 0 &&
  //         strcmp(group_by_field.field_name(), fields_[idx].field_name()) == 0) {
  //       group_by_fields_idx_.emplace_back(idx);
  //       break;
  //     }
  //   }
  // }
  // // 2. 根据分组属性将tuples_values填充到映射group_tuples_values中
  // for (auto tuple_ptr : tuples_) {
  //   GroupByValues group_by_values;
  //   Value         v;
  //   for (auto idx : group_by_fields_idx_) {
  //     tuple_ptr->cell_at(idx, v);
  //     group_by_values.data.emplace_back(v);
  //   }
  //   auto it = group_tuples_.find(group_by_values);
  //   if (it != group_tuples_.end()) {
  //     it->second.emplace_back(tuple_ptr);
  //   } else {
  //     std::vector<Tuple *> tmp;
  //     tmp.emplace_back(tuple_ptr);
  //     group_tuples_.insert({group_by_values, tmp});
  //   }
  // }

  // // having分组筛选（可优化，因为having的聚合可能和分组的聚合是重复的，规范做法是先聚集再筛选，但先筛选会比较好实现）
  // if (!having_filter_units_.empty()) {
  //   for (auto it = group_tuples_.begin(); it != group_tuples_.end();) {
  //     Value value;
  //     rc = having_filters_->get_value(it->second, value);
  //     if (!value.get_boolean()) {
  //       group_tuples_.erase(it++);
  //     } else {
  //       it++;
  //     }
  //   }
  // }

  // // 分组聚集
  // // 1. 收集tuple schema信息
  // std::vector<std::pair<Field, int>> result_field_idx;
  // for (int i = 0; i < group_by_fields_.size(); i++) {
  //   for (int idx = 0; idx < fields_expressions_.size(); idx++) {
  //     if (fields_expressions_[idx]->type() == ExprType::FIELD) {
  //       FieldExpr *field_expr = dynamic_cast<FieldExpr *>(fields_expressions_[idx]);
  //       if (group_by_fields_[i] == field_expr->field()) {
  //         result_field_idx.emplace_back(std::make_pair(group_by_fields_[i], idx));
  //         break;
  //       }
  //     }
  //   }
  // }
  // for (int i = 0; i < aggregations_.size(); i++) {
  //   for (int idx = 0; idx < fields_expressions_.size(); idx++) {
  //     if (fields_expressions_[idx]->type() == ExprType::AGGREGATION) {
  //       AggregationExpr *agg_expr = dynamic_cast<AggregationExpr *>(fields_expressions_[idx]);
  //       if (aggregations_[i].second == agg_expr->field()) {
  //         result_field_idx.emplace_back(std::make_pair(aggregations_[i].second, idx));
  //       }
  //     }
  //   }
  // }

  // // 2. 构造结果
  // // Require: 根据重构后的fields_expressions_
  // for (auto it = group_tuples_.begin(); it != group_tuples_.end(); it++) {
  //   std::vector<Value> result_value;  // 该分组的结果
  //   result_value.resize(fields_expressions_.size());
  //   // 分组属性
  //   for (int k = 0; k < group_by_fields_.size(); k++) {
  //     for (int i = 0; i < result_field_idx.size(); i++) {
  //       if (group_by_fields_[k] == result_field_idx[i].first) {
  //         result_value[result_field_idx[i].second] = it->first.data[k];
  //       }
  //     }
  //   }

  //   // 聚合属性
  //   for (auto &agg : aggregations_) {
  //     AggregationExpr agg_expr(agg.second, agg.first);
  //     Value           v;
  //     agg_expr.get_value(it->second, v);
  //     for (int i = 0; i < result_field_idx.size(); i++) {
  //       if (agg.second == result_field_idx[i].first) {
  //         result_value[result_field_idx[i].second] = v;
  //       }
  //     }
  //   }
  //   ValueListTuple vlt;
  //   vlt.set_cells(result_value);
  //   return_results_.emplace_back(vlt);
  // }
  // 废弃代码*********************************************************END

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

// void AggregatePhysicalOperator::do_max_aggregate(Field &field)
// {
//   LOG_DEBUG("========== In AggregatePhysicalOperator::do_max_aggregate(Field& field) ==========");
//   int idx;
//   for (idx = 0; idx < fields_.size(); idx++) {
//     LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
//     LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
//     LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
//     LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

//     if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
//         strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
//       break;
//     }
//   }

//   LOG_DEBUG("========== idx = %d ==========",idx);
//   // 检查是否为空
//   if (tuples_values_.empty()) {
//     Value empty_value;
//     empty_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(empty_value);
//     return;
//   }

//   // 检查是否均为null
//   bool all_null = true;
//   for (auto t : tuples_values_) {
//     Value &cur_value = t[idx];
//     if (!cur_value.is_null()) {
//       all_null = false;
//       break;
//     }
//   }
//   if (all_null) {
//     Value null_value;
//     null_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(null_value);
//     return;
//   }

//   Value max_value = tuples_values_[0][idx];

//   for (auto t : tuples_values_) {
//     Value &cur_value = t[idx];
//     if (cur_value.compare(max_value) > 0) {
//       // FIXME: 实现NULL之后修改为 cur_value != NULL && cur_value.compare(max_value) > 0
//       // RESP: 这里不影响，因为任何东西都比null小
//       max_value = cur_value;
//     }
//   }

//   aggregate_results_.emplace_back(max_value);

//   LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
// }

// void AggregatePhysicalOperator::do_min_aggregate(Field &field)
// {
//   LOG_DEBUG("========== In AggregatePhysicalOperator::do_min_aggregate(Field& field) ==========");
//   int idx;
//   for (idx = 0; idx < fields_.size(); idx++) {
//     LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
//     LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
//     LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
//     LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

//     if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
//         strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
//       break;
//     }
//   }

//   LOG_DEBUG("========== idx = %d ==========",idx);
//   // 检查是否为空
//   if (tuples_values_.empty()) {
//     Value empty_value;
//     empty_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(empty_value);
//     return;
//   }

//   // 检查是否均为null
//   bool all_null = true;
//   for (auto t : tuples_values_) {
//     Value &cur_value = t[idx];
//     if (!cur_value.is_null()) {
//       all_null = false;
//       break;
//     }
//   }
//   if (all_null) {
//     Value null_value;
//     null_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(null_value);
//     return;
//   }

//   Value min_value = tuples_values_[0][idx];

//   for (auto t : tuples_values_) {
//     Value &cur_value = t[idx];
//     if (cur_value.compare(min_value) < 0) {
//       // FIXME: 实现NULL之后修改为 cur_value != NULL && cur_value.compare(max_value) < 0
//       min_value = cur_value;
//     }
//   }

//   aggregate_results_.emplace_back(min_value);

//   LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
// }

// void AggregatePhysicalOperator::do_count_aggregate(Field &field)
// {
//   LOG_DEBUG("========== In AggregatePhysicalOperator::do_count_aggregate(Field& field) ==========");
//   int count = 0;

//   if (field.meta() == nullptr) {  // count(*)
//     LOG_DEBUG("========== do_count(*) ==========");
//     if (tuples_values_.empty()) {
//       count = 0;
//     }
//     count = tuples_values_.size();
//   } else {
//     int idx;
//     for (idx = 0; idx < fields_.size(); idx++) {
//       LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
//       LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
//       LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
//       LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

//       if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
//           strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
//         break;
//       }
//     }

//     LOG_DEBUG("========== idx = %d ==========",idx);
//     // 检查是否为空
//     if (!tuples_values_.empty()) {
//       for (auto t : tuples_values_) {
//         Value &cur_value = t[idx];
//         if (!cur_value.is_null()) {
//           count++;
//         }
//       }
//     }
//   }

//   Value count_value;
//   count_value.set_int(count);
//   aggregate_results_.emplace_back(count_value);

//   LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
// }

// void AggregatePhysicalOperator::do_avg_aggregate(Field &field)
// {
//   LOG_DEBUG("========== In AggregatePhysicalOperator::do_avg_aggregate(Field& field) ==========");
//   int idx;
//   for (idx = 0; idx < fields_.size(); idx++) {
//     LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
//     LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
//     LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
//     LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

//     if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
//         strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
//       break;
//     }
//   }

//   LOG_DEBUG("========== idx = %d ==========",idx);
//   // 检查是否为空
//   if (tuples_values_.empty()) {
//     Value empty_value;
//     empty_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(empty_value);
//     return;
//   }
//   // 检查是否均为null
//   bool all_null = true;
//   for (auto t : tuples_values_) {
//     Value &cur_value = t[idx];
//     if (!cur_value.is_null()) {
//       all_null = false;
//       break;
//     }
//   }
//   if (all_null) {
//     Value null_value;
//     null_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(null_value);
//     return;
//   }

//   Value    avg_value;
//   int      cnt       = 0;
//   AttrType attr_type = tuples_values_[0][idx].attr_type();
//   if (attr_type == INTS) {
//     int sum = 0;
//     for (auto t : tuples_values_) {
//       Value &cur_value = t[idx];
//       if (!cur_value.is_null()) {
//         sum += cur_value.get_int();
//         cnt++;
//       }
//     }
//     if (sum % cnt == 0) {
//       avg_value.set_int(sum / cnt);
//     } else {
//       avg_value.set_float((float)sum / (float)cnt);
//     }
//   } else if (attr_type == FLOATS) {
//     float sum = 0;
//     for (auto t : tuples_values_) {
//       Value &cur_value = t[idx];
//       if (!cur_value.is_null()) {
//         sum += cur_value.get_float();
//         cnt++;
//       }
//     }
//     avg_value.set_float(sum / cnt);
//   } else if (attr_type == CHARS) {
//     for (auto t : tuples_values_) {
//       float  sum       = 0;
//       Value &cur_value = t[idx];
//       if (!cur_value.is_null()) {
//         cur_value.str_to_number();
//         if (cur_value.attr_type() == INTS) {
//           sum += cur_value.get_int();
//           cnt++;
//         } else {
//           sum += cur_value.get_float();
//           cnt++;
//         }
//       }
//       avg_value.set_float(sum / cnt);
//     }
//     // if (tuples_values_[0][idx].attr_type() == INTS) {
//     //   int sum = 0;
//     //   for (auto t : tuples_values_) {
//     //     Value &cur_value = t[idx];
//     //     /*
//     //       FIXME: 实现NULL之后需要加入空值判断
//     //       if (cur_value != NULL)
//     //     */
//     //     sum += cur_value.get_int();
//     //     cnt++;
//     //   }
//     //   avg_value.set_int(sum / cnt);
//     // } else {
//     //   float sum = 0;
//     //   for (auto t : tuples_values_) {
//     //     Value &cur_value = t[idx];
//     //     /*
//     //       FIXME: 实现NULL之后需要加入空值判断
//     //       if (cur_value != NULL)
//     //     */
//     //     sum += cur_value.get_float();
//     //     cnt++;
//     //   }
//     //   avg_value.set_float(sum / cnt);
//     // }
//   } else {  // 其余类型无法求和
//     return;
//   }

//   aggregate_results_.emplace_back(avg_value);

//   LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
// }

// void AggregatePhysicalOperator::do_sum_aggregate(Field &field)
// {
//   LOG_DEBUG("========== In AggregatePhysicalOperator::do_sum_aggregate(Field& field) ==========");
//   int idx;
//   for (idx = 0; idx < fields_.size(); idx++) {
//     LOG_DEBUG("========== field.table_name() = %s ==========",field.table_name());
//     LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",fields_[idx].table_name());
//     LOG_DEBUG("========== field.field_name() = %s ==========",field.field_name());
//     LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",fields_[idx].field_name());

//     if (strcmp(field.table_name(), fields_[idx].table_name()) == 0 &&
//         strcmp(field.field_name(), fields_[idx].field_name()) == 0) {
//       break;
//     }
//   }

//   LOG_DEBUG("========== idx = %d ==========",idx);
//   // 检查是否为空
//   if (tuples_values_.empty()) {
//     Value empty_value;
//     empty_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(empty_value);
//     return;
//   }
//   // 检查是否均为null
//   bool all_null = true;
//   for (auto t : tuples_values_) {
//     Value &cur_value = t[idx];
//     if (!cur_value.is_null()) {
//       all_null = false;
//       break;
//     }
//   }
//   if (all_null) {
//     Value null_value;
//     null_value.set_type(AttrType::NONE);
//     aggregate_results_.emplace_back(null_value);
//     return;
//   }

//   Value    sum_value;
//   AttrType attr_type = tuples_values_[0][idx].attr_type();
//   if (attr_type == INTS) {
//     int sum = 0;
//     for (auto t : tuples_values_) {
//       Value &cur_value = t[idx];
//       /*
//         FIXME: 实现NULL之后需要加入空值判断
//         if (cur_value != NULL)
//       */
//       if (!cur_value.is_null()) {
//         sum += cur_value.get_int();
//       }
//     }
//     sum_value.set_int(sum);
//   } else if (attr_type == FLOATS) {
//     float sum = 0;
//     for (auto t : tuples_values_) {
//       Value &cur_value = t[idx];
//       /*
//         FIXME: 实现NULL之后需要加入空值判断
//         if (cur_value != NULL)
//       */
//       if (!cur_value.is_null()) {
//         sum += cur_value.get_float();
//       }
//     }
//     sum_value.set_float(sum);
//   } else if (attr_type == CHARS) {
//     for (auto t : tuples_values_) {
//       float  sum       = 0;
//       Value &cur_value = t[idx];
//       if (!cur_value.is_null()) {
//         cur_value.str_to_number();
//         if (cur_value.attr_type() == INTS) {
//           sum += cur_value.get_int();
//         } else {
//           sum += cur_value.get_float();
//         }
//       }
//       sum_value.set_float(sum);
//       // // 处理对字符串求和的情况
//       // for (auto t : tuples_values_) {
//       //   // 这里有点疑问，CHARS不一定可以是个数
//       //   if (!t[idx].is_null()) {
//       //     t[idx].str_to_number();
//       //   }
//       // }
//       // if (tuples_values_[0][idx].attr_type() == INTS) {
//       //   int sum = 0;
//       //   for (auto t : tuples_values_) {
//       //     Value &cur_value = t[idx];
//       //     /*
//       //       FIXME: 实现NULL之后需要加入空值判断
//       //       if (cur_value != NULL)
//       //     */
//       //     if (!cur_value.is_null()) {
//       //       sum += cur_value.get_int();
//       //     }
//       //   }
//       //   sum_value.set_int(sum);
//       // } else {
//       //   float sum = 0;
//       //   for (auto t : tuples_values_) {
//       //     Value &cur_value = t[idx];
//       //     /*
//       //       FIXME: 实现NULL之后需要加入空值判断
//       //       if (cur_value != NULL)
//       //     */
//       //     if (!cur_value.is_null()) {
//       //       sum += cur_value.get_float();
//       //     }
//       //   }
//       //   sum_value.set_float(sum);
//       // }
//     }
//   } else {  // 其余类型无法求和
//     return;
//   }

//   aggregate_results_.emplace_back(sum_value);

//   LOG_DEBUG("========== aggregate_results_.size() = %d ==========", aggregate_results_.size());
// }
