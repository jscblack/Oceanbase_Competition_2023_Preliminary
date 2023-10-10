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
// Created by Jiangshichao on 2023/9/22.
//
// #pragma once

#include "sql/operator/update_logical_operator.h"
#include "sql/optimizer/logical_plan_generator.cpp"
#include "sql/optimizer/logical_plan_generator.h"

UpdateLogicalOperator::UpdateLogicalOperator(
    Table *table, std::vector<std::string> attr_names, std::vector<ValueOrStmt> values)
    : table_(table), attr_names_(attr_names)
{
  LogicalPlanGenerator logical_plan_generator_;  ///< 根据SQL生成逻辑计划
  for (auto &value : values) {
    if (value.value_from_select) {
      std::unique_ptr<LogicalOperator> logical_operator;
      // 创建select的逻辑算子
      RC rc = logical_plan_generator_.create(value.select_stmt, logical_operator);
      if (rc != RC::SUCCESS) {
        LOG_WARN("create select logical operator failed");
        return;
      }
      values_.emplace_back(true, logical_operator);
    } else {
      values_.emplace_back(false, value.literal_value);
    }
  }
}
