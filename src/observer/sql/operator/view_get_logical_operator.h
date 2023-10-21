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
// Created by tong1heng on 2023/10/21.
//
#pragma once

#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"

/**
 * @brief 表示从视图中获取数据的算子
 * @details 基于TableGetLogicalOperator进行修改
 * @ingroup LogicalOperator
 */
class ViewGetLogicalOperator : public LogicalOperator
{
public:
  ViewGetLogicalOperator(Table *table, bool readonly);

  virtual ~ViewGetLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::VIEW_GET; }

  Table *view() const { return table_; }
  bool   readonly() const { return readonly_; }

  void                                      set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);
  std::vector<std::unique_ptr<Expression>> &predicates() { return predicates_; }

private:
  Table *table_    = nullptr;  // 视图
  bool   readonly_ = false;
  std::vector<std::unique_ptr<Expression>> predicates_;
};
