/* Copyright (c) OceanBase and/or its affiliates. All rights reserved.
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

#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/stmt/update_stmt.h"
struct ValueOrLogiOper  // update 的 value其中包含value或select_logical_operator
{
  bool                             value_from_select;        ///< 是否是子查询，默认false
  Value                            literal_value;            ///< value
  std::unique_ptr<LogicalOperator> select_logical_operator;  ///< select clause

  ValueOrLogiOper() = default;
  ValueOrLogiOper(bool from_select, const Value &value) : value_from_select(from_select), literal_value(value){};
  ValueOrLogiOper(bool from_select, std::unique_ptr<LogicalOperator> &logical_operator) : value_from_select(from_select)
  {
    select_logical_operator = std::move(logical_operator);
  };
};

/**
 * @brief 逻辑算子，用于执行update语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<std::string> attr_names, std::vector<ValueOrStmt> values);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType                 type() const override { return LogicalOperatorType::UPDATE; }
  Table                              *table() const { return table_; }
  std::vector<std::string>            attr_name() const { return attr_names_; }
  const std::vector<ValueOrLogiOper> &values() const { return values_; }

private:
  Table                       *table_ = nullptr;
  std::vector<std::string>     attr_names_;
  std::vector<ValueOrLogiOper> values_;
};
