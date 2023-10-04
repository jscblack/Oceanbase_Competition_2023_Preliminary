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

#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/optimizer/physical_plan_generator.h"

class Trx;
class UpdateStmt;

struct ValueOrPhysOper  // update 的 value其中包含value或select_physical_operator
{
  bool                              value_from_select;         ///< 是否是子查询，默认false
  Value                             literal_value;             ///< value
  std::unique_ptr<PhysicalOperator> select_physical_operator;  ///< select clause

  ValueOrPhysOper() = default;
  ValueOrPhysOper(bool from_select, const Value &value) : value_from_select(from_select), literal_value(value){};
  ValueOrPhysOper(bool from_select, std::unique_ptr<PhysicalOperator> &physical_operator)
      : value_from_select(from_select)
  {
    select_physical_operator = std::move(physical_operator);
  };
};

/**
 * @brief 物理算子，删除
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, std::vector<std::string> attr_names, const std::vector<ValueOrLogiOper> &values)
      : table_(table), attr_names_(attr_names)
  {
    PhysicalPlanGenerator physical_plan_generator_;  ///< 根据逻辑计划生成物理计划
    for (auto &value : values) {
      if (value.value_from_select) {
        std::unique_ptr<PhysicalOperator> physical_operator;
        // 创建select的物理算子
        RC rc = physical_plan_generator_.create(*value.select_logical_operator, physical_operator);
        values_.emplace_back(true, physical_operator);
      } else {
        values_.emplace_back(false, value.literal_value);
      }
    }
  }

  virtual ~UpdatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::DELETE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  Table                       *table_ = nullptr;
  Trx                         *trx_   = nullptr;
  std::vector<std::string>     attr_names_;
  std::vector<ValueOrPhysOper> values_;
};
