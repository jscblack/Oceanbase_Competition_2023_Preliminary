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
// Created by WangYunlai on 2022/07/01.
//

#pragma once

#include "sql/operator/physical_operator.h"

/**
 * @brief 选择/投影物理算子
 * @ingroup PhysicalOperator
 */
class ProjectPhysicalOperator : public PhysicalOperator
{
public:
  ProjectPhysicalOperator() {}

  virtual ~ProjectPhysicalOperator() = default;

  void add_expressions(std::vector<std::unique_ptr<Expression>> &expressions);

  PhysicalOperatorType type() const override { return PhysicalOperatorType::PROJECT; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const { return tuple_.cell_num(); }

  Tuple *current_tuple() override;
  void   set_no_table_select(bool no_table_select) { no_table_select_ = no_table_select; }
  bool   no_table_select() const { return no_table_select_; }

private:
  inline static int counter_for_select_func = -1;  // select_func只能调用一次next
  ProjectTuple      tuple_;
  bool              no_table_select_ = false;
};
