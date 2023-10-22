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
// Created by tong1heng on 2023/10/22.
//

#pragma once

#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"
#include "common/rc.h"

class Table;

/**
 * @brief 视图扫描物理算子
 * @ingroup PhysicalOperator
 */
class ViewScanPhysicalOperator : public PhysicalOperator
{
public:
  ViewScanPhysicalOperator(Table *table, bool readonly) : table_(table), readonly_(readonly) {}

  virtual ~ViewScanPhysicalOperator() = default;

  std::string param() const override;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::VIEW_SCAN; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);

private:
  RC filter(Tuple *tuple, bool &result);

private:
  Table                                   *table_    = nullptr; // view
  Trx                                     *trx_      = nullptr;
  bool                                     readonly_ = false;
  std::vector<std::unique_ptr<Expression>> predicates_;  // TODO chang predicate to table tuple filter

  // Tuple*                                   tuple_;  // 下层算子传上来的project tuple或者valuelist tuple
  ViewTuple                                   view_tuple_;  // 下层算子传上来的project/valuelist tuple转换为view tuple
};
