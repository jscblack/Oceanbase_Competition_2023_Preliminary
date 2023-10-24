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
// Created by tong1heng on 2023/10/21
//

#include "sql/operator/view_get_logical_operator.h"

ViewGetLogicalOperator::ViewGetLogicalOperator(Table *table, bool readonly) : table_(table), readonly_(readonly) {}

void ViewGetLogicalOperator::set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}
