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
// Created by LuoYuanhui on 2023/10/10
//

#include "sql/operator/sort_physical_operator.h"

RC SortPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  RC rc = children_[0]->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = children_[0]->next())) {
    Tuple *tuple = children_[0]->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }
    Tuple *new_tuple = nullptr;
    tuple->clone(new_tuple);
    sorted_tuples_.push_back(new_tuple);
  }

  auto cmp = [&](const Tuple *a, const Tuple *b) -> bool {
    for (auto &order : orders_) {
      Field        &field  = order.first;
      bool          is_asc = order.second;
      TupleCellSpec spec(field.table_name(), field.field_name(), field.field_name());
      Value         v_a, v_b;
      a->find_cell(spec, v_a);
      b->find_cell(spec, v_b);
      if (v_a.compare(v_b) == 0) {
        continue;
      } else if (v_a.compare(v_b) < 0) {
        return is_asc ? true : false;
      } else {
        return is_asc ? false : true;
      }
    }
    return true;
  };

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), cmp);
  return RC::SUCCESS;
}

RC SortPhysicalOperator::next()
{
  cur_idx_++;
  assert(cur_idx_ >= 0);
  if (cur_idx_ < sorted_tuples_.size() && sorted_tuples_.size() != 0) {
    return RC::SUCCESS;
  } else {
    return RC::RECORD_EOF;
  }
}

RC SortPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  for (Tuple *tuple : sorted_tuples_) {
    delete tuple;
  }
  return RC::SUCCESS;
}

Tuple *SortPhysicalOperator::current_tuple() { return sorted_tuples_[cur_idx_]; }