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

#include "sql/operator/view_scan_physical_operator.h"
#include "storage/table/table.h"
#include "event/sql_debug.h"

using namespace std;

RC ViewScanPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("view scan operator must has one child");
    return RC::INTERNAL;
  }
  trx_ = trx;
  return children_[0]->open(trx);
}

RC ViewScanPhysicalOperator::next()
{
  RC                rc            = RC::SUCCESS;
  bool              filter_result = false;
  PhysicalOperator *oper          = children_.front().get();

  while ((rc = oper->next()) == RC::SUCCESS) {
    Tuple *tuple = oper->current_tuple();

    // TODO: 需要将tuple转换，尤其是需要先把视图的select计算出来，调用child oper的cell_at
    // 新搞一个view tuple

    int                cell_num = tuple->cell_num();
    std::vector<Value> values;
    for (int i = 0; i < cell_num; i++) {
      Value value;
      rc = tuple->cell_at(i, value);
      values.emplace_back(value);
    }
    view_tuple_.set_cells(values);
    view_tuple_.set_table(table_);

    rc = filter(&view_tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    if (filter_result) {
      sql_debug("get a tuple from view: %s", view_tuple_.to_string().c_str());
      break;
    } else {
      sql_debug("a tuple from view is filtered: %s", view_tuple_.to_string().c_str());
      rc = RC::RECORD_EOF;
    }
  }
  return rc;
}

RC ViewScanPhysicalOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *ViewScanPhysicalOperator::current_tuple() { return &view_tuple_; }

string ViewScanPhysicalOperator::param() const { return table_->name(); }

void ViewScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

RC ViewScanPhysicalOperator::filter(Tuple *tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(*tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}
