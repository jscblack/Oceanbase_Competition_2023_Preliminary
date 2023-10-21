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

#include "sql/operator/project_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    if (no_table_select_) {
      counter_for_select_func = 0;
    }
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (no_table_select_) {
    if (0 == counter_for_select_func) {
      counter_for_select_func++;
      return RC::SUCCESS;
    }
  }
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  return children_[0]->next();
}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
Tuple *ProjectPhysicalOperator::current_tuple()
{
  if (no_table_select_) {
    tuple_.set_tuple(nullptr);
    tuple_.set_is_func(true);
    return &tuple_;
  }
  // 注意这里set_tuple是没有判断child
  ASSERT(children_[0]->current_tuple() != nullptr, "project oper's child->current_tuple() == nullptr!");
  tuple_.set_tuple(children_[0]->current_tuple());
  return &tuple_;
}

// void ProjectPhysicalOperator::add_projection(const Table *table, const FieldMeta *field_meta)
// {
//   // 对单表来说，展示的(alias) 字段总是字段名称，
//   // 对多表查询来说，展示的alias 需要带表名字
//   TupleCellSpec *spec = new TupleCellSpec(table->name(), field_meta->name(), field_meta->name());
//   tuple_.add_cell_spec(spec);
// }

void ProjectPhysicalOperator::add_expressions(std::vector<std::unique_ptr<Expression>> &expressions)
{
  tuple_.set_expressions(expressions);
}