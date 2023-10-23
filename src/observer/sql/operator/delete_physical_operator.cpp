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
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/delete_physical_operator.h"
#include "common/log/log.h"
#include "sql/stmt/delete_stmt.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC DeletePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC DeletePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }
    Table *original_table = table_;

    if (table_->table_meta().is_view()) {
      // 拿上来的一定是view tuple
      ViewTuple *view_tuple = dynamic_cast<ViewTuple *>(tuple);
      // TODO: 可以取出所有的table的映射关系
      // 注意最前面的是最底层的，因此需要reverse遍历映射回原始表

      // TODO: 将当前的table_转换成原始表
      original_table = const_cast<Table *>(view_tuple->get_view_map().begin()->second);

      // TODO: 通过view_tuple拿上来最原始的row tuple
      tuple = view_tuple->get_tuple();
      // TODO: 在原始表中删除相关记录
    }

    RowTuple *row_tuple = dynamic_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    rc                  = trx_->delete_record(original_table, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::RECORD_EOF;
}

RC DeletePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
