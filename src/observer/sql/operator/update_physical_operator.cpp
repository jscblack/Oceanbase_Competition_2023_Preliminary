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
// Created by JiangShichao on 2023/9/22.
//

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "sql/stmt/update_stmt.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
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

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple(); // 拿上来每一行
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();

    const char *old_data = record.data();
    // record的rid不会发生变化，只会更改其中的数据
    // 但是需要注意的是，这会对index有影响
    // 根据old_data去构造一个new_data
    const TableMeta &table_meta  = table_->table_meta();
    int              record_size = table_meta.record_size();
    char            *new_data    = (char *)malloc(record_size);
    memcpy(new_data, old_data, record_size);

    for (int i = 0; i < table_meta.field_num() - table_meta.sys_field_num(); i++) {
      const FieldMeta *field = table_meta.field(i + table_meta.sys_field_num());
      for (int j = 0; j < attr_names_.size(); j++) {
        if (strcmp(attr_names_[j].c_str(), field->name()) == 0) {
          // 找到了需要更新的字段
          const Value &value    = values_[j];
          size_t       copy_len = field->len();
          if (field->type() == CHARS) {
            const size_t data_len = value.length();
            if (copy_len > data_len) {
              copy_len = data_len + 1;
            }
          }
          memcpy(new_data + field->offset(), value.data(), copy_len);
        }
      }
    }

    rc = trx_->update_record(table_, record, new_data);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }

    // old_record.data();
    // Record new_record;
    // vector<Value>new_values;
    // const TableMeta &table_meta = table_->table_meta();
    // // table_->table_meta_.
    // for (int i = 0; i < table_meta.field_num()-table_meta.sys_field_num(); i++)
    // {

    // }

    // for (int i = 0; i < old_record.size(); i++) {
    //     Value value = old_record.get_value(i);
    //     new_values.push_back(value);
    // }

    // new record;
    // Record record;
    // rc = table_->make_record(static_cast<int>(values_.size()), values_.data(), record);
    // if (rc != RC::SUCCESS) {
    //     LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
    // }

    // remove old record
    // rc = trx_->delete_record(table_, record);
    // if (rc != RC::SUCCESS) {
    //     LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
    // }

    // insert new record
    // rc = trx->insert_record(table_, record);
    // if (rc != RC::SUCCESS) {
    //     LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
    // }
  }

  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
