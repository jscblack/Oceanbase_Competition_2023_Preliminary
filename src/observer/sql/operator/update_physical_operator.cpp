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

/* 细节说明：
 * 此时的UpdatePhysicalOperator，其孩子节点即为表中的筛选条件
 * 更新的select子句都在valueorphysoper中
 */
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

  bool value_got = false;  // 标记是否已经拿到了select中的值

  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();  // 拿上来每一行
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }
    // 将获得值的逻辑下移
    if (!value_got) {
      value_got = true;
      for (auto &value : values_) {
        // 这里需要判断value的类型，如果是一个表达式，那么需要计算出来
        if (value.value_from_select) {
          std::unique_ptr<PhysicalOperator> &value_select = value.select_physical_operator;
          rc                                              = value_select->open(trx_);
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to open child operator: %s", strrc(rc));
            return rc;
          }

          if (RC::SUCCESS == (rc = value_select->next())) {
            // only grab one
            Tuple *tuple = value_select->current_tuple();
            if (tuple == nullptr) {
              LOG_WARN("failed to get current record: %s", strrc(rc));
              return RC::INTERNAL;
            }
            if (tuple->cell_num() > 1) {
              LOG_WARN("invalid select result, too much columns");
              return RC::INTERNAL;
            }
            rc                      = tuple->cell_at(0, value.literal_value);
            value.value_from_select = false;  // 从select中拿到了值，不需要再计算了
            if (rc != RC::SUCCESS) {
              LOG_WARN("failed to get cell: %s", strrc(rc));
              return rc;
            }
          } else if (rc == RC::RECORD_EOF) {
            LOG_WARN("select result, no rows");
            // 查无此记录，给个none
            value.literal_value.set_type(AttrType::NONE);
            value.value_from_select = false;  // 从select中拿到了值，不需要再计算了
          } else {
            LOG_WARN("failed to get next record: %s", strrc(rc));
            return rc;
          }
          // 再尝试拿一次，如果拿到了，那么就是错误的，因为select出的结果必然只有一行
          if (RC::RECORD_EOF != (rc = value_select->next())) {
            LOG_WARN("invalid select result, too much rows");
            return RC::INTERNAL;
          }
          // 关闭子查询
          value_select->close();
        }
      }
    }


    if (table_->table_meta().is_view()) {
      // 拿上来的一定是view tuple
      ViewTuple *view_tuple = dynamic_cast<ViewTuple *>(tuple);
      // TODO: 可以取出所有的table/field的映射关系
      // 注意最前面的是最底层的，因此需要reverse遍历映射回原始表

      // TODO: 将当前的table_转换成原始表

      // TODO: 将当前要更新的列名转换为原始表的field name

      // TODO: 完成对原始表相关列的更新

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
    const FieldMeta *null_field  = table_meta.null_field();
    // 取出null_field的值
    char *null_field_data = new_data + null_field->offset();
    memcpy(new_data, old_data, record_size);

    for (int i = 0; i < table_meta.field_num() - table_meta.sys_field_num(); i++) {
      const FieldMeta *field = table_meta.field(i + table_meta.sys_field_num());
      for (int j = 0; j < attr_names_.size(); j++) {
        if (strcmp(attr_names_[j].c_str(), field->name()) == 0) {
          // 找到了需要更新的字段
          // 必然在字面量中，否则出错
          assert(values_[j].value_from_select == false);
          const Value &value = values_[j].literal_value;

          // 此处进行一次兜底的类型转换
          if (!value.is_null() && value.attr_type() != field->type()) {
            rc = value.auto_cast(field->type());
            if (rc != RC::SUCCESS) {
              LOG_WARN("failed to auto cast value: %s", strrc(rc));
              return rc;
            }
          }
          if (value.is_null()) {
            // 空值额外处理
            if (!field->nullable()) {
              LOG_WARN("field %s is not nullable",field->name());
              return RC::RECORD_UNNULLABLE;
            }
            memset(new_data + field->offset(), 0, field->len());
            // 设置null_field的值
            null_field_data[i / CHAR_BIT] |= (1 << (i % CHAR_BIT));
          } else {
            size_t copy_len = field->len();
            if (field->type() == CHARS) {
              const size_t data_len = value.length();
              if (copy_len > data_len) {
                copy_len = data_len + 1;
              }
            }
            // 设置null_field的值
            null_field_data[i / CHAR_BIT] &= ~(1 << (i % CHAR_BIT));
            memcpy(new_data + field->offset(), value.data(), copy_len);
          }
        }
      }
    }

    rc = trx_->update_record(table_, record, new_data);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
    free(new_data);
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
