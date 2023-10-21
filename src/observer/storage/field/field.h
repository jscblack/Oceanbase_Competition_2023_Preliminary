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
// Created by Wangyunlai on 2022/07/05.
//

#pragma once

#include "storage/field/field_meta.h"
#include "storage/table/table.h"

/**
 * @brief 字段
 *
 */
class Field
{
public:
  Field() = default;
  Field(const Table *table, const FieldMeta *field) : table_(table), field_(field) {}
  Field(const Field &) = default;

  bool operator==(const Field &that) const
  {
    if (this->table_ == nullptr && this->field_ == nullptr) {
      if (that.table_ == nullptr && that.field_ == nullptr) {
        return true;
      } else {
        return false;
      }
    } else if (this->table_ != nullptr && this->field_ == nullptr) {
      if (that.table_ != nullptr && that.field_ == nullptr && strcmp(this->table_name(), that.table_name()) == 0) {
        return true;
      } else {
        return false;
      }
    } else {
      if (that.table_ != nullptr && that.field_ != nullptr && strcmp(this->table_name(), that.table_name()) == 0 &&
          strcmp(this->field_name(), that.field_name()) == 0) {
        return true;
      } else {
        return false;
      }
    }
  }

  const Table     *table() const { return table_; }
  const FieldMeta *meta() const { return field_; }

  AttrType attr_type() const { return field_->type(); }

  const char *table_name() const
  {
    if (table_ == nullptr) {
      return "";
    }
    return table_->name();
  }
  const char *field_name() const
  {
    if (field_ == nullptr) {
      return "*";
    }
    return field_->name();
  }

  void set_table(const Table *table) { this->table_ = table; }
  void set_field(const FieldMeta *field) { this->field_ = field; }

  void set_int(Record &record, int value);
  int  get_int(const Record &record);

  bool        is_null();
  const char *get_data(const Record &record);

private:
  const Table     *table_ = nullptr;
  const FieldMeta *field_ = nullptr;
};
