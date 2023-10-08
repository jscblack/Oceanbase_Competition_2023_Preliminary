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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
UpdateStmt::UpdateStmt(Table *table, const std::vector<std::string> &field_names,
    const std::vector<ValueOrStmt> &values, FilterStmt *filter_stmt)
    : table_(table), field_names_(field_names), values_(values), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt)
{
  const char *table_name = update_sql.relation_name.c_str();
  // 检查参数合法
  if (nullptr == db || nullptr == table_name || update_sql.attribute_names.empty() || update_sql.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  if (update_sql.attribute_names.size() != update_sql.values.size()) {
    LOG_WARN("invalid argument. attribute_names.size()=%d, values.size()=%d", update_sql.attribute_names.size(), update_sql.values.size());
    return RC::INVALID_ARGUMENT;
  }
  // 检查表是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  // 检查属性名是否合法，value类型是否与attribute类型匹配

  // // check the fields number
  const std::string  *attribute_names = update_sql.attribute_names.data();
  const ComplexValue *complex_values  = update_sql.values.data();
  const int           value_num       = static_cast<int>(update_sql.values.size());
  const TableMeta    &table_meta      = table->table_meta();

  RC                       rc = RC::SUCCESS;
  std::vector<std::string> fields;
  std::vector<ValueOrStmt> update_values;
  fields.reserve(value_num);
  update_values.reserve(value_num);

  // check fields type
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(attribute_names[i].c_str());

    if (nullptr == field_meta) {
      LOG_WARN("no such field. table=%s, field=%s", table_name, attribute_names[i].c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    const AttrType field_type = field_meta->type();
    fields.emplace_back(field_meta->name());
    if (complex_values[i].value_from_select) {
      // from select
      // 首先得去解析select 语句是否合法
      Stmt *select_stmt = nullptr;
      rc                = SelectStmt::create(db, complex_values[i].select_sql, select_stmt);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create select statement. rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      if (reinterpret_cast<SelectStmt *>(select_stmt)->query_fields().size() != 1) {
        LOG_WARN("invalid select statement. select_stmt->query_fields().size()=%d", reinterpret_cast<SelectStmt *>(select_stmt)->query_fields().size());
        return RC::INVALID_ARGUMENT;
      }
      update_values.emplace_back(true, select_stmt);

    } else {
      // from value
      const AttrType value_type = complex_values[i].literal_value.attr_type();
      if (field_type != value_type) {  // TODO try to convert the value type to field type

        if (value_type == AttrType::NONE) {
          // 空值检查
          if (!field_meta->nullable()) {
            LOG_WARN("field can not be null. table=%s, field=%s, field type=%d, value_type=%d",
                     table_name, field_meta->name(), field_type, value_type);
            return RC::SCHEMA_FIELD_MISSING;
          }
        } else {
          // 正常转换
          rc = complex_values[i].literal_value.auto_cast(field_type);
          if (rc != RC::SUCCESS) {
            LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
            table_name, field_meta->name(), field_type, value_type);
            return rc;
          }
        }
      }
      update_values.emplace_back(false, complex_values[i].literal_value);
    }
  }

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  rc                      = FilterStmt::create(
      db, table, &table_map, update_sql.conditions.data(), static_cast<int>(update_sql.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, fields, update_values, filter_stmt);
  return rc;
}
