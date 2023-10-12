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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

RC SelectStmt::create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `from` statement
  std::vector<Table *>                     tables;
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(table_name, table));
  }
  for (auto table : table_map) {
    table_map_.insert(table);
  }

  // collect query fields in `select` statement
  std::vector<Field>                         query_fields;
  std::vector<std::pair<std::string, Field>> aggregation_func;
  std::vector<std::pair<Field, bool>>        order_by;
  for (int i = static_cast<int>(select_sql.attributes.size()) - 1; i >= 0; i--) {
    const RelAttrSqlNode &relation_attr = select_sql.attributes[i];

    if (common::is_blank(relation_attr.relation_name.c_str()) &&
        0 == strcmp(relation_attr.attribute_name.c_str(), "*")) {  // 表名为空且查询所有属性(*)
      for (Table *table : tables) {
        wildcard_fields(table, query_fields);
        // FieldExpr
      }

      // 记录field的聚合信息：这里如果有聚合，只可能是count(*)
      if (0 == strcmp(relation_attr.aggregation_func.c_str(), "COUNT")) {
        aggregation_func.emplace_back("COUNT", Field(tables[0], nullptr));
      }

    } else if (!common::is_blank(relation_attr.relation_name.c_str())) {  // 表名非空
      const char *table_name           = relation_attr.relation_name.c_str();
      const char *field_name           = relation_attr.attribute_name.c_str();
      const char *aggregation_function = relation_attr.aggregation_func.c_str();

      if (0 == strcmp(table_name, "*")) {  // table_name == "*"
        if (0 != strcmp(field_name, "*")) {
          LOG_WARN("invalid field name while table is *. attr=%s", field_name);
          return RC::SCHEMA_FIELD_MISSING;
        }
        for (Table *table : tables) {
          wildcard_fields(table, query_fields);
        }

        // 记录field的聚合信息：这里如果有聚合，只可能是count(*)
        if (0 == strcmp(aggregation_function, "COUNT")) {
          aggregation_func.emplace_back("COUNT", Field(tables[0], nullptr));
        }

      } else {  // table_name != "*"
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table *table = iter->second;
        if (0 == strcmp(field_name, "*")) {
          wildcard_fields(table, query_fields);
          // 记录field的聚合信息：这里如果有聚合，只可能是count(*)
          if (0 == strcmp(aggregation_function, "COUNT")) {
            aggregation_func.emplace_back("COUNT", Field(table, nullptr));
          }
        } else {
          const FieldMeta *field_meta = table->table_meta().field(field_name);
          if (nullptr == field_meta) {
            LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          query_fields.push_back(Field(table, field_meta));

          // 记录field的聚合信息：这里可能存在5种类型的聚合
          if (0 != strcmp(aggregation_function, "")) {
            aggregation_func.emplace_back(std::string(aggregation_function), Field(table, field_meta));
          }
        }
      }
    } else {                     // 表名为空，但不是查询所有属性
      if (tables.size() != 1) {  // table_name从from中获取，from的table必须唯一
        LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table           *table      = tables[0];
      const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name.c_str());
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      query_fields.push_back(Field(table, field_meta));

      // 记录field的聚合信息：这里可能存在5种类型的聚合
      if (0 != strcmp(relation_attr.aggregation_func.c_str(), "")) {
        aggregation_func.emplace_back(std::string(relation_attr.aggregation_func.c_str()), Field(table, field_meta));
      }
    }
  }

  LOG_INFO("got %d tables in from stmt and %d fields in query stmt", tables.size(), query_fields.size());

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db, default_table, &table_map_, select_sql.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // 在没有group by语句时，如果有聚合，则一定不能有非聚合的属性出现
  if (!aggregation_func.empty()) {
    for (auto attr : select_sql.attributes) {
      if (attr.aggregation_func == "") {
        return RC::SQL_SYNTAX;
      }
    }
  }

  // 创造order by语句
  for (int i = static_cast<int>(select_sql.orders.size()) - 1; i >= 0; i--) {
    const RelAttrSqlNode &order_attr = select_sql.orders.at(i).attr;
    bool                  is_asc     = select_sql.orders.at(i).is_asc;

    if (0 != strcmp(order_attr.aggregation_func.c_str(), "")) {  // 处理一下aggregate的特殊场景
      return RC::SQL_SYNTAX;
    }

    if (common::is_blank(order_attr.relation_name.c_str()) &&
        0 == strcmp(order_attr.attribute_name.c_str(), "*")) {  // 表名为空且查询*
      return RC::SQL_SYNTAX;
    } else if (!common::is_blank(order_attr.relation_name.c_str())) {  // 表名非空
      const char *table_name = order_attr.relation_name.c_str();
      const char *field_name = order_attr.attribute_name.c_str();
      if (0 == strcmp(table_name, "*")) {  // table_name == "*"
        return RC::SQL_SYNTAX;
      } else {
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table *table = iter->second;
        if (0 == strcmp(field_name, "*")) {
          return RC::SQL_SYNTAX;
        } else {
          const FieldMeta *field_meta = table->table_meta().field(field_name);
          if (nullptr == field_meta) {
            LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          order_by.emplace_back(Field(table, field_meta), is_asc);
        }
      }
    } else {                     // 表名为空，但不是查询所有属性
      if (tables.size() != 1) {  // table_name从from中获取，from的table必须唯一
        LOG_WARN("invalid. I do not know the attr's table. attr=%s", order_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table           *table      = tables[0];
      const FieldMeta *field_meta = table->table_meta().field(order_attr.attribute_name.c_str());
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), order_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      order_by.emplace_back(Field(table, field_meta), is_asc);
    }
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  // TODO add expression copy
  select_stmt->tables_.swap(tables);
  select_stmt->query_fields_.swap(query_fields);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->aggregation_func_.swap(aggregation_func);
  select_stmt->order_by_.swap(order_by);
  stmt = select_stmt;
  // remove table_map_
  for (auto table : table_map) {
    table_map_.erase(table.first);
  }
  return RC::SUCCESS;
}
