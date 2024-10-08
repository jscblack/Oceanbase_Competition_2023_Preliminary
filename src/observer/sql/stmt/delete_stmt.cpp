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

#include "sql/parser/parse.h"
#include "sql/stmt/delete_stmt.h"
#include "common/log/log.h"
#include "sql/parser/parse.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

DeleteStmt::DeleteStmt(Table *table, FilterStmt *filter_stmt, Stmt *view_stmt)
    : table_(table), filter_stmt_(filter_stmt), view_stmt_(view_stmt)
{}

DeleteStmt::~DeleteStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC DeleteStmt::create(Db *db, const DeleteSqlNode &delete_sql, Stmt *&stmt)
{
  const char *table_name = delete_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  std::vector<std::pair<std::string, std::string>> relation_to_alias;  // placeholder, 兼容select那边用的
  FilterStmt                                      *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map, relation_to_alias, delete_sql.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // 看一下table是否为视图
  // 如果是，在这里执行创建视图的sql语句的parse和resolve
  Stmt *view_stmt = nullptr;
  if (table->table_meta().is_view()) {
    // 1. view-sql : parse
    const std::string &view_sql = table->table_meta().view_sql();
    ParsedSqlResult    parsed_view_sql_result;

    parse(view_sql.c_str(), &parsed_view_sql_result);

    if (parsed_view_sql_result.sql_nodes().empty()) {
      LOG_WARN("create view sql parsed result empty");
      return RC::INTERNAL;
    }
    if (parsed_view_sql_result.sql_nodes().size() > 1) {
      LOG_WARN("got multi sql commands but only 1 will be handled");
    }

    // 2. view-sql : resolve
    std::unique_ptr<ParsedSqlNode> unique_ptr_sql_node = std::move(parsed_view_sql_result.sql_nodes().front());
    if (unique_ptr_sql_node->flag == SCF_ERROR) {
      return RC::SQL_SYNTAX;
      ;
    }
    ParsedSqlNode *sql_node = unique_ptr_sql_node.get();
    RC             rc       = Stmt::create_stmt(db, *sql_node, view_stmt);
    if (rc != RC::SUCCESS && rc != RC::UNIMPLENMENT) {
      LOG_WARN("failed to create view_stmt. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
  }

  stmt = new DeleteStmt(table, filter_stmt, view_stmt);
  return rc;
}
