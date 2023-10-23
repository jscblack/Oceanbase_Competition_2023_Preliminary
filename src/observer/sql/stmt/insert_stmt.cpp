/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/parser/parse.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

InsertStmt::InsertStmt(Table *table, std::vector<std::vector<Value>> values, int value_amount, int record_amount)
    : table_(table), values_(values), value_amount_(value_amount), record_amount_(record_amount)
{}

RC InsertStmt::create(Db *db, InsertSqlNode &inserts, Stmt *&stmt)
{
  RC          rc         = RC::SUCCESS;
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  while (table->table_meta().is_view()) {
    // 需要在这里拿到底层的table
    // 更新table与table_name
    // 更新inserts.values

    // 这里的value与field_meta里的信息应该是一一对应的
    // 也就是说得拿到view的字段与底层表的字段的对应关系
    // 先搞一个最naive的实现法，即simple视图 一对一 的插入情况（只用更新table）
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
    ParsedSqlNode *sql_node  = unique_ptr_sql_node.get();
    Stmt          *view_stmt = nullptr;
    RC             rc        = Stmt::create_stmt(db, *sql_node, view_stmt);
    if (rc != RC::SUCCESS && rc != RC::UNIMPLENMENT) {
      LOG_WARN("failed to create view_stmt. rc=%d:%s", rc, strrc(rc));
      return rc;
    }

    SelectStmt *view_select_stmt = dynamic_cast<SelectStmt *>(view_stmt);
    if (view_select_stmt->tables().size() != 1) {
      return RC::SCHEMA_VIEW_NOT_SIMPLE;
    }
    Table *original_table = view_select_stmt->tables().at(0);
    // 这里需要根据现有的fields与original_table的fields进行对应
    std::vector<std::vector<Value>> re_values;
    re_values.resize(inserts.values.size());
    for (int i = 0; i < inserts.values.size(); i++) {
      re_values[i].resize(original_table->table_meta().field_num() - original_table->table_meta().sys_field_num());
      // 每一个都给null value
      Value null_value;
      null_value.set_type(AttrType::NONE);
      for (int j = 0; j < re_values[i].size(); j++) {
        re_values[i][j] = null_value;
        const char *cur_fd_name =
            original_table->table_meta().field_metas()->at(j + original_table->table_meta().sys_field_num()).name();
        // 去query的field里找对应的field
        for (int k = 0; k < view_select_stmt->query_fields_expressions().size(); k++) {
          Expression *fd_expr = view_select_stmt->query_fields_expressions().at(k);
          if (fd_expr->type() == ExprType::FIELD) {
            FieldExpr *fd_field_expr = dynamic_cast<FieldExpr *>(fd_expr);
            if (strcmp(fd_field_expr->field_name(), cur_fd_name) == 0) {
              re_values[i][j] = inserts.values[i][k];
              break;
            }
          } else {
            return RC::SCHEMA_VIEW_NOT_SIMPLE;
          }
        }
      }
    }

    table          = view_select_stmt->tables().at(0);
    table_name     = table->table_meta().name();
    inserts.values = re_values;
  }
  // 真实表的原始流程
  // check the fields number
  // const std::vector<Value> *tmp = inserts.values.data();
  const std::vector<std::vector<Value>> values     = inserts.values;
  const int                             record_num = static_cast<int>(inserts.values.size());
  const int                             value_num  = static_cast<int>(inserts.values[0].size());

  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  for (int j = 0; j < record_num; j++) {
    const std::vector<Value> &cur_values = values[j];
    for (int i = 0; i < value_num; i++) {
      const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
      const AttrType   field_type = field_meta->type();
      const AttrType   value_type = cur_values[i].attr_type();

      // check the value length
      // 2023年10月17日18:22:03 取消长度检查，改为默认截断
      // if (field_type == AttrType::CHARS && value_type == AttrType::CHARS) {
      //   const int field_len = field_meta->len();
      //   const int value_len = cur_values[i].length();

      //   if (value_len > field_len) {
      //     LOG_WARN("field length mismatch. table=%s, field=%s, field length=%d, value length=%d",
      //              table_name, field_meta->name(), field_len, value_len);
      //     return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      //   }
      // }

      if (field_type != value_type) {  // TODO try to convert the value type to field type
        if (value_type == AttrType::NONE) {
          // 空值检查
          if (!field_meta->nullable()) {
            LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
                     table_name, field_meta->name(), field_type, value_type);
            return RC::SCHEMA_FIELD_MISSING;
          } else {
            continue;
          }
        }
        rc = cur_values[i].auto_cast(field_type);
        if (rc == RC::SUCCESS) {
          continue;
        }
        LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
          table_name, field_meta->name(), field_type, value_type);
        return rc;
      }
    }
  }

  // everything alright
  stmt = new InsertStmt(table, values, value_num, record_num);
  return RC::SUCCESS;
}
