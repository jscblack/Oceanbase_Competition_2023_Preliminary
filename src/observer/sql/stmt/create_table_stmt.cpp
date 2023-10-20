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
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/stmt/create_table_stmt.h"
#include "event/sql_debug.h"
#include "sql/expr/expression.h"
#include "sql/stmt/select_stmt.h"

RC CreateTableStmt::create(Db *db, const CreateTableSqlNode &create_table, Stmt *&stmt)
{
  if (create_table.from_select) {
    // create select_stmt from create_table.table_select
    Stmt *select_stmt = nullptr;
    RC    rc          = SelectStmt::create(db, create_table.table_select, select_stmt);
    if (rc != RC::SUCCESS) {
      sql_debug("create select statement failed");
      return rc;
    }

    std::vector<AttrInfoSqlNode> attr_infos = create_table.attr_infos;
    for (auto &attr : attr_infos) {
      attr.nullable = true;  // 这些必然要允许为null，因为不会有数据被插入到这些列
    }
    for (auto &expr : dynamic_cast<SelectStmt *>(select_stmt)->query_fields_expressions()) {
      AttrInfoSqlNode attr_info;
      attr_info.name = expr->alias(false);
      attr_info.type = expr->value_type();
      if (expr->type() == ExprType::FIELD) {
        // 来自已有的field
        auto field_expr    = dynamic_cast<FieldExpr *>(expr);
        attr_info.length   = field_expr->field().meta()->len();
        attr_info.nullable = field_expr->field().meta()->nullable();

      } else if (expr->type() == ExprType::AGGREGATION) {
        // 来自聚合函数
        // TODO
      }
      attr_infos.push_back(attr_info);
    }

    SelectExpr *select_expr = new SelectExpr(select_stmt);

    stmt = new CreateTableStmt(create_table.relation_name, attr_infos, select_expr);
    sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
    return RC::SUCCESS;
  }
  stmt = new CreateTableStmt(create_table.relation_name, create_table.attr_infos);
  sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
  return RC::SUCCESS;
}
