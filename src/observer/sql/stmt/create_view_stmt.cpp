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
// Created by tong1heng on 2023/10/20.
//

#include "sql/stmt/create_view_stmt.h"
#include "event/sql_debug.h"
#include "sql/expr/expression.h"
#include "sql/stmt/select_stmt.h"

RC CreateViewStmt::create(Db *db, const CreateViewSqlNode &create_view, Stmt *&stmt)
{
  Stmt *select_stmt = nullptr;
  RC rc = SelectStmt::create(db, create_view.from_select, select_stmt);
  if (rc != RC::SUCCESS) {
    sql_debug("create select statement failed");
    return rc;
  }
  
  SelectExpr *select_expr = new SelectExpr(select_stmt);

  stmt = new CreateViewStmt(create_view.view_name, select_expr);
  sql_debug("create view statement: view name %s", create_view.view_name.c_str());
  return RC::SUCCESS;
}
