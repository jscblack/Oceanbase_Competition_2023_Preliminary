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

#include "sql/executor/create_view_executor.h"

#include "session/session.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "sql/stmt/create_view_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "storage/db/db.h"

RC CreateViewExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_VIEW,
      "create table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  CreateViewStmt *create_view_stmt = static_cast<CreateViewStmt *>(stmt);

  SelectStmt *select_stmt     = dynamic_cast<SelectStmt *>(create_view_stmt->select_stmt());
  const int   attribute_count = static_cast<int>(select_stmt->query_fields_expressions().size());

  const char *view_name = create_view_stmt->view_name().c_str();

  // TODO: 还有一个sql得持久化
  const std::string &create_sql = create_view_stmt->sql();
  // 去掉 "create view view_name as " 只保留select
  int select_pos = create_sql.find("select");
  std::string select_sql = create_sql.substr(select_pos);
  LOG_DEBUG("==========================create view select sql = %s ==========================log by tyh", select_sql);

  // 在这里将select子句的查询结果的表头转换成std::vector<AttrInfoSqlNode>
  std::vector<AttrInfoSqlNode> attr_infos;
  for (auto expr : select_stmt->query_fields_expressions()) {
    AttrInfoSqlNode tmp;
    bool            with_table_name = select_stmt->tables().size() > 1;
    tmp.name = expr->alias(with_table_name);
    // 其余信息对于view而言都是不重要的
    // tmp.type = AttrType::UNDEFINED;
    // tmp.length = 0;
    // tmp.nullable = true;
    tmp.type = AttrType::FLOATS;
    tmp.length = 4;
    tmp.nullable = true;


    // 尝试给出view中每个属性的元信息，但是涉及到类型转换，在实际运行前无法准确获取
    // 以下代码被废弃（本身也不完整）
    // if (expr->type() == ExprType::FIELD) {
    //   FieldExpr *field_expr = dynamic_cast<FieldExpr *>(expr);
    //   tmp.type              = field_expr->field().meta()->type();
    //   tmp.name              = field_expr->alias(with_table_name);
    //   tmp.length            = field_expr->field().meta()->len();
    //   tmp.nullable          = field_expr->field().meta()->nullable();
    // } else if (expr->type() == ExprType::AGGREGATION) {
    //   AggregationExpr *aggregation_expr = dynamic_cast<AggregationExpr *>(expr);
    //   // 强转child为field expression
    //   FieldExpr *child_cast = dynamic_cast<FieldExpr *>(aggregation_expr->child().get());
    //   if (aggregation_expr->agg_type() == FuncName::COUNT_FUNC_ENUM) {
    //     tmp.type = AttrType::INTS;
    //   } else {
    //     tmp.type = child_cast->field().meta()->type();
    //   }
    //   tmp.name     = aggregation_expr->alias(with_table_name);
    //   tmp.length   = child_cast->field().meta()->len();
    //   tmp.nullable = child_cast->field().meta()->nullable();
    // } else if (expr->type() == ExprType::ARITHMETIC) {

    // } else {
    //   return RC::INTERNAL;
    // }

    attr_infos.emplace_back(tmp);
  }

  RC rc = session->get_current_db()->create_view(view_name, attribute_count, attr_infos.data(), select_sql);

  return rc;
}