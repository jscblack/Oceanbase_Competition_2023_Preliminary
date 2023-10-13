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
// Created by Longda on 2021/4/13.
//

#include <sstream>
#include <string>

#include "sql/executor/execute_stage.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "event/storage_event.h"
#include "session/session.h"
#include "sql/executor/command_executor.h"
#include "sql/operator/calc_physical_operator.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"
#include "storage/default/default_handler.h"

using namespace std;
using namespace common;

RC ExecuteStage::handle_request(SQLStageEvent *sql_event)
{
  RC                                  rc                = RC::SUCCESS;
  const unique_ptr<PhysicalOperator> &physical_operator = sql_event->physical_operator();
  if (physical_operator != nullptr) {
    return handle_request_with_physical_operator(sql_event);
  }

  SessionEvent *session_event = sql_event->session_event();

  Stmt *stmt = sql_event->stmt();
  if (stmt != nullptr) {
    CommandExecutor command_executor;
    rc = command_executor.execute(sql_event);
    session_event->sql_result()->set_return_code(rc);
  } else {
    return RC::INTERNAL;
  }
  return rc;
}

RC ExecuteStage::handle_request_with_physical_operator(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;

  Stmt *stmt = sql_event->stmt();
  ASSERT(stmt != nullptr, "SQL Statement shouldn't be empty!");

  unique_ptr<PhysicalOperator> &physical_operator = sql_event->physical_operator();
  ASSERT(physical_operator != nullptr, "physical operator should not be null");

  // TODO 这里也可以优化一下，是否可以让physical operator自己设置tuple schema
  TupleSchema schema;
  switch (stmt->type()) {
    case StmtType::SELECT: {
      SelectStmt *select_stmt     = static_cast<SelectStmt *>(stmt);
      bool        with_table_name = select_stmt->tables().size() > 1;
      // TODO: 临时fix aggregation 的表头输出，后续需要优化
      // if (select_stmt->aggregation_func().size() > 0) {
      //   for (const auto &aggregation_func : select_stmt->aggregation_func()) {
      //     std::string field_name = aggregation_func.first + "(" + aggregation_func.second.field_name() + ")";
      //     char       *buf        = new char[field_name.size() + 1];
      //     memcpy(buf, field_name.c_str(), field_name.size() + 1);
      //     schema.append_cell(buf);
      //   }
      // } else {
      //   for (const Field &field : select_stmt->query_fields()) {
      //     if (with_table_name) {
      //       schema.append_cell(field.table_name(), field.field_name());
      //     } else {
      //       schema.append_cell(field.field_name());
      //     }
      //   }
      // }

      for (auto query_field_expr : select_stmt->query_fields_expressions()) {
        if (query_field_expr->type() == ExprType::FIELD) {
          auto field = dynamic_cast<FieldExpr *>(query_field_expr);
          if (with_table_name) {
            schema.append_cell(field->table_name(), field->field_name());
          } else {
            schema.append_cell(field->field_name());
          }
        } else if (query_field_expr->type() == ExprType::AGGREGATION) {
          auto agg = dynamic_cast<AggregationExpr *>(query_field_expr);
          if (!with_table_name) {
            schema.append_cell((agg->aggregation_func() + "(" + agg->field_name() + ")").c_str());
          } else {
            schema.append_cell(
                (agg->aggregation_func() + "(" + agg->table_name() + "." + agg->field_name() + ")").c_str());
          }
        } else {
          ASSERT(false, "ExecuteStage::handle_request_with_physical_operator(SQLStageEvent *): Impossible!!!");
        }
      }

    } break;

    case StmtType::CALC: {
      CalcPhysicalOperator *calc_operator = static_cast<CalcPhysicalOperator *>(physical_operator.get());
      for (const unique_ptr<Expression> &expr : calc_operator->expressions()) {
        schema.append_cell(expr->name().c_str());
      }
    } break;

    case StmtType::EXPLAIN: {
      schema.append_cell("Query Plan");
    } break;
    default: {
      // 只有select返回结果
    } break;
  }

  SqlResult *sql_result = sql_event->session_event()->sql_result();
  sql_result->set_tuple_schema(schema);
  sql_result->set_operator(std::move(physical_operator));
  return rc;
}
