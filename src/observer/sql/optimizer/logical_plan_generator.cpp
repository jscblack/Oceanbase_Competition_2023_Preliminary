/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#include "sql/optimizer/logical_plan_generator.h"

#include "sql/operator/aggregate_logical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/sort_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/operator/view_get_logical_operator.h"

#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/having_filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/update_stmt.h"

using namespace std;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      CalcStmt *calc_stmt = dynamic_cast<CalcStmt *>(stmt);
      rc                  = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = dynamic_cast<SelectStmt *>(stmt);
      rc                      = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = dynamic_cast<InsertStmt *>(stmt);
      rc                      = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = dynamic_cast<DeleteStmt *>(stmt);
      rc                      = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::UPDATE: {
      UpdateStmt *update_stmt = dynamic_cast<UpdateStmt *>(stmt);
      rc                      = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = dynamic_cast<ExplainStmt *>(stmt);
      rc                        = create_plan(explain_stmt, logical_operator);
    } break;
    default: {
      rc = RC::UNIMPLENMENT;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> table_oper(nullptr);

  const std::vector<Table *> &tables = select_stmt->tables();

  const std::vector<Expression *> &all_fields_expressions = select_stmt->query_fields_expressions();
  int                              view_stmts_idx         = 0;
  for (int i = 0; i < tables.size(); i++) {
    Table *table = tables[i];
    if (table->table_meta().is_view() == false) {
      unique_ptr<LogicalOperator> table_get_oper(
          new TableGetLogicalOperator(table, true /*readonly*/, select_stmt->relation_to_alias()[i].second));

      if (table_oper == nullptr) {
        table_oper = std::move(table_get_oper);
      } else {
        JoinLogicalOperator *join_oper = new JoinLogicalOperator;
        join_oper->add_child(std::move(table_oper));
        join_oper->add_child(std::move(table_get_oper));
        table_oper = unique_ptr<LogicalOperator>(join_oper);
      }
    } else {  // 从视图中获取记录
      unique_ptr<LogicalOperator> view_get_oper(new ViewGetLogicalOperator(table, true /*readonly*/));

      // 3. view-sql : optimize
      unique_ptr<LogicalOperator> view_logical_operator;
      Stmt                       *view_stmt = select_stmt->view_stmts()[view_stmts_idx++];
      if (nullptr == view_stmt) {
        return RC::UNIMPLENMENT;
      }
      SelectStmt *view_stmt_cast = dynamic_cast<SelectStmt *>(view_stmt);
      RC          rc             = create_plan(view_stmt_cast, view_logical_operator);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      view_get_oper->add_child(std::move(view_logical_operator));

      if (table_oper == nullptr) {
        table_oper = std::move(view_get_oper);
      } else {
        JoinLogicalOperator *join_oper = new JoinLogicalOperator;
        join_oper->add_child(std::move(table_oper));
        join_oper->add_child(std::move(view_get_oper));
        table_oper = unique_ptr<LogicalOperator>(join_oper);
      }
    }
  }

  unique_ptr<LogicalOperator> predicate_oper;
  if (nullptr != select_stmt->filter_stmt()) {
    RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto                        order_by = select_stmt->order_by();
  unique_ptr<LogicalOperator> sort_oper;
  if (!order_by.empty()) {
    sort_oper = unique_ptr<LogicalOperator>(new SortLogicalOperator(order_by));
  }

  unique_ptr<LogicalOperator> project_oper(
      new ProjectLogicalOperator(all_fields_expressions, select_stmt->is_simple_select()));
  if (predicate_oper) {
    if (table_oper) {
      predicate_oper->add_child(std::move(table_oper));
    }
    if (sort_oper) {
      sort_oper->add_child(std::move(predicate_oper));
      project_oper->add_child(std::move(sort_oper));
    } else {
      project_oper->add_child(std::move(predicate_oper));
    }
  } else {
    if (table_oper) {
      if (sort_oper) {
        sort_oper->add_child(std::move(table_oper));
        project_oper->add_child(std::move(sort_oper));
      } else {
        project_oper->add_child(std::move(table_oper));
      }
    }
  }
  // 注意此时的逻辑树是， (project) -> (predicate) -> (table_oper|table_get / join)

  if (select_stmt->has_aggregation()) {
    // 存在聚合子句
    unique_ptr<LogicalOperator> aggregate_oper(
        new AggregateLogicalOperator(all_fields_expressions, select_stmt->group_by_fields_expressions()));
    aggregate_oper->add_child(std::move(project_oper));

    if (select_stmt->having_filter_stmt() != nullptr &&
        select_stmt->having_filter_stmt()->filter_expr() != nullptr) {  // 存在having子句
      unique_ptr<Expression>    having_filter_expr(select_stmt->having_filter_stmt()->filter_expr()->clone());
      AggregateLogicalOperator *aggregate_oper_cast = dynamic_cast<AggregateLogicalOperator *>(aggregate_oper.get());
      aggregate_oper_cast->add_having_filters_expression(std::move(having_filter_expr));
    }
    logical_operator.swap(aggregate_oper);
  } else {
    logical_operator.swap(project_oper);
  }

  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (filter_stmt && filter_stmt->filter_expr()) {
    unique_ptr<Expression> filter_expr(filter_stmt->filter_expr()->clone());
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::move(filter_expr)));
  }

  logical_operator = std::move(predicate_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table                 *table           = insert_stmt->table();
  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, insert_stmt->values());
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table      *table       = delete_stmt->table();
  FilterStmt *filter_stmt = delete_stmt->filter_stmt();

  if (!table->table_meta().is_view()) {
    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, false /*readonly*/));

    unique_ptr<LogicalOperator> predicate_oper;
    RC                          rc = create_plan(filter_stmt, predicate_oper);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

    if (predicate_oper) {
      predicate_oper->add_child(std::move(table_get_oper));
      delete_oper->add_child(std::move(predicate_oper));
    } else {
      delete_oper->add_child(std::move(table_get_oper));
    }

    logical_operator = std::move(delete_oper);
    return rc;
  } else {
    unique_ptr<LogicalOperator> view_get_oper(new ViewGetLogicalOperator(table, false /*readonly*/));

    // 3. view-sql : optimize
    unique_ptr<LogicalOperator> view_logical_operator;
    Stmt                       *view_stmt = delete_stmt->view_stmt();
    if (nullptr == view_stmt) {
      return RC::UNIMPLENMENT;
    }
    SelectStmt *view_stmt_cast = dynamic_cast<SelectStmt *>(view_stmt);
    RC          rc             = create_plan(view_stmt_cast, view_logical_operator);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    view_get_oper->add_child(std::move(view_logical_operator));

    unique_ptr<LogicalOperator> predicate_oper;  // where ...
    rc = create_plan(filter_stmt, predicate_oper);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

    if (predicate_oper) {
      predicate_oper->add_child(std::move(view_get_oper));
      delete_oper->add_child(std::move(predicate_oper));
    } else {
      delete_oper->add_child(std::move(view_get_oper));
    }

    logical_operator = std::move(delete_oper);
    return rc;
  }
}

RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table                          *table       = update_stmt->table();
  const std::vector<std::string> &field_names = update_stmt->field_names();
  const std::vector<ValueOrStmt> &values      = update_stmt->values();
  FilterStmt                     *filter_stmt = update_stmt->filter_stmt();

  if (!table->table_meta().is_view()) {
    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, false /*readonly*/));

    unique_ptr<LogicalOperator> predicate_oper;  // where ...
    RC                          rc = create_plan(filter_stmt, predicate_oper);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table, field_names, values));

    if (predicate_oper) {
      predicate_oper->add_child(std::move(table_get_oper));
      update_oper->add_child(std::move(predicate_oper));
    } else {
      update_oper->add_child(std::move(table_get_oper));
    }

    logical_operator = std::move(update_oper);
    return rc;
  } else {
    unique_ptr<LogicalOperator> view_get_oper(new ViewGetLogicalOperator(table, false /*readonly*/));

    // 3. view-sql : optimize
    unique_ptr<LogicalOperator> view_logical_operator;
    Stmt                       *view_stmt = update_stmt->view_stmt();
    if (nullptr == view_stmt) {
      return RC::UNIMPLENMENT;
    }
    SelectStmt *view_stmt_cast = dynamic_cast<SelectStmt *>(view_stmt);
    RC          rc             = create_plan(view_stmt_cast, view_logical_operator);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    view_get_oper->add_child(std::move(view_logical_operator));

    unique_ptr<LogicalOperator> predicate_oper;  // where ...
    rc = create_plan(filter_stmt, predicate_oper);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table, field_names, values));

    if (predicate_oper) {
      predicate_oper->add_child(std::move(view_get_oper));
      update_oper->add_child(std::move(predicate_oper));
    } else {
      update_oper->add_child(std::move(view_get_oper));
    }

    logical_operator = std::move(update_oper);
    return rc;
  }
}

RC LogicalPlanGenerator::create_plan(ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Stmt                       *child_stmt = explain_stmt->child();
  unique_ptr<LogicalOperator> child_oper;
  RC                          rc = create(child_stmt, child_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}
