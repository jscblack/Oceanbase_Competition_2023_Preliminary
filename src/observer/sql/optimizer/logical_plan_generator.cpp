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
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);
      rc                  = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
      rc                      = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);
      rc                      = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);
      rc                      = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::UPDATE: {
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
      rc                      = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);
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

  const std::vector<Table *>      &tables                 = select_stmt->tables();
  const std::vector<Expression *> &all_fields_expressions = select_stmt->query_fields_expressions();
  const std::vector<Field>        &all_fields             = select_stmt->query_fields();
  for (Table *table : tables) {
    std::vector<Field> fields;
    for (const Field &field : all_fields) {
      if (0 == strcmp(field.table_name(), table->name())) {
        fields.push_back(field);
      }
    }

    // for (auto expr : all_fields_expressions) {
    //   if (expr->type() == ExprType::FIELD) {
    //     FieldExpr *field_expr = dynamic_cast<FieldExpr *>(expr);
    //     if (0 == strcmp(field_expr->table_name(), table->name())) {
    //       fields.push_back(field_expr->field());
    //     }
    //   } else {  // expr->type() == ExprType::AGGREGATION
    //     AggregationExpr *agg_expr = dynamic_cast<AggregationExpr *>(expr);
    //     // 将count(*)展开
    //     if (agg_expr->field().meta() ==
    //         nullptr) {  // 其实不需要区分 *, *.* and t.* 因为tableget算子是以table为单位构建的
    //       const TableMeta &table_meta = table->table_meta();
    //       const int        field_num  = table_meta.field_num();
    //       for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    //         fields.push_back(Field(table, table_meta.field(i)));
    //       }
    //     }
    //   }
    // }

    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, true /*readonly*/));
    if (table_oper == nullptr) {
      table_oper = std::move(table_get_oper);
    } else {
      JoinLogicalOperator *join_oper = new JoinLogicalOperator;
      join_oper->add_child(std::move(table_oper));
      join_oper->add_child(std::move(table_get_oper));
      table_oper = unique_ptr<LogicalOperator>(join_oper);
    }
  }

  unique_ptr<LogicalOperator> predicate_oper;
  RC                          rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  auto                        order_by = select_stmt->order_by();
  unique_ptr<LogicalOperator> sort_oper;
  if (!order_by.empty()) {
    sort_oper = unique_ptr<LogicalOperator>(new SortLogicalOperator(order_by));
  }

  unique_ptr<LogicalOperator> project_oper(new ProjectLogicalOperator(all_fields));
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
  //

  const std::vector<std::pair<std::string, Field>> &all_aggregations = select_stmt->aggregation_func();
  if (!all_aggregations.empty()) {  // 存在聚合操作
    unique_ptr<LogicalOperator> aggregate_oper(
        new AggregateLogicalOperator(all_aggregations, all_fields, all_fields_expressions));
    aggregate_oper->add_child(std::move(project_oper));
    AggregateLogicalOperator *aggregate_oper_cast = dynamic_cast<AggregateLogicalOperator *>(aggregate_oper.get());

    HavingFilterStmt *having_filter_stmt = select_stmt->having_filter_stmt();
    if (!select_stmt->group_by_fields().empty()) {  // 存在group by子句
      aggregate_oper_cast->set_group_by_fields(select_stmt->group_by_fields());
    }

    if (!having_filter_stmt->filter_units().empty()) {  // 存在having子句
      std::vector<unique_ptr<Expression>>    cmp_exprs;
      const std::vector<HavingFilterUnit *> &filter_units = having_filter_stmt->filter_units();
      for (const HavingFilterUnit *filter_unit : filter_units) {
        const HavingFilterObj &filter_obj_left  = filter_unit->left();
        const HavingFilterObj &filter_obj_right = filter_unit->right();

        unique_ptr<Expression> left(
            filter_obj_left.is_attr ? static_cast<Expression *>(
                                          new AggregationExpr(filter_obj_left.field, filter_obj_left.aggregation_func_))
                                    : static_cast<Expression *>(new ValueExpr(filter_obj_left.value)));

        unique_ptr<Expression> right(filter_obj_right.is_attr
                                         ? static_cast<Expression *>(new AggregationExpr(
                                               filter_obj_right.field, filter_obj_right.aggregation_func_))
                                         : static_cast<Expression *>(new ValueExpr(filter_obj_right.value)));

        ComparisonExpr *cmp_expr = new ComparisonExpr(filter_unit->comp(), std::move(left), std::move(right));
        cmp_exprs.emplace_back(cmp_expr);
      }

      unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));

      aggregate_oper_cast->set_having_filter_units(filter_units);
      aggregate_oper_cast->add_having_filters(std::move(conjunction_expr));
    }
    logical_operator.swap(aggregate_oper);
  } else {
    logical_operator.swap(project_oper);
  }

  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  // 递归建立谓词树
  // filter_stmt->left();
  // filter_stmt->right();
  // filter_stmt->is_filter_unit();
  // filter_stmt->filter_unit();
  unique_ptr<Expression> logical_calc_oper;

  using CreateLogicalCalcExprFunc = std::function<RC(FilterStmt *, std::unique_ptr<Expression> &)>;

  CreateLogicalCalcExprFunc create_logical_calc_expr;

  create_logical_calc_expr = [&](FilterStmt *filter_stmt, std::unique_ptr<Expression> &create_expr) -> RC {
    if (filter_stmt == nullptr) {
      return RC::SUCCESS;
    }
    if (filter_stmt->is_filter_unit()) {
      FilterUnit                 *filter_unit = filter_stmt->filter_unit();
      std::unique_ptr<Expression> left_expr(filter_unit->left().expr->clone());
      std::unique_ptr<Expression> right_expr(filter_unit->right().expr->clone());
      if (filter_unit->left().expr->type() == ExprType::VALUE) {
        LOG_DEBUG("left_expr is value, value=%d", dynamic_cast<ValueExpr*>(left_expr.get())->get_value().get_int());
      }
      std::unique_ptr<ComparisonExpr> comp_expr(
          new ComparisonExpr(filter_unit->comp(), std::move(left_expr), std::move(right_expr)));
      create_expr = std::move(comp_expr);
      return RC::SUCCESS;
    } else {
      RC                          rc = RC::SUCCESS;
      std::unique_ptr<Expression> left_expr, right_expr;
      rc = create_logical_calc_expr(filter_stmt->left(), left_expr);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      rc = create_logical_calc_expr(filter_stmt->right(), right_expr);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      std::unique_ptr<LogicalCalcExpr> logi_expr(
          new LogicalCalcExpr(filter_stmt->logi(), std::move(left_expr), std::move(right_expr)));
      create_expr = std::move(logi_expr);
    }
    return RC::SUCCESS;
  };

  RC                                   rc = create_logical_calc_expr(filter_stmt, logical_calc_oper);
  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (logical_calc_oper) {
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::move(logical_calc_oper)));
  }

  logical_operator = std::move(predicate_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = insert_stmt->table();
  // vector<vector<Value>> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());

  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, insert_stmt->values());
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table             *table       = delete_stmt->table();
  FilterStmt        *filter_stmt = delete_stmt->filter_stmt();
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false /*readonly*/));

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
}

RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table                          *table       = update_stmt->table();
  const std::vector<std::string> &field_names = update_stmt->field_names();
  const std::vector<ValueOrStmt> &values      = update_stmt->values();
  FilterStmt                     *filter_stmt = update_stmt->filter_stmt();
  std::vector<Field>              fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false /*readonly*/));

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
