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

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

/**
 * @brief 将ConditionSqlNode转为表达式
 *
 * @param [in] db
 * @param [in] default_table
 * @param [in] tables
 * @param [in] cond
 * @param [in] is_having 是否为聚合类型，尚未结合考虑，可能会结合
 * @param [out] expr
 * @param [out] value_type  expression求解的类型
 * @return RC
 */
RC cond_to_expr(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    std::vector<std::pair<std::string, std::string>> &relation_to_alias, const ConditionSqlNode *cond, bool is_having,
    Expression *&expr)
{
  RC rc = RC::SUCCESS;
  expr  = nullptr;
  if (nullptr == cond) {
    return rc;
  }

  switch (cond->type) {
    case VALUE: {
      expr = cond->value;
    } break;

    case FIELD: {
      Table           *table = nullptr;
      const FieldMeta *field = nullptr;
      rc                     = get_table_and_field(db, default_table, tables, cond->attr, table, field);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot find table [%s] and attr [%s]", cond->attr.relation_name.c_str(), cond->attr.attribute_name.c_str());
        return rc;
      }
      expr = new FieldExpr(table, field);
      for (auto rel_to_ali : relation_to_alias) {
        if (rel_to_ali.second == cond->attr.relation_name) {
          expr->set_alias(cond->attr.relation_name + "." + cond->attr.attribute_name);
          break;
        }
      }
    } break;

    case SUB_SELECT: {
      Stmt *select_stmt = nullptr;
      rc                = SelectStmt::create(db, *cond->select, select_stmt);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create select statement. rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      if (reinterpret_cast<SelectStmt *>(select_stmt)->query_fields_expressions().size() != 1) {
        LOG_WARN("invalid select statement. select_stmt->query_fields().size()=%d", reinterpret_cast<SelectStmt *>(select_stmt)->query_fields_expressions().size());
        return RC::INVALID_ARGUMENT;
      }

      expr = new SelectExpr(select_stmt);
    } break;

    case ARITH: {
      // 注意类型转换
      if (cond->binary) {
        Expression *left_expr;
        Expression *right_expr;
        rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->left_cond, is_having, left_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ArithmeticExpr: Left . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->right_cond, is_having, right_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ArithmeticExpr: Right . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        if (ArithmeticExpr::is_legal_subexpr(cond->arith, left_expr, right_expr)) {
          expr = new ArithmeticExpr(cond->arith, left_expr, right_expr);
        } else {
          return RC::EXPR_TYPE_MISMATCH;
        }
      } else {
        Expression *sub_expr;
        rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->left_cond, is_having, sub_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ArithmeticExpr: Sub . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        if (ArithmeticExpr::is_legal_subexpr(cond->arith, sub_expr, nullptr)) {
          expr = new ArithmeticExpr(cond->arith, sub_expr, nullptr);
        } else {
          return RC::EXPR_TYPE_MISMATCH;
        }
      }
    } break;

    case COMP: {
      // 注意like的条件判断
      // TODO 特判一下,如果是comp, 则sub_select 不能是select *
      // 注意子查询相关的比较 （exist之类的）
      // 注意处理NULL

      // COMP应该得到comparison_expr
      if (cond->binary) {
        // 正常双目比较
        Expression *left_expr;
        Expression *right_expr;
        rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->left_cond, is_having, left_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Left . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->right_cond, is_having, right_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Right . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        // 这里的比较需要做类型转换
        rc = ComparisonExpr::cast_and_check_comparable(cond->comp, left_expr, right_expr);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Right . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        expr = new ComparisonExpr(
            cond->comp, std::unique_ptr<Expression>(left_expr), std::unique_ptr<Expression>(right_expr));
      } else {
        // TODO: 我只处理exist/not exist这种
        if (cond->comp == CompOp::EXISTS_ENUM || cond->comp == CompOp::NOT_EXISTS_ENUM) {
          Expression *left_expr;
          rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->left_cond, is_having, left_expr);
          if (OB_FAIL(rc)) {
            LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Sub . rc=%d:%s", rc, strrc(rc));
            return rc;
          }
          Expression *right_expr = new ValueExpr(Value().make_null());
          // 向前兼容，保持比较顺序不变
          expr = new ComparisonExpr(
              cond->comp, std::unique_ptr<Expression>(right_expr), std::unique_ptr<Expression>(left_expr));
        } else {
          LOG_WARN("invalid condition type: %d", cond->type);
          return RC::INVALID_ARGUMENT;
        }
      }
    } break;

    case FUNC_OR_AGG: {
      // 不能是AGG
      // TODO: 待修改完成function
      if (is_having) {  // Having子句中需要对聚集特殊处理
        // 得在这里特判 STAR，而不是往下推
        Expression             *sub_expr;
        const ConditionSqlNode *sub_cond = cond->left_cond;
        // 目前判别有点简单，更好的做法是根据Funcname来判断可以有多少个参数，否则max这种可能是聚集也可能是普通函数。
        if (cond->func >= FuncName::COUNT_FUNC_ENUM && cond->func <= FuncName::MIN_FUNC_ENUM) {  // Aggregation
          if (sub_cond->type != ConditionSqlNodeType::FIELD) {
            LOG_WARN("Aggregation's subexpr must be FieldExpr");
            return RC::SCHEMA_FIELD_TYPE_MISMATCH;
          }
          bool is_table_star = sub_cond->attr.relation_name == "*";
          bool is_field_star = sub_cond->attr.attribute_name == "*";
          if (is_table_star || is_field_star) {
            if (is_table_star && !is_field_star) {  // *.col 报错
              LOG_WARN("invalid field name while table is *. attr=%s", sub_cond->attr.attribute_name.c_str());
              return RC::SCHEMA_FIELD_MISSING;
            }
            if (cond->func != FuncName::COUNT_FUNC_ENUM) {
              LOG_WARN("invalid aggregate while table/field is *.");
              return RC::SCHEMA_FIELD_MISSING;
            }
            // 暂时没法区分COUNT(*.*)和COUNT(*)，实在有需要就加个新的状态标明一下。
            if (is_table_star) {
              sub_expr = new FieldExpr(nullptr, nullptr);
              expr     = new AggregationExpr(cond->func, sub_expr);
            } else {
              Table *table = nullptr;
              auto   iter  = tables->find(sub_cond->attr.relation_name);
              if (iter == tables->end()) {
                sub_expr = new FieldExpr(nullptr, nullptr);
              } else {
                table    = iter->second;
                sub_expr = new FieldExpr(table, nullptr);
              }
              sub_expr = new FieldExpr(table, nullptr);
              expr     = new AggregationExpr(cond->func, sub_expr);
            }
          } else {
            rc   = cond_to_expr(db, default_table, tables, relation_to_alias, sub_cond, is_having, sub_expr);
            expr = new AggregationExpr(cond->func, sub_expr);
          }
        }
      } else {  // Where子句中需要对func进行处理 （max和min）
        // TODO: 补一下 funcExpr的构造
        if (cond->func >= FuncName::LENGTH_FUNC_NUM &&
            cond->func <= FuncName::DATE_FUNC_NUM) {           // function, 暂不考虑 MAX和MIN
          std::vector<std::unique_ptr<Expression>> func_args;  // 虽然目前仅支持两参数，但还是叫参数列表
          Expression                              *first_arg;
          rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->left_cond, is_having, first_arg);
          func_args.push_back(std::unique_ptr<Expression>(first_arg));
          if (cond->right_cond != nullptr) {
            Expression *second_arg;
            rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->right_cond, is_having, second_arg);
            func_args.push_back(std::unique_ptr<Expression>(second_arg));
          }
          rc = FunctionExpr::is_subexpr_legal(cond->func, func_args);
          if (OB_FAIL(rc)) {
            return rc;
          }
          expr = new FunctionExpr(cond->func, func_args);
        }
      }
    } break;

    case LOGIC: {
      // 这里会递归生成逻辑运算表达式
      if (!cond->binary) {
        // 防御性编程，这里必是两个比较
        LOG_WARN("invalid condition type: %d", cond->type);
        return RC::INVALID_ARGUMENT;
      }
      Expression *left_expr;
      Expression *right_expr;
      rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->left_cond, is_having, left_expr);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Left . rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      rc = cond_to_expr(db, default_table, tables, relation_to_alias, cond->right_cond, is_having, right_expr);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Right . rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      expr = new LogicalCalcExpr(
          cond->logi_op, std::unique_ptr<Expression>(left_expr), std::unique_ptr<Expression>(right_expr));
    } break;

    case UNDEFINED_COND_SQL_NODE:
    default: {
      LOG_WARN("invalid ConditionSqlNode type: %d", cond->type);
      return RC::INVALID_ARGUMENT;
    } break;
  }
  return rc;
}

RC get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {
    auto iter = tables->find(attr.relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  } else {
    table = db->find_table(attr.relation_name.c_str());
  }
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}

RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    std::vector<std::pair<std::string, std::string>> &relation_to_alias, const ConditionSqlNode *conditions,
    FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;
  if (conditions == nullptr) {
    return rc;
  }
  Expression *filter_expr = nullptr;
  rc                      = cond_to_expr(db, default_table, tables, relation_to_alias, conditions, false, filter_expr);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to convert ConditionSqlNode to Expression. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  stmt               = new FilterStmt();
  stmt->filter_expr_ = filter_expr;
  return rc;
}