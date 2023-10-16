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
    const ConditionSqlNode *cond, bool is_having, Expression *&expr)
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
        LOG_WARN("cannot find attr");
        return rc;
      }
      expr = new FieldExpr(table, field);
    } break;

    case SUB_SELECT: {
      Stmt *select_stmt = nullptr;
      rc                = SelectStmt::create(db, *cond->select, select_stmt);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create select statement. rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      if (reinterpret_cast<SelectStmt *>(select_stmt)->query_fields().size() != 1) {
        LOG_WARN("invalid select statement. select_stmt->query_fields().size()=%d", reinterpret_cast<SelectStmt *>(select_stmt)->query_fields().size());
        return RC::INVALID_ARGUMENT;
      }

      expr = new SelectExpr(select_stmt);
    } break;

    case ARITH: {
      // 注意类型转换
      if (cond->binary) {
        Expression *left_expr;
        Expression *right_expr;
        rc = cond_to_expr(db, default_table, tables, cond->left_cond, is_having, left_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ArithmeticExpr: Left . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        rc = cond_to_expr(db, default_table, tables, cond->right_cond, is_having, right_expr);
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
        rc = cond_to_expr(db, default_table, tables, cond->left_cond, is_having, sub_expr);
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
        rc = cond_to_expr(db, default_table, tables, cond->left_cond, is_having, left_expr);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Left . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        rc = cond_to_expr(db, default_table, tables, cond->right_cond, is_having, right_expr);
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
          rc = cond_to_expr(db, default_table, tables, cond->left_cond, is_having, left_expr);
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
      rc = cond_to_expr(db, default_table, tables, cond->left_cond, is_having, left_expr);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Left . rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      rc = cond_to_expr(db, default_table, tables, cond->right_cond, is_having, right_expr);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to convert ConditionSqlNode to ComparisonExpr: Right . rc=%d:%s", rc, strrc(rc));
        return rc;
      }
      expr = new LogicalCalcExpr(
          cond->logi_op, std::unique_ptr<Expression>(left_expr), std::unique_ptr<Expression>(right_expr));
    } break;

    case UNDEFINED:
    default: {
      LOG_WARN("invalid ConditionSqlNode type: %d", cond->type);
      return RC::INVALID_ARGUMENT;
    } break;
  }
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
    const ConditionSqlNode *conditions, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;
  if (conditions == nullptr) {
    return rc;
  }
  Expression *filter_expr = nullptr;
  rc                      = cond_to_expr(db, default_table, tables, conditions, false, filter_expr);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to convert ConditionSqlNode to Expression. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt               = new FilterStmt();
  stmt->filter_expr_ = filter_expr;
  return rc;
}

// RC FilterStmt::create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
//     const ConditionSqlNode &condition, FilterUnit *&filter_unit)
// {
//   RC rc = RC::SUCCESS;

//   CompOp comp = condition.comp;
//   if (comp < EQUAL_TO || comp >= NO_OP) {
//     LOG_WARN("invalid compare operator : %d", comp);
//     return RC::INVALID_ARGUMENT;
//   }

//   filter_unit         = new FilterUnit;
//   AttrType type_left  = UNDEFINED;
//   AttrType type_right = UNDEFINED;
//   // TODO: 实现exist
//   switch (condition.left_type) {
//     case 0: {
//       // value (expr)
//       FilterObj filter_obj;
//       filter_obj.init_expr(condition.left_expr);
//       filter_unit->set_left(filter_obj);
//       type_left = filter_obj.expr->value_type();
//     } break;
//     case 1: {
//       // attr
//       Table           *table = nullptr;
//       const FieldMeta *field = nullptr;
//       rc                     = get_table_and_field(db, default_table, tables, condition.left_attr, table, field);
//       if (rc != RC::SUCCESS) {
//         LOG_WARN("cannot find attr");
//         return rc;
//       }
//       FilterObj filter_obj;
//       filter_obj.init_expr(new FieldExpr(table, field));
//       filter_unit->set_left(filter_obj);
//       type_left = field->type();
//     } break;
//     case 2: {
//       // sub select
//       Stmt *select_stmt = nullptr;
//       rc                = SelectStmt::create(db, *condition.left_select, select_stmt);
//       if (rc != RC::SUCCESS) {
//         LOG_WARN("failed to create select statement. rc=%d:%s", rc, strrc(rc));
//         return rc;
//       }
//       if (reinterpret_cast<SelectStmt *>(select_stmt)->query_fields().size() != 1) {
//         LOG_WARN("invalid select statement. select_stmt->query_fields().size()=%d", reinterpret_cast<SelectStmt
//         *>(select_stmt)->query_fields().size()); return RC::INVALID_ARGUMENT;
//       }
//       filter_unit->left().init_expr(new SelectExpr(select_stmt));
//       type_left = reinterpret_cast<SelectStmt *>(select_stmt)->query_fields()[0].attr_type();
//     } break;
//     default: {
//       delete filter_unit;
//       LOG_WARN("invalid left_type: %d", condition.left_type);
//       return RC::INVALID_ARGUMENT;
//     }
//   }

//   switch (condition.right_type) {
//     case 0: {
//       // value (expr)
//       FilterObj filter_obj;
//       filter_obj.init_expr(condition.right_expr);
//       filter_unit->set_right(filter_obj);
//       type_right = filter_obj.expr->value_type();
//     } break;
//     case 1: {
//       // attr
//       Table           *table = nullptr;
//       const FieldMeta *field = nullptr;
//       rc                     = get_table_and_field(db, default_table, tables, condition.right_attr, table, field);
//       if (rc != RC::SUCCESS) {
//         LOG_WARN("cannot find attr");
//         return rc;
//       }
//       FilterObj filter_obj;
//       filter_obj.init_expr(new FieldExpr(table, field));
//       filter_unit->set_right(filter_obj);
//       type_right = field->type();
//     } break;
//     case 2: {
//       // sub select
//       Stmt *select_stmt = nullptr;
//       rc                = SelectStmt::create(db, *condition.right_select, select_stmt);
//       if (rc != RC::SUCCESS) {
//         LOG_WARN("failed to create select statement. rc=%d:%s", rc, strrc(rc));
//         return rc;
//       }
//       if (reinterpret_cast<SelectStmt *>(select_stmt)->query_fields().size() != 1) {
//         LOG_WARN("invalid select statement. select_stmt->query_fields().size()=%d", reinterpret_cast<SelectStmt
//         *>(select_stmt)->query_fields().size()); return RC::INVALID_ARGUMENT;
//       }
//       filter_unit->right().init_expr(new SelectExpr(select_stmt));
//       type_right = reinterpret_cast<SelectStmt *>(select_stmt)->query_fields()[0].attr_type();
//     } break;
//     default: {
//       delete filter_unit;
//       LOG_WARN("invalid right_type: %d", condition.right_type);
//       return RC::INVALID_ARGUMENT;
//     }
//   }

//   filter_unit->set_comp(comp);

//   // like的语法检测, 必须左边是属性(字符串field), 右边是字符串
//   // 目前应该不需要支持右边是非字符串转成字符串???
//   if (LIKE_ENUM == comp || NOT_LIKE_ENUM == comp) {
//     if (condition.left_type == 1 && condition.right_type == 0) {
//       if (type_left != CHARS || type_right != CHARS) {
//         delete filter_unit;
//         LOG_WARN("attr LIKE/NOT LIKE value, attr and value must be CHARS");
//         return RC::SCHEMA_FIELD_TYPE_MISMATCH;
//       }
//     } else {  // 不满足 condition.left_is_attr && !condition.right_is_attr
//       delete filter_unit;
//       LOG_WARN("LIKE/NOT LIKE must be 'attr LIKE value'");
//       return RC::SQL_SYNTAX;
//     }
//   }

//   // fix: 这个处理可能是多余的，待查证
//   // 检查两个类型是否能够比较
//   if (type_left != type_right) {
//     if (type_left == DATES || type_right == DATES) {
//       // date conversation
//       // advance check for date
//       if (filter_unit->left().expr->type() == ExprType::VALUE &&
//           filter_unit->right().expr->type() == ExprType::FIELD) {  // left:value, right:attr
//         if (type_right == DATES) {
//           // the attr is date type, so we need to convert the value to date type
//           if (filter_unit->left().expr->value_type() == CHARS) {
//             rc = dynamic_cast<ValueExpr *>(filter_unit->left().expr)->get_value().auto_cast(DATES);
//             if (rc != RC::SUCCESS) {
//               delete filter_unit;
//               return rc;
//             }
//           }
//         }
//       } else if (filter_unit->left().expr->type() == ExprType::FIELD &&
//                  filter_unit->right().expr->type() == ExprType::VALUE) {  // left:attr, right:value
//         if (type_left == DATES) {
//           // the attr is date type, so we need to convert the value to date type
//           if (filter_unit->right().expr->value_type() == CHARS) {
//             rc = dynamic_cast<ValueExpr *>(filter_unit->right().expr)->get_value().auto_cast(DATES);
//             if (rc != RC::SUCCESS) {
//               delete filter_unit;
//               return rc;
//             }
//           }
//         }
//       }
//     } else if (type_left == CHARS && (type_right == FLOATS || type_right == INTS)) {
//       // left is a string, and right is s a number
//       // convert the string to number
//       if (filter_unit->left().expr->type() == ExprType::VALUE) {
//         // left is a value
//         rc = dynamic_cast<ValueExpr *>(filter_unit->left().expr)->get_value().str_to_number();

//         if (rc != RC::SUCCESS) {
//           delete filter_unit;
//           return rc;
//         }
//       }
//     } else if ((type_left == FLOATS || type_left == INTS) && type_right == CHARS) {
//       // left is a number, and right is a string
//       // convert the string to number
//       if (filter_unit->right().expr->type() == ExprType::VALUE) {
//         // right is a value
//         rc = dynamic_cast<ValueExpr *>(filter_unit->right().expr)->get_value().str_to_number();

//         if (rc != RC::SUCCESS) {
//           delete filter_unit;
//           return rc;
//         }
//       }
//     }
//   }

//   return rc;
// }
