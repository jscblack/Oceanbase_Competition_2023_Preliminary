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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/having_filter_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include <algorithm>

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

// static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
// {
//   const TableMeta &table_meta = table->table_meta();
//   const int        field_num  = table_meta.field_num();
//   for (int i = table_meta.sys_field_num(); i < field_num; i++) {
//     field_metas.push_back(Field(table, table_meta.field(i)));
//   }
// }

static void wildcard_fields(Table *table, std::vector<Expression *> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    Expression *field_expr = new FieldExpr(table, table_meta.field(i));
    field_metas.emplace_back(field_expr);
  }
}

/**
 * @brief 检测表名合法性（是否存在），并返回Table信息
 *
 * @param table_map
 * @param table_name
 * @param [out] table
 * @return true
 * @return false
 */
RC is_table_legal(
    const std::unordered_map<std::string, Table *> &table_map, const std::string &table_name, Table *&table)
{
  auto iter = table_map.find(table_name);
  if (iter == table_map.end()) {
    LOG_WARN("no such table in from list: %s", table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  table = iter->second;
  return RC::SUCCESS;
}

bool is_equal(const char *a, const char *b) { return 0 == strcmp(a, b); }

/**
 * @brief Get the table and field object，从filter_stmt里偷过来的
 *
 * @param db
 * @param default_table
 * @param tables
 * @param attr
 * @param [out] table
 * @param [out] field
 * @return RC
 */
RC select_get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
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

/**
 * @brief 讲select ... from 将投影列表表达式化
 *
 * @param db
 * @param default_table
 * @param tables
 * @param cond
 * @param [out] expr
 * @param [out] has_aggregation 表达式或其子表达式中是否有聚合: 注意，有aggregate必定也有field
 * @param [out] has_field 表达式或其子表达式中是否有FIELD：注意，有field未必只是field
 * @return RC
 */
RC attr_cond_to_expr(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode *cond, Expression *&expr, bool &has_aggregation, bool &has_field)
{  // 对attr内的单个ConditionSqlNode做解析
  RC rc = RC::SUCCESS;
  expr  = nullptr;

  switch (cond->type) {
    case VALUE: {
      expr = cond->value;
    } break;

    case FIELD: {
      // 得把真实的表名填入，不能为STAR, 似乎无需特殊处理STAR，因为找不到
      // if (cond->attr.attribute_name == "*" || cond->attr.relation_name == "*") {
      //   LOG_WARN("STAR * cannot be subexpr of field");
      //   return RC::SCHEMA_FIELD_MISSING;
      // }
      has_field              = true;
      Table           *table = nullptr;
      const FieldMeta *field = nullptr;
      rc                     = select_get_table_and_field(db, default_table, tables, cond->attr, table, field);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot find table [%s] and attr [%s]", cond->attr.relation_name.c_str(), cond->attr.attribute_name.c_str());
        return rc;
      }
      expr = new FieldExpr(table, field);
    } break;

    case ARITH: {
      // 注意类型转换
      if (cond->binary) {
        Expression *left_expr;
        Expression *right_expr;
        rc = attr_cond_to_expr(db, default_table, tables, cond->left_cond, left_expr, has_aggregation, has_field);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to convert ConditionSqlNode to ArithmeticExpr: Left . rc=%d:%s", rc, strrc(rc));
          return rc;
        }
        rc = attr_cond_to_expr(db, default_table, tables, cond->right_cond, right_expr, has_aggregation, has_field);
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
        rc = attr_cond_to_expr(db, default_table, tables, cond->left_cond, sub_expr, has_aggregation, has_field);
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

    case FUNC_OR_AGG: {
      // 暂时先只处理AGG， TODO：完成function
      // 得在这里特判 STAR，而不是往下推
      Expression             *sub_expr;
      const ConditionSqlNode *sub_cond = cond->left_cond;

      // 目前判别有点简单，更好的做法是根据Funcname来判断可以有多少个参数，否则max这种可能是聚集也可能是普通函数。
      if (cond->func >= FuncName::COUNT_FUNC_ENUM && cond->func <= FuncName::MIN_FUNC_ENUM) {  // Aggregation
        has_aggregation = true;
        has_field       = true;
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
          } else {  //
            Table *table;
            RC     rc = is_table_legal(*tables,
                sub_cond->attr.relation_name,
                table);  // FIXME: select count(*) 这里出错，传进去的relation_name是空的
            if (nullptr == table) {
              sub_expr = new FieldExpr(nullptr, nullptr);
            } else {
              sub_expr = new FieldExpr(table, nullptr);
            }
            expr = new AggregationExpr(cond->func, sub_expr);
          }
        } else {
          rc   = attr_cond_to_expr(db, default_table, tables, sub_cond, sub_expr, has_aggregation, has_field);
          expr = new AggregationExpr(cond->func, sub_expr);
        }
      }
    } break;

    case SUB_SELECT:
    case LOGIC:
    case COMP:
    case UNDEFINED_COND_SQL_NODE:
    default: {
      LOG_WARN("invalid ConditionSqlNode type in select attribute: %d", cond->type);
      return RC::INVALID_ARGUMENT;
    } break;
  }

  expr->set_alias(cond->alias);  // 默认为""
  return rc;
}

// /**
//  * @brief 判别一个expr及其儿子中是否有符合条件的选项，有则返回true; 想法很好但没实现child的访问统一接口，故而搁置
//  *
//  * @param expr
//  * @param judge
//  * @return true
//  * @return false
//  */
// bool judge_attr_expr(Expression *expr, std::function<bool(ExprType)> judge)
// {
//   switch (expr->type()) {
//     // 没有子表达式
//     case ExprType::FIELD:
//     case ExprType::SELECT:
//     case ExprType::VALUE:
//     case ExprType::VALUELIST: {
//       return judge(expr->type());
//     } break;
//     case ExprType::AGGREGATION: {
//       return judge(expr->type()) || judge_attr_expr(expr->child_,judge);
//     }
//     case ExprType::ARITHMETIC:
//     case ExprType::FUNCTION:
//     case ExprType::CAST:
//     case ExprType::LOGICALCALC:
//     case ExprType::COMPARISON:
//     default:
//     break;
//   }
// }

RC SelectStmt::create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `from` statement
  std::vector<Table *>                                    tables;
  std::unordered_map<std::string, Table *>                table_map;
  std::unordered_map<std::string, std::string>            alias_to_tablename;
  const std::vector<std::pair<std::string, std::string>> &relation_to_alias = select_sql.relation_to_alias;
  std::unordered_map<std::string, Table *> stash_table_map;  // 暂存的table_map，解决跨内外层表名(alias)重复时，暂存一下

  for (size_t i = 0; i < select_sql.relation_to_alias.size(); i++) {
    const char *table_name            = select_sql.relation_to_alias[i].first.c_str();
    auto        find_table_same_level = [&](std::pair<std::string, std::string> it) -> bool {
      return relation_to_alias[i].second == it.first;
    };

    if (alias_to_tablename.count(select_sql.relation_to_alias[i].second) != 0 ||
        std::find_if(relation_to_alias.begin(), relation_to_alias.end(), find_table_same_level) !=
            std::end(relation_to_alias)) {
      return RC::SCHEMA_TABLE_EXIST;  // 表名的alias重复了, 或者是与同层级的table-name同名
    } else {
      if (select_sql.relation_to_alias[i].second != "") {
        alias_to_tablename[select_sql.relation_to_alias[i].second] = select_sql.relation_to_alias[i].first;
      }
    }
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    if (relation_to_alias[i].second == "") {
      table_map.insert(std::pair<std::string, Table *>(table_name, table));
    } else {
      // 假设使用别名之后就无法使用原名（除非外层select有）, NONONO，原名别名都得有
      table_map.insert(std::pair<std::string, Table *>(relation_to_alias[i].second, table));
      table_map.insert(std::pair<std::string, Table *>(table_name, table));
    }
  }
  for (auto table : table_map) {
    if (table_map_.count(table.first) != 0) {
      stash_table_map.insert(std::pair(table.first, table_map_[table.first]));
      table_map_.erase(table.first);
      table_map_.insert(table);
    } else {
      table_map_.insert(table);
    }
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // collect query fields in `select` statement
  std::vector<Expression *> query_fields_expressions;
  bool                      attr_has_aggregation = false;  // 是否存在聚集
  bool attr_has_only_field = false;  // 是否存在：只有field参与的属性值表达式，（不带aggregate）
  {
    for (int i = static_cast<int>(select_sql.attributes.size()) - 1; i >= 0; i--) {
      Expression             *expr            = nullptr;
      const ConditionSqlNode &cond            = select_sql.attributes[i];
      bool                    has_aggregation = false;
      bool                    has_field       = false;

      // 最外层可以是*，所以需要特殊处理，而对于COUNT(*)在attr_cond_to_expr内部特判即可。
      if (cond.type == FIELD) {
        attr_has_only_field = true;                               // 肯定有只包含fieldExpr，不包含agg的。
        if (common::is_blank(cond.attr.relation_name.c_str())) {  // 表名为空
          if (cond.attr.attribute_name == "*") {                  // 表名为空，且列名为* [].*
            // 将tables展开
            for (Table *table : tables) {
              wildcard_fields(table, query_fields_expressions);
            }
          } else {                     // 表名为空，且列名不为*, [].col
            if (tables.size() != 1) {  // table_name从from中获取，from的table必须唯一
              LOG_WARN("invalid. I do not know the attr's table. attr=%s", cond.attr.attribute_name.c_str());
              return RC::SCHEMA_FIELD_MISSING;
            }
            RC rc = attr_cond_to_expr(db, default_table, &table_map, &cond, expr, has_aggregation, has_field);
            if (OB_FAIL(rc)) {
              return rc;
            }
            query_fields_expressions.push_back(expr);
          }
        } else {                                    // 表名非空
          if (cond.attr.relation_name == "*") {     // 表名为*
            if (cond.attr.attribute_name == "*") {  // *.*
              // 将tables展开
              for (Table *table : tables) {
                wildcard_fields(table, query_fields_expressions);
              }
            } else {  // *.col
              LOG_WARN("invalid field name while table is *. attr=%s", cond.attr.attribute_name.c_str());
              return RC::SCHEMA_FIELD_MISSING;
            }
          } else {  // 表名非空且非'*'
            Table *table;
            RC     rc = is_table_legal(table_map, cond.attr.relation_name, table);
            if (OB_FAIL(rc)) {
              return rc;
            }
            if (cond.attr.attribute_name == "*") {  // t.*
              wildcard_fields(table, query_fields_expressions);
            } else {  // t.col
              RC rc = attr_cond_to_expr(db, default_table, &table_map, &cond, expr, has_aggregation, has_field);
              if (OB_FAIL(rc)) {
                return rc;
              }
              query_fields_expressions.push_back(expr);
            }
          }
        }
      } else {
        RC rc = attr_cond_to_expr(db, default_table, &table_map, &cond, expr, has_aggregation, has_field);
        if (OB_FAIL(rc)) {  // LOG_WARN在上面做
          return rc;
        }
        if (has_aggregation) {
          attr_has_aggregation = true;
        } else if (!has_aggregation && has_field) {
          attr_has_only_field = true;
        }
        query_fields_expressions.push_back(expr);
      }
    }
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db, default_table, &table_map_, select_sql.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // group_by + aggregate 相关的语法合法性检测
  if (select_sql.groups.empty()) {  // 在没有group by语句时，如果有聚合，则一定不能有非聚合的属性出现
    // bool has_normal_field = false;
    // bool has_agg_field    = false;
    // for (auto query_fields_expr : query_fields_expressions) {
    //   if (query_fields_expr->type() == ExprType::FIELD) {
    //     has_normal_field = true;
    //   }
    //   if (query_fields_expr->type() == ExprType::AGGREGATION) {
    //     has_agg_field = true;
    //   }
    // }
    if (attr_has_aggregation && attr_has_only_field) {
      LOG_WARN("Aggregated-attr cannot appear with normal-attr without group-by");
      return RC::SQL_SYNTAX;
    }
  } else {
    bool                     have_agg = false;
    std::vector<FieldExpr *> not_agg_query_fields;

    // 先检查group by的表名列名的存在合法性 (不考虑上述特殊限制下的合法性)
    for (auto group : select_sql.groups) {
      if (group.relation_name.empty()) {  // group by属性的表名为空
        if (tables.size() != 1) {         // table_name从from中获取，from的table必须唯一
          LOG_WARN("invalid. I do not know the attr's table. attr=%s in group-by clause", group.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }
        if (group.attribute_name == "*") {
          LOG_WARN("invalid. ATTR in group-by clause cannot be *. attr=%s in group-by clause", group.attribute_name.c_str());
          return RC::SQL_SYNTAX;
        }
      } else {  // group by属性的表名非空
        auto table_name = group.relation_name;
        auto field_name = group.attribute_name;

        if (table_name == "*") {
          LOG_WARN("invalid. Table-Name in group-by clause cannot be *");
          return RC::SQL_SYNTAX;
        } else {
          // 检测表名是否存在
          Table *table;
          RC     rc = is_table_legal(table_map, table_name, table);
          if (OB_FAIL(rc)) {
            return rc;
          }
          // 检测（表名，列名）是否合法
          if (field_name == "*") {
            LOG_WARN("invalid. ATTR in group-by clause cannot be *. attr=%s in group-by clause", group.attribute_name.c_str());
            return RC::SQL_SYNTAX;
          } else {  // 检测是否表有这个field
            const FieldMeta *field_meta = table->table_meta().field(field_name.c_str());
            if (nullptr == field_meta) {
              LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name.c_str());
              return RC::SCHEMA_FIELD_MISSING;
            }
          }
        }
      }
    }

    // 查看是否有聚集，并收集非聚合属性
    for (auto expr : query_fields_expressions) {
      if (expr->type() == ExprType::AGGREGATION) {
        have_agg = true;
      } else {
        not_agg_query_fields.emplace_back(dynamic_cast<FieldExpr *>(expr));
      }
    }

    if (have_agg) {  // 存在聚合时，非聚合属性必须∈{group by id}
      // 在有group by语句时，如果有聚合，则非聚合的属性一定要作为group by的属性，
      // 对于非聚合属性，确认是group by的属性
      for (auto field_expr : not_agg_query_fields) {
        bool found = false;
        for (auto group : select_sql.groups) {
          auto group_table_name = group.relation_name;
          auto group_field_name = group.attribute_name;

          if (group_table_name.empty()) {
            // 肯定是只有一个表,才允许group-by不写table,
            // 因为field_expr的tablename前面已经检测过了并填充为唯一表的名字
            // 所以只需检查field
            if (is_equal(field_expr->field_name(), group_field_name.c_str())) {
              found = true;
              break;
            }
          } else {
            if (is_equal(field_expr->table_name(), group_table_name.c_str()) &&
                is_equal(field_expr->field_name(), group_field_name.c_str())) {
              found = true;
              break;
            }
          }
        }

        if (found == false) {
          return RC::SQL_SYNTAX;
        }
      }
    }
  }

  // collect group by fields
  // std::vector<Field>        group_by_fields;
  std::vector<Expression *> group_by_fields_expressions;
  {
    for (int i = 0; i < static_cast<int>(select_sql.groups.size()); i++) {
      const RelAttrSqlNode &group_attr = select_sql.groups[i];

      if (common::is_blank(group_attr.relation_name.c_str()) &&
          0 == strcmp(group_attr.attribute_name.c_str(), "*")) {  // 表名为空且分组所有属性(*)
        // 不需要处理这种分组，没有分组的意义
        LOG_WARN("group by * have no meaning");
        return RC::SQL_SYNTAX;
      } else if (!common::is_blank(group_attr.relation_name.c_str())) {  // 表名非空
        const char *table_name = group_attr.relation_name.c_str();
        const char *field_name = group_attr.attribute_name.c_str();

        if (0 == strcmp(table_name, "*")) {  // table_name == "*"
          if (0 != strcmp(field_name, "*")) {
            LOG_WARN("invalid field name while table is *. attr=%s", field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }
          // 不需要处理这种分组，没有分组的意义
          LOG_WARN("group by *.* have no meaning");
          return RC::SQL_SYNTAX;
        } else {  // table_name != "*"
          auto iter = table_map.find(table_name);
          if (iter == table_map.end()) {
            LOG_WARN("no such table in from list: %s", table_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          Table *table = iter->second;
          if (0 == strcmp(field_name, "*")) {
            // wildcard_fields(table, group_by_fields);
            wildcard_fields(table, group_by_fields_expressions);
          } else {
            const FieldMeta *field_meta = table->table_meta().field(field_name);
            if (nullptr == field_meta) {
              LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
              return RC::SCHEMA_FIELD_MISSING;
            }

            // group_by_fields.push_back(Field(table, field_meta));
            Expression *field_expr = new FieldExpr(table, field_meta);
            group_by_fields_expressions.emplace_back(field_expr);
          }
        }
      } else {  // 表名为空，但不是分组所有属性
        if (tables.size() != 1) {
          LOG_WARN("invalid. I do not know the attr's table. attr=%s", group_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table           *table      = tables[0];
        const FieldMeta *field_meta = table->table_meta().field(group_attr.attribute_name.c_str());
        if (nullptr == field_meta) {
          LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), group_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }

        // group_by_fields.push_back(Field(table, field_meta));
        Expression *field_expr = new FieldExpr(table, field_meta);
        group_by_fields_expressions.emplace_back(field_expr);
      }
    }
  }

  // collect filter conditions in 'having' statement
  // FIXME: 目前Having仍然用的是旧版的condition，过不了编，需要重新调整
  HavingFilterStmt *having_filter_stmt = nullptr;
  {
    RC rc = HavingFilterStmt::create(db, default_table, &table_map, select_sql.havings, having_filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct having filter stmt");
      return rc;
    }
  }

  // 创造order by语句
  std::vector<std::pair<Field, bool>> order_by;
  {
    for (int i = static_cast<int>(select_sql.orders.size()) - 1; i >= 0; i--) {
      const RelAttrSqlNode &order_attr = select_sql.orders.at(i).attr;
      bool                  is_asc     = select_sql.orders.at(i).is_asc;

      // if (0 != strcmp(order_attr.aggregation_func.c_str(), "")) {  // 处理一下aggregate的特殊场景
      //   return RC::SQL_SYNTAX;
      // }

      if (common::is_blank(order_attr.relation_name.c_str()) &&
          0 == strcmp(order_attr.attribute_name.c_str(), "*")) {  // 表名为空且查询*
        return RC::SQL_SYNTAX;
      } else if (!common::is_blank(order_attr.relation_name.c_str())) {  // 表名非空
        const char *table_name = order_attr.relation_name.c_str();
        const char *field_name = order_attr.attribute_name.c_str();
        if (0 == strcmp(table_name, "*")) {  // table_name == "*"
          return RC::SQL_SYNTAX;
        } else {
          auto iter = table_map.find(table_name);
          if (iter == table_map.end()) {
            LOG_WARN("no such table in from list: %s", table_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          Table *table = iter->second;
          if (0 == strcmp(field_name, "*")) {
            return RC::SQL_SYNTAX;
          } else {
            const FieldMeta *field_meta = table->table_meta().field(field_name);
            if (nullptr == field_meta) {
              LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
              return RC::SCHEMA_FIELD_MISSING;
            }

            order_by.emplace_back(Field(table, field_meta), is_asc);
          }
        }
      } else {                     // 表名为空，但不是查询所有属性
        if (tables.size() != 1) {  // table_name从from中获取，from的table必须唯一
          LOG_WARN("invalid. I do not know the attr's table. attr=%s", order_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table           *table      = tables[0];
        const FieldMeta *field_meta = table->table_meta().field(order_attr.attribute_name.c_str());
        if (nullptr == field_meta) {
          LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), order_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }

        order_by.emplace_back(Field(table, field_meta), is_asc);
      }
    }
  }

  // everything alright, set select_stmt
  SelectStmt *select_stmt = new SelectStmt();
  // TODO add expression copy
  {
    select_stmt->tables_.swap(tables);
    select_stmt->query_fields_expressions_.swap(query_fields_expressions);
    // select_stmt->query_fields_.swap(query_fields);
    select_stmt->filter_stmt_ = filter_stmt;
    // select_stmt->aggregation_func_.swap(aggregation_func);
    select_stmt->group_by_fields_expressions_.swap(group_by_fields_expressions);
    // select_stmt->group_by_fields_.swap(group_by_fields);
    select_stmt->having_filter_stmt_ = having_filter_stmt;
    select_stmt->order_by_.swap(order_by);
    select_stmt->has_aggregation_ = attr_has_aggregation;
  }

  stmt = select_stmt;
  // remove table_map_
  for (auto table : table_map) {
    table_map_.erase(table.first);
  }
  for (auto stash_table : stash_table_map) {
    table_map_.insert(stash_table);
  }

  return RC::SUCCESS;
}
