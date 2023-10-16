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

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

static void wildcard_fields(Table *table, std::vector<Expression *> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    Expression *field_expr = new FieldExpr(table, table_meta.field(i));
    field_metas.emplace_back(field_expr);
  }
}

bool is_table_legal(
    const std::unordered_map<std::string, Table *> &table_map, const std::string &table_name, Table *&table)
{
  auto iter = table_map.find(table_name);
  if (iter == table_map.end()) {
    LOG_WARN("no such table in from list: %s", table_name);
    return false;
  }
  table = iter->second;
  return true;
}

bool is_equal(const char *a, const char *b) { return 0 == strcmp(a, b); }

RC SelectStmt::create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `from` statement
  std::vector<Table *>                     tables;
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
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
    table_map.insert(std::pair<std::string, Table *>(table_name, table));
  }
  for (auto table : table_map) {
    table_map_.insert(table);
  }

  // collect query fields in `select` statement
  std::vector<Field>                         query_fields;
  std::vector<Expression *>                  query_fields_expressions;
  std::vector<std::pair<std::string, Field>> aggregation_func;
  Table                                     *default_table = nullptr;
  {
    for (int i = static_cast<int>(select_sql.attributes.size()) - 1; i >= 0; i--) {
      const RelAttrSqlNode &relation_attr = select_sql.attributes[i];

      if (common::is_blank(relation_attr.relation_name.c_str()) &&
          0 == strcmp(relation_attr.attribute_name.c_str(), "*")) {  // 表名为空且查询所有属性(*)
        for (Table *table : tables) {
          wildcard_fields(table, query_fields);
        }

        // 记录field的聚合信息：这里如果有聚合，只可能是count(*)
        if (0 == strcmp(relation_attr.aggregation_func.c_str(), "COUNT")) {
          aggregation_func.emplace_back("COUNT", Field(nullptr, nullptr));
        }

        //-----------------------------------------------------
        // 重构为expression的写法
        // select * or select count(*)
        if (0 == strcmp(relation_attr.aggregation_func.c_str(),
                     "COUNT")) {  // select count(*) 在logical operator生成的时候才将 *
                                  // 拆分，目的是为了保证与用户查询的顺序对应，同时必须要在过程中拆分出 *
          Expression *agg_expr =
              new AggregationExpr(Field(nullptr, nullptr), "COUNT");  // (nullptr, nullptr)对应 * or *.*
          query_fields_expressions.emplace_back(agg_expr);
        } else {  // 普通 select * 在这里就可以拆分
          for (Table *table : tables) {
            wildcard_fields(table, query_fields_expressions);
          }
        }
      } else if (!common::is_blank(relation_attr.relation_name.c_str())) {  // 表名非空
        const char *table_name           = relation_attr.relation_name.c_str();
        const char *field_name           = relation_attr.attribute_name.c_str();
        const char *aggregation_function = relation_attr.aggregation_func.c_str();

        if (0 == strcmp(table_name, "*")) {  // table_name == "*"
          if (0 != strcmp(field_name, "*")) {
            LOG_WARN("invalid field name while table is *. attr=%s", field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }
          for (Table *table : tables) {
            wildcard_fields(table, query_fields);
          }

          // 记录field的聚合信息：这里如果有聚合，只可能是count(*)
          if (0 == strcmp(aggregation_function, "COUNT")) {
            aggregation_func.emplace_back("COUNT", Field(nullptr, nullptr));
          }

          // select *.* or select count(*.*)
          if (0 == strcmp(aggregation_function, "COUNT")) {
            Expression *agg_expr = new AggregationExpr(Field(nullptr, nullptr), "COUNT");
            query_fields_expressions.emplace_back(agg_expr);
          } else {
            for (Table *table : tables) {
              wildcard_fields(table, query_fields_expressions);
            }
          }
        } else {  // table_name != "*"
          Table *table;
          if (!is_table_legal(table_map, table_name, table)) {
            return RC::SCHEMA_FIELD_MISSING;
          }

          if (0 == strcmp(field_name, "*")) {
            wildcard_fields(table, query_fields);
            // 记录field的聚合信息：这里如果有聚合，只可能是count(*)
            if (0 == strcmp(aggregation_function, "COUNT")) {
              aggregation_func.emplace_back("COUNT", Field(table, nullptr));
            }
            // select t.* or select count(t.*)
            if (0 == strcmp(aggregation_function, "COUNT")) {
              Expression *agg_expr =
                  new AggregationExpr(Field(table, nullptr), "COUNT");  // (table, nullptr)对应table.*
              query_fields_expressions.emplace_back(agg_expr);
            } else {
              wildcard_fields(table, query_fields_expressions);
            }
          } else {
            const FieldMeta *field_meta = table->table_meta().field(field_name);
            if (nullptr == field_meta) {
              LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
              return RC::SCHEMA_FIELD_MISSING;
            }

            query_fields.push_back(Field(table, field_meta));

            // 记录field的聚合信息：这里可能存在5种类型的聚合
            // if (0 != strcmp(aggregation_function, "")) {
            if (!common::is_blank(aggregation_function)) {
              aggregation_func.emplace_back(std::string(aggregation_function), Field(table, field_meta));
            }
            // need aggregate or not?
            if (common::is_blank(aggregation_function)) {
              Expression *field_expr = new FieldExpr(table, field_meta);
              query_fields_expressions.emplace_back(field_expr);
            } else {
              Expression *agg_expr = new AggregationExpr(Field(table, field_meta), relation_attr.aggregation_func);
              query_fields_expressions.emplace_back(agg_expr);
            }
          }
        }
      } else {                     // 表名为空，但不是查询所有属性
        if (tables.size() != 1) {  // table_name从from中获取，from的table必须唯一
          LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table           *table      = tables[0];
        const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name.c_str());
        if (nullptr == field_meta) {
          LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }

        query_fields.push_back(Field(table, field_meta));

        // 记录field的聚合信息：这里可能存在5种类型的聚合
        // if (0 != strcmp(relation_attr.aggregation_func.c_str(), "")) {
        if (!common::is_blank(relation_attr.aggregation_func.c_str())) {
          aggregation_func.emplace_back(std::string(relation_attr.aggregation_func.c_str()), Field(table, field_meta));
        }
        // need aggregate or not?
        if (common::is_blank(relation_attr.aggregation_func.c_str())) {
          Expression *field_expr = new FieldExpr(table, field_meta);
          query_fields_expressions.emplace_back(field_expr);
        } else {
          Expression *agg_expr = new AggregationExpr(Field(table, field_meta), relation_attr.aggregation_func);
          query_fields_expressions.emplace_back(agg_expr);
        }
      }
    }

    LOG_INFO("got %d tables in from stmt and %d fields in query stmt", tables.size(), query_fields_expressions.size());

    if (tables.size() == 1) {
      default_table = tables[0];
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
    // 旧版check
    // Expression *expr = query_fields_expressions.front();
    // if (expr->type() == ExprType::AGGREGATION) {
    //   for (auto attr : select_sql.attributes) {
    //     if (common::is_blank(attr.aggregation_func.c_str())) {
    //       return RC::SQL_SYNTAX;
    //     }
    //   }
    // }
    bool has_normal_field = false;
    bool has_agg_field    = false;
    for (auto query_fields_expr : query_fields_expressions) {
      if (query_fields_expr->type() == ExprType::FIELD) {
        has_normal_field = true;
      }
      if (query_fields_expr->type() == ExprType::AGGREGATION) {
        has_agg_field = true;
      }
    }
    if (has_agg_field && has_normal_field) {
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
          if (!is_table_legal(table_map, table_name, table)) {
            return RC::SCHEMA_FIELD_MISSING;
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
  std::vector<Field>        group_by_fields;
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
            wildcard_fields(table, group_by_fields);
            wildcard_fields(table, group_by_fields_expressions);
          } else {
            const FieldMeta *field_meta = table->table_meta().field(field_name);
            if (nullptr == field_meta) {
              LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
              return RC::SCHEMA_FIELD_MISSING;
            }

            group_by_fields.push_back(Field(table, field_meta));
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

        group_by_fields.push_back(Field(table, field_meta));
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

      if (0 != strcmp(order_attr.aggregation_func.c_str(), "")) {  // 处理一下aggregate的特殊场景
        return RC::SQL_SYNTAX;
      }

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
    select_stmt->query_fields_.swap(query_fields);
    select_stmt->filter_stmt_ = filter_stmt;
    select_stmt->aggregation_func_.swap(aggregation_func);
    select_stmt->group_by_fields_expressions_.swap(group_by_fields_expressions);
    select_stmt->group_by_fields_.swap(group_by_fields);
    select_stmt->having_filter_stmt_ = having_filter_stmt;
    select_stmt->order_by_.swap(order_by);
  }

  stmt = select_stmt;
  // remove table_map_
  for (auto table : table_map) {
    table_map_.erase(table.first);
  }
  return RC::SUCCESS;
}
