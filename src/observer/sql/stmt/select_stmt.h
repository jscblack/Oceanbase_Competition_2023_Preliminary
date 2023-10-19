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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <memory>
#include <vector>

#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class HavingFilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }

public:
  static RC create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt);

public:
  const std::vector<Table *>      &tables() const { return tables_; }
  const std::vector<Expression *> &query_fields_expressions() const { return query_fields_expressions_; }
  // const std::vector<Field>        &query_fields() const { return query_fields_; }
  FilterStmt *filter_stmt() const { return filter_stmt_; }
  // const std::vector<std::pair<std::string, Field>> &aggregation_func() const { return aggregation_func_; }
  const std::vector<Expression *> &group_by_fields_expressions() const { return group_by_fields_expressions_; }
  // const std::vector<Field>        &group_by_fields() const { return group_by_fields_; }
  HavingFilterStmt                          *having_filter_stmt() const { return having_filter_stmt_; }
  const std::vector<std::pair<Field, bool>> &order_by() const { return order_by_; }
  bool                                       has_aggregation() const { return has_aggregation_; }

private:
  std::vector<Expression *> query_fields_expressions_;
  // std::vector<Field>                         query_fields_;
  std::vector<Table *> tables_;
  FilterStmt          *filter_stmt_ = nullptr;
  // std::vector<std::pair<std::string, Field>> aggregation_func_;  // (aggregation_function_type, Field)
  // std::vector<Expression *>                  query_exprs_;

private:
  // 这玩意是子查询用的, create的进入和退出记得处理一下, 记录已经打开的表的信息.
  inline static std::unordered_map<std::string, Table *> table_map_;
  inline static std::unordered_map<std::string, Table *>
      stash_table_map_;  // 暂存的table_map，解决跨内外层表名(alias)重复时，暂存一下

  bool                      has_aggregation_ = false;
  std::vector<Expression *> group_by_fields_expressions_;

  // std::vector<Field>                  group_by_fields_;
  HavingFilterStmt                   *having_filter_stmt_ = nullptr;
  std::vector<std::pair<Field, bool>> order_by_;  // (Field, is_asc)
};
