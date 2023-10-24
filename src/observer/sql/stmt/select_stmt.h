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
  FilterStmt                      *filter_stmt() const { return filter_stmt_; }
  const std::vector<Expression *> &group_by_fields_expressions() const { return group_by_fields_expressions_; }
  HavingFilterStmt                *having_filter_stmt() const { return having_filter_stmt_; }
  const std::vector<std::pair<Field, bool>>              &order_by() const { return order_by_; }
  bool                                                    has_aggregation() const { return has_aggregation_; }
  bool                                                    is_simple_select() const { return is_simple_select_; }
  const std::vector<Stmt *>                              &view_stmts() const { return view_stmts_; }
  const std::vector<std::pair<std::string, std::string>> &relation_to_alias() const { return relation_to_alias_; }

private:
  std::vector<Expression *>                        query_fields_expressions_;
  std::vector<Table *>                             tables_;
  std::vector<std::pair<std::string, std::string>> relation_to_alias_;
  FilterStmt                                      *filter_stmt_ = nullptr;

private:
  // 这玩意是子查询用的, create的进入和退出记得处理一下, 记录已经打开的表的信息.
  inline static std::unordered_map<std::string, Table *> table_map_;

private:
  bool                      has_aggregation_ = false;
  std::vector<Expression *> group_by_fields_expressions_;
  HavingFilterStmt         *having_filter_stmt_ = nullptr;

private:
  bool is_simple_select_ = false;  // select without from

private:
  std::vector<std::pair<Field, bool>> order_by_;  // (Field, is_asc)

private:
  std::vector<Stmt *> view_stmts_;
};
