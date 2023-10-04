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

#pragma once

#include "common/rc.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"

class Table;
struct ValueOrStmt  // update 的 value其中包含value或select_stmt
{
  bool  value_from_select;  ///< 是否是子查询，默认false
  Value literal_value;      ///< value
  Stmt *select_stmt;        ///< select clause

  ValueOrStmt() = default;
  ValueOrStmt(bool from_select, const Value &value) : value_from_select(from_select), literal_value(value){};
  ValueOrStmt(bool from_select, Stmt *stmt) : value_from_select(from_select), select_stmt(stmt){};
};
/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  // UpdateStmt(
  //     Table *table, const char **field_names, const UpdateValue *values, int value_amount, FilterStmt *filter_stmt);
  UpdateStmt(Table *table, const std::vector<std::string> &field_names, const std::vector<ValueOrStmt> &values,
      FilterStmt *filter_stmt);
  ~UpdateStmt() override;

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  Table *table() const { return table_; }
  // const char       **field_names() const { return field_names_; }
  // const UpdateValue *values() const { return values_; }
  const std::vector<std::string> &field_names() const { return field_names_; }
  const std::vector<ValueOrStmt> &values() const { return values_; }
  // int                              value_amount() const { return value_amount_; }
  FilterStmt *filter_stmt() const { return filter_stmt_; }
  StmtType    type() const override { return StmtType::UPDATE; }

private:
  Table *table_ = nullptr;
  // const char       **field_names_  = nullptr;
  // const UpdateValue *values_       = nullptr;
  std::vector<std::string> field_names_;
  std::vector<ValueOrStmt> values_;
  // int                       value_amount_ = 0;
  FilterStmt *filter_stmt_ = nullptr;
};
