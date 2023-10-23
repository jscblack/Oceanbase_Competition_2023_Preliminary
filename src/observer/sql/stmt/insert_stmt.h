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
#include "sql/parser/value.h"
#include "sql/stmt/stmt.h"

class Table;
class Db;

/**
 * @brief 插入语句
 * @ingroup Statement
 */
class InsertStmt : public Stmt
{
public:
  InsertStmt() = default;
  InsertStmt(Table *table, std::vector<std::vector<Value>> values, int value_amount, int record_amount);

  StmtType type() const override { return StmtType::INSERT; }

public:
  static RC create(Db *db, InsertSqlNode &insert_sql, Stmt *&stmt);

public:
  Table *table() const { return table_; }
  // const Value **values() const { return values_; }
  const std::vector<std::vector<Value>> &values() const { return values_; }

  int value_amount() const { return value_amount_; }
  int record_amount() const { return record_amount_; }

private:
  Table                          *table_ = nullptr;
  std::vector<std::vector<Value>> values_;
  int                             value_amount_  = 0;  // 现在已经不用了
  int                             record_amount_ = 0;
};
