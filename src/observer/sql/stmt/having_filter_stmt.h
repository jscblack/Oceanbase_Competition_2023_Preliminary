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
// Created by tong1heng on 2023/10/10.
//

#pragma once

#include <vector>
#include <unordered_map>
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"
#include "sql/stmt/filter_stmt.h"

class Db;
class Table;
class FieldMeta;

/**
 * @brief HavingFilter/聚合过滤语句
 * @ingroup Statement (FilterStmt)
 */
class HavingFilterStmt
{
public:
  HavingFilterStmt()  = default;
  ~HavingFilterStmt() = default;

public:
  Expression *filter_expr() const { return filter_expr_; }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      std::vector<std::pair<std::string, std::string>> &relation_to_alias, const ConditionSqlNode *conditions,
      HavingFilterStmt *&stmt);

private:
  Expression *filter_expr_ = nullptr;
};
