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

class Db;
class Table;
class FieldMeta;

struct HavingFilterObj
{
  bool        is_attr;
  Field       field;  // 根据table()!=nullptr和meta()==nullptr特殊标记count(*)
  Value       value;
  std::string aggregation_func_;

  void init_attr(const Field &field, const std::string &agg_func)
  {
    is_attr                 = true;
    this->field             = field;
    this->aggregation_func_ = agg_func;
  }

  void init_value(const Value &value)
  {
    is_attr     = false;
    this->value = value;
  }
};

class HavingFilterUnit
{
public:
  HavingFilterUnit() = default;
  ~HavingFilterUnit() {}

  void set_comp(CompOp comp) { comp_ = comp; }

  CompOp comp() const { return comp_; }

  void set_left(const HavingFilterObj &obj) { left_ = obj; }
  void set_right(const HavingFilterObj &obj) { right_ = obj; }

  const HavingFilterObj &left() const { return left_; }
  const HavingFilterObj &right() const { return right_; }

private:
  CompOp          comp_ = NO_OP;
  HavingFilterObj left_;
  HavingFilterObj right_;
};

/**
 * @brief HavingFilter/聚合过滤语句
 * @ingroup Statement
 */
class HavingFilterStmt
{
public:
  HavingFilterStmt() = default;
  virtual ~HavingFilterStmt();

public:
  const std::vector<HavingFilterUnit *> &filter_units() const { return having_filter_units_; }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode *conditions, int condition_num, HavingFilterStmt *&stmt);

  static RC create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode &condition, HavingFilterUnit *&filter_unit);

private:
  std::vector<HavingFilterUnit *> having_filter_units_;  // 默认当前都是AND关系
};
