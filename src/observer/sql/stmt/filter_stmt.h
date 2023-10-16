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

#include "sql/expr/expression.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include <unordered_map>
#include <vector>

class Db;
class Table;
class FieldMeta;

struct FilterObj
{
  Expression *expr = nullptr;
  void        init_expr(Expression *expr) { this->expr = expr; }
};

class FilterUnit
{
public:
  FilterUnit() = default;
  ~FilterUnit() {}

  // void set_comp(CompOp comp) { comp_ = comp; }

  // CompOp comp() const { return comp_; }

  // void set_left(const FilterObj &obj) { left_ = obj; }
  // void set_right(const FilterObj &obj) { right_ = obj; }

  // const FilterObj &left() const { return left_; }
  // const FilterObj &right() const { return right_; }
  // FilterObj       &left() { return left_; }
  // FilterObj       &right() { return right_; }

  void       set_obj(const FilterObj &obj) { obj_ = obj; }
  FilterObj &filter_object() { return obj_; }

private:
  // FilterObj left_;
  // CompOp    comp_ = NO_OP;
  // FilterObj right_;
  FilterObj obj_;
};

/**
 * @brief Filter/谓词/过滤语句
 * @ingroup Statement
 */
class FilterStmt
{
public:
  FilterStmt()  = default;
  ~FilterStmt() = default;

public:
  Expression *filter_expr() const { return filter_expr_; }

  // FilterUnit *filter_unit() const { return filter_unit_; }
  // FilterStmt *left() const { return left_; }
  // FilterStmt *right() const { return right_; }
  // LogiOp      logi() const { return logi_; }
  // bool        is_filter_unit() const { return left_ == nullptr && right_ == nullptr && filter_unit_ != nullptr; }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode *conditions, FilterStmt *&stmt);

  static RC create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode &condition, FilterUnit *&filter_unit);

private:
  Expression *filter_expr_ = nullptr;
  // FilterUnit *filter_unit_ = nullptr;
  
  // 最终重构理论上只需要 Expression*

  // private:
  //   FilterStmt *left_  = nullptr;
  //   LogiOp      logi_  = NO_LOGI_OP;
  //   FilterStmt *right_ = nullptr;
};
