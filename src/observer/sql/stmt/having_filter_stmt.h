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

// struct HavingFilterObj
// {
//   Expression *expr = nullptr;
//   void        init_expr(Expression *expr) { this->expr = expr; }
// };

// struct HavingFilterObj
// {
//   bool        is_attr;
//   Field       field;  // 根据table()!=nullptr和meta()==nullptr特殊标记count(*)
//   Value       value;
//   std::string aggregation_func_;
//   void init_attr(const Field &field, const std::string &agg_func)
//   {
//     is_attr                 = true;
//     this->field             = field;
//     this->aggregation_func_ = agg_func;
//   }
//   void init_value(const Value &value)
//   {
//     is_attr     = false;
//     this->value = value;
//   }
// };

// class HavingFilterUnit
// {
// public:
//   HavingFilterUnit() = default;
//   ~HavingFilterUnit() {}

//   void set_comp(CompOp comp) { comp_ = comp; }

//   CompOp comp() const { return comp_; }

//   void set_left(const HavingFilterObj &obj) { left_ = obj; }
//   void set_right(const HavingFilterObj &obj) { right_ = obj; }

//   const HavingFilterObj &left() const { return left_; }
//   const HavingFilterObj &right() const { return right_; }

// private:
//   CompOp          comp_ = NO_OP;
//   HavingFilterObj left_;
//   HavingFilterObj right_;
// };

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
  // HavingFilterUnit *filter_unit() const { return filter_unit_; }
  // HavingFilterStmt *left() const { return left_; }
  // HavingFilterStmt *right() const { return right_; }
  // LogiOp            logi() const { return logi_; }
  // bool              is_filter_unit() const { return left_ == nullptr && right_ == nullptr && filter_unit_ != nullptr;
  // }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode *conditions, HavingFilterStmt *&stmt);

  // static RC create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
  //     const ConditionSqlNode &condition, HavingFilterUnit *&filter_unit);
private:
  Expression *filter_expr_ = nullptr;
  // std::vector<HavingFilterUnit *> having_filter_units_;  // 默认当前都是AND关系

  // HavingFilterUnit *filter_unit_ = nullptr;
  // HavingFilterStmt *left_        = nullptr;
  // LogiOp            logi_        = NO_LOGI_OP;
  // HavingFilterStmt *right_       = nullptr;
};
