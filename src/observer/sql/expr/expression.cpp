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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "common/rc.h"
#include "sql/expr/tuple.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"
#include <regex>

using namespace std;

RC FieldExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  value = value_;
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }

  switch (cast_type_) {
    case BOOLEANS: {
      bool val = value.get_boolean();
      cast_value.set_boolean(val);
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported convert from type %d to %d", child_->value_type(), cast_type_);
    }
  }
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &cell, Trx *trx) const
{
  RC rc = child_->get_value(tuple, cell);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(cell, cell);
}

RC CastExpr::try_get_value(Value &value) const
{
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, value);
}

Expression *CastExpr::clone() const { return new CastExpr(unique_ptr<Expression>(child_->clone()), cast_type_); }

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  // 对于like比较的补丁, 左右两边都为string假设
  if (LIKE_ENUM == comp_ || NOT_LIKE_ENUM == comp_) {
    string pattern_str(right.get_string());
    regex  reg1("%");
    pattern_str = regex_replace(pattern_str, reg1, "[^']*");
    regex reg2("_");
    pattern_str = regex_replace(pattern_str, reg2, "[^']");
    pattern_str = "^" + pattern_str + "$";
    regex pattern(pattern_str);
    if (regex_match(left.get_string(), pattern)) {
      if (LIKE_ENUM == comp_) {
        result = true;
      }
      if (NOT_LIKE_ENUM == comp_) {
        result = false;
      }
    } else {  // NOT MATCH, NOT LIKE
      if (LIKE_ENUM == comp_) {
        result = false;
      }
      if (NOT_LIKE_ENUM == comp_) {
        result = true;
      }
    }
    return RC::SUCCESS;
  }

  RC rc = RC::SUCCESS;
  int cmp_result = left.compare(right);  // 这是基于cast的比较，把null是作为最小值看待的，但实际上null不可比
  result = false;
  if (left.is_null() || right.is_null()) {
    // null的比较当中只有null is null会返回true，以及value is not null会返回true
    // 其他的比较都会返回false
    if (comp_ == IS_ENUM && left.is_null() && right.is_null()) {
      result = true;
    } else if (comp_ == IS_NOT_ENUM && (!left.is_null() || !right.is_null())) {
      result = true;
    } else {
      result = false;
    }
    return rc;
  }
  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    // TODO: IS和IS NOT的比较，不需要cast，直接比较
    // case IS_ENUM: {
    //   // null is null
    //   if (left.is_null() && right.is_null()) {
    //     result = true;
    //     break;
    //   }
    //   result = (cmp_result == 0);
    // } break;
    // case IS_NOT_ENUM: {
    //   // value is not null
    //   // null is not value
    //   if (left.is_null() && right.is_null()) {
    //     result = false;
    //     break;
    //   }
    //   result = (cmp_result != 0);
    // } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::compare_value(const Value &left, const std::vector<Value> &right, bool &value) const
{
  if (comp_ == EXISTS_ENUM || comp_ == NOT_EXISTS_ENUM) {
    if (right.empty()) {
      if (comp_ == EXISTS_ENUM) {
        value = false;
      } else {
        value = true;
      }
    } else {
      if (comp_ == EXISTS_ENUM) {
        value = true;
      } else {
        value = false;
      }
    }
    return RC::SUCCESS;
  }
  assert(comp_ == IN_ENUM || comp_ == NOT_IN_ENUM);  // 目前只处理in和not in
  bool in_right = false;
  for (int i = 0; i < right.size(); i++) {
    int result = left.compare(right[i]);
    if (result == 0) {
      in_right = true;
      break;
    }
  }
  if (comp_ == IN_ENUM) {
    value = in_right;
  } else {
    value = !in_right;
  }
  return RC::SUCCESS;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr   *left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  // 只有在有子查询的情况下，才会调用这个函数
  Value              left_value;
  Value              right_value;
  std::vector<Value> left_values;
  std::vector<Value> right_values;
  RC                 rc = RC::SUCCESS;
  if (left_ != nullptr && left_->type() == ExprType::SELECT) {
    rc = dynamic_cast<SelectExpr *>(left_.get())->get_value(tuple, left_values, trx);
    if (!left_values.empty()) {
      left_value = left_values[0];
    } else {
      LOG_WARN("left value is empty");
      // 这是个null
      left_value.set_type(AttrType::NONE);
    }
  } else if (left_ != nullptr && left_->type() == ExprType::VALUELIST) {
    rc = dynamic_cast<ValueListExpr *>(left_.get())->get_values(left_values);
  } else {
    rc = left_->get_value(tuple, left_value);
  }
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_->type() == ExprType::SELECT) {
    rc = dynamic_cast<SelectExpr *>(right_.get())->get_value(tuple, right_values, trx);
    if (!right_values.empty()) {
      right_value = right_values[0];
    } else {
      LOG_WARN("right value is empty");
      // 这是个null
      right_value.set_type(AttrType::NONE);
    }
  } else if (right_->type() == ExprType::VALUELIST) {
    rc = dynamic_cast<ValueListExpr *>(right_.get())->get_values(right_values);
  } else {
    rc = right_->get_value(tuple, right_value);
  }
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  bool bool_value = false;
  // 包括in 和 not in 也可以在这边处理？

  if (comp_ == IN_ENUM || comp_ == NOT_IN_ENUM || comp_ == EXISTS_ENUM || comp_ == NOT_EXISTS_ENUM) {
    rc = compare_value(left_value, right_values, bool_value);
  } else {
    // 需要在这里处理一下子查询返回过少的情况，因为是一个value的比较
    if (left_->type() == ExprType::SELECT && left_values.size() > 1) {
      return RC::INTERNAL;
    }
    if (right_->type() == ExprType::SELECT && right_values.size() > 1) {
      return RC::INTERNAL;
    }
    rc = compare_value(left_value, right_value, bool_value);
  }
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

Expression *ComparisonExpr::clone() const
{
  return new ComparisonExpr(comp_, unique_ptr<Expression>(left_->clone()), unique_ptr<Expression>(right_->clone()));
}

////////////////////////////////////////////////////////////////////////////////

SelectExpr::SelectExpr(Stmt *stmt) : select_stmt_(stmt) {}

RC SelectExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  // 这里得把stmt真正执行一下，然后把结果放到value里面
  // 可能是个结果集，也可能是个单值
  // 这里仅仅占位，不做实现
  return RC::UNIMPLENMENT;
}

RC SelectExpr::get_value(const Tuple &tuple, std::vector<Value> &values, Trx *trx)
{
  // 将tuple加入到tuples_里面，但还需要知道table_name
  // 外面传进来的是一个record，肯定是一个row_tuple
  // 强转
  SelectExpr::tuples_.insert(std::pair<std::string, const Tuple *>(tuple.to_string(), &tuple));
  // 这里得把stmt真正执行一下，然后把结果放到value里面
  // 从stmt开始，手动处理他的生命周期
  RC rc = RC::SUCCESS;
  // 在这里就是重写一下stmt，把在tuples_里面已知的attr信息给他变成value
  rc = rewrite_stmt(select_stmt_, &tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to rewrite stmt. rc=%s", strrc(rc));
    return rc;
  }

  // 开始真正执行，所有SelectExpr都共享tuple_
  unique_ptr<LogicalOperator>  logical_operator;
  unique_ptr<PhysicalOperator> physical_operator;
  LogicalPlanGenerator         logical_plan_generator_;
  PhysicalPlanGenerator        physical_plan_generator_;

  logical_plan_generator_.create(select_stmt_, logical_operator);
  physical_plan_generator_.create(*logical_operator, physical_operator);

  rc = physical_operator->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open physical operator. rc=%s", strrc(rc));
    return rc;
  }

  while (RC::SUCCESS == (rc = physical_operator->next())) {
    // only grab one
    Tuple *tuple = physical_operator->current_tuple();
    if (tuple == nullptr) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return RC::INTERNAL;
    }
    if (tuple->cell_num() > 1) {
      LOG_WARN("invalid select result, too much columns");
      return RC::INTERNAL;
    }
    Value value;
    rc = tuple->cell_at(0, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get cell: %s", strrc(rc));
      return rc;
    }
    values.push_back(value);
  }
  physical_operator->close();
  // 到这里他就执行完了
  rc = recover_stmt(select_stmt_, &tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to recover stmt. rc=%s", strrc(rc));
    return rc;
  }
  SelectExpr::tuples_.erase(tuple.to_string());
  return RC::SUCCESS;
}

RC SelectExpr::rewrite_stmt(Stmt *&rewrited_stmt, const Tuple *tuple)
{
  // 基于select_stmt_和tuples_，重写select_stmt_，得到rewrited_stmt
  // 需要注意的是，在这里把stmt里面的filter_stmt 与外部已知match的部分给他替换了
  // todo 重写，现在先做无依赖的
  const RowTuple *row_tuple   = static_cast<const RowTuple *>(tuple);
  SelectStmt     *select_stmt = dynamic_cast<SelectStmt *>(rewrited_stmt);
  RC              rc          = RC::SUCCESS;
  // 只要还有filter_unit，就一直处理
  // 因为就是要把filter_unit里面的filter obj修改
  for (auto filter_unit : select_stmt->filter_stmt()->filter_units()) {
    if (filter_unit->left().expr != nullptr && filter_unit->left().expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归重写
      Stmt *rewrited_sub_stmt = dynamic_cast<SelectExpr *>(filter_unit->left().expr)->select_stmt_;
      rc                      = rewrite_stmt(rewrited_sub_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(filter_unit->left().expr)->select_stmt_ = rewrited_sub_stmt;
    }
    if (filter_unit->right().expr != nullptr && filter_unit->right().expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归重写
      Stmt *rewrited_sub_stmt = dynamic_cast<SelectExpr *>(filter_unit->right().expr)->select_stmt_;
      rc                      = rewrite_stmt(rewrited_sub_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(filter_unit->right().expr)->select_stmt_ = rewrited_sub_stmt;
    }
    if (filter_unit->left().expr != nullptr && filter_unit->left().expr->type() == ExprType::FIELD &&
        strcmp(dynamic_cast<FieldExpr *>(filter_unit->left().expr)->table_name(), row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      Value tmp_value;
      rc = row_tuple->find_cell(TupleCellSpec(dynamic_cast<FieldExpr *>(filter_unit->left().expr)->table_name(),
                                    dynamic_cast<FieldExpr *>(filter_unit->left().expr)->field_name()),
          tmp_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to find cell. rc=%s", strrc(rc));
        return rc;
      }
      ValueExpr *value_expr = new ValueExpr(tmp_value);
      recover_table[value_expr] = filter_unit->left().expr;  // 用现在存的expr保存下来信息，recover的时候把指针给回去
      filter_unit->left().init_expr(value_expr);
    }
    if (filter_unit->right().expr != nullptr && filter_unit->right().expr->type() == ExprType::FIELD &&
        strcmp(dynamic_cast<FieldExpr *>(filter_unit->right().expr)->table_name(), row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      Value tmp_value;
      rc = row_tuple->find_cell(TupleCellSpec(dynamic_cast<FieldExpr *>(filter_unit->right().expr)->table_name(),
                                    dynamic_cast<FieldExpr *>(filter_unit->right().expr)->field_name()),
          tmp_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to find cell. rc=%s", strrc(rc));
        return rc;
      }
      ValueExpr *value_expr = new ValueExpr(tmp_value);
      recover_table[value_expr] = filter_unit->right().expr;  // 用现在存的expr保存下来信息，recover的时候把指针给回去
      filter_unit->right().init_expr(value_expr);
    }
  }

  return RC::SUCCESS;
}

RC SelectExpr::recover_stmt(Stmt *&rewrited_stmt, const Tuple *tuple)
{
  const RowTuple *row_tuple   = static_cast<const RowTuple *>(tuple);
  SelectStmt     *select_stmt = dynamic_cast<SelectStmt *>(rewrited_stmt);
  RC              rc          = RC::SUCCESS;
  // 只要还有filter_unit，就一直处理
  // 因为就是要把filter_unit里面的filter obj修改
  for (auto filter_unit : select_stmt->filter_stmt()->filter_units()) {
    if (filter_unit->left().expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归还原
      Stmt *rewrited_sub_stmt = dynamic_cast<SelectExpr *>(filter_unit->left().expr)->select_stmt_;
      rc                      = recover_stmt(rewrited_sub_stmt, row_tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to recover sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(filter_unit->left().expr)->select_stmt_ = rewrited_sub_stmt;
    }
    if (filter_unit->right().expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归还原
      Stmt *rewrited_sub_stmt = dynamic_cast<SelectExpr *>(filter_unit->right().expr)->select_stmt_;
      rc                      = recover_stmt(rewrited_sub_stmt, row_tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to recover sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(filter_unit->right().expr)->select_stmt_ = rewrited_sub_stmt;
    }
    if (filter_unit->left().expr->type() == ExprType::VALUE &&
        recover_table.find(filter_unit->left().expr) != recover_table.end()) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      auto *tmp_ptr = filter_unit->left().expr;
      filter_unit->left().init_expr(recover_table[filter_unit->left().expr]);
      recover_table.erase(tmp_ptr);
    }
    if (filter_unit->right().expr->type() == ExprType::VALUE &&
        recover_table.find(filter_unit->right().expr) != recover_table.end()) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      auto *tmp_ptr = filter_unit->right().expr;
      filter_unit->right().init_expr(recover_table[filter_unit->right().expr]);
      recover_table.erase(tmp_ptr);
    }
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value, trx);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

Expression *ConjunctionExpr::clone() const
{
  vector<unique_ptr<Expression>> children;
  for (const unique_ptr<Expression> &expr : children_) {
    children.push_back(unique_ptr<Expression>(expr->clone()));
  }
  return new ConjunctionExpr(conjunction_type_, children);
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();

  switch (arithmetic_type_) {
    case Type::ADD: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() + right_value.get_int());
      } else {
        value.set_float(left_value.get_float() + right_value.get_float());
      }
    } break;

    case Type::SUB: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() - right_value.get_int());
      } else {
        value.set_float(left_value.get_float() - right_value.get_float());
      }
    } break;

    case Type::MUL: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() * right_value.get_int());
      } else {
        value.set_float(left_value.get_float() * right_value.get_float());
      }
    } break;

    case Type::DIV: {
      if (target_type == AttrType::INTS) {
        if (right_value.get_int() == 0) {
          // NOTE:
          // 设置为整数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为整数最大值。
          value.set_int(numeric_limits<int>::max());
        } else {
          value.set_int(left_value.get_int() / right_value.get_int());
        }
      } else {
        if (right_value.get_float() > -EPSILON && right_value.get_float() < EPSILON) {
          // NOTE:
          // 设置为浮点数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为浮点数最大值。
          value.set_float(numeric_limits<float>::max());
        } else {
          value.set_float(left_value.get_float() / right_value.get_float());
        }
      }
    } break;

    case Type::NEGATIVE: {
      if (target_type == AttrType::INTS) {
        value.set_int(-left_value.get_int());
      } else {
        value.set_float(-left_value.get_float());
      }
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

Expression *ArithmeticExpr::clone() const
{
  return new ArithmeticExpr(
      arithmetic_type_, unique_ptr<Expression>(left_->clone()), unique_ptr<Expression>(right_->clone()));
}
