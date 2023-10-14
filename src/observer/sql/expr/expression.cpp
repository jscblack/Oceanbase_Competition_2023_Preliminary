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
    if (comp_ == NOT_IN_ENUM && right[i].is_null()) {
      // 特判not in null
      value = false;
      return RC::SUCCESS;
    }
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
    // 需要在这里处理一下子查询返回不是一个的情况，因为是一个value的比较
    if (left_->type() == ExprType::SELECT && left_values.size() != 1) {
      return RC::SUBQUERY_EXEC_FAILED;
    }
    if (right_->type() == ExprType::SELECT && right_values.size() != 1) {
      return RC::SUBQUERY_EXEC_FAILED;
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

RC ComparisonExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
{
  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuples, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("ComparisonExpr::get_value(const std::vector<Tuple *> &, Value &): failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuples, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("ComparisonExpr::get_value(const std::vector<Tuple *> &, Value &): failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  bool bool_value = false;
  rc              = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

// ConjunctionExpr 时代的眼泪，被LogiCalcExpr取代

// ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
//     : conjunction_type_(type), children_(std::move(children))
// {}

// RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const

// RC ConjunctionExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
// {
//   RC rc = RC::SUCCESS;
//   if (children_.empty()) {
//     value.set_boolean(true);
//     return rc;
//   }

//   Value tmp_value;
//   for (const unique_ptr<Expression> &expr : children_) {
//     rc = expr->get_value(tuples, tmp_value);
//     if (rc != RC::SUCCESS) {
//       LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
//       return rc;
//     }
//     bool bool_value = tmp_value.get_boolean();
//     if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
//       value.set_boolean(bool_value);
//       return rc;
//     }
//   }

//   bool default_value = (conjunction_type_ == Type::AND);
//   value.set_boolean(default_value);
//   return rc;
// }

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
  if (rc != RC::RECORD_EOF) {
    LOG_WARN("failed to exec select expr. rc=%s", strrc(rc));
    return rc;
  }
  rc = physical_operator->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close select expr operator. rc=%s", strrc(rc));
    return rc;
  }
  // 到这里他就执行完了
  rc = recover_stmt(select_stmt_, &tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to recover stmt. rc=%s", strrc(rc));
    return rc;
  }
  SelectExpr::tuples_.erase(tuple.to_string());
  return RC::SUCCESS;
}

RC SelectExpr::rewrite_stmt(FilterStmt *&rewrited_stmt, const Tuple *tuple)
{
  if (rewrited_stmt == nullptr) {
    return RC::SUCCESS;
  }
  const RowTuple *row_tuple = static_cast<const RowTuple *>(tuple);
  RC              rc        = RC::SUCCESS;
  if (rewrited_stmt->is_filter_unit()) {
    // 叶子节点
    // 重写filter_unit
    FilterUnit *filter_unit = rewrited_stmt->filter_unit();
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
      ValueExpr *value_expr     = new ValueExpr(tmp_value);
      recover_table[value_expr] = filter_unit->left().expr;
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
      ValueExpr *value_expr     = new ValueExpr(tmp_value);
      recover_table[value_expr] = filter_unit->right().expr;
      filter_unit->right().init_expr(value_expr);
    }
  } else {
    // 非叶子节点
    FilterStmt *left_stmt  = rewrited_stmt->left();
    FilterStmt *right_stmt = rewrited_stmt->right();
    if (left_stmt != nullptr) {
      rc = rewrite_stmt(left_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite left stmt. rc=%s", strrc(rc));
        return rc;
      }
    }
    if (right_stmt != nullptr) {
      rc = rewrite_stmt(right_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite right stmt. rc=%s", strrc(rc));
        return rc;
      }
    }
  }
  return rc;
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
  FilterStmt *filter_stmt = select_stmt->filter_stmt();
  // 递归重写filter_stmt
  rc = rewrite_stmt(filter_stmt, tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to rewrite filter stmt. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC SelectExpr::recover_stmt(FilterStmt *&rewrited_stmt, const Tuple *tuple)
{
  if (rewrited_stmt == nullptr) {
    return RC::SUCCESS;
  }
  const RowTuple *row_tuple = static_cast<const RowTuple *>(tuple);
  RC              rc        = RC::SUCCESS;
  if (rewrited_stmt->is_filter_unit()) {
    FilterUnit *filter_unit = rewrited_stmt->filter_unit();
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
        recover_table.find(filter_unit->left().expr) != recover_table.end() &&
        strcmp(dynamic_cast<FieldExpr *>(recover_table.at(filter_unit->left().expr))->table_name(),
            row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      auto *tmp_ptr = filter_unit->left().expr;
      filter_unit->left().init_expr(recover_table[filter_unit->left().expr]);
      recover_table.erase(tmp_ptr);
    }
    if (filter_unit->right().expr->type() == ExprType::VALUE &&
        recover_table.find(filter_unit->right().expr) != recover_table.end() &&
        strcmp(dynamic_cast<FieldExpr *>(recover_table.at(filter_unit->right().expr))->table_name(),
            row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      auto *tmp_ptr = filter_unit->right().expr;
      filter_unit->right().init_expr(recover_table[filter_unit->right().expr]);
      recover_table.erase(tmp_ptr);
    }
  } else {
    // 非叶子节点
    FilterStmt *left_stmt  = rewrited_stmt->left();
    FilterStmt *right_stmt = rewrited_stmt->right();
    if (left_stmt != nullptr) {
      rc = recover_stmt(left_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite left stmt. rc=%s", strrc(rc));
        return rc;
      }
    }
    if (right_stmt != nullptr) {
      rc = recover_stmt(right_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite right stmt. rc=%s", strrc(rc));
        return rc;
      }
    }
  }
  return rc;
}

RC SelectExpr::recover_stmt(Stmt *&rewrited_stmt, const Tuple *tuple)
{
  const RowTuple *row_tuple   = static_cast<const RowTuple *>(tuple);
  SelectStmt     *select_stmt = dynamic_cast<SelectStmt *>(rewrited_stmt);
  RC              rc          = RC::SUCCESS;
  // 只要还有filter_unit，就一直处理
  // 因为就是要把filter_unit里面的filter obj修改
  FilterStmt *filter_stmt = select_stmt->filter_stmt();
  // 递归重写filter_stmt
  rc = recover_stmt(filter_stmt, tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to recover filter stmt. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////

LogicalCalcExpr::LogicalCalcExpr(LogiOp logi, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : logi_(logi), left_(std::move(left)), right_(std::move(right))
{}

RC LogicalCalcExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  RC rc = RC::SUCCESS;
  switch (logi_) {
    case AND_ENUM: {
      Value left_value, right_value;
      rc = left_->get_value(tuple, left_value, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
        return rc;
      }
      rc = right_->get_value(tuple, right_value, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      value.set_boolean(left_value.get_boolean() && right_value.get_boolean());
    } break;
    case OR_ENUM: {
      Value left_value, right_value;
      rc = left_->get_value(tuple, left_value, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
        return rc;
      }
      rc = right_->get_value(tuple, right_value, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      value.set_boolean(left_value.get_boolean() || right_value.get_boolean());
    } break;
    case NOT_ENUM: {
      if (left_) {
        // invalid not
        LOG_WARN("invalid not");
        return RC::INTERNAL;
      }
      Value right_value;
      rc = right_->get_value(tuple, right_value, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      value.set_boolean(!right_value.get_boolean());
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported logical type. %d", logi_);
    } break;
  }
  return RC::SUCCESS;
}

RC LogicalCalcExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
{
  RC rc = RC::SUCCESS;
  switch (logi_) {
    case AND_ENUM: {
      Value left_value, right_value;
      rc = left_->get_value(tuples, left_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
        return rc;
      }
      rc = right_->get_value(tuples, right_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      value.set_boolean(left_value.get_boolean() && right_value.get_boolean());
    } break;
    case OR_ENUM: {
      Value left_value, right_value;
      rc = left_->get_value(tuples, left_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
        return rc;
      }
      rc = right_->get_value(tuples, right_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      value.set_boolean(left_value.get_boolean() || right_value.get_boolean());
    } break;
    case NOT_ENUM: {
      if (left_) {
        // invalid not
        LOG_WARN("invalid not");
        return RC::INTERNAL;
      }
      Value right_value;
      rc = right_->get_value(tuples, right_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      value.set_boolean(!right_value.get_boolean());
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported logical type. %d", logi_);
    } break;
  }
  return RC::SUCCESS;
}

Expression *LogicalCalcExpr::clone() const
{
  return new LogicalCalcExpr(logi_, unique_ptr<Expression>(left_->clone()), unique_ptr<Expression>(right_->clone()));
}

////////////////////////////////////////////////////////////////////////////////

FunctionExpr::FunctionExpr(FuncType func_type, std::vector<std::unique_ptr<Expression>> &expr_list)
    : func_type_(func_type)
{
  for (auto &expr : expr_list) {
    expr_list_.push_back(std::move(expr));
  }
}

RC FunctionExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  if (func_type_ != FuncType::MAX && func_type_ != FuncType::MIN) {
    return RC::UNIMPLENMENT;
  }
  RC                 rc = RC::SUCCESS;
  std::vector<Value> expr_values;
  for (int i = 0; i < expr_list_.size(); i++) {
    Value expr_value;
    expr_value.set_type(AttrType::NONE);
    if (expr_list_[i]->type() == ExprType::SELECT) {
      std::vector<Value> tmp_values;
      rc = dynamic_cast<SelectExpr *>(expr_list_[i].get())->get_value(tuple, tmp_values, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of expression. rc=%s", strrc(rc));
        return rc;
      }
      if (tmp_values.size() > 1) {
        LOG_WARN("invalid select result, too much result");
        return RC::INTERNAL;
      }
      if (!tmp_values.empty()) {
        expr_value = tmp_values[0];
      }
    } else {
      rc = expr_list_[i]->get_value(tuple, expr_value, trx);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of expression. rc=%s", strrc(rc));
        return rc;
      }
    }
    if (expr_value.attr_type() != AttrType::NONE) {
      expr_values.push_back(expr_value);
    }
  }
  // 检查是否为空
  if (expr_values.empty()) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  switch (func_type_) {
    case FuncType::MAX: {
      value = expr_values[0];
      for (auto val : expr_values) {
        if (value.compare(val) > 0) {
          value = val;
        }
      }
    } break;
    case FuncType::MIN: {
      value = expr_values[0];
      for (auto val : expr_values) {
        if (value.compare(val) < 0) {
          value = val;
        }
      }
    } break;
  }
  return RC::SUCCESS;
}

Expression *FunctionExpr::clone() const
{
  std::vector<std::unique_ptr<Expression>> expr_list;
  for (auto &expr : expr_list_) {
    expr_list.push_back(std::unique_ptr<Expression>(expr->clone()));
  }
  return new FunctionExpr(func_type_, expr_list);
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

    case Type::PAREN: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int());
      } else {
        value.set_float(left_value.get_float());
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

RC ArithmeticExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuples, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuples, right_value);
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
////////////////////////////////////////////////////////////////////////////////

RC AggregationExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
{
  if (tuples.empty()) {
    LOG_WARN("get value of tuples empty");
    return RC::INTERNAL;
  }

  Tuple        *tpl      = tuples.front();
  ProjectTuple *tpl_cast = dynamic_cast<ProjectTuple *>(tpl);

  if (field_.table() != nullptr && field_.meta() == nullptr) {  // 特殊判断count(*)
    do_count_aggregate(tuples, value, -1);
    return RC::SUCCESS;
  }

  int                                 idx    = 0;
  const std::vector<TupleCellSpec *> &speces = tpl_cast->get_speces();
  for (idx = 0; idx < speces.size(); idx++) {
    LOG_DEBUG("========== field_.table_name() = %s ==========",field_.table_name());
    LOG_DEBUG("========== fields_[idx].table_name() = %s ==========",speces[idx]->table_name());
    LOG_DEBUG("========== field_.field_name() = %s ==========",field_.field_name());
    LOG_DEBUG("========== fields_[idx].field_name() = %s ==========",speces[idx]->field_name());
    if (strcmp(field_.table_name(), speces[idx]->table_name()) == 0 &&
        strcmp(field_.field_name(), speces[idx]->field_name()) == 0) {
      // potential bug: field_.field_name()不一定是直接返回field_meta.field_name()
      // 基本确认目前不是bug，field_meta()如果为nullptr只是为了标记tuple schema的 '*'
      break;
    }
  }

  LOG_DEBUG("========== idx = %d ==========", idx);

  RC rc = RC::SUCCESS;
  if (aggregation_func_ == "MAX") {
    rc = do_max_aggregate(tuples, value, idx);
  } else if (aggregation_func_ == "MIN") {
    rc = do_min_aggregate(tuples, value, idx);
  } else if (aggregation_func_ == "COUNT") {
    rc = do_count_aggregate(tuples, value, idx);
  } else if (aggregation_func_ == "AVG") {
    rc = do_avg_aggregate(tuples, value, idx);
  } else if (aggregation_func_ == "SUM") {
    rc = do_sum_aggregate(tuples, value, idx);
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

Expression *AggregationExpr::clone() const { return new AggregationExpr(field_, aggregation_func_); }

RC AggregationExpr::do_max_aggregate(const std::vector<Tuple *> &tuples, Value &value, int idx) const
{
  LOG_DEBUG("========== In AggregationExpr::do_max_aggregate(const std::vector<Tuple*> &tuples, Value &value) ==========");

  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(field_.attr_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples) {
    Value cur_value;
    RC    rc = t->cell_at(idx, cur_value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  tuples[0]->cell_at(idx, value);

  for (auto t : tuples) {
    Value cur_value;
    t->cell_at(idx, cur_value);
    if (cur_value.compare(value) > 0) {
      value = cur_value;
    }
  }
}

RC AggregationExpr::do_min_aggregate(const std::vector<Tuple *> &tuples, Value &value, int idx) const
{
  LOG_DEBUG("========== In AggregationExpr::do_min_aggregate(const std::vector<Tuple*> &tuples, Value &value) ==========");

  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(field_.attr_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples) {
    Value cur_value;
    RC    rc = t->cell_at(idx, cur_value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  tuples[0]->cell_at(idx, value);

  for (auto t : tuples) {
    Value cur_value;
    t->cell_at(idx, cur_value);
    if (cur_value.compare(value) < 0) {
      value = cur_value;
    }
  }
}

RC AggregationExpr::do_count_aggregate(const std::vector<Tuple *> &tuples, Value &value, int idx) const
{
  LOG_DEBUG("========== In AggregationExpr::do_count_aggregate(const std::vector<Tuple*> &tuples, Value &value) ==========");

  int count = 0;

  if (idx == -1) {  // count(*)
    LOG_DEBUG("========== do_count(*) ==========");
    count = tuples.size();
  } else {
    // 检查是否为空
    if (!tuples.empty()) {
      for (auto t : tuples) {
        Value cur_value;
        t->cell_at(idx, cur_value);
        if (!cur_value.is_null()) {
          count++;
        }
      }
    }
  }

  value.set_int(count);
  return RC::SUCCESS;
}

RC AggregationExpr::do_avg_aggregate(const std::vector<Tuple *> &tuples, Value &value, int idx) const
{
  LOG_DEBUG("========== In AggregationExpr::do_avg_aggregate(const std::vector<Tuple*> &tuples, Value &value) ==========");

  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(field_.attr_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples) {
    Value cur_value;
    t->cell_at(idx, cur_value);
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  int   cnt = 0;
  Value attr_value;
  tuples[0]->cell_at(idx, attr_value);
  AttrType attr_type = attr_value.attr_type();
  if (attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->cell_at(idx, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_int();
        cnt++;
      }
    }
    if (sum % cnt == 0) {
      value.set_int(sum / cnt);
    } else {
      value.set_float(static_cast<float>(sum) / cnt);
    }
  } else if (attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->cell_at(idx, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_float();
        cnt++;
      }
    }
    value.set_float(sum / cnt);
  } else if (attr_type == CHARS) {
    for (auto t : tuples) {
      float sum = 0;
      Value cur_value;
      t->cell_at(idx, cur_value);
      if (!cur_value.is_null()) {
        cur_value.str_to_number();
        if (cur_value.attr_type() == INTS) {
          sum += cur_value.get_int();
          cnt++;
        } else {
          sum += cur_value.get_float();
          cnt++;
        }
      }
      value.set_float(sum / cnt);
    }
  } else {  // 其余类型无法求和
    return RC::INVALID_ARGUMENT;
  }

  return RC::SUCCESS;
}

RC AggregationExpr::do_sum_aggregate(const std::vector<Tuple *> &tuples, Value &value, int idx) const
{
  LOG_DEBUG("========== In AggregationExpr::do_sum_aggregate(const std::vector<Tuple*> &tuples, Value &value) ==========");

  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(field_.attr_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool all_null = true;
  for (auto t : tuples) {
    Value cur_value;
    t->cell_at(idx, cur_value);
    if (!cur_value.is_null()) {
      all_null = false;
      break;
    }
  }
  if (all_null) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  Value attr_value;
  tuples[0]->cell_at(idx, attr_value);
  AttrType attr_type = attr_value.attr_type();
  if (attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->cell_at(idx, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_int();
      }
    }
    value.set_int(sum);
  } else if (attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->cell_at(idx, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_float();
      }
    }
    value.set_float(sum);
  } else if (attr_type == CHARS) {
    for (auto t : tuples) {
      float sum = 0;
      Value cur_value;
      t->cell_at(idx, cur_value);
      if (!cur_value.is_null()) {
        cur_value.str_to_number();
        if (cur_value.attr_type() == INTS) {
          sum += cur_value.get_int();
        } else {
          sum += cur_value.get_float();
        }
      }
      value.set_float(sum);
    }
  } else {  // 其余类型无法求和
    return RC::INVALID_ARGUMENT;
  }

  return RC::SUCCESS;
}
