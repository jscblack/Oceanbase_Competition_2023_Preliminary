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
#include "sql/expr/tuple_cell.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"
#include <cmath>
#include <ctime>
#include <ctype.h>
#include <regex>
using namespace std;

RC FieldExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name(), alias_.c_str()), value);
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

  RC  rc = RC::SUCCESS;
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
    ValueExpr   *left_value_expr  = dynamic_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = dynamic_cast<ValueExpr *>(right_.get());
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
    if (left_->type() == ExprType::SELECT && left_values.size() > 1) {
      return RC::SUBQUERY_EXEC_FAILED;
    }
    if (right_->type() == ExprType::SELECT && right_values.size() > 1) {
      return RC::SUBQUERY_EXEC_FAILED;
    }
    rc = compare_value(left_value, right_value, bool_value);
  }
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
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

Expression *ComparisonExpr::clone() const
{
  return new ComparisonExpr(comp_, unique_ptr<Expression>(left_->clone()), unique_ptr<Expression>(right_->clone()));
}

////////////////////////////////////////////////////////////////////////////////

SelectExpr::SelectExpr(Stmt *stmt) : select_stmt_(stmt) {}

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

RC SelectExpr::get_value(std::vector<Tuple *> &tuples, Trx *trx)
{
  RC rc = RC::SUCCESS;
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
    Tuple *tuple = nullptr;
    rc           = physical_operator->current_tuple()->clone(tuple);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return RC::INTERNAL;
    }
    tuples.push_back(tuple);
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
  return rc;
}

RC SelectExpr::rewrite_expr(Expression *&original_expr, const Tuple *tuple)
{
  if (original_expr == nullptr) {
    return RC::SUCCESS;
  }
  const RowTuple *row_tuple = dynamic_cast<const RowTuple *>(tuple);
  RC              rc        = RC::SUCCESS;
  if (original_expr->type() == ExprType::COMPARISON) {
    // comp节点
    // 左右两边为比较对象
    ComparisonExpr *comparison_expr = dynamic_cast<ComparisonExpr *>(original_expr);
    Expression     *left_expr       = comparison_expr->left().get();
    Expression     *right_expr      = comparison_expr->right().get();

    if (left_expr != nullptr && left_expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归重写
      Stmt *original_sub_stmt = dynamic_cast<SelectExpr *>(left_expr)->select_stmt_;
      rc                      = rewrite_stmt(original_sub_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(left_expr)->select_stmt_ = original_sub_stmt;
    }
    if (right_expr != nullptr && right_expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归重写
      Stmt *original_sub_stmt = dynamic_cast<SelectExpr *>(right_expr)->select_stmt_;
      rc                      = rewrite_stmt(original_sub_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(right_expr)->select_stmt_ = original_sub_stmt;
    }
    if (left_expr != nullptr && left_expr->type() == ExprType::FIELD &&
        strcmp(dynamic_cast<FieldExpr *>(left_expr)->table_name(), row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      Value tmp_value;
      rc = row_tuple->find_cell(TupleCellSpec(dynamic_cast<FieldExpr *>(left_expr)->table_name(),
                                    dynamic_cast<FieldExpr *>(left_expr)->field_name()),
          tmp_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to find cell. rc=%s", strrc(rc));
        return rc;
      }
      ValueExpr *value_expr     = new ValueExpr(tmp_value);
      recover_table[value_expr] = left_expr;
      // left_expr                 = value_expr;
      comparison_expr->left().release();
      comparison_expr->left().reset(value_expr);
    }
    if (right_expr != nullptr && right_expr->type() == ExprType::FIELD &&
        strcmp(dynamic_cast<FieldExpr *>(right_expr)->table_name(), row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      Value tmp_value;
      rc = row_tuple->find_cell(TupleCellSpec(dynamic_cast<FieldExpr *>(right_expr)->table_name(),
                                    dynamic_cast<FieldExpr *>(right_expr)->field_name()),
          tmp_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to find cell. rc=%s", strrc(rc));
        return rc;
      }
      ValueExpr *value_expr     = new ValueExpr(tmp_value);
      recover_table[value_expr] = right_expr;
      // right_expr                = value_expr;
      comparison_expr->right().release();
      comparison_expr->right().reset(value_expr);
    }
  } else if (original_expr->type() == ExprType::LOGICALCALC) {
    // 非叶子节点
    // 逻辑比较节点，左右两边为COMPARISON
    LogicalCalcExpr *logical_calc_expr = dynamic_cast<LogicalCalcExpr *>(original_expr);

    Expression *left_expr  = logical_calc_expr->left().get();
    Expression *right_expr = logical_calc_expr->right().get();
    if (left_expr != nullptr) {
      rc = rewrite_expr(left_expr, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite left expr. rc=%s", strrc(rc));
        return rc;
      }
    }
    if (right_expr != nullptr) {
      rc = rewrite_expr(right_expr, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite right expr. rc=%s", strrc(rc));
        return rc;
      }
    }
  } else {
    LOG_ERROR("unsupported expr type %d", original_expr->type());
    return RC::INTERNAL;
  }
  return rc;
}

RC SelectExpr::rewrite_stmt(Stmt *&rewrited_stmt, const Tuple *tuple)
{
  // 基于select_stmt_和tuples_，重写select_stmt_，得到rewrited_stmt
  // 需要注意的是，在这里把stmt里面的filter_stmt 与外部已知match的部分给他替换了
  // todo 重写，现在先做无依赖的
  const RowTuple *row_tuple   = dynamic_cast<const RowTuple *>(tuple);
  SelectStmt     *select_stmt = dynamic_cast<SelectStmt *>(rewrited_stmt);
  RC              rc          = RC::SUCCESS;
  // 只要还有filter_unit，就一直处理
  // 因为就是要把filter_unit里面的filter obj修改
  if (select_stmt->filter_stmt() == nullptr) {
    return RC::SUCCESS;
  }
  Expression *filter_expr = select_stmt->filter_stmt()->filter_expr();
  // 递归重写filter_stmt
  rc = rewrite_expr(filter_expr, tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to rewrite filter stmt. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC SelectExpr::recover_expr(Expression *&rewrited_expr, const Tuple *tuple)
{
  if (rewrited_expr == nullptr) {
    return RC::SUCCESS;
  }
  const RowTuple *row_tuple = dynamic_cast<const RowTuple *>(tuple);
  RC              rc        = RC::SUCCESS;
  if (rewrited_expr->type() == ExprType::COMPARISON) {
    // comp节点
    // 左右两边为比较对象
    ComparisonExpr *comparison_expr = dynamic_cast<ComparisonExpr *>(rewrited_expr);
    Expression     *left_expr       = comparison_expr->left().get();
    Expression     *right_expr      = comparison_expr->right().get();

    if (left_expr != nullptr && left_expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归还原
      Stmt *rewrited_sub_stmt = dynamic_cast<SelectExpr *>(left_expr)->select_stmt_;
      rc                      = recover_stmt(rewrited_sub_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(left_expr)->select_stmt_ = rewrited_sub_stmt;
    }
    if (right_expr != nullptr && right_expr->type() == ExprType::SELECT) {
      // 是一个select_stmt，那就递归还原
      Stmt *rewrited_sub_stmt = dynamic_cast<SelectExpr *>(right_expr)->select_stmt_;
      rc                      = recover_stmt(rewrited_sub_stmt, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to rewrite sub stmt. rc=%s", strrc(rc));
        return rc;
      }
      dynamic_cast<SelectExpr *>(right_expr)->select_stmt_ = rewrited_sub_stmt;
    }

    if (left_expr->type() == ExprType::VALUE && recover_table.find(left_expr) != recover_table.end() &&
        strcmp(dynamic_cast<FieldExpr *>(recover_table.at(left_expr))->table_name(), row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      Expression *tmp_ptr = left_expr;
      left_expr           = recover_table[left_expr];
      comparison_expr->left().release();
      comparison_expr->left().reset(left_expr);
      recover_table.erase(tmp_ptr);
    }
    if (right_expr->type() == ExprType::VALUE && recover_table.find(right_expr) != recover_table.end() &&
        strcmp(dynamic_cast<FieldExpr *>(recover_table.at(right_expr))->table_name(), row_tuple->table().name()) == 0) {
      // 这是需要被替换的东西
      // 从tuples_里面找到这个tuple
      Expression *tmp_ptr = right_expr;
      right_expr          = recover_table[right_expr];
      comparison_expr->right().release();
      comparison_expr->right().reset(right_expr);
      recover_table.erase(tmp_ptr);
    }
  } else if (rewrited_expr->type() == ExprType::LOGICALCALC) {
    // 非叶子节点
    // 逻辑比较节点，左右两边为COMPARISON
    LogicalCalcExpr *logical_calc_expr = dynamic_cast<LogicalCalcExpr *>(rewrited_expr);

    Expression *left_expr  = logical_calc_expr->left().get();
    Expression *right_expr = logical_calc_expr->right().get();
    if (left_expr != nullptr) {
      rc = recover_expr(left_expr, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to recover left expr. rc=%s", strrc(rc));
        return rc;
      }
    }
    if (right_expr != nullptr) {
      rc = recover_expr(right_expr, tuple);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to recover right expr. rc=%s", strrc(rc));
        return rc;
      }
    }
  } else {
    LOG_ERROR("unsupported expr type %d", rewrited_expr->type());
    return RC::INTERNAL;
  }
  return rc;
}

RC SelectExpr::recover_stmt(Stmt *&rewrited_stmt, const Tuple *tuple)
{
  const RowTuple *row_tuple   = dynamic_cast<const RowTuple *>(tuple);
  SelectStmt     *select_stmt = dynamic_cast<SelectStmt *>(rewrited_stmt);
  RC              rc          = RC::SUCCESS;
  // 只要还有filter_unit，就一直处理
  // 因为就是要把filter_unit里面的filter obj修改
  if (select_stmt->filter_stmt() == nullptr) {
    return RC::SUCCESS;
  }
  Expression *filter_expr = select_stmt->filter_stmt()->filter_expr();

  // 递归重写filter_stmt
  rc = recover_expr(filter_expr, tuple);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to recover filter stmt. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

AttrType SelectExpr::value_type() const
{
  // 在select真正执行之前，是无法知道select的结果集的类型的
  // return AttrType::UNDEFINED;

  // TODO: 特判一下 select *
  return (reinterpret_cast<SelectStmt *>(select_stmt_)->query_fields_expressions())[0]->value_type();
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

FunctionExpr::FunctionExpr(FuncName func_type, std::vector<std::unique_ptr<Expression>> &expr_list)
    : func_type_(func_type)
{
  for (auto &expr : expr_list) {
    expr_list_.push_back(std::move(expr));
  }
}

AttrType FunctionExpr::value_type() const
{
  switch (func_type_) {
    case FuncName::LENGTH_FUNC_NUM: {
      return AttrType::INTS;
    } break;
    case FuncName::ROUND_FUNC_NUM: {
      return AttrType::FLOATS;  // FIXME 如果是整数调用ROUND呢？
    } break;
    case FuncName::DATE_FUNC_NUM: {
      return AttrType::CHARS;
    } break;
    case FuncName::MIN_FUNC_ENUM:
    case FuncName::MAX_FUNC_ENUM: {
      if (expr_list_.size() > 0) {
        Expression *expr = expr_list_[0].get();
        return expr->value_type();
      } else {
        return AttrType::UNDEFINED;
      }
    }
    default: {
      return AttrType::UNDEFINED;
    } break;
  }
}

RC FunctionExpr::get_value(const Tuple &tuple, Value &value, Trx *trx) const
{
  RC rc = RC::SUCCESS;
  if (func_type_ == FuncName::LENGTH_FUNC_NUM) {
    if (expr_list_.size() != 1) {
      return RC::FUNC_EXPR_ERROR;
    }

    Expression *expr = expr_list_[0].get();
    if (expr->value_type() != AttrType::CHARS) {
      return RC::FUNC_EXPR_ERROR;
    }

    Value expr_value;
    rc = expr->get_value(tuple, expr_value, trx);
    if (OB_FAIL(rc)) {
      return rc;
    }

    value.set_type(AttrType::INTS);
    value.set_int(expr_value.get_string().size());
    return rc;
  }

  if (func_type_ == FuncName::ROUND_FUNC_NUM) {
    if (!(expr_list_.size() == 2 || expr_list_.size() == 1)) {
      return RC::FUNC_EXPR_ERROR;
    }
    // ASSERT(expr_list_.size() == 2 || expr_list_.size() == 1, "Function(Round) must have exact two arguement");
    Expression *float_expr = expr_list_[0].get();
    if (float_expr->value_type() != AttrType::FLOATS) {
      return RC::FUNC_EXPR_ERROR;
    }

    Value float_number;
    rc = float_expr->get_value(tuple, float_number, trx);
    if (OB_FAIL(rc)) {
      return rc;
    }

    int round_digit = 0;  // 舍入位数
    if (expr_list_.size() == 2) {
      Value round_number;
      if (expr_list_[1]->value_type() != AttrType::INTS) {
        return RC::FUNC_EXPR_ERROR;
      }
      rc          = expr_list_[1]->get_value(tuple, round_number, trx);
      round_digit = round_number.get_int();
      if (round_digit < 0) {
        return RC::INTERNAL;
      }
    }

    // FIXME 不太清楚round(x,0)的情况是否要视作整数
    value.set_type(AttrType::FLOATS);
    value.set_float(std::roundf(float_number.get_float() * std::pow(static_cast<float>(10), round_digit)) /
                    std::pow(static_cast<float>(10), round_digit));
    return rc;
  }

  if (func_type_ == FuncName::DATE_FUNC_NUM) {
    if (expr_list_.size() != 2) {
      return RC::FUNC_EXPR_ERROR;
    }
    Expression *date_expr = expr_list_[0].get();

    Value date_str;
    rc = date_expr->get_value(tuple, date_str, trx);
    if (OB_FAIL(rc)) {
      return rc;
    }

    rc = date_str.auto_cast(AttrType::DATES);
    if (OB_FAIL(rc)) {
      if (rc == RC::VALUE_DATE_INVALID) {
        return RC::FUNC_EXPR_ERROR;
      }
      return rc;
    }

    Expression *format_expr = expr_list_[1].get();
    Value       format_str;
    if (format_expr->value_type() != AttrType::CHARS) {
      return RC::FUNC_EXPR_ERROR;
    }
    rc = format_expr->get_value(tuple, format_str, trx);
    if (OB_FAIL(rc)) {
      return rc;
    }

    auto strf_mysql_time = [](const char *date_str, const char *fmt_str) -> std::string {
      std::string formatted_date;
      char        year[5], month[3], day[3];

      // 解析日期字符串
      if (sscanf(date_str, "%4[0-9]-%2[0-9]-%2[0-9]", year, month, day) == 3) {
        // 将解析后的年、月、日部分转换为整数
        int year_int  = atoi(year);
        int month_int = atoi(month);
        int day_int   = atoi(day);

        // 解析自定义的格式字符串
        for (const char *ptr = fmt_str; *ptr; ++ptr) {
          if (*ptr == '%') {
            if (*(ptr + 1) == 'D') {
              if (day_int >= 11 && day_int <= 13) {
                formatted_date += std::to_string(day_int) + "th";
              } else {
                switch (day_int % 10) {
                  case 1: formatted_date += std::to_string(day_int) + "st"; break;
                  case 2: formatted_date += std::to_string(day_int) + "nd"; break;
                  case 3: formatted_date += std::to_string(day_int) + "rd"; break;
                  default: formatted_date += std::to_string(day_int) + "th";
                }
              }
            } else if (*(ptr + 1) == 'd') {
              // 2 digits day, add leading zero if needed
              std::string tmp_str = std::to_string(day_int);
              while (tmp_str.length() < 2) {
                tmp_str = "0" + tmp_str;
              }
              formatted_date += tmp_str;
            } else if (*(ptr + 1) == 'e') {
              // no need to add leading zero
              formatted_date += std::to_string(day_int);
            } else if (*(ptr + 1) == 'Y') {
              // 4 digits year, add leading zero if needed
              std::string tmp_str = std::to_string(year_int);
              while (tmp_str.length() < 4) {
                tmp_str = "0" + tmp_str;
              }
              formatted_date += tmp_str;
            } else if (*(ptr + 1) == 'y') {
              std::string tmp_str = std::to_string(year_int % 100);
              while (tmp_str.length() < 2) {
                tmp_str = "0" + tmp_str;
              }
              formatted_date += tmp_str;
            } else if (*(ptr + 1) == 'M') {
              static const char *months[] = {"January",
                  "February",
                  "March",
                  "April",
                  "May",
                  "June",
                  "July",
                  "August",
                  "September",
                  "October",
                  "November",
                  "December"};
              formatted_date += months[month_int - 1];
            } else if (*(ptr + 1) == 'c') {
              // no need to add leading zero
              std::string tmp_str = std::to_string(month_int);
              while (tmp_str.length() < 2) {
                tmp_str = "0" + tmp_str;
              }
              formatted_date += tmp_str;
            } else if (*(ptr + 1) == 'm') {
              // 2 digits month, add leading zero if needed
              std::string tmp_str = std::to_string(month_int);
              while (tmp_str.length() < 2) {
                tmp_str = "0" + tmp_str;
              }
              formatted_date += tmp_str;
            } else if (*(ptr + 1) == 'j') {
              // Day of year (001..366)
              int day_of_year = 0;
              for (int i = 1; i < month_int; ++i) {
                switch (i) {
                  case 1:
                  case 3:
                  case 5:
                  case 7:
                  case 8:
                  case 10:
                  case 12: day_of_year += 31; break;
                  case 4:
                  case 6:
                  case 9:
                  case 11: day_of_year += 30; break;
                  case 2: day_of_year += 28; break;
                }
              }
              day_of_year += day_int;
              std::string tmp_str = std::to_string(day_of_year);
              while (tmp_str.length() < 3) {
                tmp_str = "0" + tmp_str;
              }
              formatted_date += tmp_str;

            } else if (isalpha(*(ptr + 1))) {
              // %x append the value of x
              formatted_date += *(ptr + 1);
            } else if (*(ptr + 1) == '%') {
              // %
              formatted_date += "%";
            } else {
              // 未知格式标识符
              std::cerr << "Invalid format string: " << *ptr << *(ptr + 1) << std::endl;
              return "";
            }
            ptr++;  // 跳过格式标识符
          } else {
            formatted_date += *ptr;
          }
        }
      } else {
        // 输入日期字符串格式错误
        std::cerr << "Invalid date format: " << date_str << std::endl;
      }
      return formatted_date;
    };

    std::string formated_str = strf_mysql_time(date_str.get_string().c_str(), format_str.get_string().c_str());
    // int         year, month, day;
    // sscanf(date_str.get_string().c_str(), "%d-%d-%d", &year, &month, &day);
    // struct tm date = {.tm_mday = day, .tm_mon = month - 1, .tm_year = year - 1900};

    // char *tmp = (char *)malloc(512);  // 随便一个size
    // strftime(tmp, 512, format_str.get_string().c_str(), &date);

    value.set_type(AttrType::CHARS);
    value.set_string(formated_str.c_str());
    return rc;
  }

  // 下面是MAX和MIN此前的旧代码
  if (func_type_ == FuncName::MAX_FUNC_ENUM || func_type_ == FuncName::MIN_FUNC_ENUM) {
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
      case FuncName::MAX_FUNC_ENUM: {
        value = expr_values[0];
        for (auto val : expr_values) {
          if (value.compare(val) > 0) {
            value = val;
          }
        }
      } break;
      case FuncName::MIN_FUNC_ENUM: {
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
  return RC::UNIMPLENMENT;
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

ArithmeticExpr::ArithmeticExpr(ArithOp type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}

ArithmeticExpr::ArithmeticExpr(ArithOp type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != ArithOp::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();

  if (left_value.attr_type() == NONE || right_value.attr_type() == NONE) {
    value.set_type(NONE);
    return rc;
  }

  switch (arithmetic_type_) {
    case ArithOp::ADD: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() + right_value.get_int());
      } else {
        value.set_float(left_value.get_float() + right_value.get_float());
      }
    } break;

    case ArithOp::SUB: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() - right_value.get_int());
      } else {
        value.set_float(left_value.get_float() - right_value.get_float());
      }
    } break;

    case ArithOp::MUL: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() * right_value.get_int());
      } else {
        value.set_float(left_value.get_float() * right_value.get_float());
      }
    } break;

    case ArithOp::DIV: {
      // divied by zero
      if (target_type == AttrType::INTS) {
        if (right_value.get_int() == 0) {
          value.set_type(NONE);
        } else {
          value.set_int(left_value.get_int() / right_value.get_int());
        }
      } else {
        if (right_value.get_float() > -EPSILON && right_value.get_float() < EPSILON) {
          value.set_type(NONE);
        } else {
          value.set_float(left_value.get_float() / right_value.get_float());
        }
      }
    } break;

    case ArithOp::NEGATIVE: {
      if (target_type == AttrType::INTS) {
        value.set_int(-left_value.get_int());
      } else {
        value.set_float(-left_value.get_float());
      }
    } break;

    case ArithOp::POSITIVE: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int());
      } else {
        value.set_float(left_value.get_float());
      }
    } break;

    case ArithOp::PAREN: {
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
  if (arithmetic_type_ != POSITIVE && arithmetic_type_ != NEGATIVE && arithmetic_type_ != PAREN) {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
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
  if (arithmetic_type_ != POSITIVE && arithmetic_type_ != NEGATIVE && arithmetic_type_ != PAREN) {
    rc = right_->get_value(tuples, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
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
  if (arithmetic_type_ == POSITIVE || arithmetic_type_ == NEGATIVE || arithmetic_type_ == PAREN) {
    return new ArithmeticExpr(arithmetic_type_, unique_ptr<Expression>(left_->clone()), unique_ptr<Expression>());
  }

  return new ArithmeticExpr(
      arithmetic_type_, unique_ptr<Expression>(left_->clone()), unique_ptr<Expression>(right_->clone()));
}

////////////////////////////////////////////////////////////////////////////////

AggregationExpr::AggregationExpr(FuncName agg_type, Expression *child) : agg_type_(agg_type), child_(child) {}

AggregationExpr::AggregationExpr(FuncName agg_type, std::unique_ptr<Expression> child)
    : agg_type_(agg_type), child_(std::move(child))
{}

Expression *AggregationExpr::clone() const
{
  return new AggregationExpr(agg_type_, unique_ptr<Expression>(child_->clone()));
}

RC AggregationExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
{
  if (tuples.empty()) {
    if (agg_type_ == FuncName::COUNT_FUNC_ENUM) {
      value.set_int(0);
    } else {
      value.set_type(AttrType::NONE);
    }
    return RC::SUCCESS;
  }

  // 强转ProjectTuple，目的是为了拿到里面的expressions_，找到对哪一列做聚合
  Tuple        *tpl      = tuples.front();
  ProjectTuple *tpl_cast = dynamic_cast<ProjectTuple *>(tpl);

  // 强转field expression，判断是否是count(*)
  FieldExpr *child_cast = dynamic_cast<FieldExpr *>(child_.get());
  if (child_cast->field().meta() == nullptr) {
    // 这里合并处理了table()是否为空的情况，即目前没有区分*.*  *  t.*
    TupleCellSpec tcs(nullptr, nullptr);
    do_count_aggregate(tuples, value, tcs);
    return RC::SUCCESS;
  }

  TupleCellSpec tcs(child_cast->table_name(), child_cast->field_name());

  RC rc = RC::SUCCESS;
  switch (agg_type_) {
    case FuncName::MAX_FUNC_ENUM: rc = do_max_aggregate(tuples, value, tcs); break;
    case FuncName::MIN_FUNC_ENUM: rc = do_min_aggregate(tuples, value, tcs); break;
    case FuncName::COUNT_FUNC_ENUM: rc = do_count_aggregate(tuples, value, tcs); break;
    case FuncName::AVG_FUNC_ENUM: rc = do_avg_aggregate(tuples, value, tcs); break;
    case FuncName::SUM_FUNC_ENUM: rc = do_sum_aggregate(tuples, value, tcs); break;
    default: rc = RC::INVALID_ARGUMENT; break;
  }
  return rc;
}

RC AggregationExpr::do_max_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const
{
  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(child_->value_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool all_null = true;

  for (auto t : tuples) {
    Value cur_value;
    RC    rc = t->find_cell(tcs, cur_value);
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

  tuples[0]->find_cell(tcs, value);

  for (auto t : tuples) {
    Value cur_value;
    t->find_cell(tcs, cur_value);
    if (cur_value.compare(value) > 0) {
      value = cur_value;
    }
  }
  return RC::SUCCESS;
}

RC AggregationExpr::do_min_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const
{
  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(child_->value_type());
    return RC::SUCCESS;
  }

  bool all_null = true;
  for (auto t : tuples) {
    Value cur_value;
    RC    rc = t->find_cell(tcs, cur_value);
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

  tuples[0]->find_cell(tcs, value);

  for (auto t : tuples) {
    Value cur_value;
    t->find_cell(tcs, cur_value);
    if (cur_value.compare(value) < 0) {
      value = cur_value;
    }
  }
  return RC::SUCCESS;
}

RC AggregationExpr::do_count_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const
{
  int count = 0;

  if (tcs.table_name() == nullptr && tcs.field_name() == nullptr) {
    count = tuples.size();
  } else {
    // 检查是否为空
    if (!tuples.empty()) {
      for (auto t : tuples) {
        Value cur_value;
        t->find_cell(tcs, cur_value);
        if (!cur_value.is_null()) {
          count++;
        }
      }
    }
  }

  value.set_int(count);
  return RC::SUCCESS;
}

RC AggregationExpr::do_avg_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const
{
  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(child_->value_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool     all_null = true;
  AttrType attr_type;
  for (auto t : tuples) {
    Value cur_value;
    t->find_cell(tcs, cur_value);
    if (!cur_value.is_null()) {
      attr_type = cur_value.attr_type();
      all_null  = false;
      break;
    }
  }
  if (all_null) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  int   cnt = 0;
  Value attr_value;

  if (attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->find_cell(tcs, cur_value);
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
      t->find_cell(tcs, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_float();
        cnt++;
      }
    }
    value.set_float(sum / cnt);
  } else if (attr_type == CHARS) {
    float sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->find_cell(tcs, cur_value);
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
    }
    value.set_float(sum / cnt);
  } else {  // 其余类型无法求和
    return RC::INVALID_ARGUMENT;
  }

  return RC::SUCCESS;
}

RC AggregationExpr::do_sum_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const
{
  // 检查是否为空
  if (tuples.empty()) {
    value.set_type(child_->value_type());
    return RC::SUCCESS;
  }

  // 检查是否均为null
  bool     all_null = true;
  AttrType attr_type;
  for (auto t : tuples) {
    Value cur_value;
    t->find_cell(tcs, cur_value);
    if (!cur_value.is_null()) {
      all_null  = false;
      attr_type = cur_value.attr_type();
      break;
    }
  }
  if (all_null) {
    value.set_type(AttrType::NONE);
    return RC::SUCCESS;
  }

  Value attr_value;
  if (attr_type == INTS) {
    int sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->find_cell(tcs, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_int();
      }
    }
    value.set_int(sum);
  } else if (attr_type == FLOATS) {
    float sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->find_cell(tcs, cur_value);
      if (!cur_value.is_null()) {
        sum += cur_value.get_float();
      }
    }
    value.set_float(sum);
  } else if (attr_type == CHARS) {
    float sum = 0;
    for (auto t : tuples) {
      Value cur_value;
      t->find_cell(tcs, cur_value);
      if (!cur_value.is_null()) {
        cur_value.str_to_number();
        if (cur_value.attr_type() == INTS) {
          sum += cur_value.get_int();
        } else {
          sum += cur_value.get_float();
        }
      }
    }
    value.set_float(sum);

  } else {  // 其余类型无法求和
    return RC::INVALID_ARGUMENT;
  }

  return RC::SUCCESS;
}