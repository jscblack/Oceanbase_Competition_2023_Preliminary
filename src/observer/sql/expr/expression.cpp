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
#include "sql/expr/tuple.h"
#include <regex>

using namespace std;

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
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

RC CastExpr::get_value(const Tuple &tuple, Value &cell) const
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

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  bool bool_value = false;
  rc              = compare_value(left_value, right_value, bool_value);
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
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuples, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
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
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
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

RC ConjunctionExpr::get_value(const std::vector<Tuple *> &tuples, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuples, tmp_value);
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

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
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

// RC AggregationExpr::aggregate_value(const std::vector<Tuple*> &tuples, Value &value) const
// {
//   return RC::SUCCESS;
// }

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
