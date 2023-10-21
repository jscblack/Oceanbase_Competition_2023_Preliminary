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

#pragma once

#include <memory>
#include <string.h>
#include <string>

#include "common/log/log.h"
#include "sql/parser/value.h"
#include "sql/stmt/stmt.h"
// #include "sql/stmt/select_stmt.h"
#include "storage/field/field.h"

class Tuple;
class FilterStmt;
class TupleCellSpec;

/**
//  * @brief 判别一个expr及其儿子中是否有符合条件的选项，有则返回true
//  *
//  * @param expr
//  * @param judge
//  * @return true
//  * @return false
//  */
// bool judge_attr_expr(Expression *expr, std::function<bool(ExprType)> judge);

/**
 * @defgroup Expression
 * @brief 表达式
 */

/**
 * @brief 表达式类型
 * @ingroup Expression
 */
enum class ExprType
{
  NONE,
  STAR,  ///< 星号，表示所有字段
  // 以下是可比较计算的表达式：
  // ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
  // 以下是可算术计算的表达式
  // ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
  FIELD,        ///< 字段。在实际执行时，根据行数据内容提取对应字段的值
  VALUE,        ///< 常量值
  ARITHMETIC,   ///< 算术运算
  FUNCTION,     ///< 多个表达式做函数运算，比如MAX，MIN
  SELECT,       ///< select子查询
  AGGREGATION,  ///< 聚合运算
  // ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
  VALUELIST,    ///< 常量列表
  CAST,         ///< 需要做类型转换的表达式
  COMPARISON,   ///< 需要做比较的表达式
  LOGICALCALC,  ///< 多个表达式做逻辑运算
};

/**
 * @brief 表达式的抽象描述
 * @ingroup Expression
 * @details 在SQL的元素中，任何需要得出值的元素都可以使用表达式来描述
 * 比如获取某个字段的值、比较运算、类型转换
 * 当然还有一些当前没有实现的表达式，比如算术运算。
 *
 * 通常表达式的值，是在真实的算子运算过程中，拿到具体的tuple后
 * 才能计算出来真实的值。但是有些表达式可能就表示某一个固定的
 * 值，比如ValueExpr。
 */
class Expression
{
  // public:
  //   friend bool judge_attr_expr(Expression *expr, std::function<bool(ExprType)> judge);
public:
  Expression() = default;
  virtual ~Expression(){};

  /**
   * @brief 拷贝构造函数
   */
  Expression(const Expression &expr) = default;

  /**
   * @brief 赋值运算符
   */
  Expression &operator=(const Expression &expr) = default;

  /**
   * @brief 根据具体的tuple，来计算当前表达式的值。tuple有可能是一个具体某个表的行数据
   */
  virtual RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const
  {
    ASSERT(false,
        "Expr::get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const: UNIMPLEMENT, %s:%d ",
        __FILE__,
        __LINE__);
    return RC::UNIMPLENMENT;
  };

  /**
   * @brief 在没有实际运行的情况下，也就是无法获取tuple的情况下，尝试获取表达式的值
   * @details 有些表达式的值是固定的，比如ValueExpr，这种情况下可以直接获取值
   */
  virtual RC try_get_value(Value &value) const
  {
    ASSERT(false, "Expr::try_get_value(Value &value) const: UNIMPLEMENT, %s:%d ", __FILE__, __LINE__);
    return RC::UNIMPLENMENT;
  }

  /**
   * @brief 根据分组的tuples，来计算当前和聚合表达式相关的值
   */
  virtual RC get_value(const std::vector<Tuple *> &tuples, Value &value) const
  {
    ASSERT(false,
        "Expr::get_value(const std::vector<Tuple *> &tuples, Value &value) const: UNIMPLEMENT, %s:%d ",
        __FILE__,
        __LINE__);
    return RC::UNIMPLENMENT;
  }

  /**
   * @brief 表达式的类型
   * 可以根据表达式类型来转换为具体的子类
   */
  virtual ExprType type() const = 0;

  /**
   * @brief 表达式值的类型
   * @details 一个表达式运算出结果后，只有一个值
   */
  virtual AttrType value_type() const = 0;

  /**
   * @brief 表达式的名字，比如是字段名称，或者用户在执行SQL语句时输入的内容
   */
  virtual std::string name() const { return name_; }
  virtual void        set_name(std::string name) { name_ = name; }
  virtual void        set_alias(std::string alias) { alias_ = alias; }

  /**
   * @brief 克隆一个表达式，新的内存拷贝
   */
  virtual Expression *clone() const = 0;

  /**
   * @brief 表达式在表头的输出，根据with_table_name来决定是否返回表名（单表时忽略所有表名）
   */
  virtual const std::string alias(bool with_table_name) const
  {
    ASSERT(false, "Expr::const std::string alias(bool with_table_name) const: UNIMPLEMENT");
    return "";
  };

  std::string alias_ = "";  // 最顶层的alias
private:
  std::string name_;
};

/**
 * @brief 字段表达式
 * @ingroup Expression
 */
class FieldExpr : public Expression
{
public:
  FieldExpr() = default;
  FieldExpr(const Table *table, const FieldMeta *field) : field_(table, field) {}
  FieldExpr(const Field &field) : field_(field) {}
  FieldExpr(const FieldExpr &expr) : field_(expr.field_) {}
  FieldExpr &operator=(const FieldExpr &expr)
  {
    field_ = expr.field_;
    return *this;
  }

  virtual ~FieldExpr(){};

  ExprType type() const override { return ExprType::FIELD; }
  AttrType value_type() const override { return field_.attr_type(); }

  Field &field() { return field_; }

  const Field &field() const { return field_; }

  const char *table_name() const { return field_.table_name(); }

  const char *field_name() const { return field_.field_name(); }

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;

  Expression *clone() const override { return new FieldExpr(*this); }

  const std::string alias(bool with_table_name) const override
  {
    if (alias_ != "") {
      return alias_;
    }
    if (with_table_name) {
      // if (std::string(field_.table_name()) == "") {
      //   return field_.table_name() + std::string(".") + field_.field_name();
      // } else {
      //   return field_.field_name();
      // }
      return field_.table_name() + std::string(".") + field_.field_name();
    } else {
      return field_.field_name();
    }
  }

private:
  Field field_;
};

/**
 * @brief 常量值表达式
 * @ingroup Expression
 */
class ValueExpr : public Expression
{
public:
  ValueExpr() { value_.set_type(AttrType::NONE); };
  explicit ValueExpr(const Value &value) : value_(value) {}
  explicit ValueExpr(const ValueExpr &expr) : value_(expr.value_) {}
  ValueExpr &operator=(const ValueExpr &expr)
  {
    value_ = expr.value_.clone();
    return *this;
  }

  virtual ~ValueExpr(){};

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;
  RC get_value(const std::vector<Tuple *> &tuples, Value &value) const override
  {
    value = value_;
    return RC::SUCCESS;
  }
  RC try_get_value(Value &value) const override
  {
    value = value_;
    return RC::SUCCESS;
  }

  ExprType type() const override { return ExprType::VALUE; }

  AttrType value_type() const override { return value_.attr_type(); }

  void get_value(Value &value) const { value = value_; }

  const Value &get_value() const { return value_; }

  Expression       *clone() const override { return new ValueExpr(*this); }
  const std::string alias(bool with_table_name) const override
  {
    if (alias_ != "") {
      return alias_;
    }
    if (value_.attr_type() == AttrType::CHARS) {
      return "\"" + value_.to_string() + "\"";
    }
    return value_.to_string();
  }

private:
  Value value_;
};

/**
 * @brief 常量列表表达式
 * @ingroup Expression
 */
class ValueListExpr : public Expression
{
public:
  ValueListExpr() = default;
  explicit ValueListExpr(const std::vector<Value> &value_list) : value_list_(value_list) {}
  explicit ValueListExpr(const ValueListExpr &expr) : value_list_(expr.value_list_) {}
  ValueListExpr &operator=(const ValueListExpr &expr)
  {
    value_list_ = expr.value_list_;
    return *this;
  }

  virtual ~ValueListExpr(){};

  ExprType type() const override { return ExprType::VALUELIST; }

  AttrType value_type() const override { return value_list_[0].attr_type(); }

  RC get_values(std::vector<Value> &value_list) const
  {
    value_list = value_list_;
    return RC::SUCCESS;
  }

  const std::vector<Value> &get_values() const { return value_list_; }

  Expression *clone() const override { return new ValueListExpr(*this); }

private:
  std::vector<Value> value_list_;
};

// TODO 这个后面会是一个expression

/**
 * @brief 类型转换表达式
 * @ingroup Expression
 */
class CastExpr : public Expression
{
public:
  CastExpr(std::unique_ptr<Expression> child, AttrType cast_type);
  CastExpr(const CastExpr &expr)            = delete;
  CastExpr &operator=(const CastExpr &expr) = delete;

  virtual ~CastExpr();

  ExprType type() const override { return ExprType::CAST; }
  RC       get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;

  RC try_get_value(Value &value) const override;

  AttrType value_type() const override { return cast_type_; }

  std::unique_ptr<Expression> &child() { return child_; }

  Expression *clone() const override;

private:
  RC cast(const Value &value, Value &cast_value) const;

private:
  std::unique_ptr<Expression> child_;      ///< 从这个表达式转换
  AttrType                    cast_type_;  ///< 想要转换成这个类型
};

/**
 * @brief 比较表达式
 * @ingroup Expression
 */
class ComparisonExpr : public Expression
{
public:
  ComparisonExpr(CompOp comp, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  ComparisonExpr(const ComparisonExpr &expr)            = delete;
  ComparisonExpr &operator=(const ComparisonExpr &expr) = delete;

  virtual ~ComparisonExpr();

  ExprType type() const override { return ExprType::COMPARISON; }

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;

  RC get_value(const std::vector<Tuple *> &tuples, Value &value) const override;

  AttrType value_type() const override { return BOOLEANS; }

  CompOp comp() const { return comp_; }

  std::unique_ptr<Expression> &left() { return left_; }
  std::unique_ptr<Expression> &right() { return right_; }

  /**
   * 尝试在没有tuple的情况下获取当前表达式的值
   * 在优化的时候，可能会使用到
   */
  RC try_get_value(Value &value) const override;

  /**
   * compare the two tuple cells
   * @param value the result of comparison
   */
  RC compare_value(const Value &left, const Value &right, bool &value) const;

  /**
   * to handle in and not in
   * @param value the result of comparison
   */
  RC compare_value(const Value &left, const std::vector<Value> &right, bool &value) const;

  Expression *clone() const override;

  /**
   * @brief 用于ConditionSqlNode转换为Expression时，自动转换类型并检测可比性
   *
   * @param [in] comp
   * @param [out] left_expr
   * @param [out] right_expr
   * @return RC
   */
  static RC cast_and_check_comparable(CompOp comp, Expression *&left_expr, Expression *&right_expr)
  {
    //   // like的语法检测, 必须左边是属性(字符串field), 右边是字符串
    //   // 目前应该不需要支持右边是非字符串转成字符串???
    RC             rc              = RC::SUCCESS;
    const ExprType left_expr_type  = left_expr->type();
    const ExprType right_expr_type = right_expr->type();

    const AttrType type_left  = left_expr->value_type();
    const AttrType type_right = right_expr->value_type();

    if (CompOp::LIKE_ENUM == comp || CompOp::NOT_LIKE_ENUM == comp) {
      if (left_expr_type == ExprType::FIELD && right_expr_type == ExprType::VALUE) {
        if (type_left != CHARS || type_right != CHARS) {
          LOG_WARN("attr LIKE/NOT LIKE value, attr and value must be CHARS");
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      } else {
        LOG_WARN("LIKE/NOT LIKE must be 'attr LIKE value'");
        return RC::SQL_SYNTAX;
      }
    }

    // fix: 这个处理可能是多余的，待查证
    // 检查两个类型是否能够比较
    if (type_left != type_right) {
      if (type_left == DATES || type_right == DATES) {
        // date conversation
        // advance check for date
        if (left_expr_type == ExprType::VALUE && right_expr_type == ExprType::FIELD) {  // left:value, right:attr
          if (type_right == DATES && type_left == CHARS) {
            // the attr is date type, so we need to convert the value to date type
            rc = dynamic_cast<ValueExpr *>(left_expr)->get_value().auto_cast(DATES);
            if (rc != RC::SUCCESS) {
              return rc;
            }
          }
        } else if (left_expr_type == ExprType::FIELD && right_expr_type == ExprType::VALUE) {  // left:attr, right:value
          if (type_left == DATES && type_right == CHARS) {
            // the attr is date type, so we need to convert the value to date type
            rc = dynamic_cast<ValueExpr *>(right_expr)->get_value().auto_cast(DATES);
            if (rc != RC::SUCCESS) {
              return rc;
            }
          }
        }
      } else if (type_left == CHARS && (type_right == FLOATS || type_right == INTS)) {
        // left is a string, and right is s a number
        // convert the string to number
        if (left_expr_type == ExprType::VALUE) {
          // left is a value
          rc = dynamic_cast<ValueExpr *>(left_expr)->get_value().str_to_number();
          if (rc != RC::SUCCESS) {
            return rc;
          }
        }
      } else if ((type_left == FLOATS || type_left == INTS) && type_right == CHARS) {
        // left is a number, and right is a string
        // convert the string to number
        if (right_expr_type == ExprType::VALUE) {
          // right is a value
          rc = dynamic_cast<ValueExpr *>(right_expr)->get_value().str_to_number();
          if (rc != RC::SUCCESS) {
            return rc;
          }
        }
      }
    }
    return rc;
  }

private:
  CompOp                      comp_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

/**
 * @brief select子查询表达式，这其中需要处理子查询完整的生命周期
 * @ingroup Expression
 */
class SelectExpr : public Expression
{
  inline static std::unordered_map<std::string, const Tuple *>
      tuples_;  // 外层tuple的缓存，key是record所在的table的名字
  inline static std::unordered_map<Expression *, Expression *> recover_table;

public:
  SelectExpr(Stmt *select_stmt);
  SelectExpr(const SelectExpr &expr) { select_stmt_ = expr.select_stmt_; }
  SelectExpr &operator=(const SelectExpr &expr)
  {
    select_stmt_ = expr.select_stmt_;
    return *this;
  }
  virtual ~SelectExpr(){};

  /**
   * @brief 此处子查询需要返回多个值，所以在此处不做实现
   */
  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override { return RC::UNIMPLENMENT; };

  /**
   * @brief 为分组聚合留的接口，SelectExpr中不需要用
   */
  RC get_value(const std::vector<Tuple *> &tuples, Value &value) const override { return RC::UNIMPLENMENT; };

  /**
   * @brief SelectExpr的get_value，会递归调用子查询的get_value
   * @details 递归调用的时候，需要传入外层的tuple，这样子查询才能得到外层的值
   * @param tuple 外层的tuple
   * @param values 子查询的结果集
   * @param trx 事务
   */
  RC get_value(const Tuple &tuple, std::vector<Value> &values, Trx *trx);

  /**
   * @brief 执行Select语句，得到返回的tuple集
   */
  RC get_value(std::vector<Tuple *> &tuples, Trx *trx);

  ExprType type() const override { return ExprType::SELECT; }
  AttrType value_type() const override;
  RC       rewrite_stmt(Stmt *&original_stmt, const Tuple *row_tuple);
  RC       rewrite_expr(Expression *&original_expr, const Tuple *row_tuple);
  RC       recover_stmt(Stmt *&rewrited_stmt, const Tuple *row_tuple);
  RC       recover_expr(Expression *&rewrited_expr, const Tuple *row_tuple);

  Expression *clone() const override { return new SelectExpr(*this); }

private:
  Stmt *select_stmt_;  // select子查询的语句
};

/**
 * @brief 逻辑运算表达式
 * @ingroup Expression
 * 多个表达式使用同一种关系(AND或OR)来联结
 */
class LogicalCalcExpr : public Expression
{
public:
  LogicalCalcExpr(LogiOp logi, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  LogicalCalcExpr(const LogicalCalcExpr &expr)            = delete;
  LogicalCalcExpr &operator=(const LogicalCalcExpr &expr) = delete;

  virtual ~LogicalCalcExpr(){};

  ExprType type() const override { return ExprType::LOGICALCALC; }

  AttrType value_type() const override { return BOOLEANS; }

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;
  RC get_value(const std::vector<Tuple *> &tuples, Value &value) const override;

  LogiOp                       logical_calc_type() const { return logi_; }
  std::unique_ptr<Expression> &left() { return left_; }
  std::unique_ptr<Expression> &right() { return right_; }

  Expression *clone() const override;

private:
  LogiOp                      logi_;
  std::unique_ptr<Expression> left_  = nullptr;
  std::unique_ptr<Expression> right_ = nullptr;
};

/**
 * @brief 函数运算表达式
 * @ingroup Expression
 * 多个表达式的值进行函数运算
 * 如MAX，MIN
 */
class FunctionExpr : public Expression
{
public:
  FunctionExpr(FuncName func_type, std::vector<std::unique_ptr<Expression>> &expr_list);
  FunctionExpr(const FunctionExpr &expr)            = delete;
  FunctionExpr &operator=(const FunctionExpr &expr) = delete;

  virtual ~FunctionExpr(){};

  ExprType type() const override { return ExprType::FUNCTION; }

  AttrType value_type() const override;

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;

  FuncName func_type() const { return func_type_; }

  std::vector<std::unique_ptr<Expression>> &expr_list() { return expr_list_; }

  static RC is_subexpr_legal(FuncName func, std::vector<std::unique_ptr<Expression>> &expr_list)
  {
    RC rc = RC::SUCCESS;
    switch (func) {
      case FuncName::LENGTH_FUNC_NUM: {
        if (expr_list.size() != 1) {
          LOG_WARN("func length has improper argument number");
          return RC::FUNC_EXPR_ERROR;
        }
        Expression *expr = expr_list[0].get();
        if (expr->value_type() != AttrType::CHARS) {
          return RC::FUNC_EXPR_ERROR;
        }
      } break;
      case FuncName::ROUND_FUNC_NUM: {
        if (!(expr_list.size() == 2 || expr_list.size() == 1)) {
          LOG_WARN("func round has improper argument number");
          return RC::FUNC_EXPR_ERROR;
        }
        Expression *float_expr = expr_list[0].get();
        if (float_expr->value_type() != AttrType::FLOATS) {
          return RC::FUNC_EXPR_ERROR;
        }
        if (expr_list.size() == 2) {
          if (expr_list[1]->value_type() != AttrType::INTS) {
            return RC::FUNC_EXPR_ERROR;
          }
        }
      } break;
      case FuncName::DATE_FUNC_NUM: {
        if (expr_list.size() != 2) {
          LOG_WARN("func date_format has improper argument number");
          return RC::FUNC_EXPR_ERROR;
        }
        // 判断是否可以转成date，得拿出tuple之后才知道？
        // Field可以看fieldmeta
        // 由于日期不能参与算术运算，所以【0】必须是field或value
        if (expr_list[0]->type() != ExprType::FIELD && expr_list[0]->type() != ExprType::VALUE) {
          LOG_WARN("func date_format accept only FIELD or VALUE");
          return RC::FUNC_EXPR_ERROR;
        }
        if (expr_list[0]->type() == ExprType::FIELD && expr_list[0]->value_type() != AttrType::DATES) {
          return RC::FUNC_EXPR_ERROR;
        } else if (expr_list[0]->type() == ExprType::VALUE) {
          Value v;
          expr_list[0]->try_get_value(v);
          rc = v.auto_cast(AttrType::DATES);
          if (OB_FAIL(rc)) {
            return rc;
          }
        }
        if (expr_list[1]->value_type() != AttrType::CHARS) {
          return RC::FUNC_EXPR_ERROR;
        }
      } break;
      case FuncName::MAX_FUNC_ENUM:
      case FuncName::MIN_FUNC_ENUM:
      default: {
        return RC::FUNC_EXPR_ERROR;
      } break;
    }
    return rc;
  }

  const std::string alias(bool with_table_name) const override
  {
    if (alias_ != "") {
      return alias_;
    }
    switch (func_type_) {
      case FuncName::LENGTH_FUNC_NUM: {
        return std::string("LENGTH(") + expr_list_[0]->alias(with_table_name) + ")";
      } break;
      case FuncName::ROUND_FUNC_NUM: {
        if (expr_list_.size() == 1) {
          return std::string("ROUND(") + expr_list_[0]->alias(with_table_name) + ")";
        } else if (expr_list_.size() == 2) {
          return std::string("ROUND(") + expr_list_[0]->alias(with_table_name) + "," +
                 expr_list_[1]->alias(with_table_name) + ")";
        }
      } break;
      case FuncName::DATE_FUNC_NUM: {
        return std::string("DATE_FORMAT(") + expr_list_[0]->alias(with_table_name) + "," +
               expr_list_[1]->alias(with_table_name) + ")";
      } break;
      case FuncName::MAX_FUNC_ENUM:
      case FuncName::MIN_FUNC_ENUM:
      default: {
        // TODO 未实现
      } break;
    }

    ASSERT(false, "alias: undefined function type");
    return "";
  }

  Expression *clone() const override;

private:
  FuncName                                 func_type_;
  std::vector<std::unique_ptr<Expression>> expr_list_;
};

/**
 * @brief 算术表达式
 * @ingroup Expression
 */
class ArithmeticExpr : public Expression
{
public:
  ArithmeticExpr(ArithOp type, Expression *left, Expression *right);
  ArithmeticExpr(ArithOp type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  ArithmeticExpr(const ArithmeticExpr &expr)            = delete;
  ArithmeticExpr &operator=(const ArithmeticExpr &expr) = delete;

  virtual ~ArithmeticExpr(){};

  ExprType type() const override { return ExprType::ARITHMETIC; }

  AttrType value_type() const override;

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;
  // RC get_value(const Tuple &tuple, Value &value) const override; // 旧版get_value 经由sub-query更新后已废弃
  RC get_value(const std::vector<Tuple *> &tuples, Value &value) const override;
  RC try_get_value(Value &value) const override;

  ArithOp arithmetic_type() const { return arithmetic_type_; }

  /**
   * @brief Resolve Stage对算术表达式生成时的合法性检验。
   *
   * @param type
   * @param left
   * @param right
   * @return true 合法
   * @return false 非法，需要报错
   */
  // TODO 未完成，未处理NULL， 未判断表达式类型是否可计算
  static bool is_legal_subexpr(ArithOp type, const Expression *left, const Expression *right)
  {

    if (nullptr == left) {
      return false;
    } else if (nullptr == right) {
      return (type == ArithOp::POSITIVE || type == ArithOp::NEGATIVE) && (left->type() != ExprType::VALUELIST) &&
             (left->value_type() >= CHARS && left->value_type() <= FLOATS);
    } else {
      return (type >= ArithOp::ADD && type <= ArithOp::DIV) &&
             (left->value_type() >= CHARS && left->value_type() <= FLOATS) &&
             (right->value_type() >= CHARS && right->value_type() <= FLOATS);
    }
  }

  std::unique_ptr<Expression> &left() { return left_; }
  std::unique_ptr<Expression> &right() { return right_; }

  Expression       *clone() const override;
  const std::string alias(bool with_table_name) const override
  {
    if (alias_ != "") {
      return alias_;
    }
    std::string left_alias  = (left_ != nullptr) ? left_->alias(with_table_name) : "";
    std::string right_alias = (right_ != nullptr) ? right_->alias(with_table_name) : "";

    switch (arithmetic_type_) {
      case ArithOp::ADD: {
        return left_alias + "+" + right_alias;
      } break;
      case ArithOp::SUB: {
        return left_alias + "-" + right_alias;
      } break;
      case ArithOp::MUL: {
        return left_alias + "*" + right_alias;
      } break;
      case ArithOp::DIV: {
        return left_alias + "/" + right_alias;
      } break;
      case ArithOp::NEGATIVE: {
        return "-" + left_alias;
      } break;
      case ArithOp::POSITIVE: {
        return left_alias;
      } break;
      default: {
        ASSERT(false, "ArithmeticExpr::const std::string alias(bool with_table_name) UNREACHABLE!!!");
        return "";
      } break;
    }
  }

private:
  // TODO: 尚未处理运行时错误
  // 例如 子表达式返回超出预期的值（一个vector而非单行，NULL值），除零错误，
  RC calc_value(const Value &left_value, const Value &right_value, Value &value) const;

private:
  ArithOp                     arithmetic_type_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

/**
 * @brief 聚合表达式
 * @ingroup Expression
 */
class AggregationExpr : public Expression
{
public:
  AggregationExpr() = default;
  AggregationExpr(FuncName agg_type, Expression *child);
  AggregationExpr(FuncName agg_type, std::unique_ptr<Expression> child);
  AggregationExpr(const AggregationExpr &expr)            = delete;
  AggregationExpr &operator=(const AggregationExpr &expr) = delete;
  virtual ~AggregationExpr()                              = default;

  ExprType type() const override { return ExprType::AGGREGATION; }

  AttrType value_type() const override
  {
    if (agg_type_ == FuncName::COUNT_FUNC_ENUM) {
      return AttrType::INTS;
    }
    FieldExpr *field = dynamic_cast<FieldExpr *>(child_.get());
    return field->value_type();
  }

  FuncName agg_type() const { return agg_type_; }

  std::unique_ptr<Expression> &child() { return child_; }

  // 现在没法解决COUNT(*.*)的输出问题，通过nullptr无法区分(*.*)和(*)，可能不需要fix
  const std::string alias(bool with_table_name) const
  {
    if (alias_ != "") {
      return alias_;
    }
    switch (agg_type_) {
      case FuncName::MAX_FUNC_ENUM: return "MAX(" + child_->alias(with_table_name) + ")";
      case FuncName::MIN_FUNC_ENUM: return "MIN(" + child_->alias(with_table_name) + ")";
      case FuncName::COUNT_FUNC_ENUM: return "COUNT(" + child_->alias(with_table_name) + ")";
      case FuncName::AVG_FUNC_ENUM: return "AVG(" + child_->alias(with_table_name) + ")";
      case FuncName::SUM_FUNC_ENUM: return "SUM(" + child_->alias(with_table_name) + ")";
    }
    return "UNKNOWN";
  }

  Expression *clone() const override;

  RC get_value(const std::vector<Tuple *> &tuples,
      Value &value) const override;  // 传入分组的所有tuples，返回聚合运算之后的Value

  // 废弃代码*********************************************************BEGIN
  // AggregationExpr(const Table *table, const FieldMeta *field, const std::string &aggregation_func)
  //     : field_(table, field), aggregation_func_(aggregation_func)
  // {}
  // AggregationExpr(const Field &field, const std::string &aggregation_func)
  //     : field_(field), aggregation_func_(aggregation_func)
  // {}
  // AttrType value_type() const override { return field_.attr_type(); }

  // Field       &field() { return field_; }
  // std::string &aggregation_func() { return aggregation_func_; }

  // const Field       &field() const { return field_; }
  // const std::string &aggregation_func() const { return aggregation_func_; }

  // const char *table_name() const { return field_.table_name(); }
  // const char *field_name() const { return field_.field_name(); }
  // 废弃代码*********************************************************END

private:
  // TODO: 应该彻底Expression化，接收一个sub_expr，不假定其类型
  FuncName                    agg_type_;
  std::unique_ptr<Expression> child_;

  RC do_max_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const;
  RC do_min_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const;
  RC do_count_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const;
  RC do_avg_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const;
  RC do_sum_aggregate(const std::vector<Tuple *> &tuples, Value &value, TupleCellSpec &tcs) const;

  // 废弃代码*********************************************************BEGIN
  // Field       field_;
  // std::string aggregation_func_;
  // 废弃代码*********************************************************END
};