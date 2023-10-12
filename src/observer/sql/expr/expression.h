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
#include "storage/field/field.h"

class Tuple;
class FilterStmt;

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
  STAR,         ///< 星号，表示所有字段
  FIELD,        ///< 字段。在实际执行时，根据行数据内容提取对应字段的值
  VALUE,        ///< 常量值
  VALUELIST,    ///< 常量列表
  CAST,         ///< 需要做类型转换的表达式
  COMPARISON,   ///< 需要做比较的表达式
  LOGICALCALC,  ///< 多个表达式做逻辑运算
  SELECT,       ///< select子查询
  ARITHMETIC,   ///< 算术运算
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
  virtual RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const = 0;

  /**
   * @brief 在没有实际运行的情况下，也就是无法获取tuple的情况下，尝试获取表达式的值
   * @details 有些表达式的值是固定的，比如ValueExpr，这种情况下可以直接获取值
   */
  virtual RC try_get_value(Value &value) const { return RC::UNIMPLENMENT; }

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

  /**
   * @brief 克隆一个表达式，新的内存拷贝
   */
  virtual Expression *clone() const = 0;

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
    value_ = expr.value_;
    return *this;
  }

  virtual ~ValueExpr(){};

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;
  RC try_get_value(Value &value) const override
  {
    value = value_;
    return RC::SUCCESS;
  }

  ExprType type() const override { return ExprType::VALUE; }

  AttrType value_type() const override { return value_.attr_type(); }

  void get_value(Value &value) const { value = value_; }

  const Value &get_value() const { return value_; }

  Expression *clone() const override { return new ValueExpr(*this); }

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

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override { return RC::UNIMPLENMENT; };

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

  // 会递归调用get_value，即外层的tuple传给子查询，子查询再传给子查询的子查询
  // 2023年10月9日20:17:12 得想清楚这玩意儿的逻辑，事实上pred的意义在于判断一个record是否应该被放进结果集
  // 然后判断的过程就依赖于expression本身
  // 比如外层一个record，这个时候他需要去得到内层的结果集才能判断是否应该留下
  // 而内层需要知道外层才能得到结果集
  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;
  RC get_value(const Tuple &tuple, std::vector<Value> &values, Trx *trx);

  ExprType type() const override { return ExprType::SELECT; }
  AttrType value_type() const override
  {
    // 在select真正执行之前，是无法知道select的结果集的类型的
    return AttrType::UNDEFINED;
  }
  RC rewrite_stmt(Stmt *&rewrited_stmt, const Tuple *row_tuple);
  RC rewrite_stmt(FilterStmt *&rewrited_stmt, const Tuple *row_tuple);
  RC recover_stmt(Stmt *&rewrited_stmt, const Tuple *row_tuple);
  RC recover_stmt(FilterStmt *&rewrited_stmt, const Tuple *row_tuple);

  Expression *clone() const override { return new SelectExpr(*this); }

private:
  Stmt *select_stmt_;  // select子查询的语句
};

/**
 * @brief 逻辑运算表达式
 * @ingroup Expression
 * 多个表达式使用同一种关系(AND或OR)来联结
 * 当前miniob仅有AND操作
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
 * @brief 算术表达式
 * @ingroup Expression
 */
class ArithmeticExpr : public Expression
{
public:
  enum class Type
  {
    ADD,
    SUB,
    MUL,
    DIV,
    NEGATIVE,
  };

public:
  ArithmeticExpr(Type type, Expression *left, Expression *right);
  ArithmeticExpr(Type type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  ArithmeticExpr(const ArithmeticExpr &expr) = delete;
  // {
  //   arithmetic_type_ = expr.arithmetic_type_;
  //   left_            = std::unique_ptr<Expression>(new decltype (*expr.left_)(*expr.left_));
  //   right_           = std::unique_ptr<Expression>(new decltype (*expr.right_)(*expr.right_));
  // }
  ArithmeticExpr &operator=(const ArithmeticExpr &expr) = delete;

  virtual ~ArithmeticExpr(){};

  ExprType type() const override { return ExprType::ARITHMETIC; }

  AttrType value_type() const override;

  RC get_value(const Tuple &tuple, Value &value, Trx *trx = nullptr) const override;
  RC try_get_value(Value &value) const override;

  Type arithmetic_type() const { return arithmetic_type_; }

  std::unique_ptr<Expression> &left() { return left_; }
  std::unique_ptr<Expression> &right() { return right_; }

  Expression *clone() const override;

private:
  RC calc_value(const Value &left_value, const Value &right_value, Value &value) const;

private:
  Type                        arithmetic_type_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};