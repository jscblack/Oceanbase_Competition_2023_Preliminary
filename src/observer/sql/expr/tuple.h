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
// Created by Wangyunlai on 2021/5/14.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/expr/tuple_cell.h"
#include "sql/parser/parse.h"
#include "sql/parser/value.h"
#include "storage/record/record.h"

class Table;

/**
 * @defgroup Tuple
 * @brief Tuple 元组，表示一行数据，当前返回客户端时使用
 * @details
 * tuple是一种可以嵌套的数据结构。
 * 比如select t1.a+t2.b from t1, t2;
 * 需要使用下面的结构表示：
 * @code {.cpp}
 *  Project(t1.a+t2.b)
 *        |
 *      Joined
 *      /     \
 *   Row(t1) Row(t2)
 * @endcode
 *
 */

/**
 * @brief 元组的结构，包含哪些字段(这里成为Cell)，每个字段的说明
 * @ingroup Tuple
 */
class TupleSchema
{
public:
  void                 append_cell(const TupleCellSpec &cell) { cells_.push_back(cell); }
  void                 append_cell(const char *table, const char *field) { append_cell(TupleCellSpec(table, field)); }
  void                 append_cell(const char *alias) { append_cell(TupleCellSpec(alias)); }
  int                  cell_num() const { return static_cast<int>(cells_.size()); }
  const TupleCellSpec &cell_at(int i) const { return cells_[i]; }

private:
  std::vector<TupleCellSpec> cells_;
};

/**
 * @brief 元组的抽象描述
 * @ingroup Tuple
 */
class Tuple
{
public:
  Tuple()          = default;
  virtual ~Tuple() = default;

  /**
   * @brief 获取元组中的Cell的个数
   * @details 个数应该与tuple_schema一致
   */
  virtual int cell_num() const = 0;

  /**
   * @brief 获取指定位置的Cell
   *
   * @param index 位置
   * @param[out] cell  返回的Cell
   */
  virtual RC cell_at(int index, Value &cell) const = 0;

  /**
   * @brief 根据cell的描述，获取cell的值
   *
   * @param spec cell的描述
   * @param[out] cell 返回的cell
   */
  virtual RC find_cell(const TupleCellSpec &spec, Value &cell) const = 0;

  virtual RC clone(Tuple *&tuple) const = 0;

  virtual std::string to_string() const
  {
    std::string str;
    const int   cell_num = this->cell_num();
    for (int i = 0; i < cell_num - 1; i++) {
      Value cell;
      cell_at(i, cell);
      str += cell.to_string();
      str += ", ";
    }

    if (cell_num > 0) {
      Value cell;
      cell_at(cell_num - 1, cell);
      str += cell.to_string();
    }
    return str;
  }
};

/**
 * @brief 一行数据的元组
 * @ingroup Tuple
 * @details 直接就是获取表中的一条记录
 */
class RowTuple : public Tuple
{
public:
  RowTuple() = default;
  // RowTupe(const Table *table) : table_(table) {}
  virtual ~RowTuple()
  {
    delete record_;
    // for (FieldExpr *spec : speces_) {
    //   delete spec;
    // }
    // speces_.clear();
  }

  void set_record(Record *record)
  {
    Record *new_record = new Record(*record);
    this->record_      = new_record;
  }

  void set_table(const Table *table) { table_ = table; }

  // void set_schema(const Table *table, const std::vector<FieldMeta> *fields)
  // {
  //   table_ = table;
  //   this->speces_.reserve(fields->size());
  //   for (const FieldMeta &field : *fields) {
  //     speces_.push_back(new FieldExpr(table, &field));
  //   }
  // }

  int cell_num() const override { return table_->table_meta().field_metas()->size(); }

  RC cell_at(int index, Value &cell) const override
  {
    // if (index < 0 || index >= static_cast<int>(speces_.size())) {
    //   LOG_WARN("invalid argument. index=%d", index);
    //   return RC::INVALID_ARGUMENT;
    // }

    // FieldExpr       *field_expr = speces_[index];
    // const FieldMeta *field_meta = field_expr->field().meta();

    if (index < 0 || index >= table_->table_meta().field_metas()->size()) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }

    const FieldMeta *field_meta = table_->table_meta().field(index);
    // 在这里需要判断一下，record当中，这个字段是否为空
    if (field_meta->nullable() && table_->table_meta().is_field_null(record_->data(), field_meta->name())) {
      // 为null
      cell.set_type(AttrType::NONE);
    } else {
      // 确有值
      cell.set_type(field_meta->type());
      cell.set_data(this->record_->data() + field_meta->offset(), field_meta->len());
    }

    return RC::SUCCESS;
  }

  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    const char *table_name = spec.table_name();
    const char *field_name = spec.field_name();
    if (0 != strcmp(table_name, table_->name())) {
      return RC::NOTFOUND;
    }

    auto ret = table_->table_meta().field(field_name);
    if (nullptr == ret) {
      return RC::NOTFOUND;
    }

    return cell_at(table_->table_meta().find_field_index_by_name(field_name), cell);

    // for (size_t i = 0; i < speces_.size(); ++i) {
    //   const FieldExpr *field_expr = speces_[i];
    //   const Field     &field      = field_expr->field();
    //   if (0 == strcmp(field_name, field.field_name())) {
    //     return cell_at(i, cell);
    //   }
    // }
    // return RC::NOTFOUND;
  }

#if 0
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }
#endif

  RC clone(Tuple *&tuple) const override
  {
    RowTuple *row_tuple = new RowTuple();
    Record   *record =
        new Record(*record_);  // 假定data被深拷贝了,但其实得看record是否是data的owner,不知道data的生命周期如何
    row_tuple->record_ = record;
    row_tuple->table_  = table_;
    // for (const FieldExpr *field_expr : speces_) {
    //   row_tuple->speces_.push_back(new FieldExpr(field_expr->field()));
    // }
    tuple = row_tuple;
    return RC::SUCCESS;
  }

  Record &record() { return *record_; }

  const Record &record() const { return *record_; }

  const Table &table() const { return *table_; }

private:
  Record      *record_ = nullptr;
  const Table *table_  = nullptr;

  // 旧版的tuple的field是由query_field一路往后传的，也就是限制了tuple的可选列
  // std::vector<FieldExpr *> speces_;
};

/**
 * @brief 从一行数据中，选择部分字段组成的元组，也就是投影操作
 * @ingroup Tuple
 * @details 一般在select语句中使用。
 * 投影也可以是很复杂的操作，比如某些字段需要做类型转换、重命名、表达式运算、函数计算等。
 * 当前的实现是比较简单的，只是选择部分字段，不做任何其他操作。
 */
class ProjectTuple : public Tuple
{
public:
  ProjectTuple() = default;

  virtual ~ProjectTuple()
  {
    // for (TupleCellSpec *spec : speces_) {
    //   delete spec;
    // }
    // speces_.clear();
    // delete tuple_;
  }

  void set_tuple(Tuple *tuple) { this->tuple_ = tuple; }

  // const std::vector<TupleCellSpec *> &get_speces() const { return speces_; }
  // void add_cell_spec(TupleCellSpec *spec) { speces_.push_back(spec); }

  void set_expressions(const std::vector<std::unique_ptr<Expression>> &exprs)
  {
    for (int i = 0; i < exprs.size(); i++) {
      expressions_.push_back(std::unique_ptr<Expression>(exprs[i]->clone()));
    }
  }

  int cell_num() const override { return expressions_.size(); }

  const std::vector<std::unique_ptr<Expression>> &expressions() { return expressions_; }

  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(expressions_.size())) {
      return RC::INTERNAL;
    }
    if (is_func_) {
      RowTuple tmp_tuple;
      return expressions_[index]->get_value(tmp_tuple, cell);
    }
    if (tuple_ == nullptr) {
      return RC::INTERNAL;
    }

    return expressions_[index]->get_value(*tuple_, cell);
    // const TupleCellSpec *spec = speces_[index];
    // return tuple_->find_cell(*spec, cell);
  }

  RC find_cell(const TupleCellSpec &spec, Value &cell) const override { return tuple_->find_cell(spec, cell); }

  RC clone(Tuple *&tuple) const override
  {
    ProjectTuple *project_tuple = new ProjectTuple();
    tuple_->clone(project_tuple->tuple_);
    for (int i = 0; i < expressions_.size(); i++) {
      project_tuple->expressions_.push_back(std::unique_ptr<Expression>(expressions_[i]->clone()));
    }

    // for (auto s : speces_) {
    //   project_tuple->speces_.push_back(new TupleCellSpec(s->table_name(), s->field_name()));
    // }
    tuple = project_tuple;
    return RC::SUCCESS;
  }

public:
  void set_is_func(bool is_func) { is_func_ = is_func; }

#if 0
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::NOTFOUND;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }
#endif
private:
  // std::vector<TupleCellSpec *> speces_;
  std::vector<std::unique_ptr<Expression>> expressions_;
  Tuple                                   *tuple_   = nullptr;
  bool                                     is_func_ = false;
};

class ExpressionTuple : public Tuple
{
public:
  ExpressionTuple(std::vector<std::unique_ptr<Expression>> &expressions) : expressions_(expressions) {}

  virtual ~ExpressionTuple() {}

  int cell_num() const override { return expressions_.size(); }

  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(expressions_.size())) {
      return RC::INTERNAL;
    }

    const Expression *expr = expressions_[index].get();
    return expr->try_get_value(cell);
  }

  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    for (const std::unique_ptr<Expression> &expr : expressions_) {
      if (0 == strcmp(spec.alias(), expr->name().c_str())) {
        return expr->try_get_value(cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC clone(Tuple *&tuple) const override
  {
    tuple = nullptr;
    return RC::INTERNAL;
  }

private:
  const std::vector<std::unique_ptr<Expression>> &expressions_;
};

/**
 * @brief 一些常量值组成的Tuple
 * @ingroup Tuple
 */
class ValueListTuple : public Tuple
{
public:
  ValueListTuple()          = default;
  virtual ~ValueListTuple() = default;

  void set_cells(const std::vector<Value> &cells) { cells_ = cells; }

  virtual int cell_num() const override { return static_cast<int>(cells_.size()); }

  virtual RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::NOTFOUND;
    }

    cell = cells_[index];
    return RC::SUCCESS;
  }

  virtual RC find_cell(const TupleCellSpec &spec, Value &cell) const override { return RC::INTERNAL; }

  RC clone(Tuple *&tuple) const override
  {
    tuple = nullptr;
    return RC::INTERNAL;
  }

private:
  std::vector<Value> cells_;
};

/**
 * @brief 将两个tuple合并为一个tuple
 * @ingroup Tuple
 * @details 在join算子中使用
 */
class JoinedTuple : public Tuple
{
public:
  JoinedTuple() = default;
  virtual ~JoinedTuple()
  {
    delete left_;
    delete right_;
  }

  void set_left(Tuple *left) { left->clone(left_); }
  void set_right(Tuple *right) { right->clone(right_); }

  int cell_num() const override { return left_->cell_num() + right_->cell_num(); }

  RC cell_at(int index, Value &value) const override
  {
    const int left_cell_num = left_->cell_num();
    if (index > 0 && index < left_cell_num) {
      return left_->cell_at(index, value);
    }

    if (index >= left_cell_num && index < left_cell_num + right_->cell_num()) {
      return right_->cell_at(index - left_cell_num, value);
    }

    return RC::NOTFOUND;
  }

  RC find_cell(const TupleCellSpec &spec, Value &value) const override
  {
    // TODO: 自连接时spec无法判断来自左或右
    // 问题来源应该是是alias的问题，tuplecellsepc没有考虑到alias
    RC rc = left_->find_cell(spec, value);
    if (rc == RC::SUCCESS || rc != RC::NOTFOUND) {
      return rc;
    }

    return right_->find_cell(spec, value);
  }

  RC clone(Tuple *&tuple) const override
  {
    JoinedTuple *joined_tuple = new JoinedTuple();
    left_->clone(joined_tuple->left_);
    right_->clone(joined_tuple->right_);
    tuple = joined_tuple;
    return RC::SUCCESS;
  }

private:
  Tuple *left_  = nullptr;
  Tuple *right_ = nullptr;
};
