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
// Created by tong1heng on 2023/10/22.
//

#include "sql/operator/view_scan_physical_operator.h"
#include "storage/table/table.h"
#include "event/sql_debug.h"

using namespace std;

RC ViewScanPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("view scan operator must has one child");
    return RC::INTERNAL;
  }
  trx_ = trx;
  return children_[0]->open(trx);
}

RC ViewScanPhysicalOperator::next()
{
  RC                rc            = RC::SUCCESS;
  bool              filter_result = false;
  PhysicalOperator *oper          = children_.front().get();

  while ((rc = oper->next()) == RC::SUCCESS) {
    Tuple *tuple = oper->current_tuple();

    // TODO: 需要将tuple转换，尤其是需要先把视图的select计算出来，调用child oper的cell_at
    // 新搞一个view tuple

    int                cell_num = tuple->cell_num();
    std::vector<Value> values;
    for (int i = 0; i < cell_num; i++) {
      Value value;
      rc = tuple->cell_at(i, value);
      values.emplace_back(value);
    }
    view_tuple_.set_cells(values);
    view_tuple_.set_table(table_);

    // TODO: 在这里构建table/field map，并传递row tuple

    if (oper->type() == PhysicalOperatorType::PROJECT) {  // 不考虑agg的情况
      ProjectTuple                                   *project_tuple       = dynamic_cast<ProjectTuple *>(tuple);
      Tuple                                          *project_child_tuple = project_tuple->get_tuple();
      const std::vector<std::unique_ptr<Expression>> &expressions         = project_tuple->expressions();

      if (project_child_tuple->type() == TupleType::ROW_TUPLE) {  // 对应原始table的view
        view_tuple_.set_tuple(project_child_tuple);  // 只有在视图update/delete时用到，set的是row tuple
        RowTuple *project_child_tuple_cast = dynamic_cast<RowTuple *>(project_child_tuple);
        view_tuple_.add_view_map(table_, project_child_tuple_cast->get_table());

        // 下层的project是单表还是多表，即alias的参数是否为true
        bool                     with_table_name = false;  // 单表
        std::vector<std::string> table_name;
        for (int i = 0; i < expressions.size(); i++) {
          if (expressions[i]->type() == ExprType::FIELD) {
            FieldExpr *field_expr = dynamic_cast<FieldExpr *>(expressions[i].get());
            table_name.emplace_back(field_expr->table_name());
          } else {  // 其他类型不可更新？
            // TODO: 在view tuple里面做一个不可更新的标记
          }
        }
        for (int i = 0; i < table_name.size() - 1; i++) {
          if (table_name[i] != table_name[i + 1]) {
            with_table_name = true;
          }
        }

        // 添加映射关系
        const TableMeta                   &table_meta = table_->table_meta();
        std::map<std::string, std::string> field_map;
        for (int i = 0; i < table_meta.field_num() - table_meta.sys_field_num(); i++) {
          const FieldMeta *field = table_meta.field(i + table_meta.sys_field_num());
          for (int k = 0; k < expressions.size(); k++) {
            if (strcmp(field->name(), expressions[k]->alias(with_table_name).c_str()) == 0) {
              // 真实的底层的field name的映射关系
              if (expressions[k]->type() == ExprType::FIELD) {
                FieldExpr *field_expr = dynamic_cast<FieldExpr *>(expressions[k].get());
                field_map.insert({std::string(field->name()), std::string(field_expr->field_name())});
              }
            }
          }
        }
        view_tuple_.add_all_field_maps(field_map);

      } else if (project_child_tuple->type() == TupleType::VIEW_TUPLE) {  // 多层view
        ViewTuple *project_child_tuple_cast = dynamic_cast<ViewTuple *>(project_child_tuple);
        view_tuple_.set_tuple(
            project_child_tuple_cast->get_tuple());  // 只有在视图update/delete时用到，set的是row tuple

        // 转移之前层的view map
        std::vector<std::pair<const Table *, const Table *>> &old_view_map = project_child_tuple_cast->get_view_map();
        for (int i = 0; i < old_view_map.size(); i++) {
          view_tuple_.add_view_map(old_view_map[i].first, old_view_map[i].second);
        }
        // 添加本层的view map
        view_tuple_.add_view_map(table_, project_child_tuple_cast->get_table());

        // 转移之前层的field map
        auto old_all_field_maps = project_child_tuple_cast->get_all_field_maps();
        for (int i = 0; i < old_all_field_maps.size(); i++) {
          view_tuple_.add_all_field_maps(old_all_field_maps[i]);
        }
        // 添加本层的field map
        bool                     with_table_name = false;  // 单表
        std::vector<std::string> table_name;
        for (int i = 0; i < expressions.size(); i++) {
          if (expressions[i]->type() == ExprType::FIELD) {
            FieldExpr *field_expr = dynamic_cast<FieldExpr *>(expressions[i].get());
            table_name.emplace_back(field_expr->table_name());
          } else {  // 其他类型不可更新？
            // TODO: 在view tuple里面做一个不可更新的标记
          }
        }
        for (int i = 0; i < table_name.size() - 1; i++) {
          if (table_name[i] != table_name[i + 1]) {
            with_table_name = true;
          }
        }
        const TableMeta                   &table_meta = table_->table_meta();
        std::map<std::string, std::string> field_map;
        for (int i = 0; i < table_meta.field_num() - table_meta.sys_field_num(); i++) {
          const FieldMeta *field = table_meta.field(i + table_meta.sys_field_num());
          for (int k = 0; k < expressions.size(); k++) {
            if (strcmp(field->name(), expressions[k]->alias(with_table_name).c_str()) == 0) {
              // 真实的底层的field name的映射关系
              if (expressions[k]->type() == ExprType::FIELD) {
                FieldExpr *field_expr = dynamic_cast<FieldExpr *>(expressions[k].get());
                field_map.insert({std::string(field->name()), std::string(field_expr->field_name())});
              }
            }
          }
        }
        view_tuple_.add_all_field_maps(field_map);

      } else if (project_child_tuple->type() == TupleType::JOINED_TUPLE) {  // 暂时不考虑是join tuple的情况
        // rc = RC::INTERNAL;
        // return rc;
      } else {  // 其余情况不需要处理 expression tuple
        rc = RC::INTERNAL;
        return rc;
      }
    }

    rc = filter(&view_tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    if (filter_result) {
      sql_debug("get a tuple from view: %s", view_tuple_.to_string().c_str());
      break;
    } else {
      sql_debug("a tuple from view is filtered: %s", view_tuple_.to_string().c_str());
      rc = RC::RECORD_EOF;
    }
  }
  return rc;
}

RC ViewScanPhysicalOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *ViewScanPhysicalOperator::current_tuple() { return &view_tuple_; }

string ViewScanPhysicalOperator::param() const { return table_->name(); }

void ViewScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

RC ViewScanPhysicalOperator::filter(Tuple *tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(*tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}
