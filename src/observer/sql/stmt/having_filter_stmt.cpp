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

#include "sql/stmt/having_filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC HavingFilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode *conditions, HavingFilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;
  if (conditions == nullptr) {
    return rc;
  }

  HavingFilterStmt *cur_stmt = new HavingFilterStmt();
  if (conditions->inner_node) {
    HavingFilterStmt *cur_left_stmt  = nullptr;
    HavingFilterStmt *cur_right_stmt = nullptr;
    rc                               = create(db, default_table, tables, conditions->left_cond, cur_left_stmt);
    if (OB_FAIL(rc)) {
      delete cur_stmt;
      LOG_WARN("HavingFilterStmt::create: failed to create filter unit. condition index=%d", 123456);
      return rc;
    }
    rc = create(db, default_table, tables, conditions->right_cond, cur_right_stmt);
    if (OB_FAIL(rc)) {
      delete cur_stmt;
      LOG_WARN("HavingFilterStmt::create: failed to create filter unit. condition index=%d", 123456);
      return rc;
    }
    cur_stmt->left_  = cur_left_stmt;
    cur_stmt->right_ = cur_right_stmt;
    cur_stmt->logi_  = conditions->logi_op;
  } else {
    HavingFilterUnit *filter_unit = nullptr;
    rc == create_filter_unit(db, default_table, tables, *conditions, filter_unit);
    if (OB_FAIL(rc)) {
      delete cur_stmt;
      LOG_WARN("HavingFilterStmt::create: failed to create filter unit. condition index=%d", 123456);
    }
    cur_stmt->filter_unit_ = filter_unit;
  }
  stmt = cur_stmt;
  return rc;
}

// HavingFilterStatement_get_table_and_field
// FIXME: 待重构。。。。。。。。。。。。。。。。。。。。。。。。。。。 尚未观看，跳过先看create_filter_unit
RC hfs_get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {
    auto iter = tables->find(attr.relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  } else {
    table = db->find_table(attr.relation_name.c_str());
  }
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  if (attr.attribute_name == "*" && attr.aggregation_func == "COUNT") {  // 特殊处理count(*)
    field = nullptr;
    return RC::SUCCESS;
  }

  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}

RC HavingFilterStmt::create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode &condition, HavingFilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;

  CompOp comp = condition.comp;
  if (comp < EQUAL_TO || comp >= NO_OP) {
    LOG_WARN("invalid compare operator : %d", comp);
    return RC::INVALID_ARGUMENT;
  }

  filter_unit         = new HavingFilterUnit;
  AttrType type_left  = UNDEFINED;
  AttrType type_right = UNDEFINED;

  switch (condition.left_type) {
    case VALUE: {
      HavingFilterObj filter_obj;
      filter_obj.init_expr(condition.left_expr);
      filter_unit->set_left(filter_obj);
      type_left = filter_obj.expr->value_type();
    } break;
    case ATTR: {
      Table           *table = nullptr;
      const FieldMeta *field = nullptr;
      rc                     = hfs_get_table_and_field(db, default_table, tables, condition.left_attr, table, field);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot find attr");
        return rc;
      }
      HavingFilterObj filter_obj;
      // filter_obj.init_expr(static_cast<Expression*>(new AggregationExpr(Field(table,field),)))
      

    } break;

    case SUB_SELECT:  // having内暂不支持子查询
    default: {
      delete filter_unit;
      LOG_WARN("invalid left_type: %d", condition.left_type);
      return RC::INVALID_ARGUMENT;
    } break;
  }

  if (condition.left_is_attr) {
    Table           *table = nullptr;
    const FieldMeta *field = nullptr;
    rc                     = hfs_get_table_and_field(db, default_table, tables, condition.left_attr, table, field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
    HavingFilterObj having_filter_obj;
    having_filter_obj.init_attr(Field(table, field), condition.left_attr.aggregation_func);
    having_filter_unit->set_left(having_filter_obj);
    // FIXME: field == nullptr 的情况尚未处理
    type_left = (field) ? field->type() : AttrType::UNDEFINED;

  } else {
    HavingFilterObj having_filter_obj;
    having_filter_obj.init_value(condition.left_value);
    having_filter_unit->set_left(having_filter_obj);
    type_left = having_filter_obj.value.attr_type();
  }

  if (condition.right_is_attr) {
    Table           *table = nullptr;
    const FieldMeta *field = nullptr;
    rc                     = hfs_get_table_and_field(db, default_table, tables, condition.right_attr, table, field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
    HavingFilterObj having_filter_obj;
    having_filter_obj.init_attr(Field(table, field), condition.right_attr.aggregation_func);
    having_filter_unit->set_right(having_filter_obj);
    type_right = (field) ? field->type() : AttrType::UNDEFINED;
  } else {
    HavingFilterObj having_filter_obj;
    having_filter_obj.init_value(condition.right_value);
    having_filter_unit->set_right(having_filter_obj);
    type_right = having_filter_obj.value.attr_type();
  }

  having_filter_unit->set_comp(comp);

  // like的语法检测, 必须左边是属性(字符串field), 右边是字符串
  // 目前应该不需要支持右边是非字符串转成字符串???
  if (LIKE_ENUM == comp || NOT_LIKE_ENUM == comp) {
    if (condition.left_is_attr && !condition.right_is_attr) {
      if (type_left != CHARS || type_right != CHARS) {
        delete having_filter_unit;
        LOG_WARN("attr LIKE/NOT LIKE value, attr and value must be CHARS");
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    } else {  // 不满足 condition.left_is_attr && !condition.right_is_attr
      delete having_filter_unit;
      LOG_WARN("LIKE/NOT LIKE must be 'attr LIKE value'");
      return RC::SQL_SYNTAX;
    }
  }

  // // fix: 这个处理可能是多余的，待查证
  // // 检查两个类型是否能够比较
  // if (type_left != type_right) {
  //   if (type_left == DATES || type_right == DATES) {
  //     // date conversation
  //     // advance check for date
  //     if (!having_filter_unit->left().is_attr && having_filter_unit->right().is_attr) {  // left:value, right:attr
  //       if (type_right == DATES) {
  //         // the attr is date type, so we need to convert the value to date type
  //         if (having_filter_unit->left().value.attr_type() == CHARS) {
  //           rc = having_filter_unit->left().value.auto_cast(DATES);
  //           if (rc != RC::SUCCESS) {
  //             delete having_filter_unit;
  //             return rc;
  //           }
  //         }
  //       }
  //     } else if (having_filter_unit->left().is_attr && !having_filter_unit->right().is_attr) {  // left:attr,
  //     right:value
  //       if (type_left == DATES) {
  //         // the attr is date type, so we need to convert the value to date type
  //         if (having_filter_unit->right().value.attr_type() == CHARS) {
  //           rc = having_filter_unit->right().value.auto_cast(DATES);
  //           if (rc != RC::SUCCESS) {
  //             delete having_filter_unit;
  //             return rc;
  //           }
  //         }
  //       }
  //     }
  //   } else if (type_left == CHARS && (type_right == FLOATS || type_right == INTS)) {
  //     // left is a string, and right is s a number
  //     // convert the string to number
  //     if (!having_filter_unit->left().is_attr) {
  //       // left is a value
  //       rc = having_filter_unit->left().value.str_to_number();
  //       if (rc != RC::SUCCESS) {
  //         delete having_filter_unit;
  //         return rc;
  //       }
  //     }
  //   } else if ((type_left == FLOATS || type_left == INTS) && type_right == CHARS) {
  //     // left is a number, and right is a string
  //     // convert the string to number
  //     if (!having_filter_unit->right().is_attr) {
  //       // right is a value
  //       rc = having_filter_unit->right().value.str_to_number();
  //       if (rc != RC::SUCCESS) {
  //         delete having_filter_unit;
  //         return rc;
  //       }
  //     }
  //   }
  // }

  return rc;
}
