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
    std::vector<std::pair<std::string, std::string>> &relation_to_alias, const ConditionSqlNode *conditions,
    HavingFilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;
  if (conditions == nullptr) {
    return rc;
  }

  Expression *filter_expr = nullptr;
  rc                      = cond_to_expr(db, default_table, tables, relation_to_alias, conditions, true, filter_expr);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to convert ConditionSqlNode to Expression. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  stmt               = new HavingFilterStmt();
  stmt->filter_expr_ = filter_expr;
  return rc;
}