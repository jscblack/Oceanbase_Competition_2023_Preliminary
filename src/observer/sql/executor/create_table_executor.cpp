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
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/executor/create_table_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/create_table_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC CreateTableExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_TABLE,
      "create table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  CreateTableStmt *create_table_stmt = static_cast<CreateTableStmt *>(stmt);
  if (create_table_stmt->from_select()) {
    RC rc = RC::SUCCESS;
    // Expression *select_expr = create_table_stmt->select_expr();
    SelectExpr *select_expr = dynamic_cast<SelectExpr *>(create_table_stmt->select_expr());
    // 得到事务id
    SessionEvent *session_event = sql_event->session_event();
    Session      *session       = session_event->session();
    Trx          *trx           = session->current_trx();

    std::vector<Tuple *> tuples;
    // 执行这个，拿出东西，然后拼装回现在的逻辑
    rc = select_expr->get_value(tuples, trx);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    // 创建表
    const int attribute_count = static_cast<int>(create_table_stmt->attr_infos().size());

    const char *table_name = create_table_stmt->table_name().c_str();
    rc = session->get_current_db()->create_table(table_name, attribute_count, create_table_stmt->attr_infos().data());
    if (rc != RC::SUCCESS) {
      return rc;
    }
    // 完成数据的插入
    Table *table = session->get_current_db()->find_table(table_name);
    for (auto tuple : tuples) {
      Record             record;
      std::vector<Value> values;
      for (int i = 0; i < tuple->cell_num(); i++) {
        Value tmp_value;
        rc = tuple->cell_at(i, tmp_value);
        values.push_back(tmp_value);
      }
      // 拼装record的过程
      RC rc = table->make_record(static_cast<int>(values.size()), values.data(), record);

      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to make record. rc=%s", strrc(rc));
        return rc;
      }

      rc = trx->insert_record(table, record);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
        return rc;
      }
    }
    return rc;
  }
  const int attribute_count = static_cast<int>(create_table_stmt->attr_infos().size());

  const char *table_name = create_table_stmt->table_name().c_str();
  RC rc = session->get_current_db()->create_table(table_name, attribute_count, create_table_stmt->attr_infos().data());

  return rc;
}