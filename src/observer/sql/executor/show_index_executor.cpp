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
// Created by Wangyunlai on 2023/6/14.
//

#include <memory>

#include "sql/executor/show_index_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/operator/string_list_physical_operator.h"
#include "sql/stmt/show_index_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

using namespace std;

RC ShowIndexExecutor::execute(SQLStageEvent *sql_event)
{
  RC            rc            = RC::SUCCESS;
  Stmt         *stmt          = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session      *session       = session_event->session();
  ASSERT(stmt->type() == StmtType::SHOW_INDEX,
      "show index executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  ShowIndexStmt *show_index_stmt = static_cast<ShowIndexStmt *>(stmt);

  SqlResult *sql_result = session_event->sql_result();

  const char *table_name = show_index_stmt->table_name().c_str();

  Db    *db    = session->get_current_db();
  Table *table = db->find_table(table_name);
  if (table != nullptr) {

    TupleSchema tuple_schema;
    tuple_schema.append_cell(TupleCellSpec("", "Table", "Table"));
    tuple_schema.append_cell(TupleCellSpec("", "Non_unique", "Non_unique"));
    tuple_schema.append_cell(TupleCellSpec("", "Key_name", "Key_name"));
    tuple_schema.append_cell(TupleCellSpec("", "Seq_in_index", "Seq_in_index"));
    tuple_schema.append_cell(TupleCellSpec("", "Column_name", "Column_name"));
    sql_result->set_tuple_schema(tuple_schema);

    auto oper = new StringListPhysicalOperator;

    const TableMeta &table_meta = table->table_meta();

    for (int i = 0; i < table_meta.index_num(); i++) {
      const IndexMeta *index_meta = table_meta.index(i);
      oper->append({table_meta.name(),
          index_meta->type() == IndexType::NonUnique ? "1" : "0",// non_unique
          index_meta->name(),
          std::to_string(i + 1),
          index_meta->field()});
    }
    sql_result->set_operator(unique_ptr<PhysicalOperator>(oper));
  } else {
    sql_result->set_return_code(RC::SCHEMA_TABLE_NOT_EXIST);
    sql_result->set_state_string("Table not exists");
  }
  return rc;
}