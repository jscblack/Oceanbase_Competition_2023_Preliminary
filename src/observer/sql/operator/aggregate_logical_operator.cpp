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
// Created by tong1heng on 2023/10/03
//

#include "sql/operator/aggregate_logical_operator.h"

AggregateLogicalOperator::AggregateLogicalOperator(const std::vector<Expression *> &fields_expressions,
    const std::vector<Expression *> &group_by_fields_expressions)
    : fields_expressions_(fields_expressions), group_by_fields_expressions_(group_by_fields_expressions)
{}

// AggregateLogicalOperator::AggregateLogicalOperator(const std::vector<std::pair<std::string, Field>> &aggregations,
//     const std::vector<Field> &fields, const std::vector<Expression *> &fields_expressions)
//     : aggregations_(aggregations), fields_(fields), fields_expressions_(fields_expressions)
// {}
