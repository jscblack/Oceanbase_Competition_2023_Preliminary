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
// Created by Meiyi
//

#pragma once

#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "sql/parser/value.h"
// #include "sql/expr/expression.h"

class Expression;

/**
 * @defgroup SQLParser SQL Parser
 */

/**
 * @brief 描述一个属性
 * @ingroup SQLParser
 * @details 属性，或者说字段(column, field)
 * Rel -> Relation
 * Attr -> Attribute
 */
struct RelAttrSqlNode
{
  std::string relation_name;   ///< relation name (may be NULL) 表名
  std::string attribute_name;  ///< attribute name              属性名
  // std::string aggregation_func;  ///< aggregation function        聚合函数类型 max/min/count/avg/sum
};

/**
 * @brief 描述比较运算符
 * @ingroup SQLParser
 */
enum CompOp
{
  EQUAL_TO,         ///< "="
  LESS_EQUAL,       ///< "<="
  NOT_EQUAL,        ///< "<>"
  LESS_THAN,        ///< "<"
  GREAT_EQUAL,      ///< ">="
  GREAT_THAN,       ///< ">"
  LIKE_ENUM,        ///< "LIKE"
  NOT_LIKE_ENUM,    ///< "NOT LIKE"
  IS_NOT_ENUM,      ///< "IS NOT"
  IS_ENUM,          ///< "IS"
  NOT_IN_ENUM,      ///< "NOT IN"
  IN_ENUM,          ///< "IN"
  NOT_EXISTS_ENUM,  ///< "NOT EXISTS"
  EXISTS_ENUM,      ///< "EXISTS"
  NO_OP
};
/**
 * @brief 描述逻辑运算符
 * @ingroup SQLParser
 */
enum LogiOp
{
  AND_ENUM,  ///< "AND"
  OR_ENUM,   ///< "OR"
  NOT_ENUM,  ///< "NOT"
  NO_LOGI_OP
};

/**
 * @brief 描述算术运算符
 *
 */
enum ArithOp
{
  ADD,
  SUB,
  MUL,
  DIV,
  NEGATIVE,
  POSITIVE,
  // PAREN,  // 括号 似乎用不上
};

/**
 * @brief 描述函数名, 排在UNDEFINE和NO_FUNC之间的是聚集函数, 随后才是其他函数
 * @ingroup SQLParser
 */
enum FuncName
{
  UNDEFINED_FUNC_ENUM,
  COUNT_FUNC_ENUM,  ///< "COUNT"
  AVG_FUNC_ENUM,    ///< "AVG"
  SUM_FUNC_ENUM,    ///< "SUM"
  MAX_FUNC_ENUM,    ///< "MAX"
  MIN_FUNC_ENUM,    ///< "MIN"
  LENGTH_FUNC_NUM,
  ROUND_FUNC_NUM,
  DATE_FUNC_NUM,
  NO_FUNC_ENUM
};

/**
 * @brief 描述条件比较的类型
 * @ingroup SQLParser
 */
enum ConditionSqlNodeType
{
  UNDEFINED_COND_SQL_NODE = -1,  // 没有定义
  VALUE                   = 0,   // 单个Value: 在YACC处转为ValueExpr / ValueListExpr， 对应_value
  FIELD,                         // 单个Field: 在YACC处为RelAttrSqlNode，对应_attr
  SUB_SELECT,                    // 单个子查询: _select
  // 下面大多是binary-node，也有NOT这种unary的，并且会用到 sub_cond
  ARITH,        // 算术表达式: _cond + arith
  COMP,         // 比较表达式: _cond + comp
  FUNC_OR_AGG,  // 函数或聚集表达式 （语法层面无法区分）: _cond + func
  LOGIC         // 逻辑运算表达式: _cond + logi_op
};

struct SelectSqlNode;
/**
 * @brief 表示一个表达式 （沿用旧名叫ConditionSqlNode)
 * @ingroup SQLParser
 * @details 根据left_type和right_type来判断表达式的类型
 * 如果表达式仅为单值，默认只用left_type （同时right_type=UNDEFINED)
 */
// where 1:1 condition
struct ConditionSqlNode
{
  bool                 binary = false;  ///< TRUE 如果有子表达式则为true，如果为单值则为false
  ConditionSqlNodeType type   = UNDEFINED_COND_SQL_NODE;  ///< TRUE if left-hand side is an attribute

  RelAttrSqlNode    attr;                  ///< left-hand side attribute
  SelectSqlNode    *select     = nullptr;  ///< left-hand side select
  Expression       *value      = nullptr;  ///< left-hand side ValueExpr / ExprListExpr?
  ConditionSqlNode *left_cond  = nullptr;  ///< right-hand side sub-cond, 即sub-expr
  ConditionSqlNode *right_cond = nullptr;  ///< right-hand side sub-cond
  std::string       alias      = "";

  CompOp   comp;     ///< comparison operator
  FuncName func;     ///< function operator
  ArithOp  arith;    ///< arithmetic operator
  LogiOp   logi_op;  ///< logic operator
  // ConditionSqlNodeType right_type = UNDEFINED;
  // RelAttrSqlNode       right_attr;              ///< right-hand side attribute if right_is_attr = TRUE 右边的属性
  // SelectSqlNode       *right_select = nullptr;  ///< right-hand side select
  // Expression          *right_value  = nullptr;  ///< left-hand side ValueExpr / ExprListExpr?
  // ConditionSqlNode() = default;
  // ConditionSqlNode(ConditionSqlNode *left, LogiOp op, ConditionSqlNode *right)
  //     : inner_node(true), left_cond(left), logi_op(op), right_cond(right){};
  // ///< 2时，操作符左边是子查询，1时，操作符左边是属性名，0时，是属性值 （旧版ConditionSqlNodeType的注释）
  // 即将废弃的Expression
  // 现阶段 expression里面只包含value
  // Expression    *left_expr = nullptr;    ///< left-hand side value if left_is_attr = FALSE
  // Expression    *right_expr = nullptr;    ///< right-hand side value if right_is_attr = FALSE
};

// /**
//  * @brief 描述ExprNode中表达式的类型
//  * @ingroup SQLParser
//  */
// enum ExprNodeType
// {
//   UNDEFINED = 0,
//   VALUE,        // 单个Value （转成ValueExpr）
//   FIELD,        // 单个Field （转成FieldExpr）
//   ARITH,        // 算术表达式
//   COMP,         // 比较表达式
//   FUNC_OR_AGG,  // 函数或聚集表达式 （语法层面无法区分）
//   SUB_SELECT,   // 子查询表达式
//   LOGIC         // 逻辑运算表达式
// };

// /**
//  * @brief 描述一个表达式
//  * @ingroup SQLParser
//  * @details 现已被ConditionSqlNode替代，理论上更好的写法和命名
//  */
// struct ExprNode
// {
//   ExprNodeType type;
//   ExprNode* left_expr;
//   ExprNode* right_expr;
// };

struct OrderSqlNode
{
  bool           is_asc;  // TRUE if asc （升序）
  RelAttrSqlNode attr;    // 需要排序的属性
};

/**
 * @brief 描述一个select语句
 * @ingroup SQLParser
 * @details 一个正常的select语句描述起来比这个要复杂很多，这里做了简化。
 * 一个select语句由三部分组成，分别是select, from, where。
 * select部分表示要查询的字段，from部分表示要查询的表，where部分表示查询的条件。
 * 比如 from 中可以是多个表，也可以是另一个查询语句，这里仅仅支持表，也就是 relations。
 * where 条件 conditions，这里表示使用AND串联起来多个条件。正常的SQL语句会有OR，NOT等，
 * 甚至可以包含复杂的表达式。
 */

struct SelectSqlNode
{
  std::vector<ConditionSqlNode>                    attributes;            ///< attributes in select clause
  std::vector<std::pair<std::string, std::string>> relation_to_alias;     ///< alias默认为空串
  ConditionSqlNode                                *conditions = nullptr;  ///< 查询条件树
  std::vector<OrderSqlNode>                        orders;                // 排序条件，可能有多列需求
  std::vector<RelAttrSqlNode>                      groups;                ///< 分组的属性
  ConditionSqlNode *havings = nullptr;  ///< 分组筛选条件，同样是使用AND串联起来多个条件

  // std::vector<std::string>                     relations;             ///< 查询的表
  // std::unordered_map<std::string, std::string> alias_to_relation;     ///< 记录alias->relation的映射
  // std::vector<ConditionSqlNode> conditions;  ///< 查询条件，使用AND串联起来多个条件 旧版查询条件
};

/**
 * @brief 算术表达式计算的语法树
 * @ingroup SQLParser
 */
struct CalcSqlNode
{
  std::vector<Expression *> expressions;  ///< calc clause

  ~CalcSqlNode();
};

/**
 * @brief 描述一个insert语句
 * @ingroup SQLParser
 * @details 于Selects类似，也做了很多简化
 */
struct InsertSqlNode
{
  std::string                     relation_name;  ///< Relation to insert into
  std::vector<std::vector<Value>> values;         ///< 要插入的值
};

/**
 * @brief 描述一个delete语句
 * @ingroup SQLParser
 */
struct DeleteSqlNode
{
  std::string       relation_name;  ///< Relation to delete from
  ConditionSqlNode *conditions;     ///< 查询条件树
};

/**
 * @brief 描述一个update的value，用来支持复杂的update语句
 * @ingroup SQLParser
 */
struct ComplexValue
{
  bool          value_from_select;  ///< 是否是子查询，默认false
  Value         literal_value;      ///< value
  SelectSqlNode select_sql;         ///< select clause

  ComplexValue() = default;
  ComplexValue(bool from_select, const Value &value) : value_from_select(from_select), literal_value(value){};
  ComplexValue(bool from_select, const SelectSqlNode &select_sql)
      : value_from_select(from_select), select_sql(select_sql){};
};

/**
 * @brief 描述一个update语句
 * @ingroup SQLParser
 */
struct UpdateSqlNode
{
  std::string               relation_name;    ///< Relation to update
  std::vector<std::string>  attribute_names;  ///< 更新的字段，支持多个字段
  std::vector<ComplexValue> values;           ///< 更新的值，支持多个字段，并且支持简单的子查询
  ConditionSqlNode         *conditions = nullptr;  ///< 查询条件树
};

/**
 * @brief 描述一个属性
 * @ingroup SQLParser
 * @details 属性，或者说字段(column, field)
 * Rel -> Relation
 * Attr -> Attribute
 */
struct AttrInfoSqlNode
{
  AttrType    type;      ///< Type of attribute
  std::string name;      ///< Attribute name
  size_t      length;    ///< Length of attribute
  bool        nullable;  ///< 是否可空
};

/**
 * @brief 描述一个create table语句
 * @ingroup SQLParser
 * @details 这里也做了很多简化。
 */
struct CreateTableSqlNode
{
  std::string                  relation_name;  ///< Relation name
  std::vector<AttrInfoSqlNode> attr_infos;     ///< attributes
};

/**
 * @brief 描述一个drop table语句
 * @ingroup SQLParser
 */
struct DropTableSqlNode
{
  std::string relation_name;  ///< 要删除的表名
};

/**
 * @brief 描述一个create index语句
 * @ingroup SQLParser
 * @details 创建索引时，需要指定索引名，表名，字段名。
 * 正常的SQL语句中，一个索引可能包含了多个字段，这里仅支持一个字段。
 */
struct CreateIndexSqlNode
{
  std::string              index_name;       ///< Index name
  std::string              relation_name;    ///< Relation name
  bool                     is_unique;        ///< 是否为唯一索引(默认false)
  std::vector<std::string> attribute_names;  ///< 支持多索引
};

/**
 * @brief 描述一个drop index语句
 * @ingroup SQLParser
 */
struct DropIndexSqlNode
{
  std::string index_name;     ///< Index name
  std::string relation_name;  ///< Relation name
};

/**
 * @brief 描述一个desc table语句
 * @ingroup SQLParser
 * @details desc table 是查询表结构信息的语句
 */
struct DescTableSqlNode
{
  std::string relation_name;
};

/**
 * @brief 描述一个show index语句
 * @ingroup SQLParser
 * @details show index 是查询表索引信息的语句
 */
struct ShowIndexSqlNode
{
  std::string relation_name;
};

/**
 * @brief 描述一个load data语句
 * @ingroup SQLParser
 * @details 从文件导入数据到表中。文件中的每一行就是一条数据，每行的数据类型、字段个数都与表保持一致
 */
struct LoadDataSqlNode
{
  std::string relation_name;
  std::string file_name;
};

/**
 * @brief 设置变量的值
 * @ingroup SQLParser
 * @note 当前还没有查询变量
 */
struct SetVariableSqlNode
{
  std::string name;
  Value       value;
};

class ParsedSqlNode;

/**
 * @brief 描述一个explain语句
 * @ingroup SQLParser
 * @details 会创建operator的语句，才能用explain输出执行计划。
 * 一个command就是一个语句，比如select语句，insert语句等。
 * 可能改成SqlCommand更合适。
 */
struct ExplainSqlNode
{
  std::unique_ptr<ParsedSqlNode> sql_node;
};

/**
 * @brief 解析SQL语句出现了错误
 * @ingroup SQLParser
 * @details 当前解析时并没有处理错误的行号和列号
 */
struct ErrorSqlNode
{
  std::string error_msg;
  int         line;
  int         column;
};

/**
 * @brief 表示一个SQL语句的类型
 * @ingroup SQLParser
 */
enum SqlCommandFlag
{
  SCF_ERROR = 0,
  SCF_CALC,
  SCF_SELECT,
  SCF_INSERT,
  SCF_UPDATE,
  SCF_DELETE,
  SCF_CREATE_TABLE,
  SCF_DROP_TABLE,
  SCF_CREATE_INDEX,
  SCF_DROP_INDEX,
  SCF_SHOW_INDEX,
  SCF_SYNC,
  SCF_SHOW_TABLES,
  SCF_DESC_TABLE,
  SCF_BEGIN,  ///< 事务开始语句，可以在这里扩展只读事务
  SCF_COMMIT,
  SCF_CLOG_SYNC,
  SCF_ROLLBACK,
  SCF_LOAD_DATA,
  SCF_HELP,
  SCF_EXIT,
  SCF_EXPLAIN,
  SCF_SET_VARIABLE,  ///< 设置变量
};
/**
 * @brief 表示一个SQL语句
 * @ingroup SQLParser
 */
class ParsedSqlNode
{
public:
  enum SqlCommandFlag flag;
  ErrorSqlNode        error;
  CalcSqlNode         calc;
  SelectSqlNode       selection;
  InsertSqlNode       insertion;
  DeleteSqlNode       deletion;
  UpdateSqlNode       update;
  CreateTableSqlNode  create_table;
  DropTableSqlNode    drop_table;
  CreateIndexSqlNode  create_index;
  DropIndexSqlNode    drop_index;
  DescTableSqlNode    desc_table;
  ShowIndexSqlNode    show_index;
  LoadDataSqlNode     load_data;
  ExplainSqlNode      explain;
  SetVariableSqlNode  set_variable;

public:
  ParsedSqlNode();
  explicit ParsedSqlNode(SqlCommandFlag flag);
};

/**
 * @brief 表示语法解析后的数据
 * @ingroup SQLParser
 */
class ParsedSqlResult
{
public:
  void                                         add_sql_node(std::unique_ptr<ParsedSqlNode> sql_node);
  std::vector<std::unique_ptr<ParsedSqlNode>> &sql_nodes() { return sql_nodes_; }

private:
  std::vector<std::unique_ptr<ParsedSqlNode>> sql_nodes_;  // < 这里记录SQL命令。虽然看起来支持多个，但是当前仅处理一个
};
