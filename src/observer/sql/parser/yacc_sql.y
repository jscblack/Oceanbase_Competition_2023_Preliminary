
%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/value.h"
#include "sql/parser/yacc_sql.hpp"
#include "sql/parser/lex_sql.h"
#include "sql/expr/expression.h"

using namespace std;

string token_name(const char *sql_string, YYLTYPE *llocp)
{
  return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, const char *msg)
{
  std::unique_ptr<ParsedSqlNode> error_sql_node = std::make_unique<ParsedSqlNode>(SCF_ERROR);
  error_sql_node->error.error_msg = msg;
  error_sql_node->error.line = llocp->first_line;
  error_sql_node->error.column = llocp->first_column;
  sql_result->add_sql_node(std::move(error_sql_node));
  return 0;
}

ArithmeticExpr *create_arithmetic_expression(ArithOp type,
                                             Expression *left,
                                             Expression *right,
                                             const char *sql_string,
                                             YYLTYPE *llocp)
{
  ArithmeticExpr *expr = new ArithmeticExpr(type, left, right);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

ConditionSqlNode *create_arith_condition(ArithOp type, ConditionSqlNode *left_cond, ConditionSqlNode *right_cond) {
  ConditionSqlNode *ret = new ConditionSqlNode;
  ret->type = ARITH;
  ret->arith = type;
  if(type == ArithOp::NEGATIVE || type == ArithOp::POSITIVE) {
    ret->binary = false;
    ret->left_cond = left_cond;
  } else {
    ret->binary = true;
    ret->left_cond = left_cond;
    ret->right_cond = right_cond;
  }
  return ret;
}

ConditionSqlNode *create_logic_condition(LogiOp op, ConditionSqlNode *left_cond, ConditionSqlNode *right_cond) {
  ConditionSqlNode *ret = new ConditionSqlNode;
  ret->type = LOGIC;
  ret->binary = true;
  ret->logi_op = op;
  ret->left_cond = left_cond;
  ret->right_cond = right_cond;
  return ret;
}

ConditionSqlNode *create_compare_condition(CompOp op, ConditionSqlNode *left_cond, ConditionSqlNode *right_cond) {
  ConditionSqlNode *ret = new ConditionSqlNode;
   ret->type = COMP;
   ret->comp = op;
   if(op == EXISTS_ENUM || op == NOT_EXISTS_ENUM) {
    ret->binary = false;
    ret->left_cond = left_cond;
  } else {
    ret->binary = true;
    ret->left_cond = left_cond;
    ret->right_cond = right_cond;
  }
   return ret;
}


%}

%define api.pure full
%define parse.error verbose
/** 启用位置标识 **/
%locations
%lex-param { yyscan_t scanner }
/** 这些定义了在yyparse函数中的参数 **/
%parse-param { const char * sql_string }
%parse-param { ParsedSqlResult * sql_result }
%parse-param { void * scanner }

//标识tokens
%token  SEMICOLON
        CREATE
        DROP
        TABLE
        TABLES
        INDEX
        CALC
        SELECT
        DESC
        ASC
        AGG_MAX
        AGG_MIN
        AGG_COUNT
        AGG_AVG
        AGG_SUM
        GROUP_BY
        NULLABLE
        UNNULLABLE
        SHOW
        SYNC
        INSERT
        UNIQUE
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        DATE_T
        NULL_T
        TEXT_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        OR
        HAVING
        AND
        SET
        ON
        LOAD
        DATA
        INFILE
        EXPLAIN
        EQ
        LT
        GT
        LE
        GE
        NE
        LIKE
        NOT_LIKE
        IS
        IS_NOT
        INNER_JOIN
        NOT_IN
        IN
        NOT_EXISTS
        EXISTS
        ORDER_BY

/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                   sql_node; 
  ConditionSqlNode *                a_expr; 
  Value *                           value;
  enum CompOp                       comp;
  enum FuncName                     func_name;
  RelAttrSqlNode *                  rel_attr; 
  std::vector<AttrInfoSqlNode> *    attr_infos;
  AttrInfoSqlNode *                 attr_info;
  Expression *                      expression;
  std::vector<Expression *> *       expression_list;
  std::vector<std::vector<Value>> * insert_list;
  std::vector<Value> *              value_list;
  // std::vector<ConditionSqlNode> *   condition_list; //TODO：待检查是否已重构完成
  // ConditionSqlNode *                condition_tree; //TODO：待检查是否已重构完成
  std::vector<OrderSqlNode> *       order_by_list;
  std::vector<ConditionSqlNode> *   a_expr_list;
  std::vector<RelAttrSqlNode> *     rel_attr_list;
  std::vector<std::string> *        relation_list;
  std::pair<std::vector<std::string>,ConditionSqlNode *> * join_list; //TODO：待检查是否已重构完成 // relateion_list + condition_list，
  std::pair<std::vector<std::string>,std::vector<ComplexValue>> * update_expr;
  char *                            string;
  int                               number;
  float                             floats;
  char *                             dates;
  bool                             boolean;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <dates> DATE
%token <string> ID
%token <string> SSS
//非终结符

/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type <number>              type
/* %type <condition>           condition  //TODO：待检查是否已重构完成 */
%type <value>               value
%type <value>               value_with_MINUS
%type <number>              number
/* %type <comp>                comp_op */
%type <func_name>           func_name
%type <func_name>           func_LA
%type <rel_attr>            rel_attr
%type <attr_infos>          attr_def_list
%type <attr_info>           attr_def
%type <insert_list>         insert_list
/* %type <value_list>          value_list */
%type <value_list>          value_list_LA
%type <value_list>          value_list_with_MINUS
%type <a_expr>              value_list_LALR
%type <boolean>             unique_marker
%type <boolean>             asc_or_desc
%type <boolean>             nullable_marker
%type <a_expr>              where 
/* %type <condition_list>      condition_list //TODO：待检查是否已重构完成 */
/* %type <condition_tree>      condition_tree //TODO：待检查是否已重构完成 */
%type <a_expr>              a_expr
%type <a_expr>              c_expr
%type <a_expr>              select_stmt_with_paren
%type <rel_attr_list>       group_by // 传统的attr_list
%type <a_expr>              having 
%type <a_expr_list>         select_attr
%type <update_expr>         update_expr
%type <update_expr>         update_expr_list
%type <relation_list>       rel_list
%type <a_expr>              join_equal
%type <join_list>           join_list
%type <join_list>           join
%type <rel_attr_list>       attr_list // 传统的attr_list
%type <a_expr_list>         a_expr_list
%type <order_by_list>       order
%type <order_by_list>       order_list
%type <a_expr>              function 
%type <expression>          expression 
%type <expression_list>     expression_list 
%type <sql_node>            calc_stmt
%type <sql_node>            select_stmt
%type <sql_node>            insert_stmt
%type <sql_node>            update_stmt
%type <sql_node>            delete_stmt
%type <sql_node>            create_table_stmt
%type <sql_node>            drop_table_stmt
%type <sql_node>            show_tables_stmt
%type <sql_node>            show_index_stmt
%type <sql_node>            desc_table_stmt
%type <sql_node>            create_index_stmt
%type <sql_node>            drop_index_stmt
%type <sql_node>            sync_stmt
%type <sql_node>            begin_stmt
%type <sql_node>            commit_stmt
%type <sql_node>            rollback_stmt
%type <sql_node>            load_data_stmt
%type <sql_node>            explain_stmt
%type <sql_node>            set_variable_stmt
%type <sql_node>            help_stmt
%type <sql_node>            exit_stmt
%type <sql_node>            command_wrapper
// commands should be a list but I use a single command instead
%type <sql_node>            commands


/* %left ON  */
%left OR
%left AND
/* %left NOT */
%left IS IS_NOT
%left EQ LT GT LE GE NE 
%left EXISTS NOT_EXISTS IN NOT_IN
%left LIKE NOT_LIKE
%left '+' '-'
%left '*' '/'
%nonassoc UMINUS
%left INNER_JOIN
/* %left '(' ')' */
%left LBRACE RBRACE
%%

commands: command_wrapper opt_semicolon  //commands or sqls. parser starts here.
  {
    std::unique_ptr<ParsedSqlNode> sql_node = std::unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
  }
  ;

command_wrapper:
    calc_stmt
  | select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  | create_table_stmt
  | drop_table_stmt
  | show_tables_stmt
  | show_index_stmt
  | desc_table_stmt
  | create_index_stmt
  | drop_index_stmt
  | sync_stmt
  | begin_stmt
  | commit_stmt
  | rollback_stmt
  | load_data_stmt
  | explain_stmt
  | set_variable_stmt
  | help_stmt
  | exit_stmt
    ;

exit_stmt:      
    EXIT {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      $$ = new ParsedSqlNode(SCF_EXIT);
    };

help_stmt:
    HELP {
      $$ = new ParsedSqlNode(SCF_HELP);
    };

sync_stmt:
    SYNC {
      $$ = new ParsedSqlNode(SCF_SYNC);
    }
    ;

begin_stmt:
    TRX_BEGIN  {
      $$ = new ParsedSqlNode(SCF_BEGIN);
    }
    ;

commit_stmt:
    TRX_COMMIT {
      $$ = new ParsedSqlNode(SCF_COMMIT);
    }
    ;

rollback_stmt:
    TRX_ROLLBACK  {
      $$ = new ParsedSqlNode(SCF_ROLLBACK);
    }
    ;

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $3;
      free($3);
    };

show_tables_stmt:
    SHOW TABLES {
      $$ = new ParsedSqlNode(SCF_SHOW_TABLES);
    }
    ;

show_index_stmt:
    SHOW INDEX FROM ID {
      $$ = new ParsedSqlNode(SCF_SHOW_INDEX);
      $$->show_index.relation_name = $4;
      free($4);
    }
    ;

desc_table_stmt:
    DESC ID  {
      $$ = new ParsedSqlNode(SCF_DESC_TABLE);
      $$->desc_table.relation_name = $2;
      free($2);
    }
    ;
unique_marker:
    /* empty */
    {
      $$ = false;
    }
    | UNIQUE
    {
      $$ = true;
    }
    ;
create_index_stmt:    /*create index 语句的语法解析树*/
    CREATE unique_marker INDEX ID ON ID LBRACE ID rel_list RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
      CreateIndexSqlNode &create_index = $$->create_index;
      create_index.index_name = $4;
      create_index.is_unique=$2;
      create_index.relation_name = $6;
      if ($9 != nullptr) {
        create_index.attribute_names.swap(*$9);
        delete $9;
      }
      create_index.attribute_names.emplace_back($8);
      std::reverse(create_index.attribute_names.begin(), create_index.attribute_names.end());
      free($4);
      free($6);
      free($8);
    }
    ;
drop_index_stmt:      /*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_INDEX);
      $$->drop_index.index_name = $3;
      $$->drop_index.relation_name = $5;
      free($3);
      free($5);
    }
    ;

nullable_marker:
    /* empty */
    {
      $$ = false;
    }
    | UNNULLABLE
    {
      $$ = false;
    }
    | NULLABLE
    {
      $$ = true;
    }
    ;

create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      free($3);

      std::vector<AttrInfoSqlNode> *src_attrs = $6;

      if (src_attrs != nullptr) {
        create_table.attr_infos.swap(*src_attrs);
      }
      create_table.attr_infos.emplace_back(*$5);
      std::reverse(create_table.attr_infos.begin(), create_table.attr_infos.end());
      delete $5;
    }
    ;
attr_def_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA attr_def attr_def_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<AttrInfoSqlNode>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
    
attr_def:
    ID type nullable_marker LBRACE number RBRACE 
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = $5;
      $$->nullable=$3;
      free($1);
    }
    | ID type nullable_marker
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      if ($$->type == DATES){
        $$->length = 10;//XXXX-XX-XX
      }
      if ($$->type == TEXTS) {
        $$->length = 4096;
      }
      $$->nullable=$3;
      free($1);
    }
    ;
number:
    NUMBER {$$ = $1;}
    ;
type:
    INT_T      { $$=INTS; }
    | STRING_T { $$=CHARS; }
    | TEXT_T   { $$=TEXTS; }
    | FLOAT_T  { $$=FLOATS; }
    | DATE_T  { $$=DATES; }
    ;
insert_stmt:        /*insert   语句的语法解析树*/
    INSERT INTO ID VALUES LBRACE value_with_MINUS value_list_with_MINUS RBRACE insert_list
    {
      $$ = new ParsedSqlNode(SCF_INSERT);
      $$->insertion.relation_name = $3;
      if ($9 != nullptr) {
        $$->insertion.values.swap(*$9);
      }
      if ($7 != nullptr) {
        $7->emplace_back(*$6);
        // reverse the order of values
        std::reverse($7->begin(), $7->end());
        $$->insertion.values.emplace_back(*$7);
        delete $7;
      }else{
        // a tmp vector<vector<Value>> to store the values
        std::vector<Value> *tmp = new std::vector<Value>;
        tmp->emplace_back(*$6);
        $$->insertion.values.emplace_back(*tmp);
        delete tmp;
      }
      std::reverse($$->insertion.values.begin(), $$->insertion.values.end());
      delete $6;
      free($3);
    }
    ;

insert_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA LBRACE value_with_MINUS value_list_with_MINUS RBRACE insert_list {
      if ($6 != nullptr) {
        $$ = $6;
      } else {
        $$ = new std::vector<std::vector<Value>>;
      }
      if($4!=nullptr){
        $4->emplace_back(*$3);
        // reverse the order of values
        std::reverse($4->begin(), $4->end());
        $$->emplace_back(*$4);
        delete $4;
      }else{
        std::vector<Value> *tmp = new std::vector<Value>;
        tmp->emplace_back(*$3);
        $$->emplace_back(*tmp);
        delete tmp;
      }
      delete $3;
      // $$->emplace_back(*$3);
      // delete $3;
    }
    ;

value_list_with_MINUS: // 不可被用作表达式中
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA value_with_MINUS value_list_with_MINUS  { 
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<Value>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
value_with_MINUS:
    NULL_T {
      $$ = new Value();
      $$->set_type(AttrType::NONE);
    }
    |NUMBER {
      $$ = new Value((int)$1);
      @$ = @1;
    }
    | '-' NUMBER {
      $$ = new Value(-(int)$2);
      @$ = @2;
    }
    |FLOAT {
      $$ = new Value((float)$1);
      @$ = @1;
    }
    |DATE {
      char *tmpDate = common::substr($1,1,strlen($1)-2);/*trim the*/
      $$ = new Value(tmpDate);
      free(tmpDate);
    }
    |SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp);
      free(tmp);
      free($1);
    }
    ;
/* value_list:
    // empty 
    {
      $$ = nullptr;
    }
    | COMMA value value_list  { 
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<Value>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ; */
value:
    NULL_T {
      $$ = new Value();
      $$->set_type(AttrType::NONE);
    }
    |NUMBER {
      $$ = new Value((int)$1);
      @$ = @1;
    }
    /* | '-' NUMBER {
      $$ = new Value(-(int)$2);
      @$ = @2;
    } */
    |FLOAT {
      $$ = new Value((float)$1);
      @$ = @1;
    }
    |DATE {
      char *tmpDate = common::substr($1,1,strlen($1)-2);/*trim the*/
      $$ = new Value(tmpDate);
      free(tmpDate);
    }
    |SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp);
      free(tmp);
      free($1);
    }
    ;
    
delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      $$->deletion.relation_name = $3;
      if ($4 != nullptr) {
        $$->deletion.conditions = $4;
      }
      free($3);
    }
    ;
update_stmt:      /*  update 语句的语法解析树*/
    UPDATE ID SET update_expr where {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.attribute_names = $4->first;
      $$->update.values = $4->second;
      free($4);
      $$->update.relation_name = $2;
      std::reverse($$->update.attribute_names.begin(), $$->update.attribute_names.end());
      std::reverse($$->update.values.begin(), $$->update.values.end());
      if ($5 != nullptr) {
        $$->update.conditions = $5;
      }
      free($2);
    }
    ;

update_expr:
    ID EQ LBRACE select_stmt RBRACE update_expr_list
    {
      // TODO: 
      if($6 != nullptr){
        $$ = $6;
      }else{
        $$ = new std::pair<std::vector<std::string>,std::vector<ComplexValue>>;
      }
      $$->first.emplace_back($1);
      $$->second.emplace_back(true,$4->selection);
      free($1);
      delete $4;
    }
    | ID EQ value update_expr_list {
      if($4 != nullptr){
        $$ = $4;
      }else{
        $$ = new std::pair<std::vector<std::string>,std::vector<ComplexValue>>;
      }
      $$->first.emplace_back($1);
      $$->second.emplace_back(false,*$3);
      free($1);
      delete $3;
    }
    ;

update_expr_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA ID EQ LBRACE select_stmt RBRACE update_expr_list {
       // TODO
        if($7 != nullptr){
          $$ = $7;
        }else{
          $$ = new std::pair<std::vector<std::string>,std::vector<ComplexValue>>;
        }
        $$->first.emplace_back($2);
        $$->second.emplace_back(true,$5->selection);
        free($2);
        delete $5;
    }
    | COMMA ID EQ value update_expr_list {
        if($5 != nullptr){
          $$ = $5;
        }else{
          $$ = new std::pair<std::vector<std::string>,std::vector<ComplexValue>>;
        }
        $$->first.emplace_back($2);
        $$->second.emplace_back(false,*$4);
        free($2);
        delete $4;
    }
    ;

select_stmt:        /*  select 语句的语法解析树*/
    SELECT select_attr FROM ID rel_list where order group_by having
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) {
        $$->selection.attributes.swap(*$2);
        delete $2;
      }
      if ($5 != nullptr) {
        LOG_INFO("rel_list not here");
        $$->selection.relations.swap(*$5);
        delete $5;
      }
      LOG_INFO("id:=%s", $4);
      $$->selection.relations.push_back($4);
      std::reverse($$->selection.relations.begin(), $$->selection.relations.end());
 
      if ($6 != nullptr) {
        $$->selection.conditions = $6;
      }
      if ($7 != nullptr) {
        $$->selection.orders.swap(*$7);
        delete $7;
      }

      if ($8 != nullptr) {
        $$->selection.groups.swap(*$8);
        std::reverse($$->selection.groups.begin(), $$->selection.groups.end());
        delete $8;
      }

      if ($9 != nullptr) {
        // $$->selection.havings.swap(*$9);
        $$->selection.havings = $9;
        // delete $9;
      }

      free($4);
    }
    | SELECT select_attr FROM join where order group_by having
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) {
        $$->selection.attributes.swap(*$2);
        delete $2;
      }
      // 把id挂在where下
      if ($4 != nullptr) {
        $$->selection.relations.swap($4->first);
        std::reverse($$->selection.relations.begin(), $$->selection.relations.end());
        $$->selection.conditions = $4->second;
      }
      // 在现有where的基础上加上过滤条件
      if($5 != nullptr) {
        if($$->selection.conditions==nullptr){
          $$->selection.conditions = $5;
        }else{
          $$->selection.conditions = create_logic_condition(LogiOp::AND_ENUM,$$->selection.conditions,$5);
        }
      }
      if ($6 != nullptr) {
        $$->selection.orders.swap(*$6);
        delete $6;
      }

      if($7 != nullptr) {
        $$->selection.groups.swap(*$7);
        std::reverse($$->selection.groups.begin(), $$->selection.groups.end());
        delete $7;
      }

      if ($8 != nullptr) {
        // $$->selection.havings.swap(*$8);
        $$->selection.havings = $8;
        // delete $8;
      }
    }
    ;
join:
    ID INNER_JOIN ID join_equal join_list
    {
      if($5 != nullptr) {
        $$ = $5;
        $$->second = create_logic_condition(LogiOp::AND_ENUM,$5->second,$4);
      } else {
        $$ = new std::pair<std::vector<std::string>,ConditionSqlNode*>;
        $$->second = $4;
      }
      $$->first.emplace_back($3);
      free($3);
      $$->first.emplace_back($1);
      free($1);
    }
    ;
join_list:
    INNER_JOIN ID join_equal join_list
    {
      if($4 != nullptr) {
        $$ = $4;
        $$->second = create_logic_condition(LogiOp::AND_ENUM,$4->second,$3);
      } else {
        $$ = new std::pair<std::vector<std::string>,ConditionSqlNode*>;
        $$->second = $3;
      }
      $$->first.emplace_back($2);
      free($2);
    }
    | /* empty */
    {
      $$ = nullptr;
    }
    ;
join_equal:
    /* empty */
    {
      $$ = nullptr;
    }
    | ON a_expr
    {
      $$ = $2;
    }
    ;
calc_stmt: 
    CALC expression_list
    {
      $$ = new ParsedSqlNode(SCF_CALC);
      std::reverse($2->begin(), $2->end());
      $$->calc.expressions.swap(*$2);
      delete $2;
    }
    ;
expression_list: //保留给CALC
    expression
    {
      $$ = new std::vector<Expression*>;
      $$->emplace_back($1);
    }
    | expression COMMA expression_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<Expression *>;
      }
      $$->emplace_back($1);
    }
    ;
expression: // 保留给CALC
    expression '+' expression {
      $$ = create_arithmetic_expression(ArithOp::ADD, $1, $3, sql_string, &@$);
    }
    | expression '-' expression {
      $$ = create_arithmetic_expression(ArithOp::SUB, $1, $3, sql_string, &@$);
    }
    | expression '*' expression {
      $$ = create_arithmetic_expression(ArithOp::MUL, $1, $3, sql_string, &@$);
    }
    | expression '/' expression {
      $$ = create_arithmetic_expression(ArithOp::DIV, $1, $3, sql_string, &@$);
    }
    | LBRACE expression RBRACE {
      $$ = $2;
      // if($2->size()==1) {
      //   $$ = $2->at(0);
      //   $$->set_name(token_name(sql_string, &@$));
      // }
      //  else {
        // 之前用在condition中，括号内是expression_list，现在废弃，因为expression仅用在CALC
        // // 这个时候应该处理ValueListExpr
        // std::vector<Value> values;
        // values.resize($2->size());
        // for (int i = 0; i < $2->size(); i++) {
        //   // assert($2->at(i)->type() == ExprType::VALUE)
        //   $2->at(i)->try_get_value(values[i]);
        // }
        // $$ = new ValueListExpr(values);
        // $$->set_name(token_name(sql_string, &@$));
      // }
    }
    | value {
      // single value
      $$ = new ValueExpr(*$1);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | '-' expression %prec UMINUS {
      $$ = create_arithmetic_expression(ArithOp::NEGATIVE, $2, nullptr, sql_string, &@$);
    }
    ;

a_expr:
    value {
      // single value
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = VALUE;
      $$->value = new ValueExpr(*$1);
      $$->value->set_name(token_name(sql_string, &@$));

      delete $1;
    }
    | rel_attr {
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = FIELD;
      $$->attr = *$1;

      delete $1;
    }
    | select_stmt_with_paren {
      $$ = $1;
    }
    | c_expr {
      $$ = $1;
    }
    | '-' a_expr %prec UMINUS {
      $$ = create_arith_condition(NEGATIVE, $2, nullptr);
    }
    | '+' a_expr %prec UMINUS { //TODO 原本没有要求的补充实现，物理层面尚未实现
      $$ = create_arith_condition(POSITIVE, $2, nullptr);
    } 
    | a_expr '+' a_expr {
      $$ = create_arith_condition(ADD, $1, $3);
    }
    | a_expr '-' a_expr {
      $$ = create_arith_condition(SUB, $1, $3);
      // $$ = create_arithmetic_expression(ArithOpSUB, $1, $3, sql_string, &@$);
    }
    | a_expr '*' a_expr {
      $$ = create_arith_condition(MUL, $1, $3);
      // $$ = create_arithmetic_expression(ArithOpMUL, $1, $3, sql_string, &@$);
    }
    | a_expr '/' a_expr {
      $$ = create_arith_condition(DIV, $1, $3);
      // $$ = create_arithmetic_expression(ArithOpDIV, $1, $3, sql_string, &@$);
    }
    | a_expr AND a_expr {
      $$ = create_logic_condition(AND_ENUM, $1, $3);
    }
    | a_expr OR a_expr {
      $$ = create_logic_condition(OR_ENUM, $1, $3);
    }
    | a_expr EQ a_expr {
      $$ = create_compare_condition(EQUAL_TO, $1, $3);
    }
    | a_expr LT a_expr {
      $$ = create_compare_condition(LESS_THAN, $1, $3);
    }
    | a_expr GT a_expr {
      $$ = create_compare_condition(GREAT_THAN, $1, $3);
    }
    | a_expr LE a_expr {
      $$ = create_compare_condition(LESS_EQUAL, $1, $3);
    }
    | a_expr GE a_expr {
      $$ = create_compare_condition(GREAT_EQUAL, $1, $3);
    }
    | a_expr NE a_expr {
      $$ = create_compare_condition(NOT_EQUAL, $1, $3);
    }
    | a_expr LIKE a_expr {
      $$ = create_compare_condition(LIKE_ENUM, $1, $3);
    }
    | a_expr NOT_LIKE a_expr {
      $$ = create_compare_condition(NOT_LIKE_ENUM, $1, $3);
    }
    | a_expr IS_NOT a_expr {
      $$ = create_compare_condition(IS_NOT_ENUM, $1, $3);
    }
    | a_expr IS a_expr {
      $$ = create_compare_condition(IS_ENUM, $1, $3);
    }
    | a_expr NOT_IN a_expr {
      $$ = create_compare_condition(NOT_IN_ENUM, $1, $3);
    }
    | a_expr IN a_expr {
      $$ = create_compare_condition(IN_ENUM, $1, $3);
    }
    | EXISTS LBRACE select_stmt RBRACE {
      // ASSERT(SUB_SELECT == $2->type, "EXIST(a_expr), a_expr must be sub_select");
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = COMP;
      $$->comp = EXISTS_ENUM;

      ConditionSqlNode * sub = new ConditionSqlNode;
      $$->binary = false;
      $$->type = SUB_SELECT;
      $$->select = &($3->selection);
      $$->left_cond = sub;
    }
    | NOT_EXISTS LBRACE select_stmt RBRACE {
      // ASSERT(SUB_SELECT == $2->type, "NOT EXIST(a_expr), a_expr must be sub_select");
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = COMP;
      $$->comp = NOT_EXISTS_ENUM;

      ConditionSqlNode * sub = new ConditionSqlNode;
      $$->binary = false;
      $$->type = SUB_SELECT;
      $$->select = &($3->selection);
      $$->left_cond = sub;
    } 
    /* | a_expr comp_op a_expr %prec COMPARE { // 正规fix是需要展开正规comp_op, 否则无法知道优先级
      $$ = new ConditionSqlNode;
      $$->binary = true;
      $$->type = COMP;
      $$->comp = $2;
      $$->left_cond = $1;
      $$->right_cond = $3;
    } */
    ;
c_expr:
    LBRACE a_expr RBRACE {
      $$ = $2;
    } 
    | function {
      $$ = $1;
    }
    | value_list_LALR %prec UMINUS {
      $$ = $1;
    } 
    ;
value_list_LALR:
    LBRACE value_list_LA RBRACE { //简单fix括号列表, 优先级比普通的括号更高
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = VALUE;
      $$->value = new ValueListExpr(*$2);
      $$->value->set_name(token_name(sql_string, &@$));
      
      delete $2;
    }
    ;
select_stmt_with_paren:
    LBRACE select_stmt RBRACE {
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = SUB_SELECT;
      $$->select = &($2->selection);
    }
    ; 
value_list_LA: 
    value_list_LA COMMA value {
      $$ = $1;
      $$->emplace_back(*$3);
      delete $3;
    }
    | value COMMA value {
      $$ = new std::vector<Value>;
      $$->emplace_back(*$1);
      $$->emplace_back(*$3);
      delete $1;
      delete $3;
    }
    ;
select_attr:
    '*' a_expr_list{
      if ($2 != nullptr) {
        $$ = $2;
      } else {
        $$ = new std::vector<ConditionSqlNode>;
      }

      ConditionSqlNode attr;
      attr.binary = false;
      attr.type = FIELD;
      attr.attr.relation_name = "";
      attr.attr.attribute_name = "*";

      $$->emplace_back(attr);
    }
    | a_expr a_expr_list {
      if ($2 != nullptr) {
        $$ = $2;
      } else {
        $$ = new std::vector<ConditionSqlNode>;
      }

      $$->emplace_back(*$1);
      delete $1;
    }
    ;
// 是否为合法的聚集表达式，将在Resolve阶段判断
// TODO: 尚未完成列表参数(参数是表达式列表)的部分
function: // 特殊的表达式，可能有括号内列表，注意无法在此生成Expression，可能有field需要后面才能知道 
    func_LA a_expr RBRACE { 
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = FUNC_OR_AGG;
      $$->func = $1;
      $$->left_cond = $2;
    }
    | func_LA '*' RBRACE {  
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = FUNC_OR_AGG;
      $$->func = $1;

      ConditionSqlNode *sub_attr = new ConditionSqlNode;
      sub_attr->binary = false;
      sub_attr->type = FIELD;
      sub_attr->attr.relation_name = "";
      sub_attr->attr.attribute_name = "*";
      $$->left_cond = sub_attr;
    }
    /* | AGG_COUNT LBRACE NUMBER RBRACE { // FIXME: count(1) 和 count(*) 好像有差别 // 会有移进规约冲突 因为a_expr也可以是NUMBER，所以在后面解决
      $$ = new ConditionSqlNode;
      $$->binary = false;
      $$->type = FUNC_OR_AGG;
      $$->func = COUNT;

      ConditionSqlNode * sub_attr = new ConditionSqlNode;
      sub_attr->binary = false;
      sub_attr->type = FIELD;
      sub_attr->attr.relation_name = "";
      sub_attr->attr.attribute_name = "*";
      $$->left_cond = sub_attr;
    } */
    ;
func_LA:
    func_name LBRACE {
      $$ = $1;
    }
    ;
func_name: 
    AGG_MAX  {
      $$ = MAX_FUNC_ENUM;
    } 
    | AGG_MIN {
      $$ = MIN_FUNC_ENUM;
    } 
    | AGG_COUNT {
      $$ = COUNT_FUNC_ENUM;
    }
    | AGG_AVG {
      $$ = AVG_FUNC_ENUM;
    }
    | AGG_SUM {
      $$ = SUM_FUNC_ENUM;
    }
    ;
rel_attr:
    ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      free($1);
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      free($1);
      free($3);
    }
    ;
attr_list: // group-by等会使用到，旧版的attr_list
    /* empty */ {
      $$ = nullptr;
    }
    | COMMA rel_attr attr_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<RelAttrSqlNode>;
      }

      $$->emplace_back(*$2);
      delete $2;
    }
    ;
a_expr_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA a_expr a_expr_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<ConditionSqlNode>;
      }

      $$->emplace_back(*$2); // 最后再reverse顺序(例如select_attr)
      delete $2;
    }
    ;
rel_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA ID rel_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<std::string>;
      }

      $$->push_back($2);
      free($2);
    }
    ;

group_by:
    {
      $$ = nullptr;
    }
    | GROUP_BY rel_attr attr_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<RelAttrSqlNode>;
      }

      $$->emplace_back(*$2);
      delete $2;
    }
    ;
having:
    {
      $$ = nullptr;
    }
    | HAVING a_expr {
      $$ = $2;
    }
    ;
order:
    /* empty */
    {
      $$ = nullptr;
    }
    | ORDER_BY rel_attr asc_or_desc order_list {
      if($4 != nullptr) {
        $$ = $4;
      } else{
        $$ = new std::vector<OrderSqlNode>;
      }
      OrderSqlNode tmp;
      tmp.is_asc = $3;
      tmp.attr = *$2;
      $$->emplace_back(tmp);
      delete $2;
    }
    ;
order_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA rel_attr asc_or_desc order_list {
      if($4 != nullptr) {
        $$ = $4;
      } else{
        $$ = new std::vector<OrderSqlNode>;
      }
      OrderSqlNode tmp;
      tmp.is_asc = $3;
      tmp.attr = *$2;
      $$->emplace_back(tmp);
      delete $2;
    }
    ;
asc_or_desc:
    /* empty */ 
    {
      $$ = true;
    } 
    | ASC {
      $$ = true;
    }
    | DESC {
      $$ = false;
    }
where:
    /* empty */
    {
      $$ = nullptr;
    }
    | WHERE a_expr {
      $$ = $2;  
    }
    ;
/* condition_list:
    // empty 
    {
      $$ = nullptr;
    }
    | condition {
      $$ = new std::vector<ConditionSqlNode>;
      $$->emplace_back(*$1);
      delete $1;
    }
    | condition AND condition_list {
      $$ = $3;
      $$->emplace_back(*$1);
      delete $1;
    }
    ; */
/* condition_tree:
    // empty 
    {
      $$ = nullptr;
    }
    | condition {
      $$ = $1;
    }
    | LBRACE condition_tree RBRACE {
      $$ = $2;
    }
    | condition_tree AND condition_tree {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_EXPR;// 防御性编程，防止被认为是其他的类型
      $$->left_cond = $1;
      $$->right_type = SUB_EXPR;
      $$->right_cond = $3;
      $$->logi_op = AND_ENUM;
    }
    | condition_tree OR condition_tree {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_EXPR;
      $$->left_cond = $1;
      $$->right_type = SUB_EXPR;
      $$->right_cond = $3;
      $$->logi_op = OR_ENUM;
    }
    ; */
/* condition:
    EXISTS LBRACE select_stmt RBRACE {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = VALUE;
      $$->left_expr=new ValueExpr();
      $$->right_type = SUB_SELECT;
      $$->right_select = &($3->selection);
      $$->comp = EXISTS_ENUM;
    }
    | NOT_EXISTS LBRACE select_stmt RBRACE {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = VALUE;
      $$->left_expr = new ValueExpr();
      $$->right_type = SUB_SELECT;
      $$->right_select = &($3->selection);
      $$->comp = NOT_EXISTS_ENUM;
    }
    | LBRACE select_stmt RBRACE comp_op LBRACE select_stmt RBRACE
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_SELECT;
      $$->left_select = &($2->selection);
      $$->right_type = SUB_SELECT;
      $$->right_select = &($6->selection);
      $$->comp = $4;
    }
    | rel_attr comp_op LBRACE select_stmt RBRACE
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = FIELD;
      $$->left_attr = *$1;
      $$->right_type = SUB_SELECT;
      $$->right_select = &($4->selection);
      $$->comp = $2;

      delete $1;
    }
    | LBRACE select_stmt RBRACE comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_SELECT;
      $$->left_select = &($2->selection);
      $$->right_type = FIELD;
      $$->right_attr = *$5;
      $$->comp = $4;

      delete $5;
    }
    | expression comp_op LBRACE select_stmt RBRACE
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_COND;
      $$->left_expr = $1;
      $$->right_type = SUB_SELECT;
      $$->right_select = &($4->selection);
      $$->comp = $2;
    }
    | LBRACE select_stmt RBRACE comp_op expression
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_SELECT;
      $$->left_select = &($2->selection);
      $$->right_type = SUB_COND;
      $$->right_expr = $5;
      $$->comp = $4;
    }
    | rel_attr comp_op expression
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = FIELD;
      $$->left_attr = *$1;
      $$->right_type = SUB_COND;
      $$->right_expr = $3;
      $$->comp = $2;

      delete $1;
    }
    | expression comp_op expression 
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_COND;
      $$->left_expr = $1;
      $$->right_type = SUB_COND;
      $$->right_expr = $3;
      $$->comp = $2;
    }
    | rel_attr comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = FIELD;
      $$->left_attr = *$1;
      $$->right_type = FIELD;
      $$->right_attr = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | expression comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->inner_node = true;
      $$->left_type = SUB_COND;
      $$->left_expr = $1;
      $$->right_type = FIELD;
      $$->right_attr = *$3;
      $$->comp = $2;

      delete $3;
    }
    ; */
/* comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
    | LIKE { $$ = LIKE_ENUM; }
    | NOT_LIKE { $$ = NOT_LIKE_ENUM; }
    | IS_NOT { $$ = IS_NOT_ENUM; }
    | IS { $$ = IS_ENUM; }
    | EXISTS { $$ = EXISTS_ENUM; }
    | NOT_EXISTS { $$ = NOT_EXISTS_ENUM; }
    | NOT_IN { $$ = NOT_IN_ENUM; }
    | IN { $$ = IN_ENUM; }
    ; */

load_data_stmt:
    LOAD DATA INFILE SSS INTO TABLE ID 
    {
      char *tmp_file_name = common::substr($4, 1, strlen($4) - 2);
      
      $$ = new ParsedSqlNode(SCF_LOAD_DATA);
      $$->load_data.relation_name = $7;
      $$->load_data.file_name = tmp_file_name;
      free($7);
      free(tmp_file_name);
      free($4);
    }
    ;

explain_stmt:
    EXPLAIN command_wrapper
    {
      $$ = new ParsedSqlNode(SCF_EXPLAIN);
      $$->explain.sql_node = std::unique_ptr<ParsedSqlNode>($2);
    }
    ;

set_variable_stmt:
    SET ID EQ value
    {
      $$ = new ParsedSqlNode(SCF_SET_VARIABLE);
      $$->set_variable.name  = $2;
      $$->set_variable.value = *$4;
      free($2);
      delete $4;
    }
    ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
  yyscan_t scanner;
  yylex_init(&scanner);
  scan_string(s, scanner);
  int result = yyparse(s, sql_result, scanner);
  yylex_destroy(scanner);
  return result;
}
