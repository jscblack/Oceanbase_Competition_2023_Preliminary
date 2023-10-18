#!/bin/bash
flex --outfile lex_sql.cpp --header-file=lex_sql.h lex_sql.l
`which bison` --report=lookahead,solved -d --output yacc_sql.cpp yacc_sql.y --verbose -Wcounterexamples