/*-------------------------------------------------------------------------
 *
 * parse_cte.h
 *	  handle MATCH_RECOGNIZE in parser
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_match_recognize.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_RPR_H
#define PARSE_RPR_H

#include "parser/parse_node.h"

extern ParseNamespaceItem *transformMatchRecognizeClause(ParseState *pstate, MatchRecognizeClause *rpr_clause);

#endif							/* PARSE_RPR_H */
