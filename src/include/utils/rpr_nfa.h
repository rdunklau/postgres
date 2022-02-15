/*-------------------------------------------------------------------------
 *
 * rpr_nfa.h
 *	  Interface for building and executing an NFA in RPR.
 *
 *	  This looks a lot like what we use in the regex package, but isn
 *	  signifantly simpler, as we don't need LACON, or any advanced coloring
 *	  support.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/include/utils/rpr_nfa.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_RPR_NFA_H
#define PG_RPR_NFA_H
#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"

typedef struct rpr_nfa_state
{
	int	no; /* state number */
	int type;
	List	*ins;
	List	*outs;
	struct rpr_nfa_state *tmp;
} rpr_nfa_state;

typedef struct rpr_nfa_arc
{
	int	type;
	int	rpvarno;
	rpr_nfa_state *from;
	rpr_nfa_state *to;
} rpr_nfa_arc;

typedef struct rpr_nfa
{
	rpr_nfa_state *init;
	rpr_nfa_state *final;
	List *states;
} rpr_nfa;

rpr_nfa* build_nfa_for_matchrecognize(RowPattern *pattern, RowPatternVarDef **rpv);

#endif							/* PG_RPR_NFA_H */



