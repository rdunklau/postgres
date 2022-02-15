/*-------------------------------------------------------------------------
 *
 * planrpr.c
 *	  Special planning for MATCH_RECOGNIZE queries
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planrpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "nodes/primnodes.h"
#include "utils/rpr_nfa.h"

typedef enum state_type {
	INIT,
	FINAL,
	NORMAL
} state_type;

typedef enum arc_type {
	EMPTY,
	RPV,
} arc_type;

static rpr_nfa_state * newstate(rpr_nfa* nfa, state_type type);
static rpr_nfa_arc* newarc(rpr_nfa* nfa, rpr_nfa_state *from,
						   rpr_nfa_state *to, int rpvarno);
static void apply_quantifiers(rpr_nfa *nfa, rpr_nfa_state *ls,
							  rpr_nfa_state *rs, int m, int n);

static void dupnfa(rpr_nfa *nfa, rpr_nfa_state *start, rpr_nfa_state *stop,
				   rpr_nfa_state *ls, rpr_nfa_state *rs);
static void duptraverse(rpr_nfa *nfa, rpr_nfa_state *s, rpr_nfa_state *stmp);
static void cleartraverse(rpr_nfa_state *s);
static void moveouts(rpr_nfa *nfa, rpr_nfa_state *old_state, rpr_nfa_state *new_state);
static void moveins(rpr_nfa *nfa, rpr_nfa_state *old_state, rpr_nfa_state *new_state);




static rpr_nfa_state *
newstate(rpr_nfa* nfa, state_type type)
{
	rpr_nfa_state *state = palloc0(sizeof(rpr_nfa_state));
	state->no = list_length(nfa->states);
	state->ins = NIL;
	state->outs = NIL;
	nfa->states = lappend(nfa->states, state);
	state->type = type;
	return state;
}

static rpr_nfa_arc*
newarc(rpr_nfa* nfa, rpr_nfa_state *from,
	   rpr_nfa_state *to, int rpvarno)
{
	rpr_nfa_arc *arc = palloc0(sizeof(rpr_nfa_arc));
	arc->type = rpvarno == 0 ? EMPTY : RPV;
	arc->rpvarno = rpvarno;
	arc->from = from;
	arc->to = to;
	from->outs = lappend(from->outs, arc);
	to->ins = lappend(to->ins, arc);
	return arc;
}

static void cleartraverse(rpr_nfa_state *s)
{
	ListCell *lc;
	rpr_nfa_arc *a;
	if (s->tmp == NULL)
		return;
	s->tmp = NULL;
	foreach(lc, s->outs)
	{
		a = (rpr_nfa_arc *) lfirst(lc);
		cleartraverse(a->to);
	}
}

static void
dupnfa(rpr_nfa *nfa, rpr_nfa_state *start, rpr_nfa_state *stop,
			 rpr_nfa_state *ls, rpr_nfa_state *rs)
{
	if (start == stop)
	{
		newarc(nfa, ls, rs, 0);
		return;
	}

	stop->tmp = (struct rpr_nfa_state *) rs;
	duptraverse(nfa, start, ls);
	stop->tmp = NULL;
	cleartraverse(start);
}

static void
duptraverse(rpr_nfa *nfa, rpr_nfa_state *s, rpr_nfa_state *stmp)
{
	rpr_nfa_arc *a;
	ListCell *lc;
	/* Already visited */
	if (s->tmp != NULL)
		return;
	s->tmp = (struct rpr_nfa_state *) ((stmp == NULL) ? newstate(nfa, NORMAL) : stmp);
	foreach (lc, s->outs)
	{
		a = lfirst(lc);
		duptraverse(nfa, a->to, NULL);
		newarc(nfa, (rpr_nfa_state *) s->tmp, (rpr_nfa_state *) a->to->tmp, a->rpvarno);
	}

}

static void
moveouts(rpr_nfa *nfa, rpr_nfa_state *old_state, rpr_nfa_state *new_state)
{
	ListCell *lc;
	foreach(lc, old_state->outs)
	{
		rpr_nfa_arc *a = lfirst(lc);
		newarc(nfa, new_state, a->to, a->rpvarno);
		/* Fixme: avoid linear search */
		a->to->ins = list_delete_ptr(a->to->ins, a);
		pfree(a);
	}
	list_free(old_state->outs);
	old_state->outs = NIL;
}

static void
moveins(rpr_nfa *nfa, rpr_nfa_state *old_state, rpr_nfa_state *new_state)
{
	ListCell *lc;
	foreach(lc, old_state->ins)
	{
		rpr_nfa_arc *a = lfirst(lc);
		newarc(nfa, a->from, new_state, a->rpvarno);
		/* Fixme: avoid linear search */
		a->from->outs = list_delete_ptr(a->from->outs, a);
		pfree(a);
	}
	list_free(old_state->ins);
	old_state->ins = NIL;
}

static void
apply_quantifiers(rpr_nfa *nfa, rpr_nfa_state *ls,
				  rpr_nfa_state *rs,
				  int m,
				  int n)
{
	rpr_nfa_state *s;
	rpr_nfa_state *s2;
#define SOME 2
#define INF 3
#define REDUCE(x) ((x) == ROWPATTERN_QUANTIFIER_INF ? INF : ((x) > 1 ? SOME : (x)))
#define PAIR(x, y) ((x) * 4 + (y))
	int rm = REDUCE(m);
	int rn = REDUCE(n);

	/* We take care of the "empty" repeat before hand */
	Assert(PAIR(rm, rn) != 0);
	Assert(rn >= rm);
	switch (PAIR(rm, rn))
	{
		case PAIR(0, 1):
			newarc(nfa, ls, rs, 0);
			break;
		case PAIR(0, SOME):
			apply_quantifiers(nfa, ls, rs, 1, n);
			newarc(nfa, ls, rs, 0);
			break;
		case PAIR(0, INF):
			s = newstate(nfa, NORMAL);
			moveouts(nfa, ls, s);
			moveins(nfa, rs, s);
			newarc(nfa, ls, s, 0);
			newarc(nfa, s, rs, 0);
			break;
		case PAIR(1, 1):
			break;
		case PAIR(1, SOME):
			s = newstate(nfa, NORMAL);
			moveouts(nfa, ls, s);
			dupnfa(nfa, s, rs, ls, s);
			apply_quantifiers(nfa, ls, s, 1, n - 1);
			newarc(nfa, ls, s, 0);
			break;
		case PAIR(1, INF):
			s = newstate(nfa, NORMAL);
			s2 = newstate(nfa, NORMAL);
			moveouts(nfa, ls, s);
			moveins(nfa, rs, s2);
			newarc(nfa, ls, s, 0);
			newarc(nfa, s2, rs, 0);
			newarc(nfa, s2, s, 0);
			break;
		case PAIR(SOME, SOME):
			s = newstate(nfa, NORMAL);
			moveouts(nfa, ls, s);
			dupnfa(nfa, s, rs, ls, s);
			apply_quantifiers(nfa, ls, s, m - 1, n - 1);
			break;
		case PAIR(SOME, INF):
			s = newstate(nfa, NORMAL);
			moveouts(nfa, ls, s);
			dupnfa(nfa, s, rs, ls, s);
			apply_quantifiers(nfa, ls, s, m - 1, n);
			break;
		default:
			/* should never happen */
			elog(ERROR, "Invalid quantifier pair: %d %d", m, n);
			break;
	}
}


static void
build_nfa_for_pattern_recurse(RowPattern *pattern, rpr_nfa *nfa,
								   rpr_nfa_state* ls, rpr_nfa_state* rs)
{
	/* Bail out quickly if we have maximum zero occurences */
	if (pattern->quantifier &&
		pattern->quantifier->ub == 0)
	{
		return;
	}

	switch (pattern->kind)
	{
		case ROWPATTERN_VARREF:
			/* An input symbol. */
			{
				RowPatternVarDef *def;
				rpr_nfa_state * state;
				Assert(list_length(pattern->args) == 1);
				def = linitial(pattern->args);

				state = newstate(nfa, NORMAL);
				/* Connect it to the left and right sides */
				newarc(nfa, ls, state, def->varno);
				newarc(nfa, state, rs, 0);

				/* Apply quantifiers */
			}
			break;
		case ROWPATTERN_CONCATENATION:
			/* Concatenation */
			{
				ListCell *lc;
				rpr_nfa_state *concat_ls = newstate(nfa, NORMAL);
				rpr_nfa_state *concat_rs = NULL;
				newarc(nfa, ls, concat_ls, EMPTY);
				foreach(lc, pattern->args)
				{
					concat_rs = newstate(nfa, NORMAL);
					build_nfa_for_pattern_recurse((RowPattern *) lfirst(lc),
												   nfa, concat_ls, concat_rs);
					concat_ls = concat_rs;
				}
				newarc(nfa, concat_rs, rs, EMPTY);
			}
			break;
		case ROWPATTERN_ALTERNATION:
			/* Alternation */
			{
				ListCell *lc;
				rpr_nfa_state *altern_ls = newstate(nfa, NORMAL);
				rpr_nfa_state *altern_rs = newstate(nfa, NORMAL);
				newarc(nfa, ls, altern_ls, EMPTY);
				newarc(nfa, altern_rs, rs, EMPTY);
				foreach(lc, pattern->args)
				{
					build_nfa_for_pattern_recurse((RowPattern *) lfirst(lc),
												  nfa, altern_ls, altern_rs);
				}
				break;
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Row pattern kind not supported: %d", pattern->kind)),
					 errposition(pattern->location));
			break;
	}
	/* Apply the quantifier if any */
	if (pattern->quantifier)
		apply_quantifiers(nfa, ls, rs, pattern->quantifier->lb, pattern->quantifier->ub);
}


static void dump_nfa_recurse(rpr_nfa_state *state, int depth, StringInfo str, RowPatternVarDef **rpvs)
{
	char * statetype;
	ListCell *lc;
	if (state->tmp != NULL)
		return;
	state->tmp = state;
	switch (state->type)
	{
		case NORMAL:
			statetype = "";
			break;
		case INIT:
			statetype = "(I)";
			break;
		case FINAL:
			statetype = "(F)";
			break;
	}
	appendStringInfoSpaces(str, depth);
	appendStringInfo(str, "State %d %s\n",
					 state->no,
					 statetype);
	depth++;
	foreach(lc, state->outs)
	{
		rpr_nfa_arc * a = (rpr_nfa_arc *) lfirst(lc);
		char * rpvname;
		if (a->rpvarno == 0)
			rpvname = "ε";
		else
			rpvname = rpvs[a->rpvarno]->name;
		appendStringInfoSpaces(str, depth);
		if (a->to->tmp == NULL)
		{
			appendStringInfo(str, "%s -> \n", rpvname);
			dump_nfa_recurse(a->to, depth+1, str, rpvs);
		}
		else
		{
			appendStringInfo(str, "%s -> %d\n", rpvname, a->to->no);
		}
	}
}

StringInfo
dump_nfa(rpr_nfa *nfa, RowPatternVarDef **rpvs)
{
	StringInfo str = makeStringInfo();
	dump_nfa_recurse(nfa->init, 0, str, rpvs);
	cleartraverse(nfa->init);
	return str;
}


rpr_nfa*
build_nfa_for_matchrecognize(RowPattern *pattern, RowPatternVarDef **rpvs)
{
	rpr_nfa *nfa = palloc0(sizeof(rpr_nfa));
	/* Build init and final state */
	nfa->init = newstate(nfa, INIT);
	nfa->final = newstate(nfa, FINAL);
	build_nfa_for_pattern_recurse(pattern, nfa, nfa->init, nfa->final);
	printf("%s", dump_nfa(nfa, rpvs)->data);
	return nfa;
}
