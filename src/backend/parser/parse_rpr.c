/*-------------------------------------------------------------------------
 *
 * parse_rpr.c
 *	  handle match recognize (Row Patter Recognition) in parser
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_rpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "optimizer/optimizer.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_rpr.h"
#include "parser/parse_target.h"

static void check_match_recognize_nesting(ParseState *pstate,
										  MatchRecognizeClause *mr_clause);
static RowPatternVar *findRowPatternVar(char *name,
										List **rowpatternvariables,
										bool match_universal);
static RowPattern *transformPatternRecurse(ParseState *pstate,
										   RowPattern * pattern,
										   List** rowpatternvariables);
static void transformRowPatternVariables(ParseState *pstate,
										  MatchRecognize *mr,
										  ParseNamespaceItem *input_ns);
static ParseNamespaceItem * addRangeTableEntryForRPV(ParseState *pstate,
													 RowPatternVar *var,
													 ParseNamespaceItem *input_ns,
													 Index urpv_index);
static ParseNamespaceItem *process_match_recognize_inputrel(ParseState *pstate,
															MatchRecognizeClause *mrclause,
															MatchRecognize *mr);




static ParseNamespaceItem *
addRangeTableEntryForRPV(ParseState *pstate, RowPatternVar *rpv, ParseNamespaceItem *input_ns, Index urpv_index)
{
	RangeTblEntry *rte;
	char *refname = rpv->name;
	int nbcolumns = list_length(input_ns->p_names->colnames);
	ParseNamespaceItem *nsitem;
	Index rtindex;
	Assert(pstate != NULL);
	/* All universal RPVs are merged to the same RTE */
	if (rpv->expr == NULL && urpv_index > 0)
	{
		rte = list_nth(pstate->p_rtable, urpv_index - 1);
		rtindex = urpv_index;
	}
	else {
		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_ROW_PATTERN_VAR;
		rte->relid = InvalidOid;
		rte->subquery = NULL;
		rte->alias = NULL;
		rte->eref = makeAlias(refname, list_copy_deep(input_ns->p_names->colnames));
		rte->lateral = false;
		rte->inh = false;
		rte->inFromCl = false;
		rte->selectedCols = NULL;
		rte->insertedCols = NULL;
		rte->updatedCols = NULL;
		rte->extraUpdatedCols = NULL;
		pstate->p_rtable = lappend(pstate->p_rtable, rte);
		rtindex = list_length(pstate->p_rtable);
	}
	/* The columns from the RPV are actually the same as the ones from the
	 * input relation, we adjust them to the new varno.
	 */
	nsitem = (ParseNamespaceItem *) palloc0(sizeof(ParseNamespaceItem));
	nsitem->p_rel_visible = true;
	nsitem->p_names = makeAlias(rpv->name, rte->eref->colnames);
	nsitem->p_rte = rte;
	nsitem->p_rtindex = rtindex;
	nsitem->p_nscolumns = (ParseNamespaceColumn *) palloc(nbcolumns * sizeof(ParseNamespaceColumn));
	memcpy(nsitem->p_nscolumns, input_ns->p_nscolumns, nbcolumns * sizeof(ParseNamespaceColumn));
	for(int i=0; i < list_length(input_ns->p_names->colnames); i++)
	{
		nsitem->p_nscolumns[i].p_varno = rtindex;
	}
	return nsitem;
}

/* Extract all row pattern variables in the clause, and build RTE for them.
 * We return the list of transformed rpvs.
 */
static void
transformRowPatternVariables(ParseState *pstate, MatchRecognize *mr, ParseNamespaceItem *input_ns)
{
	List *rpvs = NIL;
	ListCell* lc;
	RowPatternVar *rpv;
	ParseNamespaceItem *defaultns;

	/* First add an RTE and an anonymous namespace for the default universal RPV.
	 * The RTE itself will be reused by other universal RPVs.
	 */
	rpv = makeNode(RowPatternVar);
	rpv->name = "";
	rpv->expr = NULL;
	rpvs = lappend(rpvs, rpv);
	defaultns = addRangeTableEntryForRPV(pstate, rpv, input_ns, 0);
	defaultns->p_rel_visible = false;
	defaultns->p_cols_visible = true;
	pstate->p_namespace = lappend(pstate->p_namespace, defaultns);


	/* Extract all row pattern variables from the define, and subset 
	 * clauses, and add a new RTE and nsitem for them.
	 * We do not transform the expressions themselves yet, as they can reference
	 * each other.
	 */
	foreach(lc, mr->rowpatternvariables)
	{
		rpv = lfirst(lc);
		rpvs = lappend(rpvs, rpv);
		pstate->p_namespace = lappend(pstate->p_namespace,
									  addRangeTableEntryForRPV(pstate, rpv, input_ns, defaultns->p_rtindex));
	}

	/* Now that the proper namespaces have been set up, we can transform them.
	 */
	foreach(lc, rpvs)
	{
		RowPatternVar *rpv = lfirst(lc);
		rpv->expr = transformExpr(pstate, rpv->expr, EXPR_KIND_MATCH_RECOGNIZE);
	}
}


/* Per the standard, we are not allowed to nest a match recognize inside another one. */
static void
check_match_recognize_nesting(ParseState *pstate, MatchRecognizeClause *mr_clause)
{
	while (pstate != NULL)
	{
		if (pstate->p_expr_kind == EXPR_KIND_MATCH_RECOGNIZE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("nesting MATCH_RECOGNIZE clauses is prohibited"),
					 parser_errposition(pstate, mr_clause->location)));
		pstate = pstate->parentParseState;
	}
}

/* 
 * Process everything related to the match reocgnize input rel.
 * This includes:
 *  - transforming the input rel
 *  - wrapping it in a subquery
 *  - transforming the PARTITION and ORDER BY clauses
 */
static ParseNamespaceItem *
process_match_recognize_inputrel(ParseState *pstate, MatchRecognizeClause *mr_clause, MatchRecognize *mr)
{
	Query * query = makeNode(Query);
	ParseNamespaceItem *input_nsitem;
	ParseState *input_pstate = make_parsestate(pstate);

	/* Transform the input relation */
	transformFromClause(input_pstate, list_make1(mr_clause->relation));
	/* The input namespace should consist of only one rel as we cannot have
	 * joins in here.
	 */
	Assert(list_length(input_pstate->p_namespace) == 1);
	input_nsitem = linitial(input_pstate->p_namespace);
	
	query = makeNode(Query);
	query->commandType = CMD_SELECT;
	query->targetList = ExpandAllTables(input_pstate, 0);
	query->rtable = input_pstate->p_rtable;
	query->jointree = makeFromExpr(input_pstate->p_joinlist, NULL);


	mr->orderClause = transformSortClause(input_pstate,
									  mr_clause->sortClause,
									  &query->targetList,
									  EXPR_KIND_MATCH_RECOGNIZE,
									  true);

	mr->subquery = (Node *) query;
	mr->partitionClause = transformGroupClause(input_pstate,
											  mr_clause->partitionClause,
											  NULL,
											  &query->targetList,
											  mr->orderClause,
											  EXPR_KIND_MATCH_RECOGNIZE,
											  true);

	pfree(input_pstate);
	return input_nsitem;
}

/*
 * transformMatchRecognizeClause -
 *	Transform a raw MatchRecognizeClause into a MatchRecognize.
 *	The input relation is transformed into a subquery SELECT ... GROUP BY ..
 *	ORDER BY.
 * */
ParseNamespaceItem *
transformMatchRecognizeClause(ParseState *pstate, MatchRecognizeClause *mr_clause)
{
	ListCell *lc;
	List *target_list = NIL;
	MatchRecognize *m = makeNode(MatchRecognize);
	ParseNamespaceItem *input_nsitem;
	ParseState *inner_pstate = make_parsestate(pstate);
	Query *query;

	check_match_recognize_nesting(pstate, mr_clause);

	/* Namespaces associated with the match_recognize inner parse state
	 * should not be visible from children parse states (ie, subqueries)
	 */
	inner_pstate->p_visible_by_children = false;

	/* Process everything related to the input relation. */
	input_nsitem = process_match_recognize_inputrel(pstate, mr_clause, m);
	query = m->subquery;

	m->rowpatternvariables = list_concat(mr_clause->defineClause, mr_clause->subsetClause);
	/* Transform the pattern. This will add new universal rpvs for the ones not present in subset or define.
	 * We use a fresh parse context for this, as only the RPVs are visible in the measures and define clauses.
	 */
	m->pattern = transformPatternRecurse(inner_pstate, mr_clause->pattern, &m->rowpatternvariables);
	/* Transform the rpvs themselves, setting up RTEs and nsitems for them,
	 * which are more or less duplicates from the input_nsitem.*/
	transformRowPatternVariables(inner_pstate, m, input_nsitem);
	/* Replace the symbol name in the MatchSkipClause by the actual RPV */
	m->skipClause = mr_clause->skipClause;
	if (m->skipClause->rpv != NULL)
		m->skipClause->rpv = (Node *) findRowPatternVar(strVal(m->skipClause->rpv),
														&m->rowpatternvariables, false);

	/* The output columns are the columns from the partition clause, followed by
	 * the columns of the measures.
	 */
	foreach(lc, m->partitionClause)
	{
		SortGroupClause *sortClause = (SortGroupClause *) lfirst(lc);
		TargetEntry *inner_tle = get_sortgroupref_tle(sortClause->tleSortGroupRef, query->targetList);
		/* Make sure the inner tle is not resjunk, as we will need it on the
		 * outer query.
		 */
		inner_tle->resjunk = false;

		/* Add a new target entry pointing to the corresponding TLE from the
		 * input relation.
		 */
		Var * var = makeVar(1, inner_tle->resno,
							exprType((Node *) inner_tle->expr),
							exprTypmod((Node *) inner_tle->expr),
							exprCollation((Node *) inner_tle->expr),
							0);
		target_list = lappend(target_list,
							  makeTargetEntry((Expr *) var,
											  inner_pstate->p_next_resno++,
											  inner_tle->resname,
											  false));
	}

	m->measures = transformTargetList(inner_pstate, mr_clause->measureClause, EXPR_KIND_MATCH_RECOGNIZE);
	foreach(lc, m->measures)
	{
		target_list = lappend(target_list, lfirst(lc));
	}
	m->targetList = target_list;
	m->rtable = inner_pstate->p_rtable;
	pfree(inner_pstate);
	/* Finally, add an RTE for the match recognize clause. */
	return addRangeTableEntryForMatchRecognize(pstate,
											   m,
											   target_list,
											   mr_clause->alias,
											   input_nsitem->p_rtindex);
}


static RowPatternVar *
findRowPatternVar(char *name, List **rowpatternvariables, bool match_universal)
{
	ListCell *lc;
	RowPatternVar *rpv;
	foreach(lc, *rowpatternvariables)
	{
		rpv = lfirst(lc);
		if (strcmp(rpv->name, name) == 0)
			return rpv;
	}
	/* If the row pattern var doesn't exists, return the universal
	 * row pattern variable if allowed by the caller
	 */
	if (match_universal)
	{
		rpv = makeNode(RowPatternVar);
		rpv->name = name;
		rpv->expr = NULL;
		*rowpatternvariables = lappend(*rowpatternvariables, rpv);
		return rpv;
	}
	return NULL;
}



static RowPattern *
transformPatternRecurse(ParseState *pstate, RowPattern * pattern, List **rowpatternvariables)
{
	ListCell *lc;
	List *newargs;
	switch(pattern->kind)
	{
		case ROWPATTERN_EMPTY:
			/* Nothing to do for an empty pattern */
			return pattern;
		case ROWPATTERN_VARREF:
			{
				RowPatternVar *rpv;
				char *symbolname;
				symbolname = strVal(linitial(pattern->args));
				rpv = findRowPatternVar(symbolname, rowpatternvariables, true);
				pattern->args = list_make1(rpv);
				return pattern;
			}
		case ROWPATTERN_CONCATENATION:
		case ROWPATTERN_ALTERNATION:
		case ROWPATTERN_QUANTIFIER:
		case ROWPATTERN_GROUPING:
		case ROWPATTERN_PERMUTE:
		case ROWPATTERN_EXCLUDE:
		case ROWPATTERN_ANCHOR:
			newargs = NIL;
			foreach(lc, pattern->args)
			{
				newargs = lappend(newargs, transformPatternRecurse(pstate, (RowPattern *) lfirst(lc),
										   rowpatternvariables));
			}
			pattern->args = newargs;
			return pattern;
		default:
			elog(ERROR, "Unrecognized pattern kind: %d", pattern->kind);
	}
}

