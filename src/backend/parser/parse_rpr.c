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
static RowPatternVarDef *findRowPatternVarDef(char *name,
										List **rowpatternvariables,
										bool match_universal);
static RowPattern *transformPatternRecurse(ParseState *pstate,
										   RowPattern * pattern,
										   List** rowpatternvariables);
static void transformRowPatternVarDefs(ParseState *pstate,
										  MatchRecognize *mr,
										  ParseNamespaceItem *input_ns);
static ParseNamespaceItem * addRangeTableEntryForRPV(ParseState *pstate,
													 RowPatternVarDef *var,
													 ParseNamespaceItem *input_ns);
static ParseNamespaceItem *process_match_recognize_inputrel(ParseState *pstate,
															MatchRecognizeClause *mrclause,
															MatchRecognize *mr);
typedef struct
{
	Index restrict_to_rpv;
	bool single_rpv;
	List *rpv_list;
	ParseState *pstate;
} rpv_mutator_context;

static Node* rpv_mutator(Node *node, rpv_mutator_context * cxt);
static Node* transformRPVRefs(ParseState *pstate, Node *node);

static Node*
transformRPVRefs(ParseState *pstate, Node *node)
{
	rpv_mutator_context context;
	context.pstate = pstate;
	context.restrict_to_rpv = 0;
	context.single_rpv = false;
	return expression_tree_mutator((Node *) node,
								   rpv_mutator,
								   (void *) &context);
}

static Node* rpv_mutator(Node *node, rpv_mutator_context * cxt)
{
	if (node == NULL)
		return NULL;

	/* 
	 * We shouldn't have any aggref in a match recognize clause.
	 * Any aggregate should have been replaced by a window function instead
	 */
	Assert(!IsA(node, Aggref));

	/* Var nodes are replaced with RowPatternVars */
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		RowPatternVar *rpv = makeNode(RowPatternVar);
		/* If we need to restrict ourself to a single_rpv, set it now or throw
		 * an error if it's already set.
		 */
		if (cxt->single_rpv)
		{
			if (cxt->restrict_to_rpv == 0)
				cxt->restrict_to_rpv = var->varno;
			else if (cxt->restrict_to_rpv != var->varno)
			{
				RowPatternVarDef *def1 = (RowPatternVarDef *) list_nth(cxt->rpv_list, cxt->restrict_to_rpv);
				RowPatternVarDef *def2 = (RowPatternVarDef *) list_nth(cxt->rpv_list, var->varno);
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("mixing row pattern variables is not allowed in aggregates / navigation functions"),
						 errdetail("Current row pattern variable is %s, found %s",
								   def1->name, def2->name),
					     parser_errposition(cxt->pstate, var->location)));
			}
		}
		rpv->rpvno = var->varno;
		rpv->rpvattno = var->varattno;
		rpv->rpvtype = var->vartype;
		rpv->rpvtypmod = var->vartypmod;
		rpv->rpvcollid = var->varcollid;
		rpv->location = var->location;
		return (Node*) rpv;
	}
	/* For a window function, wrap it in a match func
	 * in.
	 */
	if (IsA(node, WindowFunc))
	{
		WindowFunc *wf = (WindowFunc *) node;
		if (cxt->single_rpv)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("nested window / row pattern navigation calls are not allowed"),
					 parser_errposition(cxt->pstate, wf->location)));

		/* Forbid having different rpvs in a window function call. */
		cxt->single_rpv = true;
		wf = (WindowFunc *) expression_tree_mutator(node, rpv_mutator, cxt);
		cxt->single_rpv = false;
		cxt->restrict_to_rpv = 0;
		return (Node *) wf;
	}
	if (IsA(node, MatchFunc))
	{
		MatchFunc *mf = (MatchFunc *) node;
		if (cxt->single_rpv)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("nested window / row pattern navigation calls are not allowed"),
					 parser_errposition(cxt->pstate, mf->location)));
		cxt->single_rpv = true;
		mf = (MatchFunc*) expression_tree_mutator(node, rpv_mutator, cxt);
		mf->rpvref = cxt->restrict_to_rpv;
		cxt->restrict_to_rpv = 0;
		cxt->single_rpv = false;
		return (Node *) mf;
	}
	return expression_tree_mutator(node, rpv_mutator, cxt);
}



static ParseNamespaceItem *
addRangeTableEntryForRPV(ParseState *pstate, RowPatternVarDef *rpv, ParseNamespaceItem *input_ns)
{
	RangeTblEntry *rte;
	char *refname = rpv->name;
	int nbcolumns = list_length(input_ns->p_names->colnames);
	ParseNamespaceItem *nsitem;
	Index rtindex;
	Assert(pstate != NULL);
	/* All universal RPVs are merged to the same RTE */
	if (rpv->expr == NULL && list_length(pstate->p_rtable) >= 1)
	{
		rte = linitial(pstate->p_rtable);
		rtindex = 1;
	}
	else {
		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RESULT;
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
transformRowPatternVarDefs(ParseState *pstate, MatchRecognize *mr, ParseNamespaceItem *input_ns)
{
	List *rpvs = NIL;
	ListCell* lc;
	RowPatternVarDef *rpv;
	RowPatternVarDef *urpv;
	ParseNamespaceItem *defaultns;

	/* First add an RTE and an anonymous namespace for the default universal RPV.
	 * The RTE itself will be reused by other universal RPVs.
	 */
	urpv = makeNode(RowPatternVarDef);
	urpv->name = "";
	urpv->expr = NULL;
	urpv->varno = 1;
	defaultns = addRangeTableEntryForRPV(pstate, urpv, input_ns);
	defaultns->p_rel_visible = true;
	defaultns->p_cols_visible = true;
	pstate->p_namespace = lappend(pstate->p_namespace, defaultns);


	/* Extract all row pattern variables from the define, and subset
	 * clauses, and add a new RTE and nsitem for them.
	 * We do not transform the expressions themselves yet, as they can reference
	 * each other.
	 */
	foreach(lc, mr->rowpatternvariables)
	{
		ParseNamespaceItem *item;
		rpv = lfirst(lc);
		rpvs = lappend(rpvs, rpv);
		item = addRangeTableEntryForRPV(pstate, rpv, input_ns);
		rpv->varno = item->p_rtindex;
		pstate->p_namespace = lappend(pstate->p_namespace, item);
	}

	/* Now that the proper namespaces have been set up, we can transform them.
	 */
	foreach(lc, rpvs)
	{
		RowPatternVarDef *rpv = lfirst(lc);
		rpv->expr = transformExpr(pstate, rpv->expr, EXPR_KIND_MATCH_RECOGNIZE_DEFINE);
		/* Perform some specific RPV processing */
		rpv->expr = transformRPVRefs(pstate, rpv->expr);
	}
}


/* Per the standard, we are not allowed to nest a match recognize inside another one. */
static void
check_match_recognize_nesting(ParseState *pstate, MatchRecognizeClause *mr_clause)
{
	while (pstate != NULL)
	{
		if (pstate->p_expr_kind == EXPR_KIND_MATCH_RECOGNIZE_DEFINE ||
			pstate->p_expr_kind == EXPR_KIND_MATCH_RECOGNIZE_MEASURES)
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
									  EXPR_KIND_WINDOW_ORDER,
									  true);

	mr->subquery = (Node *) query;
	mr->partitionClause = transformGroupClause(input_pstate,
											  mr_clause->partitionClause,
											  NULL,
											  &query->targetList,
											  mr->orderClause,
											  EXPR_KIND_WINDOW_PARTITION,
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
	inner_pstate->p_matchrecognize = m;

	/* Namespaces associated with the match_recognize inner parse state
	 * should not be visible from children parse states (ie, subqueries)
	 */
	inner_pstate->p_visible_by_children = false;

	/* Process everything related to the input relation. */
	input_nsitem = process_match_recognize_inputrel(pstate, mr_clause, m);
	query = (Query *) m->subquery;

	m->rowpatternvariables = list_concat(mr_clause->defineClause, mr_clause->subsetClause);
	/* Transform the pattern. This will add new universal rpvs for the ones not present in subset or define.
	 * We use a fresh parse context for this, as only the RPVs are visible in the measures and define clauses.
	 */
	m->pattern = transformPatternRecurse(inner_pstate, mr_clause->pattern, &m->rowpatternvariables);

	/* Transform the rpvs themselves, setting up RTEs and nsitems for them,
	 * which are more or less duplicates from the input_nsitem.*/
	transformRowPatternVarDefs(inner_pstate, m, input_nsitem);
	/* Replace the symbol name in the MatchSkipClause by the actual RPV */
	m->skipClause = mr_clause->skipClause;
	if (m->skipClause->rpv != NULL)
		m->skipClause->rpv = (Node *) findRowPatternVarDef(strVal(m->skipClause->rpv),
														&m->rowpatternvariables, false);

	/* The output columns are the columns from the partition clause, followed by
	 * the columns of the measures.
	 */
	foreach(lc, m->partitionClause)
	{
		SortGroupClause *sortClause = (SortGroupClause *) lfirst(lc);
		TargetEntry *inner_tle = get_sortgroupref_tle(sortClause->tleSortGroupRef, query->targetList);
		/* Add a new target entry pointing to the corresponding TLE from the
		 * input relation.
		 */
		Var * var = makeVar(1, inner_tle->resno,
							exprType((Node *) inner_tle->expr),
							exprTypmod((Node *) inner_tle->expr),
							exprCollation((Node *) inner_tle->expr),
							0);
		/* Make sure the inner tle is not resjunk, as we will need it on the
		 * outer query.
		 */
		inner_tle->resjunk = false;
		target_list = lappend(target_list,
							  makeTargetEntry((Expr *) var,
											  inner_pstate->p_next_resno++,
											  inner_tle->resname,
											  false));
	}

	m->measures = transformTargetList(inner_pstate, mr_clause->measureClause, EXPR_KIND_MATCH_RECOGNIZE_MEASURES);
	foreach(lc, m->measures)
	{
		target_list = lappend(target_list, lfirst(lc));
	}

	/* Now we recurse into the define and measures clause to:
	 *   - transform Var references to RPVRefs
	 *   - check the legality of constructs
	 */
	target_list = (List *) transformRPVRefs(pstate, (Node *) target_list);
	m->targetList = target_list;
	pfree(inner_pstate);
	/* Finally, add an RTE for the match recognize clause. */
	return addRangeTableEntryForMatchRecognize(pstate,
											   m,
											   target_list,
											   mr_clause->alias,
											   input_nsitem->p_rtindex);
}


static RowPatternVarDef *
findRowPatternVarDef(char *name, List **rowpatternvariables, bool match_universal)
{
	ListCell *lc;
	RowPatternVarDef *rpv;
	RowPatternVarDef *urpv = NULL;
	foreach(lc, *rowpatternvariables)
	{
		rpv = lfirst(lc);
		if (strcmp(rpv->name, name) == 0)
			return rpv;
		if (strcmp(rpv->name, "") == 0)
			urpv = rpv;
	}
	/* If the row pattern var doesn't exists, return the universal
	 * row pattern variable if allowed by the caller
	 */
	if (match_universal)
	{
		if (urpv == NULL)
		{
			urpv = makeNode(RowPatternVarDef);
			urpv->name = name;
			urpv->expr = NULL;
			*rowpatternvariables = lappend(*rowpatternvariables, urpv);
		}
		return urpv;
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
				RowPatternVarDef *rpv;
				char *symbolname;
				symbolname = strVal(linitial(pattern->args));
				rpv = findRowPatternVarDef(symbolname, rowpatternvariables, true);
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

