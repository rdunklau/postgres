/*-------------------------------------------------------------------------
 *
 * nodeMatchrecognizescan.c
 *	  routines to handle MatchRecognizeScan nodes.
 *
 *
 *	The execution model works as follow:
 *		- for each partition, we feed the input nodes into the partitionstore
 *		- upon adding a node in the partitionstore, we see if it can match
 *		anywhere. For this, we instantiate a new matchstate for the initial
 *		state of the NFA, and try all transitions for every current matchstate.
 *		- dead-ends are discarded immediately
 *		- if the tuple match, compute all information
 *		needed for future arcs evaluation, and store a MatchTuple in the
 *		matchbuffer. The matchtuple is tagged with the information.
 *		- Whenever we split because of several arcs being possible, build a new
 *		matchstate for the new run, with a version number making it compatible
 *		with the previous one.
 *		- the match state contains a read pointer into the version_info
 *		tuplestore, to look for the previous tuple in the match according to
 *		version information. (FIXME: add an optimization when there is only one
 *		to bypass the version_info, by instead storing a read pointer to the
 *		matchbuffer directly).
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMatchrecognizescan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeMatchrecognizescan.h"
#include "executor/partitionbuffer.h"
#include "miscadmin.h"


struct version_number {
	int nbparts;
	int *version_number;
};

struct versioned_pointer {
	struct version_number version_number;
	int computation_state_pointer; /* Read pointer in the computation state tuple store */
};

struct version_information {
	int nbversions;
	struct versioned_pointer *ptrs;
};

struct nfa_computation_state {
	struct version_number vno;
	int	state_no;
	int most_recent_tuple; /* ReadPointer in the partition tuplestore */
	Datum *values; /* Array of computed values referenced in define clause */
	int nb_previous;
	int nb_next;
	struct versioned_pointer *previous;
	struct versioned_pointer *next;
};

static TupleTableSlot * ExecMatchRecognizeScan(PlanState *pstate)
{
	MatchRecognizeScanState *matchstate = castNode(MatchRecognizeScanState, pstate);
	ExprContext *econtext;
	Tuplestorestate *tupstore;

	CHECK_FOR_INTERRUPTS();

	if (!partitionbuffer_isready(matchstate->partitionbuffer))
	{
		/* Initialize first partition */
		prepare_partition(matchstate->partitionbuffer, matchstate->subplan);
		/* Setup any read pointers we might need */
		matchstate->currentpos = 0;
		/* Start the partition */
		start_partition(matchstate->partitionbuffer);
	} else {
		matchstate->currentpos++;
	}

	/* Spool tuples. If we need to, move the next partition. */
	switch(spool_tuples(matchstate->partitionbuffer,
						matchstate->subplan,
						matchstate->currentpos))
	{
			case SPOOL_INPUT_END:
				return NULL;
			case SPOOL_PARTITION_END:
				matchstate->currentpos = 0;
				release_partition(matchstate->partitionbuffer);
				prepare_partition(matchstate->partitionbuffer, matchstate->subplan);
				start_partition(matchstate->partitionbuffer);
				break;
			default:
				break;
	}



	/* Final output execution is in ps_ExprContext */
	econtext = matchstate->ss.ps.ps_ExprContext;

	/* Clear it for current row */
	ResetExprContext(econtext);

	/* Fetch the current tuple from the underlying buffer */
	tupstore = partitionbuffer_tuplestore(matchstate->partitionbuffer);
	tuplestore_gettupleslot(tupstore, true, true, matchstate->ss.ss_ScanTupleSlot);
	print_slot(matchstate->ss.ss_ScanTupleSlot);

	/* Return a projection tuple using the windowfunc and matchfunc results and
	 * the current row.
	 */
	econtext->ecxt_outertuple = matchstate->ss.ss_ScanTupleSlot;
	econtext->ecxt_scantuple = matchstate->ss.ss_ScanTupleSlot;

	return ExecProject(matchstate->ss.ps.ps_ProjInfo);
}


MatchRecognizeScanState *
ExecInitMatchRecognizeScan(MatchRecognizeScan *node, EState *estate, int eflags)
{
	MatchRecognizeScanState *matchstate;
	TupleDesc intermediataTupDesc = ExecTypeFromTL(node->measures_list);
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	matchstate = makeNode(MatchRecognizeScanState);
	matchstate->ss.ps.plan = (Plan *) node;
	matchstate->ss.ps.state = estate;
	matchstate->ss.ps.ExecProcNode = ExecMatchRecognizeScan;

	ExecAssignExprContext(estate, &matchstate->ss.ps);

	matchstate->subplan = ExecInitNode(node->subplan, estate, eflags);
	ExecInitScanTupleSlot(estate, &matchstate->ss,
						  ExecGetResultType(matchstate->subplan),
						  &TTSOpsMinimalTuple);

	matchstate->ss.ps.scanopsset = true;
	matchstate->ss.ps.scanops = ExecGetResultSlotOps(matchstate->subplan,
													 &matchstate->ss.ps.scanopsfixed);

	matchstate->ss.ps.outeropsset = true;
	matchstate->ss.ps.outerops = &TTSOpsMinimalTuple;
	matchstate->ss.ps.outeropsfixed = true;


	matchstate->partitionbuffer = (PartitionBuffer) make_partition_buffer(estate,
														&matchstate->ss,
														node->partNumCols,
														node->partColIdx,
														node->partOperators,
														node->partCollations);

	/* We need an intermediate slot for the match results, before the final
	 * projection. */
	matchstate->matchslot = ExecInitExtraTupleSlot(estate, intermediataTupDesc,
												   &TTSOpsMinimalTuple);
	

	matchstate->ss.ps.ps_ResultTupleDesc = intermediataTupDesc;
	ExecInitResultSlot(&matchstate->ss.ps, &TTSOpsMinimalTuple);

	matchstate->ss.ps.ps_ProjInfo = ExecBuildProjectionInfo(node->measures_list,
													 matchstate->ss.ps.ps_ExprContext,
													 matchstate->ss.ps.ps_ResultTupleSlot,
													 &matchstate->ss.ps,
													 NULL);

	return matchstate;
}


void
ExecEndMatchRecognizeScan(MatchRecognizeScanState *node)
{
	free_partitionbuffer(node->partitionbuffer);
	ExecFreeExprContext(&node->ss.ps);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	ExecEndNode(node->subplan);
}
