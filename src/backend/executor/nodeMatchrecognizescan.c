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
 *		- if 
 *		
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
#include "miscadmin.h"


static TupleTableSlot *MatchRecognizeScanNext(MatchRecognizeScanState *node);

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

static TupleTableSlot *
MatchRecognizeScanNext(MatchRecognizeScanState *node)
{
	TupleTableSlot *slot;
	slot = ExecProcNode(node->subplan);
	return slot;
}


static bool
MatchRecognizeScanRecheck(SubqueryScanState *node, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot * ExecMatchRecognizeScan(PlanState *pstate)
{
	MatchRecognizeScanState *matchstate = castNode(MatchRecognizeScanState, pstate);

	if (matchstate->partitionbuffer == NULL)
	{
		/* Initialize first partition */
		// begin_partition(matchstate);
	}
	else
	{
		matchstate->currentpos++;
	}

	CHECK_FOR_INTERRUPTS();

	return ExecScan(&matchstate->ss,
					(ExecScanAccessMtd) MatchRecognizeScanNext,
					(ExecScanRecheckMtd) MatchRecognizeScanRecheck);
}


MatchRecognizeScanState *
ExecInitMatchRecognizeScan(MatchRecognizeScan *node, EState *estate, int eflags)
{
	MatchRecognizeScanState *matchstate;
	TupleDesc	scanDesc;
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	matchstate = makeNode(MatchRecognizeScanState);
	matchstate->ss.ps.plan = (Plan *) node;
	matchstate->ss.ps.state = estate;
	matchstate->ss.ps.ExecProcNode = ExecMatchRecognizeScan;
	matchstate->subplan = ExecInitNode(node->subplan, estate, eflags);
	ExecCreateScanSlotFromOuterPlan(estate, &matchstate->ss, &TTSOpsMinimalTuple);


	ExecAssignExprContext(estate, &matchstate->ss.ps);

	/* Similarly to WindowAgg, initialize a partition context */
	matchstate->partcontext =
		AllocSetContextCreate(CurrentMemoryContext, "MatchRecognize Partition",
							  ALLOCSET_DEFAULT_SIZES);

	matchstate->ss.ps.outeropsset = true;
	matchstate->ss.ps.outerops = &TTSOpsMinimalTuple;
	matchstate->ss.ps.outeropsfixed = true;

	matchstate->first_part_slot = ExecInitExtraTupleSlot(estate, scanDesc,
													   &TTSOpsMinimalTuple);


	ExecInitResultTupleSlotTL(&matchstate->ss.ps, &TTSOpsVirtual);
	/* We don't project anything (yet) */
	ExecAssignProjectionInfo(&matchstate->ss.ps, NULL);
	scanDesc = matchstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	/* If we have a partition clause, set up data fro comparing tuples */
	matchstate->partEqfunction = execTuplesMatchPrepare(scanDesc,
													   node->partNumCols,
													   node->partColIdx,
													   node->partOperators,
													   node->partCollations,
													   &matchstate->ss.ps);

	matchstate->first_part_slot =ExecInitExtraTupleSlot(estate, scanDesc,
														&TTSOpsMinimalTuple);

	matchstate->partition_spooled = false;
	matchstate->more_partitions = false;
	return matchstate;

}


void
ExecEndMatchRecognizeScan(MatchRecognizeScanState *node)
{
	ExecFreeExprContext(&node->ss.ps);
	ExecEndNode(outerPlanState(node));
}
