/*-------------------------------------------------------------------------
 *
 * nodeMatchrecognizescan.c
 *	  routines to handle MatchRecognizeScan nodes.
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


static TupleTableSlot *MatchRecognizeScanNext(MatchRecognizeScanState *node);

struct version_number {
	int nbparts;
	int *version_number;
};

struct versioned_pointer {
	struct version_number version_number;
	int computation_state_pointer; /* Read pointer in the computation state tuple store */
};

struct matchbuffer_tuple {
	int nbversions;
};

struct nfa_computation_state {
	struct version_number vno;
	int	state_no;
	int most_recent_tuple; /* ReadPointer in the common shared buffer */
	Datum *values; /* Array of computed values referenced in define clause */
	int nbpointers;
	struct versioned_pointer *versioned_pointers;
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
	MatchRecognizeScanState *node = castNode(MatchRecognizeScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) MatchRecognizeScanNext,
					(ExecScanRecheckMtd) MatchRecognizeScanRecheck);
}


MatchRecognizeScanState *
ExecInitMatchRecognizeScan(MatchRecognizeScan *node, EState *estate, int eflags)
{
	MatchRecognizeScanState *scanstate;
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	scanstate = makeNode(MatchRecognizeScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecMatchRecognizeScan;
	scanstate->subplan = ExecInitNode(node->subplan, estate, eflags);

	ExecInitResultTupleSlotTL(&scanstate->ss.ps, &TTSOpsVirtual);
	ExecAssignExprContext(estate, &scanstate->ss.ps);
	return scanstate;

}


void
ExecEndMatchRecognizeScan(MatchRecognizeScanState *node)
{
	ExecFreeExprContext(&node->ss.ps);
	ExecEndNode(outerPlanState(node));
}
