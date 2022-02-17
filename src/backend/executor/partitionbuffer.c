/*-------------------------------------------------------------------------
 *
 * partitionbuffer.c
 *	  common code to nodes which perform PARTITION
 *
 *	Both WindowAgg and MatchRecognizeScan perform a partition of their
 *	subplan. The common code for handling that resides here.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/partitionbuffer.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/partitionbuffer.h"
#include "miscadmin.h"

typedef struct PartitionBufferData
{
	Tuplestorestate *tupstore;
	TupleTableSlot  *first_part_slot;
	int			current_ptr;
	int64		spooled_rows;
	bool 		ready;
	bool		partition_spooled;
	bool		more_partitions;
	ExprState  *partEqfunction;
	ExprContext *tmpcontext;
	MemoryContext partitioncontext;

} PartitionBufferData;



PartitionBuffer
make_partition_buffer(EState *estate, ScanState *parent,
					  int numCols,
					  const AttrNumber *keyColIdx,
					  const Oid *eqOperators,
					  const Oid *collations
					  )
{
	TupleDesc scanDesc = parent->ss_ScanTupleSlot->tts_tupleDescriptor;
	PartitionBuffer pbuffer = palloc0(sizeof(PartitionBufferData));

 	pbuffer->tmpcontext = CreateExprContext(estate);
	/* Initialize a tuple slot for the first partition tuple */
	pbuffer->first_part_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);

	/* Set up data for comparing tuples if needed */
	if (numCols > 0)
		pbuffer->partEqfunction = execTuplesMatchPrepare(scanDesc,
														 numCols,
														 keyColIdx,
														 eqOperators,
														 collations, &parent->ps);

	pbuffer->current_ptr = 0;
	pbuffer->spooled_rows = 0;
	pbuffer->more_partitions = false;
	pbuffer->ready = false;
	return pbuffer;

}


void
prepare_partition(PartitionBuffer pbuffer, PlanState *outerPlan)
{
	MemoryContext oldcontext;
	pbuffer->partition_spooled = false;
	pbuffer->spooled_rows = 0;
	oldcontext = MemoryContextSwitchTo(pbuffer->tmpcontext->ecxt_per_query_memory);

	if (TupIsNull(pbuffer->first_part_slot))
	{
		TupleTableSlot *outerslot = ExecProcNode(outerPlan);

		if (!TupIsNull(outerslot))
			ExecCopySlot(pbuffer->first_part_slot, outerslot);
		else
		{
			/* outer plan is empty, so we have nothing to do */
			pbuffer->partition_spooled = true;
			pbuffer->more_partitions = false;
			return;
		}
	}
	pbuffer->tupstore = tuplestore_begin_heap(false, false, work_mem);
	pbuffer->current_ptr = 0;
	pbuffer->ready = false;
	tuplestore_set_eflags(pbuffer->tupstore, 0);
	MemoryContextSwitchTo(oldcontext);
}

void
start_partition(PartitionBuffer pbuffer)
{
	Assert(!pbuffer->ready);
	tuplestore_puttupleslot(pbuffer->tupstore, pbuffer->first_part_slot);
	pbuffer->spooled_rows++;
	pbuffer->ready = true;
}



int64
spool_tuples(PartitionBuffer pbuffer, PlanState *outerPlan, int64 pos)
{
	TupleTableSlot *outerslot;
	MemoryContext	oldcontext;


	/*
	 * If the tuplestore has spilled to disk, alternate reading and writing
	 * becomes quite expensive due to frequent pbuffer flushes.  It's cheaper
	 * to force the entire partition to get spooled in one go.
	 *
	 * XXX this is a horrid kluge --- it'd be better to fix the performance
	 * problem inside tuplestore.  FIXME
	 */
	if (!tuplestore_in_memory(pbuffer->tupstore))
		pos = -1;

	/* Must be in query context to call outerplan */
	oldcontext = MemoryContextSwitchTo(pbuffer->tmpcontext->ecxt_per_query_memory);

	while (pbuffer->spooled_rows <= pos || pos == -1)
	{
		outerslot = ExecProcNode(outerPlan);
		if (TupIsNull(outerslot))
		{
			/* reached the end of the last partition */
			pbuffer->partition_spooled = true;
			pbuffer->more_partitions = false;
			break;
		}

		if (pbuffer->partEqfunction != NULL)
		{
			ExprContext *econtext = pbuffer->tmpcontext;

			econtext->ecxt_innertuple = pbuffer->first_part_slot;
			econtext->ecxt_outertuple = outerslot;

			/* Check if this tuple still belongs to the current partition */
			if (!ExecQualAndReset(pbuffer->partEqfunction, econtext))
			{
				/*
				 * end of partition; copy the tuple for the next cycle.
				 */
				ExecCopySlot(pbuffer->first_part_slot, outerslot);
				pbuffer->partition_spooled = true;
				pbuffer->more_partitions = true;
				break;
			}
		}

		/* Still in partition, so save it into the tuplestore */
		tuplestore_puttupleslot(pbuffer->tupstore, outerslot);
		pbuffer->spooled_rows++;
	}

	MemoryContextSwitchTo(oldcontext);
	/* Couldn't spool as many rows as we wanted. */
	if (pbuffer->spooled_rows <= pos)
	{
		if (pbuffer->partition_spooled)
		{
			if (!pbuffer->more_partitions)
				return SPOOL_INPUT_END;
			return SPOOL_PARTITION_END;					/* whole partition done already */
		}
	}
	return pbuffer->spooled_rows;
}

void
release_partition(PartitionBuffer pbuffer)
{
	if (pbuffer->tupstore)
		tuplestore_end(pbuffer->tupstore);
	pbuffer->tupstore = NULL;
	pbuffer->partition_spooled = false;
}


Tuplestorestate *
partitionbuffer_tuplestore(PartitionBuffer node)
{
	return node->tupstore;
}

bool
partitionbuffer_isready(PartitionBuffer node)
{
	return node->ready;
}

void
free_partitionbuffer(PartitionBuffer node)
{
	release_partition(node);
	pfree(node);
}
