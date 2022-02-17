/*-------------------------------------------------------------------------
 *
 * partitionbuffer.h
 *	  prototypes for partitionbuffer.c
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/partitionbuffer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTITIONBUFFER_H
#define PARTITIONBUFFER_H

#include "nodes/execnodes.h"


#define SPOOL_PARTITION_END -1
#define SPOOL_INPUT_END -2

extern PartitionBuffer make_partition_buffer(EState *estate, ScanState *parent,
											 int numCols,
											 const AttrNumber *keyColIdx,
											 const Oid *eqOperators,
											 const Oid *collations);

extern void prepare_partition(PartitionBuffer node, PlanState *outerPlan);
extern void start_partition(PartitionBuffer node);
extern void release_partition(PartitionBuffer node);
extern int64 spool_tuples(PartitionBuffer buffer, PlanState *planstate, int64 pos);

extern Tuplestorestate * partitionbuffer_tuplestore(PartitionBuffer node);
extern bool partitionbuffer_isready(PartitionBuffer node);
void free_partitionbuffer(PartitionBuffer node);


#endif							/* PARTITIONBUFFER_H */
