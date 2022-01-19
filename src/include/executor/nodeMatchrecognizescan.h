/*-------------------------------------------------------------------------
 *
 * nodeMatchrecognizescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeMatchrecognizescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMATCHRECOGNIZESCAN_H
#define NODEMATCHRECOGNIZESCAN_H

#include "nodes/execnodes.h"

extern MatchRecognizeScanState *ExecInitMatchRecognizeScan(MatchRecognizeScan *node, EState *estate, int eflags);
extern void ExecEndMatchRecognizeScan(MatchRecognizeScanState *node);
extern void ExecReScanMatchRecognizeScan(MatchRecognizeScanState *node);

#endif							/* NODEMATCHRECOGNIZESCAN_H */
