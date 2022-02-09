/*-------------------------------------------------------------------------
 *
 * matchfuncs.c
 *	  Standard matchrecognize functions defined in SQL spec.
 *
 * Portions Copyright (c) 2000-2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/matchfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"

Datum
match_number(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
match_classifier(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
match_prev(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
match_prev_offset(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
match_next(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
match_next_offset(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}
