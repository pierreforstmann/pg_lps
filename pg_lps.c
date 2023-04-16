/*-------------------------------------------------------------------------
 *
 * pg_lps
 *  
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *          
 * Copyright (c) 2023, Pierre Forstmann.
 *            
 *-------------------------------------------------------------------------
*/
#include "postgres.h"
#include "executor/executor.h"
#include "storage/proc.h"
#include "access/xact.h"

#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;


/* Saved hook values in case of unload */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

static void pglps_ExecutorStart(QueryDesc *queryDesc, int eflags);

static void pglps_ExecutorRun(QueryDesc *queryDesc,
            			 ScanDirection direction,
				 long unsigned count,
				 bool execute_once
);
static void pglps_ExecutorFinish(QueryDesc *queryDesc);



/*
 * Module load callback
 */
void
_PG_init(void)
{
	elog(DEBUG5, "pg_lps:_PG_init():entry");

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pglps_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pglps_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pglps_ExecutorFinish;

	elog(DEBUG5, "pg_lps:_PG_init():exit");
}


/*
 *  Module unload callback
 */
void
_PG_fini(void)
{
	elog(DEBUG5, "pg_lps:_PG_fini():entry");

	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;

	elog(DEBUG5, "pg_lps:_PG_fini():entry");
}

static void
pglps_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	uint64	queryId;
	uint64 numParams;

	elog(DEBUG5, "pg_lps: pglps_ExecutorStart: entry");
	queryId = queryDesc->plannedstmt->queryId;
	ereport(LOG, (errmsg("pg_lps: queryId=%ld", queryId)));
	if (queryDesc->params != NULL)
		numParams = queryDesc->params->numParams;
	else
		numParams = 0;
	ereport(LOG, (errmsg("pg_lps: numParams=%ld", numParams)));

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
	elog(DEBUG5, "pg_lps: pglps_ExecutorStart: exit");
}

static void
pglps_ExecutorRun(QueryDesc *queryDesc,
		 ScanDirection direction,
		 long unsigned count,
		 bool execute_once)
{
	elog(DEBUG5, "pg_lps: pglps__ExecutorRun: entry");

	if (prev_ExecutorRun)
		prev_ExecutorRun(queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);

	elog(DEBUG5, "pg_lps: pglps_ExecutorRun: exit");
}

static void
pglps_ExecutorFinish(QueryDesc *queryDesc)
{
	elog(DEBUG5, "pg_lps: pglps_ExecutorFinish: entry");

	if (prev_ExecutorFinish)
		prev_ExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);

	elog(DEBUG5, "pg_lps: pglgs_ExecutorFinish: exit");
}
