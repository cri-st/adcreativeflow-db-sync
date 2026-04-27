import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
import { Logger, truncateSql, LogEntry } from '../logger';
import { SyncJobConfig, BigQuerySyncConfig } from '../types/funnel';
export type { SyncJobConfig, BigQuerySyncConfig };
import { 
    generateAddColumnsSQL, 
    generateCreateTableSQL, 
    generateDropColumnsSQL,
    SchemaField,
    detectSchemaChanges,
    validateUpsertColumns,
    buildUpsertValidationError 
} from './schema';

export interface GlobalAuth {
    googleServiceAccount: string;
    supabaseUrl: string;
    supabaseKey: string;
}

export interface SyncResult {
    hasMore: boolean;
    nextBatch: number;
    rowsProcessed: number;
    rowsDeleted: number;
    stats?: {
        totalRows: number;
        totalBatches: number;
        durationMs: number;
    };
    logs?: LogEntry[];
}

interface SyncState {
    lastSyncDate: string | null;
    bqFields: SchemaField[];
    totalRows: number;
    startTime: number;
    schemaSyncDone?: boolean;
    lastCursor?: { [key: string]: any };
    hasNewColumns?: boolean;
    newColumnNames?: string[];
}

/**
 * Serialize row keys for Set comparison
 */
function serializeKey(row: any, columns: string[]): string {
    return JSON.stringify(columns.map(col => row[col]));
}

/**
 * Detect and delete rows that exist in Supabase but not in BigQuery
 * @returns Number of rows deleted
 */
async function detectAndDeleteRemovedRows(
    bq: BigQueryClient,
    sb: SupabaseClient,
    job: BigQuerySyncConfig,
    logger: Logger
): Promise<number> {
    const { upsertColumns } = job.supabase;
    const { projectId, datasetId, tableOrView } = job.bigquery;
    
    logger.info('DELETE_DETECTION', 'Starting delete detection phase');

    // Phase 1: Fetch all IDs from BigQuery into a memory-efficient Set
    const bqIdQuery = `
        SELECT ${upsertColumns.join(', ')} 
        FROM \`${projectId}.${datasetId}.${tableOrView}\`
    `;
    
    logger.info('DELETE_DETECTION', 'Fetching BigQuery IDs', { columns: upsertColumns });
    const bqRows = await bq.queryPaginated<any>(projectId, bqIdQuery);
    
    // Circuit breaker: Abort if BigQuery returns 0 rows
    if (bqRows.length === 0) {
        logger.warning('DELETE_DETECTION', 'BigQuery returned 0 rows - aborting delete detection to prevent accidental mass deletion');
        return 0;
    }
    
    // Safety threshold: skip delete detection for extremely large tables to avoid OOM
    const DELETE_DETECTION_MAX_ROWS = 100000;
    if (bqRows.length > DELETE_DETECTION_MAX_ROWS) {
        logger.warning('DELETE_DETECTION', `BigQuery has ${bqRows.length} rows, exceeding safety threshold of ${DELETE_DETECTION_MAX_ROWS}. Skipping delete detection.`);
        return 0;
    }
    
    const bqSet = new Set<string>();
    for (const row of bqRows) {
        bqSet.add(serializeKey(row, upsertColumns));
    }
    
    logger.info('DELETE_DETECTION', 'BigQuery IDs indexed', { count: bqSet.size });

    // Phase 2: Stream Supabase pages and accumulate only rows to delete
    const PAGE_SIZE = 5000;
    const idsToDelete: any[][] = [];
    let page = 0;
    let hasMorePages = true;
    let totalSupabaseRows = 0;
    
    while (hasMorePages) {
        const query = `
            SELECT ${upsertColumns.join(', ')} 
            FROM "${job.supabase.tableName}" 
            ORDER BY ${upsertColumns.join(', ')}
            LIMIT ${PAGE_SIZE} OFFSET ${page * PAGE_SIZE}
        `;
        const pageData = await sb.executeQuery(query);
        
        // Optimization: Skip if Supabase is empty on first page (first sync)
        if (page === 0 && pageData.length === 0) {
            logger.info('DELETE_DETECTION', 'Supabase table is empty - skipping delete detection');
            return 0;
        }
        
        for (const row of pageData) {
            if (!bqSet.has(serializeKey(row, upsertColumns))) {
                // Store only the primitive key values, not the full row object
                idsToDelete.push(upsertColumns.map(col => row[col]));
            }
        }
        
        totalSupabaseRows += pageData.length;
        hasMorePages = pageData.length === PAGE_SIZE;
        page++;
    }
    
    logger.info('DELETE_DETECTION', 'Supabase scan complete', { totalSupabaseRows, rowsToDelete: idsToDelete.length });

    // Circuit breaker: Abort if deletes > 50% of Supabase rows
    if (idsToDelete.length > totalSupabaseRows * 0.5) {
        const errorMsg = `Delete detection aborted: ${idsToDelete.length} rows to delete exceeds 50% of ${totalSupabaseRows} Supabase rows`;
        logger.error('DELETE_DETECTION', errorMsg);
        throw new Error(errorMsg);
    }
    
    if (idsToDelete.length === 0) {
        logger.info('DELETE_DETECTION', 'No rows to delete');
        return 0;
    }
    
    logger.info('DELETE_DETECTION', 'Rows identified for deletion', { count: idsToDelete.length });

    // Phase 3: Delete in batches
    const deletedCount = await sb.deleteRows(job.supabase.tableName, upsertColumns, idsToDelete);
    
    logger.success('DELETE_DETECTION', 'Delete phase complete', { rowsDeleted: deletedCount });
    
    return deletedCount;
}

export async function handleSync(
    auth: GlobalAuth, 
    job: SyncJobConfig, 
    runId: string, 
    kvNamespace: KVNamespace,
    batchNumber: number = 1
): Promise<SyncResult> {
    const logger = new Logger(job.id, job.name, runId);
    await logger.startRun(kvNamespace);
    await logger.flushNow(); // Ensure initial state is visible immediately

    if (job.type === 'sheets-to-bq') {
        logger.error('SYNC_ERROR', 'Sheets sync not supported in this handler');
        await logger.endRun(kvNamespace, 'error');
        throw new Error('Sheets sync not supported in this handler');
    }

    const bqJob = job as BigQuerySyncConfig;

    try {
        const bq = new BigQueryClient(auth.googleServiceAccount);
        const sb = new SupabaseClient(auth.supabaseUrl, auth.supabaseKey);

        const stateKey = `sync_state:${bqJob.id}:${runId}`;
        let lastSyncDate: string | null = null;
        let bqFields: SchemaField[] = [];
        let totalRows = 0;
        let startTime = Date.now();
        let loadedCursor: { [key: string]: any } | undefined = undefined;
        let hasNewColumns = false;
        let newColumnNames: string[] = [];

        if (batchNumber === 1) {
            logger.info('SYNC_START', 'Starting sync', { bigquery: bqJob.bigquery, supabase: bqJob.supabase });

            const t0 = Date.now();
            logger.info('SCHEMA_SYNC', 'Fetching BigQuery metadata', { table: bqJob.bigquery.tableOrView });
            const bqMetadata = await bq.getTableMetadata(bqJob.bigquery.projectId, bqJob.bigquery.datasetId, bqJob.bigquery.tableOrView);
            bqFields = bqMetadata.schema.fields;
            logger.info('SCHEMA_SYNC', `BigQuery schema fetched in ${Date.now() - t0}ms`, { fieldCount: bqFields.length });

            const t1 = Date.now();
            logger.info('SCHEMA_SYNC', 'Ensuring Supabase table exists', { tableName: bqJob.supabase.tableName });
            const createSql = generateCreateTableSQL(bqJob.supabase.tableName, bqFields, bqJob.supabase.upsertColumns);
            await sb.executeRawSQL(createSql);
            logger.info('SCHEMA_SYNC', `Table ensured in ${Date.now() - t1}ms`);

            const validation = validateUpsertColumns(bqJob.supabase.upsertColumns, bqFields);
            if (!validation.valid) {
                throw new Error(buildUpsertValidationError(validation.invalidColumns));
            }

            const t2 = Date.now();
            const supabaseSchema = await sb.getTableSchema(bqJob.supabase.tableName);
            const schemaChanges = detectSchemaChanges(bqFields, supabaseSchema);
            logger.info('SCHEMA_SYNC', `Schema comparison done in ${Date.now() - t2}ms`, {
                columnsToAdd: schemaChanges.columnsToAdd.map(c => c.name),
                columnsToDrop: schemaChanges.columnsToDrop
            });

            if (schemaChanges.columnsToAdd.length > 0) {
                const t3 = Date.now();
                const addSql = generateAddColumnsSQL(bqJob.supabase.tableName, schemaChanges.columnsToAdd);
                await sb.executeRawSQL(addSql);
                logger.info('SCHEMA_SYNC', `Added ${schemaChanges.columnsToAdd.length} columns in ${Date.now() - t3}ms`);
            }

            if (schemaChanges.columnsToDrop.length > 0) {
                const t4 = Date.now();
                const dropSql = generateDropColumnsSQL(bqJob.supabase.tableName, schemaChanges.columnsToDrop);
                await sb.executeRawSQL(dropSql);
                logger.info('SCHEMA_SYNC', `Dropped ${schemaChanges.columnsToDrop.length} columns in ${Date.now() - t4}ms`);
            }

            hasNewColumns = schemaChanges.columnsToAdd.length > 0;
            newColumnNames = schemaChanges.columnsToAdd.map((c: SchemaField) => c.name);
            const hasDroppedColumns = schemaChanges.columnsToDrop.length > 0;
            
            if (hasNewColumns || hasDroppedColumns) {
                logger.info('SCHEMA_SYNC', 'Schema changes applied, waiting for propagation');
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

            logger.info('INCREMENTAL', 'Determining last sync date');
            
            const incrementalColumn = bqJob.bigquery.incrementalColumn;
            const shouldUseIncremental = incrementalColumn && !hasNewColumns;
            
            if (hasNewColumns) {
                logger.info('INCREMENTAL', 'New columns detected - forcing full sync to backfill data', {
                    newColumns: schemaChanges.columnsToAdd.map(c => c.name)
                });
            }
            
            if (shouldUseIncremental && incrementalColumn) {
                try {
                    lastSyncDate = await sb.getLastSyncDateFromTable(bqJob.supabase.tableName, incrementalColumn);
                } catch (e: any) {
                    logger.warning('INCREMENTAL', 'Could not fetch last sync date', { reason: e.message });
                }
            } else if (incrementalColumn) {
                logger.info('INCREMENTAL', 'Performing full sync to populate new columns');
            }
            
            logger.info('INCREMENTAL', 'Last sync date determined', { lastSyncDate: lastSyncDate || 'NONE' });

            await kvNamespace.put(stateKey, JSON.stringify({
                lastSyncDate,
                bqFields,
                totalRows: 0,
                startTime,
                schemaSyncDone: true,
                lastCursor: undefined,
                hasNewColumns,
                newColumnNames
            }), { expirationTtl: 86400 });
        
        } else {
            logger.info('BATCH_START', `Starting batch ${batchNumber}`);
            const state = await kvNamespace.get<SyncState>(stateKey, 'json');
            if (!state) {
                throw new Error(`Sync state not found for runId ${runId} (batch ${batchNumber}). The run may have expired or failed.`);
            }
            if (!state.schemaSyncDone) {
                throw new Error(`Schema sync not completed for runId ${runId}. Cannot proceed with batch ${batchNumber}.`);
            }
            lastSyncDate = state.lastSyncDate;
            bqFields = state.bqFields;
            totalRows = state.totalRows || 0;
            startTime = state.startTime || Date.now();
            loadedCursor = state.lastCursor;
            hasNewColumns = state.hasNewColumns || false;
            newColumnNames = state.newColumnNames || [];
        }

        let filter = '';
        let orderBy = '';
        const cursorColumn = bqJob.bigquery.incrementalColumn || bqJob.supabase.upsertColumns[0];
        const tieBreaker = bqJob.supabase.upsertColumns[0];
        
        if (bqJob.bigquery.incrementalColumn) {
            if (lastSyncDate) {
                const incrementalField = bqFields.find(
                    f => f.name.toLowerCase() === bqJob.bigquery.incrementalColumn!.toLowerCase()
                );
                const operator = '>';
                filter = `WHERE ${bqJob.bigquery.incrementalColumn} ${operator} '${lastSyncDate}'`;
            }
            orderBy = `ORDER BY ${bqJob.bigquery.incrementalColumn} ASC, ${tieBreaker} ASC`;
        } else {
            if (bqJob.supabase.upsertColumns.length > 0) {
                orderBy = `ORDER BY ${bqJob.supabase.upsertColumns.join(', ')} ASC`;
            }
        }

        if (batchNumber > 1 && loadedCursor && loadedCursor[cursorColumn] !== undefined) {
            const cursorValue = loadedCursor[cursorColumn];
            const tieValue = loadedCursor[tieBreaker];
            const quotedCursor = typeof cursorValue === 'string' ? `'${cursorValue}'` : cursorValue;
            const quotedTie = typeof tieValue === 'string' ? `'${tieValue}'` : tieValue;
            
            // Compound cursor condition: (incCol > cursor) OR (incCol = cursor AND tieBreaker > cursorTie)
            const cursorFilter = tieValue !== undefined && cursorColumn !== tieBreaker
                ? `((${cursorColumn} > ${quotedCursor}) OR (${cursorColumn} = ${quotedCursor} AND ${tieBreaker} > ${quotedTie}))`
                : `${cursorColumn} > ${quotedCursor}`;
            filter = filter ? `${filter} AND ${cursorFilter}` : `WHERE ${cursorFilter}`;
        }

        const BATCH_LIMIT = 1000;

        const sql = `
            SELECT * 
            FROM \`${bqJob.bigquery.projectId}.${bqJob.bigquery.datasetId}.${bqJob.bigquery.tableOrView}\`
            ${filter}
            ${orderBy}
            LIMIT ${BATCH_LIMIT}
        `;

        logger.info('DATA_FETCH', `Fetching batch ${batchNumber}`, { 
            limit: BATCH_LIMIT,
            sql: sql.replace(/\s+/g, ' ').trim()
        });
        const tFetch = Date.now();
        const data = await bq.queryPaginated<any>(bqJob.bigquery.projectId, sql, bqJob.bigquery.forceStringFields);
        logger.info('DATA_FETCH', `BigQuery returned ${data.length} rows in ${Date.now() - tFetch}ms`);
        
        if (data.length > 0) {
            const firstRow = data[0];
            const lastRow = data[data.length - 1];
            logger.success('DATA_FETCH', `Batch ${batchNumber} fetched`, { 
                count: data.length,
                batchLimit: BATCH_LIMIT,
                hasMore: data.length === BATCH_LIMIT,
                fieldsInFirstRow: Object.keys(firstRow),
                sampleFirstRow: Object.fromEntries(
                    Object.entries(firstRow).slice(0, 5).map(([k, v]) => [k, v])
                ),
                fieldsInLastRow: Object.keys(lastRow)
            });
        } else {
            logger.success('DATA_FETCH', `Batch ${batchNumber} fetched - no data`, { 
                count: 0
            });
        }

        if (data.length > 0) {
            const UPSERT_BATCH_SIZE = 500;
            const tUpsertStart = Date.now();
            for (let j = 0; j < data.length; j += UPSERT_BATCH_SIZE) {
                const upsertBatch = data.slice(j, j + UPSERT_BATCH_SIZE);
                
                if (hasNewColumns && upsertBatch.length > 0) {
                    const sampleRow = upsertBatch[0];
                    logger.info('UPSERT_NEW_COLUMNS', 'Checking new column values in batch', {
                        newColumns: newColumnNames,
                        valuesInFirstRow: Object.fromEntries(
                            newColumnNames.map((col: string) => [col, sampleRow[col]])
                        ),
                        rowHasNewColumns: newColumnNames.every((col: string) => col in sampleRow)
                    });
                }
                
                const tSub = Date.now();
                logger.debug('UPSERT', `Upserting sub-batch ${Math.floor(j/UPSERT_BATCH_SIZE)+1}`, { count: upsertBatch.length });
                await sb.upsertTableData(bqJob.supabase.tableName, upsertBatch, bqJob.supabase.upsertColumns.join(','));
                logger.debug('UPSERT', `Sub-batch ${Math.floor(j/UPSERT_BATCH_SIZE)+1} done in ${Date.now() - tSub}ms`);
            }
            logger.info('UPSERT', `Total upsert time for ${data.length} rows: ${Date.now() - tUpsertStart}ms`);
        }

        let lastCursor: { [key: string]: any } | undefined = undefined;
        if (data.length > 0) {
            const lastRow = data[data.length - 1];
            lastCursor = { 
                [cursorColumn]: lastRow[cursorColumn],
                [tieBreaker]: lastRow[tieBreaker]
            };
        }

        totalRows += data.length;
        const hasMore = data.length === BATCH_LIMIT;

        // DIAGNOSTIC: Log continuation decision
        logger.info('CONTINUATION_DECISION', `Batch ${batchNumber} complete`, {
            dataFetched: data.length,
            batchLimit: BATCH_LIMIT,
            hasMore,
            nextBatch: batchNumber + 1,
            totalRowsSoFar: totalRows,
            cursorColumn,
            lastCursorValue: lastCursor?.[cursorColumn]
        });

        if (!hasMore) {
            let rowsDeleted = 0;
            try {
                const tDelete = Date.now();
                rowsDeleted = await detectAndDeleteRemovedRows(bq, sb, bqJob, logger);
                logger.info('DELETE_DETECTION', `Delete detection completed in ${Date.now() - tDelete}ms`, { rowsDeleted });
            } catch (deleteError: any) {
                logger.error('DELETE_DETECTION', 'Delete detection failed', { error: deleteError.message });
                throw deleteError;
            }
            
            const durationMs = Date.now() - startTime;
            const minutes = Math.floor(durationMs / 60000);
            const seconds = Math.floor((durationMs % 60000) / 1000);
            const durationStr = `${minutes}m ${seconds}s`;

            logger.success('SYNC_COMPLETE', 'Job finished successfully', { 
                totalBatches: batchNumber, 
                totalRows,
                rowsDeleted,
                duration: durationStr
            });
            
            await logger.endRun(kvNamespace, 'success');
            await kvNamespace.delete(stateKey);

            return { 
                hasMore, 
                nextBatch: batchNumber + 1, 
                rowsProcessed: data.length,
                rowsDeleted,
                stats: {
                    totalRows,
                    totalBatches: batchNumber,
                    durationMs
                },
                logs: logger.getLogs()
            };
        } else {
            await kvNamespace.put(stateKey, JSON.stringify({ 
                lastSyncDate, 
                bqFields,
                totalRows,
                startTime,
                schemaSyncDone: true,
                lastCursor
            }), { expirationTtl: 86400 });

            logger.success('BATCH_COMPLETE', `Batch ${batchNumber} completed. Proceeding to next batch.`);
            await logger.flushNow();
            return { hasMore, nextBatch: batchNumber + 1, rowsProcessed: data.length, rowsDeleted: 0, logs: logger.getLogs() };
        }

    } catch (err: any) {
        logger.error('SYNC_ERROR', 'Sync failed', { error: err.message, stack: err.stack?.substring(0, 500) });
        await logger.endRun(kvNamespace, 'error');
        throw err;
    }
}
