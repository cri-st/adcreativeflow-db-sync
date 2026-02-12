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

    // Phase 1: Fetch all IDs from BigQuery (full table, ignore incrementalColumn)
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
    
    logger.info('DELETE_DETECTION', 'BigQuery IDs fetched', { count: bqRows.length });

    // Phase 1b: Fetch all IDs from Supabase (paginated)
    const PAGE_SIZE = 10000;
    const supabaseRows: any[] = [];
    let page = 0;
    let hasMorePages = true;
    
    while (hasMorePages) {
        const query = `
            SELECT ${upsertColumns.join(', ')} 
            FROM "${job.supabase.tableName}" 
            ORDER BY ${upsertColumns.join(', ')}
            LIMIT ${PAGE_SIZE} OFFSET ${page * PAGE_SIZE}
        `;
        const pageData = await sb.executeQuery(query);
        supabaseRows.push(...pageData);
        hasMorePages = pageData.length === PAGE_SIZE;
        page++;
    }
    
    // Optimization: Skip if Supabase has 0 rows (first sync)
    if (supabaseRows.length === 0) {
        logger.info('DELETE_DETECTION', 'Supabase table is empty - skipping delete detection');
        return 0;
    }
    
    logger.info('DELETE_DETECTION', 'Supabase IDs fetched', { count: supabaseRows.length });

    // Phase 2: Compare using Sets
    const bqSet = new Set(bqRows.map(row => serializeKey(row, upsertColumns)));
    
    const idsToDelete = supabaseRows.filter(row => !bqSet.has(serializeKey(row, upsertColumns)));
    
    // Circuit breaker: Abort if deletes > 50% of Supabase rows
    if (idsToDelete.length > supabaseRows.length * 0.5) {
        const errorMsg = `Delete detection aborted: ${idsToDelete.length} rows to delete exceeds 50% of ${supabaseRows.length} Supabase rows`;
        logger.error('DELETE_DETECTION', errorMsg);
        throw new Error(errorMsg);
    }
    
    if (idsToDelete.length === 0) {
        logger.info('DELETE_DETECTION', 'No rows to delete');
        return 0;
    }
    
    logger.info('DELETE_DETECTION', 'Rows identified for deletion', { count: idsToDelete.length });

    // Phase 3: Delete in batches
    const deletePayload = idsToDelete.map(row => 
        upsertColumns.map(col => row[col])
    );
    
    const deletedCount = await sb.deleteRows(job.supabase.tableName, upsertColumns, deletePayload);
    
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

        if (batchNumber === 1) {
            logger.info('SYNC_START', 'Starting sync', { bigquery: bqJob.bigquery, supabase: bqJob.supabase });

            logger.info('SCHEMA_SYNC', 'Fetching BigQuery metadata', { table: bqJob.bigquery.tableOrView });
            const bqMetadata = await bq.getTableMetadata(bqJob.bigquery.projectId, bqJob.bigquery.datasetId, bqJob.bigquery.tableOrView);
            bqFields = bqMetadata.schema.fields;
            
            logger.info('SCHEMA_SYNC', 'BigQuery schema fetched', { 
                fieldCount: bqFields.length,
                fields: bqFields.map(f => ({ name: f.name, type: f.type }))
            });

            logger.info('SCHEMA_SYNC', 'Ensuring Supabase table exists', { tableName: bqJob.supabase.tableName });
            const createSql = generateCreateTableSQL(bqJob.supabase.tableName, bqFields, bqJob.supabase.upsertColumns);
            await sb.executeRawSQL(createSql);

            const validation = validateUpsertColumns(bqJob.supabase.upsertColumns, bqFields);
            if (!validation.valid) {
                throw new Error(buildUpsertValidationError(validation.invalidColumns));
            }

            const supabaseSchema = await sb.getTableSchema(bqJob.supabase.tableName);
            const schemaChanges = detectSchemaChanges(bqFields, supabaseSchema);

            logger.info('SCHEMA_SYNC', 'Schema comparison details', {
                bqFieldsCount: bqFields.length,
                bqFields: bqFields.map(f => f.name),
                supabaseFieldsCount: supabaseSchema.length,
                supabaseFields: supabaseSchema.map(f => f.name),
                columnsToAdd: schemaChanges.columnsToAdd.map(c => c.name),
                columnsToDrop: schemaChanges.columnsToDrop
            });

            if (schemaChanges.columnsToAdd.length > 0) {
                logger.success('SCHEMA_SYNC', 'Adding new columns', { count: schemaChanges.columnsToAdd.length, columns: schemaChanges.columnsToAdd.map(c => c.name) });
                const addSql = generateAddColumnsSQL(bqJob.supabase.tableName, schemaChanges.columnsToAdd);
                await sb.executeRawSQL(addSql);
            }

            if (schemaChanges.columnsToDrop.length > 0) {
                logger.warning('SCHEMA_SYNC', 'Dropping obsolete columns', { count: schemaChanges.columnsToDrop.length, columns: schemaChanges.columnsToDrop });
                const dropSql = generateDropColumnsSQL(bqJob.supabase.tableName, schemaChanges.columnsToDrop);
                await sb.executeRawSQL(dropSql);
            }

            if (schemaChanges.columnsToAdd.length > 0 || schemaChanges.columnsToDrop.length > 0) {
                logger.info('SCHEMA_SYNC', 'Schema changes applied, waiting for propagation');
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

            logger.info('INCREMENTAL', 'Determining last sync date');
            if (bqJob.bigquery.incrementalColumn) {
                try {
                    lastSyncDate = await sb.getLastSyncDateFromTable(bqJob.supabase.tableName, bqJob.bigquery.incrementalColumn);
                } catch (e: any) {
                    logger.warning('INCREMENTAL', 'Could not fetch last sync date', { reason: e.message });
                }
            }
            logger.info('INCREMENTAL', 'Last sync date determined', { lastSyncDate: lastSyncDate || 'NONE' });

            await kvNamespace.put(stateKey, JSON.stringify({ 
                lastSyncDate, 
                bqFields,
                totalRows: 0,
                startTime,
                schemaSyncDone: true,
                lastCursor: undefined
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

        const BATCH_LIMIT = 5000;

        const sql = `
            SELECT * 
            FROM \`${bqJob.bigquery.projectId}.${bqJob.bigquery.datasetId}.${bqJob.bigquery.tableOrView}\`
            ${filter}
            ${orderBy}
            LIMIT ${BATCH_LIMIT}
        `;

        logger.info('DATA_FETCH', `Fetching batch ${batchNumber}`, { limit: BATCH_LIMIT });
        const data = await bq.queryPaginated<any>(bqJob.bigquery.projectId, sql, bqJob.bigquery.forceStringFields);
        logger.success('DATA_FETCH', `Batch ${batchNumber} fetched`, { 
            count: data.length,
            batchLimit: BATCH_LIMIT,
            hasMore: data.length === BATCH_LIMIT
        });

        if (data.length > 0) {
            const UPSERT_BATCH_SIZE = 2500;
            for (let j = 0; j < data.length; j += UPSERT_BATCH_SIZE) {
                const upsertBatch = data.slice(j, j + UPSERT_BATCH_SIZE);
                logger.debug('UPSERT', `Upserting sub-batch ${Math.floor(j/UPSERT_BATCH_SIZE)+1}`, { count: upsertBatch.length });
                await sb.upsertTableData(bqJob.supabase.tableName, upsertBatch, bqJob.supabase.upsertColumns.join(','));
            }
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
                rowsDeleted = await detectAndDeleteRemovedRows(bq, sb, bqJob, logger);
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
            return { hasMore, nextBatch: batchNumber + 1, rowsProcessed: data.length, rowsDeleted: 0, logs: logger.getLogs() };
        }

    } catch (err: any) {
        logger.error('SYNC_ERROR', 'Sync failed', { error: err.message, stack: err.stack?.substring(0, 500) });
        await logger.endRun(kvNamespace, 'error');
        throw err;
    }
}
