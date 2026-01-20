import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
import { Logger, truncateSql, LogEntry } from '../logger';
import { 
    generateAddColumnsSQL, 
    generateCreateTableSQL, 
    generateDropColumnsSQL,
    SchemaField,
    detectSchemaChanges,
    validateUpsertColumns,
    buildUpsertValidationError 
} from './schema';

export interface SyncJobConfig {
    id: string;
    name: string;
    enabled: boolean;

    bigquery: {
        projectId: string;
        datasetId: string;
        tableOrView: string;
        incrementalColumn?: string;
        forceStringFields?: string[];
    };

    supabase: {
        tableName: string;
        upsertColumns: string[];
    };

    lastRun?: string;
    lastStatus?: 'success' | 'error';
    lastError?: string;
    lastSummary?: string;
}

export interface GlobalAuth {
    googleServiceAccount: string;
    supabaseUrl: string;
    supabaseKey: string;
}

export interface SyncResult {
    hasMore: boolean;
    nextBatch: number;
    rowsProcessed: number;
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

export async function handleSync(
    auth: GlobalAuth, 
    job: SyncJobConfig, 
    runId: string, 
    kvNamespace: KVNamespace,
    batchNumber: number = 1
): Promise<SyncResult> {
    const logger = new Logger(job.id, job.name, runId);
    await logger.startRun(kvNamespace);

    try {
        const bq = new BigQueryClient(auth.googleServiceAccount);
        const sb = new SupabaseClient(auth.supabaseUrl, auth.supabaseKey);

        const stateKey = `sync_state:${job.id}:${runId}`;
        let lastSyncDate: string | null = null;
        let bqFields: SchemaField[] = [];
        let totalRows = 0;
        let startTime = Date.now();
        let loadedCursor: { [key: string]: any } | undefined = undefined;

        if (batchNumber === 1) {
            logger.info('SYNC_START', 'Starting sync', { bigquery: job.bigquery, supabase: job.supabase });

            logger.info('SCHEMA_SYNC', 'Fetching BigQuery metadata', { table: job.bigquery.tableOrView });
            const bqMetadata = await bq.getTableMetadata(job.bigquery.projectId, job.bigquery.datasetId, job.bigquery.tableOrView);
            bqFields = bqMetadata.schema.fields;

            logger.info('SCHEMA_SYNC', 'Ensuring Supabase table exists', { tableName: job.supabase.tableName });
            const createSql = generateCreateTableSQL(job.supabase.tableName, bqFields, job.supabase.upsertColumns);
            await sb.executeRawSQL(createSql);

            const validation = validateUpsertColumns(job.supabase.upsertColumns, bqFields);
            if (!validation.valid) {
                throw new Error(buildUpsertValidationError(validation.invalidColumns));
            }

            const supabaseSchema = await sb.getTableSchema(job.supabase.tableName);
            const schemaChanges = detectSchemaChanges(bqFields, supabaseSchema);

            if (schemaChanges.columnsToAdd.length > 0) {
                logger.success('SCHEMA_SYNC', 'Adding new columns', { count: schemaChanges.columnsToAdd.length, columns: schemaChanges.columnsToAdd.map(c => c.name) });
                const addSql = generateAddColumnsSQL(job.supabase.tableName, schemaChanges.columnsToAdd);
                await sb.executeRawSQL(addSql);
            }

            if (schemaChanges.columnsToDrop.length > 0) {
                logger.warning('SCHEMA_SYNC', 'Dropping obsolete columns', { count: schemaChanges.columnsToDrop.length, columns: schemaChanges.columnsToDrop });
                const dropSql = generateDropColumnsSQL(job.supabase.tableName, schemaChanges.columnsToDrop);
                await sb.executeRawSQL(dropSql);
            }

            if (schemaChanges.columnsToAdd.length > 0 || schemaChanges.columnsToDrop.length > 0) {
                logger.info('SCHEMA_SYNC', 'Schema changes applied, waiting for propagation');
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

            logger.info('INCREMENTAL', 'Determining last sync date');
            if (job.bigquery.incrementalColumn) {
                try {
                    lastSyncDate = await sb.getLastSyncDateFromTable(job.supabase.tableName, job.bigquery.incrementalColumn);
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
        const cursorColumn = job.bigquery.incrementalColumn || job.supabase.upsertColumns[0];
        const tieBreaker = job.supabase.upsertColumns[0];
        
        if (job.bigquery.incrementalColumn) {
            if (lastSyncDate) {
                const incrementalField = bqFields.find(
                    f => f.name.toLowerCase() === job.bigquery.incrementalColumn!.toLowerCase()
                );
                const operator = '>';
                filter = `WHERE ${job.bigquery.incrementalColumn} ${operator} '${lastSyncDate}'`;
            }
            orderBy = `ORDER BY ${job.bigquery.incrementalColumn} ASC, ${tieBreaker} ASC`;
        } else {
            if (job.supabase.upsertColumns.length > 0) {
                orderBy = `ORDER BY ${job.supabase.upsertColumns.join(', ')} ASC`;
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
            FROM \`${job.bigquery.projectId}.${job.bigquery.datasetId}.${job.bigquery.tableOrView}\`
            ${filter}
            ${orderBy}
            LIMIT ${BATCH_LIMIT}
        `;

        logger.info('DATA_FETCH', `Fetching batch ${batchNumber}`, { limit: BATCH_LIMIT });
        const data = await bq.queryPaginated<any>(job.bigquery.projectId, sql, job.bigquery.forceStringFields);
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
                await sb.upsertTableData(job.supabase.tableName, upsertBatch, job.supabase.upsertColumns.join(','));
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
            const durationMs = Date.now() - startTime;
            const minutes = Math.floor(durationMs / 60000);
            const seconds = Math.floor((durationMs % 60000) / 1000);
            const durationStr = `${minutes}m ${seconds}s`;

            logger.success('SYNC_COMPLETE', 'Job finished successfully', { 
                totalBatches: batchNumber, 
                totalRows,
                duration: durationStr
            });
            
            await logger.endRun(kvNamespace, 'success');
            await kvNamespace.delete(stateKey);

            return { 
                hasMore, 
                nextBatch: batchNumber + 1, 
                rowsProcessed: data.length,
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
            return { hasMore, nextBatch: batchNumber + 1, rowsProcessed: data.length, logs: logger.getLogs() };
        }

    } catch (err: any) {
        logger.error('SYNC_ERROR', 'Sync failed', { error: err.message, stack: err.stack?.substring(0, 500) });
        await logger.endRun(kvNamespace, 'error');
        throw err;
    }
}
