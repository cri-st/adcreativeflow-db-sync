import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
import { Logger, truncateSql } from '../logger';
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

    // BigQuery Source
    bigquery: {
        projectId: string;
        datasetId: string;
        tableOrView: string;
        incrementalColumn?: string; // ej: "date_monday"
    };

    // Supabase Destination
    supabase: {
        tableName: string;
        upsertColumns: string[]; // ej: ["date_monday", "campaign_id"]
    };

    // Execution (Stored in KV)
    lastRun?: string;
    lastStatus?: 'success' | 'error';
    lastError?: string;
}

export interface GlobalAuth {
    googleServiceAccount: string;
    supabaseUrl: string;
    supabaseKey: string;
}

export async function handleSync(auth: GlobalAuth, job: SyncJobConfig, runId: string, kvNamespace: KVNamespace) {
    const logger = new Logger(job.id, job.name, runId);
    await logger.startRun(kvNamespace);

    try {
        const bq = new BigQueryClient(auth.googleServiceAccount);
        const sb = new SupabaseClient(auth.supabaseUrl, auth.supabaseKey);

        logger.info('SYNC_START', 'Starting sync', { bigquery: job.bigquery, supabase: job.supabase });

    // --- PHASE 0: Schema Sync ---
    logger.info('SCHEMA_SYNC', 'Fetching BigQuery metadata', { table: job.bigquery.tableOrView });
    const bqMetadata = await bq.getTableMetadata(job.bigquery.projectId, job.bigquery.datasetId, job.bigquery.tableOrView);
    const bqFields: SchemaField[] = bqMetadata.schema.fields;

    logger.info('SCHEMA_SYNC', 'Ensuring Supabase table exists', { tableName: job.supabase.tableName });
    const createSql = generateCreateTableSQL(job.supabase.tableName, bqFields, job.supabase.upsertColumns);
    await sb.executeRawSQL(createSql);

    // Validate upsert columns exist in BigQuery schema
    const validation = validateUpsertColumns(job.supabase.upsertColumns, bqFields);
    if (!validation.valid) {
        throw new Error(buildUpsertValidationError(validation.invalidColumns));
    }

    // Detect schema drift
    const supabaseSchema = await sb.getTableSchema(job.supabase.tableName);
    const schemaChanges = detectSchemaChanges(bqFields, supabaseSchema);

    // Add new columns
    if (schemaChanges.columnsToAdd.length > 0) {
        logger.success('SCHEMA_SYNC', 'Adding new columns', { count: schemaChanges.columnsToAdd.length, columns: schemaChanges.columnsToAdd.map(c => c.name) });
        const addSql = generateAddColumnsSQL(job.supabase.tableName, schemaChanges.columnsToAdd);
        await sb.executeRawSQL(addSql);
    }

    // Drop removed columns
    if (schemaChanges.columnsToDrop.length > 0) {
        logger.warning('SCHEMA_SYNC', 'Dropping obsolete columns', { count: schemaChanges.columnsToDrop.length, columns: schemaChanges.columnsToDrop });
        const dropSql = generateDropColumnsSQL(job.supabase.tableName, schemaChanges.columnsToDrop);
        await sb.executeRawSQL(dropSql);
    }

    // Wait after schema changes for DB propagation
    if (schemaChanges.columnsToAdd.length > 0 || schemaChanges.columnsToDrop.length > 0) {
        logger.info('SCHEMA_SYNC', 'Schema changes applied, waiting for propagation');
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // --- PHASE 1: Data Sync ---
    logger.info('INCREMENTAL', 'Determining last sync date');
    let lastSyncDate = null;
    if (job.bigquery.incrementalColumn) {
        try {
            lastSyncDate = await sb.getLastSyncDateFromTable(job.supabase.tableName, job.bigquery.incrementalColumn);
        } catch (e: any) {
            logger.warning('INCREMENTAL', 'Could not fetch last sync date', { reason: e.message });
        }
    }

    logger.info('INCREMENTAL', 'Last sync date determined', { lastSyncDate: lastSyncDate || 'NONE' });

    let filter = '';
    if (lastSyncDate && job.bigquery.incrementalColumn) {
        // Detect column type from BigQuery schema
        const incrementalField = bqFields.find(
            f => f.name.toLowerCase() === job.bigquery.incrementalColumn!.toLowerCase()
        );
        
        // Use strict > for TIMESTAMP (millisecond precision), >= for DATE (day precision)
        const operator = incrementalField?.type === 'TIMESTAMP' ? '>' : '>=';
        filter = `WHERE ${job.bigquery.incrementalColumn} ${operator} '${lastSyncDate}'`;
        
        logger.debug('INCREMENTAL', 'Incremental filter applied', { operator, columnType: incrementalField?.type || 'UNKNOWN', filter });
    }

    const sql = `
    SELECT * 
    FROM \`${job.bigquery.projectId}.${job.bigquery.datasetId}.${job.bigquery.tableOrView}\`
    ${filter}
  `;

    logger.info('DATA_FETCH', 'Fetching data from BigQuery');
    const data = await bq.queryPaginated<any>(job.bigquery.projectId, sql);
    logger.success('DATA_FETCH', 'Data fetched from BigQuery', { totalRecords: data.length });

    if (data.length === 0) {
        logger.info('DATA_FETCH', 'No new data to sync');
        await logger.endRun(kvNamespace, 'success');
        return;
    }

    // Hybrid batching: Process data in page-batch chunks for better memory management
    // 5 pages * 5k rows = 25k rows per page-batch, then split into 2500-row upserts
    logger.info('UPSERT', 'Starting hybrid batch processing');
    const PAGES_PER_BATCH = 5; // 5 pages * 5k rows = 25k rows per batch
    const ROWS_PER_PAGE = 5000;
    const BATCH_SIZE = 2500; // Supabase upsert batch size
    
    for (let i = 0; i < data.length; i += (PAGES_PER_BATCH * ROWS_PER_PAGE)) {
        const pageBatch = data.slice(i, i + (PAGES_PER_BATCH * ROWS_PER_PAGE));
        logger.info('UPSERT', 'Processing page-batch', { rowsInBatch: pageBatch.length });
        
        // Upsert in smaller chunks to Supabase
        for (let j = 0; j < pageBatch.length; j += BATCH_SIZE) {
            const upsertBatch = pageBatch.slice(j, j + BATCH_SIZE);
            logger.debug('UPSERT', 'Upserting sub-batch', { batchNum: Math.floor(j / BATCH_SIZE) + 1, batchSize: upsertBatch.length });
            await sb.upsertTableData(job.supabase.tableName, upsertBatch, job.supabase.upsertColumns.join(','));
        }
        
        logger.success('UPSERT', 'Page-batch completed', { batchNum: Math.floor(i / (PAGES_PER_BATCH * ROWS_PER_PAGE)) + 1 });
    }

    logger.success('SYNC_COMPLETE', 'Job finished successfully', { totalRecords: data.length });
        await logger.endRun(kvNamespace, 'success');
    } catch (err: any) {
        logger.error('SYNC_ERROR', 'Sync failed', { error: err.message, stack: err.stack?.substring(0, 500) });
        await logger.endRun(kvNamespace, 'error');
        throw err;
    }
}
