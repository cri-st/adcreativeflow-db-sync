import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
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

export async function handleSync(auth: GlobalAuth, job: SyncJobConfig) {
    const bq = new BigQueryClient(auth.googleServiceAccount);
    const sb = new SupabaseClient(auth.supabaseUrl, auth.supabaseKey);

    console.log(`[SYNC START] Job: ${job.name} (${job.id})`);

    // --- PHASE 0: Schema Sync ---
    console.log(`[PHASE 0] Fetching BigQuery metadata for ${job.bigquery.tableOrView}...`);
    const bqMetadata = await bq.getTableMetadata(job.bigquery.projectId, job.bigquery.datasetId, job.bigquery.tableOrView);
    const bqFields: SchemaField[] = bqMetadata.schema.fields;

    console.log(`[PHASE 0] Ensuring table ${job.supabase.tableName} exists...`);
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
        console.log(`[PHASE 0] Adding ${schemaChanges.columnsToAdd.length} new columns...`);
        const addSql = generateAddColumnsSQL(job.supabase.tableName, schemaChanges.columnsToAdd);
        await sb.executeRawSQL(addSql);
    }

    // Drop removed columns
    if (schemaChanges.columnsToDrop.length > 0) {
        console.log(`[PHASE 0] Dropping ${schemaChanges.columnsToDrop.length} obsolete columns...`);
        const dropSql = generateDropColumnsSQL(job.supabase.tableName, schemaChanges.columnsToDrop);
        await sb.executeRawSQL(dropSql);
    }

    // Wait after schema changes for DB propagation
    if (schemaChanges.columnsToAdd.length > 0 || schemaChanges.columnsToDrop.length > 0) {
        console.log(`[PHASE 0] Schema changes applied. Waiting 1000ms...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // --- PHASE 1: Data Sync ---
    console.log(`[PHASE 1] Determining last sync date...`);
    let lastSyncDate = null;
    if (job.bigquery.incrementalColumn) {
        try {
            lastSyncDate = await sb.getLastSyncDateFromTable(job.supabase.tableName, job.bigquery.incrementalColumn);
        } catch (e: any) {
            console.warn(`[PHASE 1] Could not fetch last sync date (table might be new): ${e.message}`);
        }
    }

    console.log(`[PHASE 1] Last sync date: ${lastSyncDate || 'NONE'}`);

    let filter = '';
    if (lastSyncDate && job.bigquery.incrementalColumn) {
        filter = `WHERE ${job.bigquery.incrementalColumn} >= '${lastSyncDate}'`;
    }

    const sql = `
    SELECT * 
    FROM \`${job.bigquery.projectId}.${job.bigquery.datasetId}.${job.bigquery.tableOrView}\`
    ${filter}
  `;

    console.log('[PHASE 2] Fetching data from BigQuery...');
    const data = await bq.query<any>(job.bigquery.projectId, sql);
    console.log(`[PHASE 2] Fetched ${data.length} records.`);

    if (data.length === 0) {
        console.log('[PHASE 2] No new data to sync.');
        return;
    }

    // Usamos PostgREST para el UPSERT de datos (sigue siendo más rápido para batches)
    // El bypass de PostgREST lo hacemos solo para DDL/Checks que fallaban por el cache.
    const batchSize = 2500;
    for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        console.log(`[PHASE 3] Upserting batch ${Math.floor(i / batchSize) + 1} (${batch.length} records)...`);
        await sb.upsertTableData(job.supabase.tableName, batch, job.supabase.upsertColumns.join(','));
    }

    console.log(`[SYNC COMPLETE] Job: ${job.name} finished successfully.`);
}
