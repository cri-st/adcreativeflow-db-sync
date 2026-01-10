import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
import { generateAddColumnSQL, generateAddColumnsSQL, generateCreateTableSQL, SchemaField } from './schema';

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
    supabasePostgresUrl?: string;
}

export async function handleSync(auth: GlobalAuth, job: SyncJobConfig) {
    const bq = new BigQueryClient(auth.googleServiceAccount);
    const sb = new SupabaseClient(auth.supabaseUrl, auth.supabaseKey, auth.supabasePostgresUrl);

    console.log(`Starting sync job: ${job.name} (${job.id})`);

    // --- PHASE 0: Schema Sync ---
    console.log('Synchronizing schema...');
    const bqMetadata = await bq.getTableMetadata(job.bigquery.projectId, job.bigquery.datasetId, job.bigquery.tableOrView);
    const bqFields: SchemaField[] = bqMetadata.schema.fields;

    const exists = await sb.tableExists(job.supabase.tableName);
    if (!exists) {
        console.log(`Table ${job.supabase.tableName} does not exist. Creating...`);
        const createSql = generateCreateTableSQL(job.supabase.tableName, bqFields, job.supabase.upsertColumns);
        await sb.executeRawSQL(createSql);
    } else if (auth.supabasePostgresUrl) {
        console.log(`Checking columns for ${job.supabase.tableName}...`);
        const existingColumns = await sb.getTableColumns(job.supabase.tableName);
        const missingFields = bqFields.filter(f => !existingColumns.includes(f.name));

        if (missingFields.length > 0) {
            console.log(`Adding ${missingFields.length} missing columns: ${missingFields.map(f => f.name).join(', ')}`);
            const addSql = generateAddColumnsSQL(job.supabase.tableName, missingFields);
            await sb.executeRawSQL(addSql);
        }
    }

    // --- PHASE 1: Data Sync ---
    let lastSyncDate = null;
    if (job.bigquery.incrementalColumn) {
        lastSyncDate = await sb.getLastSyncDateFromTable(job.supabase.tableName, job.bigquery.incrementalColumn);
    }

    console.log(`Last sync date in Supabase [${job.supabase.tableName}]: ${lastSyncDate || 'None'}`);

    let filter = '';
    if (lastSyncDate && job.bigquery.incrementalColumn) {
        filter = `WHERE ${job.bigquery.incrementalColumn} >= '${lastSyncDate}'`;
    }

    const sql = `
    SELECT * 
    FROM \`${job.bigquery.projectId}.${job.bigquery.datasetId}.${job.bigquery.tableOrView}\`
    ${filter}
  `;

    console.log('Fetching data from BigQuery...');
    const data = await bq.query<any>(job.bigquery.projectId, sql);
    console.log(`Fetched ${data.length} records.`);

    if (data.length === 0) {
        console.log('No new data to sync.');
        return;
    }

    console.log(`Upserting ${data.length} records to ${job.supabase.tableName} in bulk...`);
    await sb.bulkUpsert(job.supabase.tableName, data, job.supabase.upsertColumns);

    console.log(`Sync job ${job.name} completed successfully.`);
}
