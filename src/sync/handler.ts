import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
import { generateAddColumnsSQL, generateCreateTableSQL, SchemaField } from './schema';

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

    console.log(`Starting sync job: ${job.name} (${job.id})`);

    // --- PHASE 0: Schema Sync ---
    console.log('Ensuring table exists and schema is up to date...');
    const bqMetadata = await bq.getTableMetadata(job.bigquery.projectId, job.bigquery.datasetId, job.bigquery.tableOrView);
    const bqFields: SchemaField[] = bqMetadata.schema.fields;

    const createSql = generateCreateTableSQL(job.supabase.tableName, bqFields, job.supabase.upsertColumns);
    await sb.executeRawSQL(createSql);

    // Pequeño delay para dar tiempo a que PostgREST recargue el schema (pgrst reload)
    await new Promise(resolve => setTimeout(resolve, 800));

    console.log(`Schema synchronized for ${job.supabase.tableName}.`);

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

    // Aumentamos batchSize a 2000 para reducir subrequests drasticamente
    const batchSize = 2500;
    for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        console.log(`Upserting batch ${Math.floor(i / batchSize) + 1} (${batch.length} records) to ${job.supabase.tableName}...`);
        await sb.upsertTableData(job.supabase.tableName, batch, job.supabase.upsertColumns.join(','));
    }

    console.log(`Sync job ${job.name} completed successfully.`);
}
