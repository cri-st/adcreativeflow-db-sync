import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';

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

    // 1. Get the last synced date from Supabase table specified in job
    // We need to modify SupabaseClient to accept table name
    let lastSyncDate = null;
    if (job.bigquery.incrementalColumn) {
        lastSyncDate = await getTableLastSyncDate(sb, job.supabase.tableName, job.bigquery.incrementalColumn);
    }

    console.log(`Last sync date in Supabase [${job.supabase.tableName}]: ${lastSyncDate || 'None'}`);

    // 2. Build query
    let filter = '';
    if (lastSyncDate && job.bigquery.incrementalColumn) {
        filter = `WHERE ${job.bigquery.incrementalColumn} >= '${lastSyncDate}'`;
    }

    const sql = `
    SELECT * 
    FROM \`${job.bigquery.projectId}.${job.bigquery.datasetId}.${job.bigquery.tableOrView}\`
    ${filter}
  `;

    // 3. Fetch from BigQuery
    console.log('Fetching data from BigQuery...');
    const data = await bq.query<any>(job.bigquery.projectId, sql);
    console.log(`Fetched ${data.length} records.`);

    if (data.length === 0) {
        console.log('No new data to sync.');
        return;
    }

    // 4. Batch upsert to Supabase
    const batchSize = 500;
    for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        console.log(`Upserting batch ${Math.floor(i / batchSize) + 1} to ${job.supabase.tableName}...`);
        await sb.upsertTableData(job.supabase.tableName, batch, job.supabase.upsertColumns.join(','));
    }

    console.log(`Sync job ${job.name} completed successfully.`);
}

// Helper to support dynamic table names in Supabase
async function getTableLastSyncDate(sb: SupabaseClient, tableName: string, column: string): Promise<string | null> {
    // We'll need to update SupabaseClient or use it raw
    // For now I'll assume we update SupabaseClient
    return await sb.getLastSyncDateFromTable(tableName, column);
}
