import { BigQueryClient } from '../bigquery/client';
import { SupabaseClient } from '../supabase/client';
import { FunnelData } from '../types/funnel';

export interface SyncConfig {
    googleServiceAccount: string;
    googleProjectId: string;
    supabaseUrl: string;
    supabaseKey: string;
}

export async function handleSync(config: SyncConfig) {
    const bq = new BigQueryClient(config.googleServiceAccount);
    const sb = new SupabaseClient(config.supabaseUrl, config.supabaseKey);

    // 1. Get the last synced date from Supabase
    const lastSyncDate = await sb.getLastSyncDate();
    console.log(`Last sync date in Supabase: ${lastSyncDate || 'None'}`);

    // 2. Build query
    // We use date_monday to filter new data.
    // We subtract 7 days to be safe in case of delayed reporting in BigQuery,
    // since upsert handles duplicates.
    let filter = '';
    if (lastSyncDate) {
        // If we have a last sync date, we look for data from that date onwards.
        // Using >= and the unique constraint (date_monday, campaign_id) 
        // ensures we don't duplicate but we DO update existing records for that Monday
        // if BigQuery has updated them (common in attribution windows).
        filter = `WHERE date_monday >= '${lastSyncDate}'`;
    }

    const sql = `
    SELECT * 
    FROM \`acf-ecomerce-database.shm_ebra.vw_shm_funnel\`
    ${filter}
    ORDER BY date_monday ASC
  `;

    // 3. Fetch from BigQuery
    console.log('Fetching data from BigQuery...');
    const data = await bq.query<FunnelData>(config.googleProjectId, sql);
    console.log(`Fetched ${data.length} records.`);

    if (data.length === 0) {
        console.log('No new data to sync.');
        return;
    }

    // 4. Batch upsert to Supabase (500 per batch)
    const batchSize = 500;
    for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        console.log(`Upserting batch ${Math.floor(i / batchSize) + 1}...`);
        await sb.upsertFunnelData(batch);
    }

    console.log('Sync completed successfully.');
}
