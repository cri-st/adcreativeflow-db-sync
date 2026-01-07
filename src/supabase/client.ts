import { createClient } from '@supabase/supabase-js';
import { FunnelData } from '../types/funnel';

export class SupabaseClient {
    private client;

    constructor(url: string, key: string) {
        this.client = createClient(url, key);
    }

    async upsertFunnelData(data: FunnelData[]) {
        if (data.length === 0) return;

        const { error } = await this.client
            .from('vw_shm_funnel')
            .upsert(data, {
                onConflict: 'date_monday,campaign_id',
                ignoreDuplicates: false // Set to false to update if data changes, as requested "data nueva", but user also said "lo q ya hay mantenerlo ahi". 
                // Actually, user said: "traer la data nueva, lo q ya hay mantenerlo ahi". 
                // Usually in marketing data, "new data" means new records OR updates to recent days.
                // I'll keep ignoreDuplicates: false to allow updates if a campaign's data changes for a date.
            });

        if (error) {
            throw new Error(`Supabase Error: ${error.message}`);
        }
    }

    async getLastSyncDate(): Promise<string | null> {
        const { data, error } = await this.client
            .from('vw_shm_funnel')
            .select('date_monday')
            .order('date_monday', { ascending: false })
            .limit(1)
            .single();

        if (error && error.code !== 'PGRST116') { // PGRST116 is "no rows returned"
            throw new Error(`Supabase Query Error: ${error.message}`);
        }

        return data?.date_monday || null;
    }
}
