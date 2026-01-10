import { createClient } from '@supabase/supabase-js';

export class SupabaseClient {
    private client;

    constructor(url: string, key: string) {
        this.client = createClient(url, key);
    }

    async upsertTableData(tableName: string, data: any[], onConflict: string) {
        if (data.length === 0) return;

        const { error } = await this.client
            .from(tableName)
            .upsert(data, {
                onConflict: onConflict,
                ignoreDuplicates: false
            });

        if (error) {
            throw new Error(`Supabase Error (${tableName}): ${error.message}`);
        }
    }

    async getLastSyncDateFromTable(tableName: string, column: string): Promise<string | null> {
        const { data, error } = await this.client
            .from(tableName)
            .select(column)
            .order(column, { ascending: false })
            .limit(1)
            .single();

        if (error && error.code !== 'PGRST116' && error.code !== '42P01') {
            throw new Error(`Supabase Query Error (${tableName}): ${error.message}`);
        }

        return data ? (data as any)[column] : null;
    }

    async executeRawSQL(query: string) {
        const { error } = await this.client.rpc('exec_sql', { query });
        if (error) {
            throw new Error(`Supabase DDL Error: ${error.message}. Ensure the 'exec_sql' RPC function exists in Supabase.`);
        }
    }
}
