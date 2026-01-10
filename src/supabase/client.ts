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
        const query = `SELECT "${column}" FROM "${tableName}" ORDER BY "${column}" DESC LIMIT 1`;
        const result = await this.executeQuery(query);

        if (result && result.length > 0) {
            return (result[0] as any)[column];
        }
        return null;
    }

    async executeRawSQL(query: string) {
        const { error } = await this.client.rpc('exec_sql', { query });
        if (error) {
            throw new Error(`Supabase DDL Error: ${error.message}. Ensure 'exec_sql' exists.`);
        }
    }

    async executeQuery(query: string): Promise<any[]> {
        const { data, error } = await this.client.rpc('exec_sql_query', { query });
        if (error) {
            // Manejar error de tabla no existente de forma amigable para el sync
            if (error.message.includes('does not exist')) return [];
            throw new Error(`Supabase Query Error: ${error.message}`);
        }
        return data || [];
    }
}
