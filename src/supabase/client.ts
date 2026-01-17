import { createClient } from '@supabase/supabase-js';
import { SchemaField } from '../sync/schema';

function mapPostgresToBigQueryType(pgType: string): string {
    switch (pgType.toLowerCase()) {
        case 'text':
            return 'STRING';
        case 'bigint':
            return 'INTEGER';
        case 'double precision':
            return 'FLOAT';
        case 'boolean':
            return 'BOOLEAN';
        case 'date':
            return 'DATE';
        case 'timestamp without time zone':
            return 'DATETIME';
        case 'timestamp with time zone':
            return 'TIMESTAMP';
        case 'numeric':
            return 'NUMERIC';
        default:
            return 'STRING';
    }
}

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

    async getTableSchema(tableName: string): Promise<SchemaField[]> {
        const query = `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${tableName}' AND table_schema = 'public' ORDER BY ordinal_position`;
        const result = await this.executeQuery(query);
        
        return result
            .filter((row: { column_name: string }) => row.column_name !== 'synced_at')
            .map((row: { column_name: string; data_type: string }) => ({
                name: row.column_name,
                type: mapPostgresToBigQueryType(row.data_type)
            }));
    }
}
