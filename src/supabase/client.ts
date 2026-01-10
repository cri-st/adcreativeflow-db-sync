import { createClient } from '@supabase/supabase-js';
import postgres from 'postgres';

export class SupabaseClient {
    private client;
    private sql: postgres.Sql<{}> | null = null;

    constructor(url: string, key: string, postgresUrl?: string) {
        this.client = createClient(url, key);
        if (postgresUrl) {
            this.sql = postgres(postgresUrl, {
                ssl: 'require',
                prepare: false // Necessary for connection pooling (Supabase)
            });
        }
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
        if (!this.sql) {
            // Fallback to RPC if postgresUrl is missing
            const { error } = await this.client.rpc('exec_sql', { query });
            if (error) {
                throw new Error(`Supabase DDL Error: ${error.message}. Please provide SUPABASE_POSTGRES_URL or ensure exec_sql function exists.`);
            }
            return;
        }
        return await this.sql.unsafe(query);
    }

    async tableExists(tableName: string): Promise<boolean> {
        try {
            const { error } = await this.client
                .from(tableName)
                .select('count')
                .limit(0);

            if (error && error.code === '42P01') return false; // Undefined table
            return true;
        } catch {
            return false;
        }
    }

    async getTableColumns(tableName: string): Promise<string[]> {
        if (!this.sql) {
            // Fallback to information_schema via standard query if possible, 
            // but information_schema is usually restricted in PostgREST.
            // For now, let's assume we need direct SQL for this.
            throw new Error('SUPABASE_POSTGRES_URL is required for schema inspection.');
        }

        const columns = await this.sql`
          SELECT column_name 
          FROM information_schema.columns 
          WHERE table_name = ${tableName}
          AND table_schema = 'public'
      `;
        return columns.map(c => c.column_name);
    }
}
