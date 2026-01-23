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

    /**
     * Delete rows from a table by ID columns
     * @param tableName - Target table name
     * @param idColumns - Array of column names that form the unique key
     * @param idsToDelete - Array of row identifiers (each is an array of values matching idColumns order)
     */
    async deleteRows(tableName: string, idColumns: string[], idsToDelete: any[][]): Promise<number> {
        if (idsToDelete.length === 0) return 0;

        const DELETE_BATCH_SIZE = 200;
        let totalDeleted = 0;

        for (let i = 0; i < idsToDelete.length; i += DELETE_BATCH_SIZE) {
            const batch = idsToDelete.slice(i, i + DELETE_BATCH_SIZE);

            if (idColumns.length === 1) {
                // Single column: use .in() filter
                const column = idColumns[0];
                const values = batch.map(row => row[0]);
                const { error, count } = await this.client
                    .from(tableName)
                    .delete({ count: 'exact' })
                    .in(column, values);

                if (error) {
                    throw new Error(`Failed to delete rows from ${tableName}: ${error.message}`);
                }
                totalDeleted += count || 0;
            } else {
                // Composite key: use .or() with multiple conditions
                const orConditions = batch.map(row => {
                    const conditions = idColumns.map((col, idx) => {
                        const value = row[idx];
                        const escapedValue = typeof value === 'string' ? value.replace(/'/g, "''") : value;
                        return `${col}.eq.${escapedValue}`;
                    }).join(',');
                    return `and(${conditions})`;
                }).join(',');

                const { error, count } = await this.client
                    .from(tableName)
                    .delete({ count: 'exact' })
                    .or(orConditions);

                if (error) {
                    throw new Error(`Failed to delete rows from ${tableName}: ${error.message}`);
                }
                totalDeleted += count || 0;
            }
        }

        return totalDeleted;
    }
}
