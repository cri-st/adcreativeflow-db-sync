export interface SchemaField {
    name: string;
    type: string;
    mode?: string;
}

export function mapBigQueryTypeToPostgres(bqType: string): string {
    switch (bqType) {
        case 'STRING':
            return 'TEXT';
        case 'INTEGER':
        case 'INT64':
            return 'BIGINT';
        case 'FLOAT':
        case 'FLOAT64':
            return 'DOUBLE PRECISION';
        case 'BOOLEAN':
        case 'BOOL':
            return 'BOOLEAN';
        case 'DATE':
            return 'DATE';
        case 'DATETIME':
            return 'TIMESTAMP';
        case 'TIMESTAMP':
            return 'TIMESTAMPTZ';
        case 'NUMERIC':
        case 'BIGNUMERIC':
            return 'NUMERIC';
        default:
            return 'TEXT';
    }
}

export function generateCreateTableSQL(tableName: string, fields: SchemaField[], upsertColumns: string[]): string {
    const columnDefs = fields.map(f => {
        const pgType = mapBigQueryTypeToPostgres(f.type);
        return `"${f.name}" ${pgType}`;
    });

    // Basic columns that should always be there if not present
    const sql = `
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      ${columnDefs.join(',\n      ')},
      "synced_at" TIMESTAMPTZ DEFAULT NOW()
    );
  `;

    // We should also ensure the unique constraint exists for upsert
    const constraintName = `${tableName}_unique_idx`;
    const constraintSql = `
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '${constraintName}') THEN
        ALTER TABLE "${tableName}" ADD CONSTRAINT "${constraintName}" UNIQUE (${upsertColumns.map(c => `"${c}"`).join(', ')});
      END IF;
    END $$;
  `;

    return sql + '\n' + constraintSql;
}

export function generateAddColumnSQL(tableName: string, field: SchemaField): string {
    const pgType = mapBigQueryTypeToPostgres(field.type);
    return `ALTER TABLE "${tableName}" ADD COLUMN IF NOT EXISTS "${field.name}" ${pgType};`;
}

export function generateAddColumnsSQL(tableName: string, fields: SchemaField[]): string {
    if (fields.length === 0) return '';
    const additions = fields.map(f => {
        const pgType = mapBigQueryTypeToPostgres(f.type);
        return `ADD COLUMN IF NOT EXISTS "${f.name}" ${pgType}`;
    });
    return `ALTER TABLE "${tableName}" ${additions.join(',\n  ')};`;
}
