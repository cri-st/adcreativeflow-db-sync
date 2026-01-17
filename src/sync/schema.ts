export interface SchemaField {
    name: string;
    type: string;
    mode?: string;
}

export interface SchemaChanges {
    columnsToAdd: SchemaField[];
    columnsToDrop: string[];
}

export function detectSchemaChanges(bqFields: SchemaField[], supabaseFields: SchemaField[]): SchemaChanges {
    const columnsToAdd = bqFields.filter(bq =>
        !supabaseFields.some(sb =>
            sb.name.toLowerCase() === bq.name.toLowerCase()
        )
    );

    const columnsToDrop = supabaseFields
        .filter(sb => sb.name.toLowerCase() !== 'synced_at')
        .filter(sb =>
            !bqFields.some(bq =>
                bq.name.toLowerCase() === sb.name.toLowerCase()
            )
        )
        .map(sb => sb.name);

    return { columnsToAdd, columnsToDrop };
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

export function generateDropColumnsSQL(tableName: string, columnNames: string[]): string {
    if (columnNames.length === 0) return '';
    const drops = columnNames.map(col => `DROP COLUMN IF EXISTS "${col}"`);
    return `ALTER TABLE "${tableName}" ${drops.join(', ')};`;
}

export interface UpsertValidationResult {
    valid: boolean;
    invalidColumns: string[];
}

export function validateUpsertColumns(upsertColumns: string[], bqFields: SchemaField[]): UpsertValidationResult {
    const invalidColumns = upsertColumns.filter(col =>
        !bqFields.some(field =>
            field.name.toLowerCase() === col.toLowerCase()
        )
    );

    return {
        valid: invalidColumns.length === 0,
        invalidColumns
    };
}

export function buildUpsertValidationError(invalidColumns: string[]): string {
    return `Upsert columns not found in BigQuery schema: ${invalidColumns.join(', ')}. Check your sync configuration.`;
}
