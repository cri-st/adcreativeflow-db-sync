import { describe, it, expect } from 'vitest';
import {
    SchemaField,
    SchemaChanges,
    detectSchemaChanges,
    generateDropColumnsSQL,
} from './schema';

describe('detectSchemaChanges', () => {
    it('should return empty arrays when schemas are identical', () => {
        const bqFields: SchemaField[] = [
            { name: 'id', type: 'INTEGER' },
            { name: 'name', type: 'STRING' },
        ];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'name', type: 'TEXT' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([]);
        expect(result.columnsToDrop).toEqual([]);
    });

    it('should detect new columns in BigQuery not in Supabase', () => {
        const bqFields: SchemaField[] = [
            { name: 'id', type: 'INTEGER' },
            { name: 'name', type: 'STRING' },
            { name: 'email', type: 'STRING' },
        ];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'name', type: 'TEXT' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([{ name: 'email', type: 'STRING' }]);
        expect(result.columnsToDrop).toEqual([]);
    });

    it('should detect dropped columns in Supabase not in BigQuery', () => {
        const bqFields: SchemaField[] = [
            { name: 'id', type: 'INTEGER' },
        ];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'old_column', type: 'TEXT' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([]);
        expect(result.columnsToDrop).toEqual(['old_column']);
    });

    it('should detect both new and dropped columns', () => {
        const bqFields: SchemaField[] = [
            { name: 'id', type: 'INTEGER' },
            { name: 'new_col', type: 'STRING' },
        ];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'old_col', type: 'TEXT' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([{ name: 'new_col', type: 'STRING' }]);
        expect(result.columnsToDrop).toEqual(['old_col']);
    });

    it('should perform case-insensitive column name comparison', () => {
        const bqFields: SchemaField[] = [
            { name: 'ID', type: 'INTEGER' },
            { name: 'Name', type: 'STRING' },
        ];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'name', type: 'TEXT' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([]);
        expect(result.columnsToDrop).toEqual([]);
    });

    it('should exclude synced_at from columnsToDrop', () => {
        const bqFields: SchemaField[] = [
            { name: 'id', type: 'INTEGER' },
        ];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'synced_at', type: 'TIMESTAMPTZ' },
            { name: 'old_column', type: 'TEXT' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([]);
        expect(result.columnsToDrop).toEqual(['old_column']);
        expect(result.columnsToDrop).not.toContain('synced_at');
    });

    it('should handle empty BigQuery schema', () => {
        const bqFields: SchemaField[] = [];
        const supabaseFields: SchemaField[] = [
            { name: 'id', type: 'BIGINT' },
            { name: 'synced_at', type: 'TIMESTAMPTZ' },
        ];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([]);
        expect(result.columnsToDrop).toEqual(['id']);
    });

    it('should handle empty Supabase schema', () => {
        const bqFields: SchemaField[] = [
            { name: 'id', type: 'INTEGER' },
            { name: 'name', type: 'STRING' },
        ];
        const supabaseFields: SchemaField[] = [];

        const result = detectSchemaChanges(bqFields, supabaseFields);

        expect(result.columnsToAdd).toEqual([
            { name: 'id', type: 'INTEGER' },
            { name: 'name', type: 'STRING' },
        ]);
        expect(result.columnsToDrop).toEqual([]);
    });
});

describe('generateDropColumnsSQL', () => {
    it('should return empty string for empty array', () => {
        const result = generateDropColumnsSQL('users', []);
        expect(result).toBe('');
    });

    it('should generate SQL for single column', () => {
        const result = generateDropColumnsSQL('users', ['old_col']);
        expect(result).toBe('ALTER TABLE "users" DROP COLUMN IF EXISTS "old_col";');
    });

    it('should generate SQL for multiple columns', () => {
        const result = generateDropColumnsSQL('users', ['col1', 'col2']);
        expect(result).toBe('ALTER TABLE "users" DROP COLUMN IF EXISTS "col1", DROP COLUMN IF EXISTS "col2";');
    });

    it('should quote table and column names properly', () => {
        const result = generateDropColumnsSQL('user-data', ['my-column']);
        expect(result).toBe('ALTER TABLE "user-data" DROP COLUMN IF EXISTS "my-column";');
    });
});
