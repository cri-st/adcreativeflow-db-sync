import { describe, it, expect } from 'vitest';
import { convertTimestampToBigQueryFormat, formatBigQueryTimestamp, isTimestampColumn } from './timestamp-utils';

describe('Timestamp Utils', () => {
    describe('formatBigQueryTimestamp', () => {
        it('should pad single digit hours with leading zero', () => {
            const result = formatBigQueryTimestamp({
                year: '2025',
                month: '02',
                day: '23',
                hour: '9',
                minute: '48',
                second: '20'
            });
            expect(result).toBe('2025-02-23 09:48:20');
        });

        it('should keep double digit hours as-is', () => {
            const result = formatBigQueryTimestamp({
                year: '2025',
                month: '02',
                day: '23',
                hour: '14',
                minute: '30',
                second: '45'
            });
            expect(result).toBe('2025-02-23 14:30:45');
        });
    });

    describe('convertTimestampToBigQueryFormat', () => {
        it('should return null for empty string', () => {
            expect(convertTimestampToBigQueryFormat('')).toBeNull();
        });

        it('should return null for undefined-like empty value', () => {
            expect(convertTimestampToBigQueryFormat('')).toBeNull();
        });

        it('should handle ISO 8601 format with T separator', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23T09:48:20');
            expect(result).toBe('2025-02-23 09:48:20');
        });

        it('should handle ISO 8601 format with timezone offset', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23T09:48:20+00:00');
            expect(result).toBe('2025-02-23 09:48:20');
        });

        it('should handle space-separated format with 2-digit hour', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 09:48:20');
            expect(result).toBe('2025-02-23 09:48:20');
        });

        it('should handle space-separated format with 1-digit hour (THE BUG)', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 9:48:20');
            expect(result).toBe('2025-02-23 09:48:20');
        });

        it('should handle format without seconds using T separator', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23T09:48');
            expect(result).toBe('2025-02-23 09:48:00');
        });

        it('should handle format without seconds using space separator', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 9:48');
            expect(result).toBe('2025-02-23 09:48:00');
        });

        it('should handle format without seconds with 2-digit hour', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 14:30');
            expect(result).toBe('2025-02-23 14:30:00');
        });

        it('should trim whitespace from input', () => {
            const result = convertTimestampToBigQueryFormat('  2025-02-23 9:48:20  ');
            expect(result).toBe('2025-02-23 09:48:20');
        });

        it('should return original value for non-matching format', () => {
            const input = 'not-a-timestamp';
            const result = convertTimestampToBigQueryFormat(input);
            expect(result).toBe(input);
        });

        it('should handle midnight hour correctly', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 0:00:00');
            expect(result).toBe('2025-02-23 00:00:00');
        });

        it('should handle noon hour correctly', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 12:00:00');
            expect(result).toBe('2025-02-23 12:00:00');
        });

        it('should handle single digit hour with leading zeros in minute and second', () => {
            const result = convertTimestampToBigQueryFormat('2025-02-23 9:05:03');
            expect(result).toBe('2025-02-23 09:05:03');
        });
    });

    describe('isTimestampColumn', () => {
        it('should detect time-related columns', () => {
            expect(isTimestampColumn('created_time')).toBe(true);
            expect(isTimestampColumn('start_time')).toBe(true);
            expect(isTimestampColumn('event_time')).toBe(true);
        });

        it('should detect date-related columns', () => {
            expect(isTimestampColumn('order_date')).toBe(true);
            expect(isTimestampColumn('start_date')).toBe(true);
            expect(isTimestampColumn('birth_date')).toBe(true);
        });

        it('should detect _at columns (created_at, updated_at, etc.)', () => {
            expect(isTimestampColumn('product_created_at')).toBe(true);
            expect(isTimestampColumn('updated_at')).toBe(true);
            expect(isTimestampColumn('deleted_at')).toBe(true);
            expect(isTimestampColumn('created_at')).toBe(true);
        });

        it('should detect timestamp columns', () => {
            expect(isTimestampColumn('event_timestamp')).toBe(true);
            expect(isTimestampColumn('timestamp')).toBe(true);
        });

        it('should detect created/updated/deleted columns', () => {
            expect(isTimestampColumn('created')).toBe(true);
            expect(isTimestampColumn('updated')).toBe(true);
            expect(isTimestampColumn('deleted')).toBe(true);
            expect(isTimestampColumn('modified')).toBe(true);
        });

        it('should be case insensitive', () => {
            expect(isTimestampColumn('Created_At')).toBe(true);
            expect(isTimestampColumn('ORDER_DATE')).toBe(true);
            expect(isTimestampColumn('Product_Created_At')).toBe(true);
        });

        it('should not detect non-timestamp columns', () => {
            expect(isTimestampColumn('name')).toBe(false);
            expect(isTimestampColumn('email')).toBe(false);
            expect(isTimestampColumn('amount')).toBe(false);
            expect(isTimestampColumn('status')).toBe(false);
            expect(isTimestampColumn('category')).toBe(false);
        });
    });
});
