import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BigQueryClient } from './client';

vi.mock('jose', () => ({
    SignJWT: class {
        setProtectedHeader() { return this; }
        setIssuedAt() { return this; }
        setExpirationTime() { return this; }
        async sign() { return 'mock-jwt-token'; }
    },
    importPKCS8: async () => 'mock-key',
}));

const mockServiceAccount = JSON.stringify({
    client_email: 'test@project.iam.gserviceaccount.com',
    private_key_id: 'key123',
    private_key: 'mock-private-key',
});

function createMockSchema() {
    return {
        fields: [
            { name: 'id', type: 'INTEGER' },
            { name: 'name', type: 'STRING' },
            { name: 'score', type: 'FLOAT' },
        ],
    };
}

function createMockRow(id: number, name: string, score: number) {
    return {
        f: [
            { v: String(id) },
            { v: name },
            { v: String(score) },
        ],
    };
}

function createTokenResponse() {
    return {
        access_token: 'mock-token-12345',
        expires_in: 3600,
    };
}

describe('BigQueryClient.queryPaginated', () => {
    const originalFetch = globalThis.fetch;
    let fetchMock: ReturnType<typeof vi.fn>;
    let consoleSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        globalThis.fetch = fetchMock as typeof fetch;
        consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    afterEach(() => {
        globalThis.fetch = originalFetch;
        consoleSpy.mockRestore();
    });

    it('handles single page result (no pageToken in response)', async () => {
        const schema = createMockSchema();
        const rows = [
            createMockRow(1, 'Alice', 95.5),
            createMockRow(2, 'Bob', 87.3),
        ];

        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        jobComplete: true,
                        jobReference: { jobId: 'job-123' },
                        schema,
                        rows,
                        totalRows: '2',
                        pageToken: '',
                    }),
            });

        const client = new BigQueryClient(mockServiceAccount);
        const result = await client.queryPaginated<{ id: number; name: string; score: number }>(
            'my-project',
            'SELECT * FROM table'
        );

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual({ id: 1, name: 'Alice', score: 95.5 });
        expect(result[1]).toEqual({ id: 2, name: 'Bob', score: 87.3 });
        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(consoleSpy).toHaveBeenCalledWith('[BQ PAGINATION] Page 1/1 (2 rows fetched)');
    });

    it('handles multi-page result (3 pages, verifies 3 fetches)', async () => {
        const schema = createMockSchema();

        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        jobComplete: true,
                        jobReference: { jobId: 'job-456' },
                        schema,
                        rows: [createMockRow(1, 'Page1-A', 10.0), createMockRow(2, 'Page1-B', 20.0)],
                        totalRows: '15000',
                        pageToken: 'token-page-2',
                    }),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        rows: [createMockRow(3, 'Page2-A', 30.0), createMockRow(4, 'Page2-B', 40.0)],
                        pageToken: 'token-page-3',
                    }),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        rows: [createMockRow(5, 'Page3-A', 50.0), createMockRow(6, 'Page3-B', 60.0)],
                        pageToken: '',
                    }),
            });

        const client = new BigQueryClient(mockServiceAccount);
        const result = await client.queryPaginated<{ id: number; name: string; score: number }>(
            'my-project',
            'SELECT * FROM large_table'
        );

        expect(result).toHaveLength(6);
        expect(result.map((r) => r.id)).toEqual([1, 2, 3, 4, 5, 6]);
        expect(result.map((r) => r.name)).toEqual([
            'Page1-A',
            'Page1-B',
            'Page2-A',
            'Page2-B',
            'Page3-A',
            'Page3-B',
        ]);

        expect(fetchMock).toHaveBeenCalledTimes(4);

        const thirdCall = fetchMock.mock.calls[2];
        expect(thirdCall[0]).toContain('pageToken=token-page-2');
        expect(thirdCall[0]).toContain('maxResults=5000');

        const fourthCall = fetchMock.mock.calls[3];
        expect(fourthCall[0]).toContain('pageToken=token-page-3');

        expect(consoleSpy).toHaveBeenCalledWith('[BQ PAGINATION] Page 1/3 (2 rows fetched)');
        expect(consoleSpy).toHaveBeenCalledWith('[BQ PAGINATION] Page 2/3 (4 rows fetched)');
        expect(consoleSpy).toHaveBeenCalledWith('[BQ PAGINATION] Page 3/3 (6 rows fetched)');
    });

    it('handles empty result (0 rows)', async () => {
        const schema = createMockSchema();

        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        jobComplete: true,
                        jobReference: { jobId: 'job-789' },
                        schema,
                        rows: undefined,
                        totalRows: '0',
                        pageToken: '',
                    }),
            });

        const client = new BigQueryClient(mockServiceAccount);
        const result = await client.queryPaginated<{ id: number; name: string; score: number }>(
            'my-project',
            'SELECT * FROM empty_table'
        );

        expect(result).toHaveLength(0);
        expect(result).toEqual([]);
        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(consoleSpy).toHaveBeenCalledWith('[BQ PAGINATION] Page 1/1 (0 rows fetched)');
    });

    it('throws error when initial query fails', async () => {
        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        error: { message: 'Invalid query syntax' },
                    }),
            });

        const client = new BigQueryClient(mockServiceAccount);

        await expect(
            client.queryPaginated('my-project', 'INVALID SQL')
        ).rejects.toThrow('BigQuery Error: Invalid query syntax');
    });

    it('throws error when job does not complete', async () => {
        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        jobComplete: false,
                        jobReference: { jobId: 'job-slow' },
                    }),
            });

        const client = new BigQueryClient(mockServiceAccount);

        await expect(
            client.queryPaginated('my-project', 'SELECT * FROM slow_table')
        ).rejects.toThrow('BigQuery Error: Query did not complete. Consider increasing timeout.');
    });

    it('throws error on pagination request failure', async () => {
        const schema = createMockSchema();

        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        jobComplete: true,
                        jobReference: { jobId: 'job-fail-page' },
                        schema,
                        rows: [createMockRow(1, 'Test', 1.0)],
                        totalRows: '100',
                        pageToken: 'next-token',
                    }),
            })
            .mockResolvedValueOnce({
                json: () =>
                    Promise.resolve({
                        error: { message: 'Rate limit exceeded' },
                    }),
            });

        const client = new BigQueryClient(mockServiceAccount);

        await expect(
            client.queryPaginated('my-project', 'SELECT * FROM rate_limited')
        ).rejects.toThrow('BigQuery Pagination Error: Rate limit exceeded');
    });
});

describe('BigQueryClient.query (parseRows integration)', () => {
    const originalFetch = globalThis.fetch;
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        globalThis.fetch = fetchMock as typeof fetch;
    });

    afterEach(() => {
        globalThis.fetch = originalFetch;
    });

    it('correctly parses INTEGER, FLOAT, and STRING types', async () => {
        const schema = {
            fields: [
                { name: 'count', type: 'INT64' },
                { name: 'average', type: 'FLOAT64' },
                { name: 'label', type: 'STRING' },
            ],
        };
        const rows = [
            { f: [{ v: '42' }, { v: '3.14159' }, { v: 'test-label' }] },
        ];

        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () => Promise.resolve({ schema, rows }),
            });

        const client = new BigQueryClient(mockServiceAccount);
        const result = await client.query<{ count: number; average: number; label: string }>(
            'my-project',
            'SELECT count, average, label FROM test'
        );

        expect(result[0].count).toBe(42);
        expect(result[0].average).toBeCloseTo(3.14159);
        expect(result[0].label).toBe('test-label');
    });

    it('handles null values correctly', async () => {
        const schema = {
            fields: [
                { name: 'id', type: 'INTEGER' },
                { name: 'value', type: 'STRING' },
            ],
        };
        const rows = [
            { f: [{ v: '1' }, { v: null }] },
        ];

        fetchMock
            .mockResolvedValueOnce({
                json: () => Promise.resolve(createTokenResponse()),
            })
            .mockResolvedValueOnce({
                json: () => Promise.resolve({ schema, rows }),
            });

        const client = new BigQueryClient(mockServiceAccount);
        const result = await client.query<{ id: number; value: string | null }>(
            'my-project',
            'SELECT id, value FROM test'
        );

        expect(result[0].id).toBe(1);
        expect(result[0].value).toBeNull();
    });
});
