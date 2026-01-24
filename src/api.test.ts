import { describe, it, expect, vi, beforeEach } from 'vitest';
import worker from './index';
import { SHEETS_WHITELIST } from './types/funnel';

if (!(globalThis as any).crypto) {
    (globalThis as any).crypto = { randomUUID: () => 'test-uuid' };
}

const mockKV = {
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn()
};

const mockEnv = {
    SYNC_CONFIGS: mockKV,
    SYNC_API_KEY: 'test-key',
    ASSETS: { fetch: vi.fn() }
};

const mockCtx = {
    waitUntil: vi.fn(),
    passThroughOnException: vi.fn()
};

describe('API Routes', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('POST /api/configs validates sheets url and extracts ID', async () => {
        const body = {
            type: 'sheets-to-bq',
            name: 'Test Sheet',
            enabled: true,
            sheets: {
                spreadsheetUrl: 'https://docs.google.com/spreadsheets/d/12345abcde/edit',
                sheetName: SHEETS_WHITELIST[0]
            },
            bigquery: {}
        };

        const req = new Request('http://localhost/api/configs', {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer test-key',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        const res = await worker.fetch(req, mockEnv as any, mockCtx as any);
        expect(res.status).toBe(200);
        
        expect(mockKV.put).toHaveBeenCalled();
        const callArgs = mockKV.put.mock.calls[0];
        const savedJob = JSON.parse(callArgs[1]);
        
        expect(savedJob.sheets.spreadsheetId).toBe('12345abcde');
        expect(savedJob.sheets.range).toBe(SHEETS_WHITELIST[0]);
    });

    it('POST /api/configs rejects invalid sheet name', async () => {
         const body = {
            type: 'sheets-to-bq',
            name: 'Test Sheet',
            enabled: true,
            sheets: {
                spreadsheetUrl: 'https://docs.google.com/spreadsheets/d/12345abcde/edit',
                sheetName: 'Invalid Name'
            },
            bigquery: {}
        };
        
        const req = new Request('http://localhost/api/configs', {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer test-key',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        const res = await worker.fetch(req, mockEnv as any, mockCtx as any);
        expect(res.status).toBe(400);
        const json = await res.json() as any;
        expect(json.error).toContain('Invalid sheet name');
    });

    it('POST /api/configs rejects invalid url', async () => {
         const body = {
            type: 'sheets-to-bq',
            name: 'Test Sheet',
            enabled: true,
            sheets: {
                spreadsheetUrl: 'https://invalid-url.com',
                sheetName: SHEETS_WHITELIST[0]
            },
            bigquery: {}
        };
        
        const req = new Request('http://localhost/api/configs', {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer test-key',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        const res = await worker.fetch(req, mockEnv as any, mockCtx as any);
        expect(res.status).toBe(400);
        const json = await res.json() as any;
        expect(json.error).toBe('Invalid spreadsheet URL');
    });
    
    it('PUT /api/configs/:id validates config', async () => {
         const body = {
            type: 'sheets-to-bq',
            id: 'existing-id',
            name: 'Test Sheet',
            enabled: true,
            sheets: {
                spreadsheetUrl: 'https://docs.google.com/spreadsheets/d/updated-id/edit',
                sheetName: SHEETS_WHITELIST[0]
            },
            bigquery: {}
        };
        
        const req = new Request('http://localhost/api/configs/existing-id', {
            method: 'PUT',
            headers: {
                'Authorization': 'Bearer test-key',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        const res = await worker.fetch(req, mockEnv as any, mockCtx as any);
        expect(res.status).toBe(200);
        
        expect(mockKV.put).toHaveBeenCalledWith('job:existing-id', expect.any(String));
        const callArgs = mockKV.put.mock.calls[0];
        const savedJob = JSON.parse(callArgs[1]);
        
        expect(savedJob.sheets.spreadsheetId).toBe('updated-id');
    });
});
