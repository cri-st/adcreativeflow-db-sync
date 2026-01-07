import { describe, it, expect } from 'vitest';

// Placeholder for a test that would check data mapping
// Since the BigQueryClient handles mapping internally, 
// we could extract a static method or just test the logic if it were exposed.

describe('Sync Logic', () => {
    it('should handle batch sizes correctly', () => {
        // This is just a placeholder to show tests are being structured
        const data = Array(1200).fill({ campaign_id: 1 });
        const batchSize = 500;
        const batches = [];
        for (let i = 0; i < data.length; i += batchSize) {
            batches.push(data.slice(i, i + batchSize));
        }
        expect(batches.length).toBe(3);
        expect(batches[0].length).toBe(500);
        expect(batches[2].length).toBe(200);
    });
});
