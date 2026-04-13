import { describe, it, expect, vi, beforeEach } from 'vitest';
import { cronMatchesTime, filterJobsByCron } from './queue';
import { SyncJobConfig } from '../types/funnel';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a UTC Date from parts — avoids local-timezone confusion in tests */
function utc(year: number, month: number, day: number, hour: number, minute: number): Date {
	return new Date(Date.UTC(year, month - 1, day, hour, minute, 0, 0));
}

/** Minimal valid SyncJobConfig fixture */
function makeJob(overrides: Partial<SyncJobConfig> = {}): SyncJobConfig {
	return {
		id: 'job-1',
		name: 'Test Job',
		enabled: true,
		type: 'bq-to-supabase',
		...overrides,
	} as SyncJobConfig;
}

// ---------------------------------------------------------------------------
// cronMatchesTime — wildcard
// ---------------------------------------------------------------------------

describe('cronMatchesTime — wildcard (*)', () => {
	it('matches any time when all fields are *', () => {
		expect(cronMatchesTime('* * * * *', utc(2026, 4, 13, 12, 0))).toBe(true);
		expect(cronMatchesTime('* * * * *', utc(2026, 1, 1, 0, 0))).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — specific values
// ---------------------------------------------------------------------------

describe('cronMatchesTime — specific numeric values', () => {
	it('matches exact minute', () => {
		// 30 * * * *  — fires at minute 30 of every hour
		expect(cronMatchesTime('30 * * * *', utc(2026, 4, 13, 14, 30))).toBe(true);
	});

	it('does not match wrong minute', () => {
		expect(cronMatchesTime('30 * * * *', utc(2026, 4, 13, 14, 31))).toBe(false);
	});

	it('matches exact hour', () => {
		// 0 12 * * *  — noon every day
		expect(cronMatchesTime('0 12 * * *', utc(2026, 4, 13, 12, 0))).toBe(true);
	});

	it('does not match wrong hour', () => {
		expect(cronMatchesTime('0 12 * * *', utc(2026, 4, 13, 11, 0))).toBe(false);
	});

	it('matches specific day-of-month', () => {
		// 0 0 15 * *  — 15th of each month at midnight
		expect(cronMatchesTime('0 0 15 * *', utc(2026, 4, 15, 0, 0))).toBe(true);
	});

	it('does not match wrong day-of-month', () => {
		expect(cronMatchesTime('0 0 15 * *', utc(2026, 4, 14, 0, 0))).toBe(false);
	});

	it('matches specific month', () => {
		// 0 0 1 4 *  — 1st April at midnight
		expect(cronMatchesTime('0 0 1 4 *', utc(2026, 4, 1, 0, 0))).toBe(true);
	});

	it('does not match wrong month', () => {
		expect(cronMatchesTime('0 0 1 4 *', utc(2026, 3, 1, 0, 0))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — step expressions
// ---------------------------------------------------------------------------

describe('cronMatchesTime — step expressions (*/step)', () => {
	it('matches every-6-hours schedule at hour 0', () => {
		// 0 */6 * * *  — 00:00, 06:00, 12:00, 18:00
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 0, 0))).toBe(true);
	});

	it('matches every-6-hours schedule at hour 6', () => {
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 6, 0))).toBe(true);
	});

	it('matches every-6-hours schedule at hour 12', () => {
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 12, 0))).toBe(true);
	});

	it('matches every-6-hours schedule at hour 18', () => {
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 18, 0))).toBe(true);
	});

	it('does not match every-6-hours at hour 3', () => {
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 3, 0))).toBe(false);
	});

	it('matches */15 minute steps at 0, 15, 30, 45', () => {
		const cron = '*/15 * * * *';
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 0))).toBe(true);
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 15))).toBe(true);
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 30))).toBe(true);
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 45))).toBe(true);
	});

	it('does not match */15 at minute 7', () => {
		expect(cronMatchesTime('*/15 * * * *', utc(2026, 4, 13, 10, 7))).toBe(false);
	});

	it('matches range/step: 1-23/2 at odd hours', () => {
		// hours 1, 3, 5, 7 ...
		expect(cronMatchesTime('0 1-23/2 * * *', utc(2026, 4, 13, 1, 0))).toBe(true);
		expect(cronMatchesTime('0 1-23/2 * * *', utc(2026, 4, 13, 3, 0))).toBe(true);
	});

	it('does not match range/step: 1-23/2 at even hours', () => {
		expect(cronMatchesTime('0 1-23/2 * * *', utc(2026, 4, 13, 2, 0))).toBe(false);
		expect(cronMatchesTime('0 1-23/2 * * *', utc(2026, 4, 13, 4, 0))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — ranges
// ---------------------------------------------------------------------------

describe('cronMatchesTime — ranges (N-M)', () => {
	it('matches hour within range', () => {
		// 0 8-17 * * *  — business hours
		expect(cronMatchesTime('0 8-17 * * *', utc(2026, 4, 13, 8, 0))).toBe(true);
		expect(cronMatchesTime('0 8-17 * * *', utc(2026, 4, 13, 12, 0))).toBe(true);
		expect(cronMatchesTime('0 8-17 * * *', utc(2026, 4, 13, 17, 0))).toBe(true);
	});

	it('does not match hour outside range', () => {
		expect(cronMatchesTime('0 8-17 * * *', utc(2026, 4, 13, 7, 0))).toBe(false);
		expect(cronMatchesTime('0 8-17 * * *', utc(2026, 4, 13, 18, 0))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — lists
// ---------------------------------------------------------------------------

describe('cronMatchesTime — comma-separated lists', () => {
	it('matches hour in list', () => {
		// 0 0,12 * * *  — midnight and noon
		expect(cronMatchesTime('0 0,12 * * *', utc(2026, 4, 13, 0, 0))).toBe(true);
		expect(cronMatchesTime('0 0,12 * * *', utc(2026, 4, 13, 12, 0))).toBe(true);
	});

	it('does not match hour not in list', () => {
		expect(cronMatchesTime('0 0,12 * * *', utc(2026, 4, 13, 6, 0))).toBe(false);
	});

	it('matches minute in list', () => {
		// 0,15,30,45 * * * *  — every quarter-hour
		const cron = '0,15,30,45 * * * *';
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 0))).toBe(true);
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 15))).toBe(true);
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 30))).toBe(true);
		expect(cronMatchesTime(cron, utc(2026, 4, 13, 10, 45))).toBe(true);
	});

	it('does not match minute not in list', () => {
		expect(cronMatchesTime('0,15,30,45 * * * *', utc(2026, 4, 13, 10, 1))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — day-of-week
// ---------------------------------------------------------------------------

describe('cronMatchesTime — day-of-week', () => {
	// 2026-04-13 is a Monday (dow = 1)
	// 2026-04-12 is a Sunday  (dow = 0)
	// 2026-04-14 is a Tuesday (dow = 2)

	it('matches Monday when cron uses dow=1', () => {
		expect(cronMatchesTime('0 8 * * 1', utc(2026, 4, 13, 8, 0))).toBe(true);
	});

	it('does not match Monday when cron specifies another day', () => {
		expect(cronMatchesTime('0 8 * * 2', utc(2026, 4, 13, 8, 0))).toBe(false);
	});

	it('matches Sunday with dow=0', () => {
		expect(cronMatchesTime('0 0 * * 0', utc(2026, 4, 12, 0, 0))).toBe(true);
	});

	it('matches Sunday with dow=7 (treated as 0)', () => {
		expect(cronMatchesTime('0 0 * * 7', utc(2026, 4, 12, 0, 0))).toBe(true);
	});

	it('does not match Sunday for a weekday-only range 1-5', () => {
		expect(cronMatchesTime('0 8 * * 1-5', utc(2026, 4, 12, 8, 0))).toBe(false);
	});

	it('matches Tuesday within weekday range 1-5', () => {
		// 2026-04-14 Tuesday
		expect(cronMatchesTime('0 8,12 * * 1-5', utc(2026, 4, 14, 12, 0))).toBe(true);
	});

	it('matches any day with dow=*', () => {
		expect(cronMatchesTime('0 0 * * *', utc(2026, 4, 12, 0, 0))).toBe(true);
		expect(cronMatchesTime('0 0 * * *', utc(2026, 4, 13, 0, 0))).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — spec acceptance scenarios
// ---------------------------------------------------------------------------

describe('cronMatchesTime — spec acceptance scenarios', () => {
	it('Scenario: step-based schedule matches the scheduled execution time', () => {
		// GIVEN the scheduled execution time is 2026-04-13T12:00:00Z
		// AND a job has cron schedule `0 */6 * * *`
		// THEN the job is considered due
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 12, 0))).toBe(true);
	});

	it('Scenario: trigger cron string does not control due status', () => {
		// The job schedule is `0 */6 * * *` and the platform trigger was `0 * * * *`
		// At 12:00 UTC, the job IS due regardless of what trigger fired
		const scheduledTime = utc(2026, 4, 13, 12, 0);
		// Old code: matchesCron('0 */6 * * *', '0 * * * *') === false (wrong)
		// New code: cronMatchesTime('0 */6 * * *', scheduledTime) === true (correct)
		expect(cronMatchesTime('0 */6 * * *', scheduledTime)).toBe(true);
	});

	it('Scenario: lists and ranges are supported', () => {
		// 2026-04-14 is a Tuesday (dow = 2), at 12:00
		expect(cronMatchesTime('0 8,12 * * 1-5', utc(2026, 4, 14, 12, 0))).toBe(true);
	});

	it('Scenario: non-matching time does not run the job', () => {
		// 0 */6 * * * does NOT match 07:00
		expect(cronMatchesTime('0 */6 * * *', utc(2026, 4, 13, 7, 0))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — invalid input
// ---------------------------------------------------------------------------

describe('cronMatchesTime — invalid / edge-case input', () => {
	it('returns false for empty string', () => {
		expect(cronMatchesTime('', utc(2026, 4, 13, 12, 0))).toBe(false);
	});

	it('returns false for wrong field count (4 fields)', () => {
		expect(cronMatchesTime('0 */6 * *', utc(2026, 4, 13, 12, 0))).toBe(false);
	});

	it('returns false for wrong field count (6 fields)', () => {
		expect(cronMatchesTime('0 */6 * * * *', utc(2026, 4, 13, 12, 0))).toBe(false);
	});

	it('returns false for non-numeric fields', () => {
		expect(cronMatchesTime('abc */6 * * *', utc(2026, 4, 13, 12, 0))).toBe(false);
	});

	it('does not throw for garbage input', () => {
		expect(() => cronMatchesTime('!!!', utc(2026, 4, 13, 12, 0))).not.toThrow();
	});

	it('returns false for null-ish expression', () => {
		expect(cronMatchesTime(null as unknown as string, utc(2026, 4, 13, 12, 0))).toBe(false);
		expect(cronMatchesTime(undefined as unknown as string, utc(2026, 4, 13, 12, 0))).toBe(false);
	});

	it('handles extra surrounding whitespace gracefully', () => {
		expect(cronMatchesTime('  0 */6 * * *  ', utc(2026, 4, 13, 12, 0))).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — bounds validation (out-of-range values must return false)
// ---------------------------------------------------------------------------

describe('cronMatchesTime — out-of-range bounds validation', () => {
	const anyTime = utc(2026, 4, 13, 8, 0); // Monday 08:00 UTC

	it('returns false for out-of-range hour range: 0 8-99 * * *', () => {
		expect(cronMatchesTime('0 8-99 * * *', anyTime)).toBe(false);
	});

	it('returns false for out-of-range range/step: 0 1-99/2 * * *', () => {
		expect(cronMatchesTime('0 1-99/2 * * *', anyTime)).toBe(false);
	});

	it('returns false for out-of-range minute: 99 * * * *', () => {
		expect(cronMatchesTime('99 * * * *', anyTime)).toBe(false);
	});

	it('returns false for out-of-range month: * * * 13 *', () => {
		expect(cronMatchesTime('* * * 13 *', anyTime)).toBe(false);
	});

	it('returns false for out-of-range day-of-month: * * 32 * *', () => {
		expect(cronMatchesTime('* * 32 * *', anyTime)).toBe(false);
	});

	it('returns false for out-of-range dow: 0 0 * * 8', () => {
		// dow 8 is invalid — neither a standard weekday nor the 7=Sunday alias
		expect(cronMatchesTime('0 0 * * 8', utc(2026, 4, 12, 0, 0))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// cronMatchesTime — day-of-week ranges including 7 (Sunday alias)
// ---------------------------------------------------------------------------

describe('cronMatchesTime — day-of-week ranges with Sunday alias (7)', () => {
	// 2026-04-10 Friday  (dow = 5)
	// 2026-04-11 Saturday (dow = 6)
	// 2026-04-12 Sunday  (dow = 0)
	// 2026-04-13 Monday  (dow = 1)

	it('* * * * 5-7 matches Friday', () => {
		expect(cronMatchesTime('* * * * 5-7', utc(2026, 4, 10, 9, 0))).toBe(true);
	});

	it('* * * * 5-7 matches Saturday', () => {
		expect(cronMatchesTime('* * * * 5-7', utc(2026, 4, 11, 9, 0))).toBe(true);
	});

	it('* * * * 5-7 matches Sunday (7 treated as 0)', () => {
		expect(cronMatchesTime('* * * * 5-7', utc(2026, 4, 12, 9, 0))).toBe(true);
	});

	it('* * * * 5-7 does not match Monday', () => {
		expect(cronMatchesTime('* * * * 5-7', utc(2026, 4, 13, 9, 0))).toBe(false);
	});

	it('* * * * 5-7 does not match Tuesday', () => {
		expect(cronMatchesTime('* * * * 5-7', utc(2026, 4, 14, 9, 0))).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// filterJobsByCron — Date-based filtering
// ---------------------------------------------------------------------------

describe('filterJobsByCron', () => {
	const t12 = utc(2026, 4, 13, 12, 0); // Monday, 12:00 UTC
	const t07 = utc(2026, 4, 13, 7, 0);  // Monday, 07:00 UTC

	it('returns only jobs whose cron matches the scheduled time', () => {
		const jobs: SyncJobConfig[] = [
			makeJob({ id: 'a', cronSchedule: '0 */6 * * *' }), // matches 12:00
			makeJob({ id: 'b', cronSchedule: '0 7 * * *' }),   // does not match 12:00
		];
		const result = filterJobsByCron(jobs, t12);
		expect(result).toHaveLength(1);
		expect(result[0].id).toBe('a');
	});

	it('uses default schedule 0 */6 * * * for jobs without cronSchedule', () => {
		const jobs: SyncJobConfig[] = [
			makeJob({ id: 'default' }), // no cronSchedule — defaults to 0 */6 * * *
		];
		// 12:00 matches 0 */6 * * *
		expect(filterJobsByCron(jobs, t12)).toHaveLength(1);
		// 07:00 does NOT match 0 */6 * * *
		expect(filterJobsByCron(jobs, t07)).toHaveLength(0);
	});

	it('excludes disabled jobs even when cron matches', () => {
		const jobs: SyncJobConfig[] = [
			makeJob({ id: 'disabled', enabled: false, cronSchedule: '0 */6 * * *' }),
		];
		expect(filterJobsByCron(jobs, t12)).toHaveLength(0);
	});

	it('returns empty array when no jobs match', () => {
		const jobs: SyncJobConfig[] = [
			makeJob({ id: 'a', cronSchedule: '0 7 * * *' }),
			makeJob({ id: 'b', cronSchedule: '0 18 * * *' }),
		];
		expect(filterJobsByCron(jobs, t12)).toHaveLength(0);
	});

	it('returns all matching enabled jobs', () => {
		const jobs: SyncJobConfig[] = [
			makeJob({ id: 'a', cronSchedule: '0 */6 * * *' }),  // matches 12:00
			makeJob({ id: 'b', cronSchedule: '0 12 * * *' }),   // matches 12:00
			makeJob({ id: 'c', cronSchedule: '0 8 * * *' }),    // no match
			makeJob({ id: 'd', enabled: false, cronSchedule: '0 12 * * *' }), // disabled
		];
		const result = filterJobsByCron(jobs, t12);
		expect(result).toHaveLength(2);
		expect(result.map(j => j.id).sort()).toEqual(['a', 'b']);
	});

	it('is decoupled from trigger string — jobs matching 0 */6 are due at 12:00 even if trigger was 0 * * * *', () => {
		// This is the core regression guard
		const jobs: SyncJobConfig[] = [
			makeJob({ id: 'six-hourly', cronSchedule: '0 */6 * * *' }),
		];
		// trigger string is irrelevant — we only care about the scheduled time
		expect(filterJobsByCron(jobs, t12)).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// originUrl threading — scheduled handler integration smoke test
// ---------------------------------------------------------------------------

describe('scheduled handler — originUrl threading', () => {
	it('worker exports a scheduled handler', async () => {
		const worker = (await import('../index')).default;
		expect(typeof worker.scheduled).toBe('function');
	});

	it('scheduled handler accepts an event with scheduledTime', async () => {
		const worker = (await import('../index')).default;

		const mockKV = {
			list: vi.fn().mockResolvedValue({ keys: [] }),
			get: vi.fn().mockResolvedValue(null),
			put: vi.fn().mockResolvedValue(undefined),
		};

		const mockEnv = {
			SYNC_CONFIGS: mockKV,
			SYNC_LOGS: mockKV,
			SYNC_API_KEY: 'test-key',
			WORKER_URL: 'https://my-worker.example.com',
			GOOGLE_SERVICE_ACCOUNT_JSON: '{}',
			GOOGLE_PROJECT_ID: 'proj',
			SUPABASE_URL: 'https://db.supabase.co',
			SUPABASE_SERVICE_KEY: 'key',
			ASSETS: { fetch: vi.fn() },
		};

		const mockCtx = {
			waitUntil: vi.fn(),
			passThroughOnException: vi.fn(),
		};

		// 12:00 UTC on a Monday — should match 0 */6 * * *
		const mockEvent = {
			scheduledTime: utc(2026, 4, 13, 12, 0).getTime(),
			cron: '0 * * * *', // trigger cron — must NOT influence job selection
		} as ScheduledEvent;

		// Should not throw when no matching jobs exist
		await expect(worker.scheduled(mockEvent, mockEnv as any, mockCtx as any)).resolves.not.toThrow();
	});

	it('warns when WORKER_URL is missing', async () => {
		const worker = (await import('../index')).default;
		const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

		const mockKV = {
			list: vi.fn().mockResolvedValue({ keys: [] }),
			get: vi.fn().mockResolvedValue(null),
			put: vi.fn().mockResolvedValue(undefined),
		};

		const mockEnvWithoutUrl = {
			SYNC_CONFIGS: mockKV,
			SYNC_LOGS: mockKV,
			SYNC_API_KEY: 'test-key',
			// WORKER_URL intentionally omitted
			GOOGLE_SERVICE_ACCOUNT_JSON: '{}',
			GOOGLE_PROJECT_ID: 'proj',
			SUPABASE_URL: 'https://db.supabase.co',
			SUPABASE_SERVICE_KEY: 'key',
			ASSETS: { fetch: vi.fn() },
		};

		const mockCtx = {
			waitUntil: vi.fn(),
			passThroughOnException: vi.fn(),
		};

		const mockEvent = {
			scheduledTime: utc(2026, 4, 13, 12, 0).getTime(),
			cron: '0 * * * *',
		} as ScheduledEvent;

		await worker.scheduled(mockEvent, mockEnvWithoutUrl as any, mockCtx as any);

		expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('WORKER_URL'));
		consoleSpy.mockRestore();
	});
});
