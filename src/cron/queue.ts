import { SyncJobConfig } from '../types/funnel';

export interface CronSchedule {
	id: string;
	name: string;
	expression: string;
	description: string;
	enabled: boolean;
}

export interface QueueState {
	jobs: QueuedJob[];
	isRunning: boolean;
	currentJobIndex: number;
	startedAt: string;
	completedAt?: string;
}

export interface QueuedJob {
	jobId: string;
	jobName: string;
	status: 'pending' | 'running' | 'completed' | 'error';
	startedAt?: string;
	completedAt?: string;
	error?: string;
}

export const DEFAULT_CRON_SCHEDULES: CronSchedule[] = [
	{
		id: 'default',
		name: 'Default (Every 6 hours)',
		expression: '0 */6 * * *',
		description: 'Runs every 6 hours at minute 0',
		enabled: true
	},
	{
		id: 'hourly',
		name: 'Hourly',
		expression: '0 * * * *',
		description: 'Runs every hour at minute 0',
		enabled: false
	},
	{
		id: 'daily',
		name: 'Daily (Midnight)',
		expression: '0 0 * * *',
		description: 'Runs daily at midnight UTC',
		enabled: false
	},
	{
		id: 'daily-morning',
		name: 'Daily (8 AM UTC)',
		expression: '0 8 * * *',
		description: 'Runs daily at 8:00 AM UTC',
		enabled: false
	},
	{
		id: 'twice-daily',
		name: 'Twice Daily',
		expression: '0 0,12 * * *',
		description: 'Runs at midnight and noon UTC',
		enabled: false
	},
	{
		id: 'weekly',
		name: 'Weekly (Monday Midnight)',
		expression: '0 0 * * 1',
		description: 'Runs every Monday at midnight UTC',
		enabled: false
	}
];

export const DEFAULT_JOB_DELAY_MS = 5000;
export const MAX_CONCURRENT_JOBS = 1;
export const QUEUE_KEY_PREFIX = 'queue:';
export const CRON_CONFIG_KEY = 'system:cron_schedules';

export function validateCronExpression(expression: string): boolean {
	if (!expression || typeof expression !== 'string') return false;
	
	const parts = expression.trim().split(/\s+/);
	if (parts.length !== 5) return false;
	
	const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
	
	const isValidField = (field: string, min: number, max: number): boolean => {
		if (field === '*') return true;
		if (field === '?') return true;
		
		if (field.includes('/')) {
			const [base, step] = field.split('/');
			if (base !== '*' && !isValidRange(base, min, max)) return false;
			if (!/^\d+$/.test(step)) return false;
			return true;
		}
		
		if (field.includes(',')) {
			return field.split(',').every(f => isValidField(f.trim(), min, max));
		}
		
		return isValidRange(field, min, max);
	};
	
	const isValidRange = (field: string, min: number, max: number): boolean => {
		if (field.includes('-')) {
			const [start, end] = field.split('-');
			if (!/^\d+$/.test(start) || !/^\d+$/.test(end)) return false;
			const s = parseInt(start, 10);
			const e = parseInt(end, 10);
			return s >= min && s <= max && e >= min && e <= max && s <= e;
		}
		if (!/^\d+$/.test(field)) return false;
		const val = parseInt(field, 10);
		return val >= min && val <= max;
	};
	
	return (
		isValidField(minute, 0, 59) &&
		isValidField(hour, 0, 23) &&
		isValidField(dayOfMonth, 1, 31) &&
		isValidField(month, 1, 12) &&
		isValidField(dayOfWeek, 0, 7)
	);
}

export function getCronDescription(expression: string): string {
	if (!validateCronExpression(expression)) return 'Invalid cron expression';
	
	const parts = expression.trim().split(/\s+/);
	const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
	
	if (expression === '0 */6 * * *') return 'Every 6 hours';
	if (expression === '0 * * * *') return 'Every hour';
	if (expression === '0 0 * * *') return 'Daily at midnight UTC';
	if (expression === '0 8 * * *') return 'Daily at 8:00 AM UTC';
	if (expression === '0 0,12 * * *') return 'Twice daily (midnight & noon UTC)';
	if (expression === '0 0 * * 1') return 'Weekly on Monday at midnight UTC';
	if (expression === '*/30 * * * *') return 'Every 30 minutes';
	if (expression === '*/15 * * * *') return 'Every 15 minutes';
	if (expression === '*/5 * * * *') return 'Every 5 minutes';
	
	if (minute === '0' && hour !== '*') {
		const hourDesc = hour.includes(',') 
			? hour.split(',').map(h => `${h}:00`).join(' and ')
			: `${hour}:00`;
		return `Daily at ${hourDesc} UTC`;
	}
	
	return `Custom schedule: ${expression}`;
}

/**
 * Evaluates whether a 5-field cron expression matches a given point in time (UTC).
 *
 * Supported syntax per field:
 *   - `*`          wildcard (always matches)
 *   - `N`          specific numeric value
 *   - `N-M`        inclusive range
 *   - `* /step`    step over full range  (spaces intentional — remove in code)
 *   - `base/step`  step over a range
 *   - `a,b,c`      comma-separated list of any of the above
 *
 * Field order: minute(0-59) hour(0-23) day-of-month(1-31) month(1-12) day-of-week(0-7)
 * Day-of-week: 0 and 7 both represent Sunday.
 *
 * Returns false for any invalid or unparseable expression — never throws.
 */
export function cronMatchesTime(expression: string, date: Date): boolean {
	if (!expression || typeof expression !== 'string') return false;

	try {
		const parts = expression.trim().split(/\s+/);
		if (parts.length !== 5) return false;

		const [minuteField, hourField, domField, monthField, dowField] = parts;

		const minute = date.getUTCMinutes();
		const hour = date.getUTCHours();
		const dom = date.getUTCDate();
		const month = date.getUTCMonth() + 1; // getUTCMonth is 0-indexed
		const dow = date.getUTCDay(); // 0 = Sunday

		const matchesField = (field: string, value: number, min: number, max: number): boolean => {
			if (field === '*' || field === '?') return true;

			// comma-separated list — recurse on each element
			if (field.includes(',')) {
				return field.split(',').some(part => matchesField(part.trim(), value, min, max));
			}

			// step expression: base/step or */step
			if (field.includes('/')) {
				const [base, stepStr] = field.split('/');
				const step = parseInt(stepStr, 10);
				if (!Number.isFinite(step) || step <= 0) return false;

				if (base === '*') {
					return value % step === 0;
				}

				// range/step  e.g. 1-23/2
				if (base.includes('-')) {
					const [startStr, endStr] = base.split('-');
					const start = parseInt(startStr, 10);
					const end = parseInt(endStr, 10);
					if (!Number.isFinite(start) || !Number.isFinite(end)) return false;
					// bounds check: both range endpoints must be within field bounds
					if (start < min || start > max || end < min || end > max) return false;
					if (value < start || value > end) return false;
					return (value - start) % step === 0;
				}

				// single-value base/step (unusual but valid)
				const start = parseInt(base, 10);
				if (!Number.isFinite(start)) return false;
				// bounds check: base must be within field bounds
				if (start < min || start > max) return false;
				if (value < start || value > max) return false;
				return (value - start) % step === 0;
			}

			// range: N-M
			if (field.includes('-')) {
				const [startStr, endStr] = field.split('-');
				const start = parseInt(startStr, 10);
				const end = parseInt(endStr, 10);
				if (!Number.isFinite(start) || !Number.isFinite(end)) return false;
				// bounds check: both endpoints must be within field bounds
				if (start < min || start > max || end < min || end > max) return false;
				return value >= start && value <= end;
			}

			// exact numeric value
			const num = parseInt(field, 10);
			if (!Number.isFinite(num)) return false;
			// bounds check: single value must be within field bounds
			if (num < min || num > max) return false;
			return value === num;
		};

		// Day-of-week: treat 7 as Sunday (same as 0).
		// We expand the dow field into a set of matching values, normalizing 7 → 0
		// so that ranges like 5-7 (Fri-Sun) correctly produce [5, 6, 0].
		const dowMatches = (() => {
			if (dowField === '*' || dowField === '?') return true;

			// Build the set of matching dow values (0-6) from the field expression.
			const matchingDows = new Set<number>();

			const addDowToken = (token: string): void => {
				const t = token.trim();

				if (t.includes('/')) {
					const [base, stepStr] = t.split('/');
					const step = parseInt(stepStr, 10);
					if (!Number.isFinite(step) || step <= 0) return;

					let rangeStart: number;
					let rangeEnd: number;

					if (base === '*') {
						rangeStart = 0;
						rangeEnd = 6;
					} else if (base.includes('-')) {
						const [s, e] = base.split('-').map(v => parseInt(v, 10));
						if (!Number.isFinite(s) || !Number.isFinite(e)) return;
						// normalize 7 → 6 as upper bound (but produce 0 for value 7 below)
						rangeStart = s === 7 ? 0 : s;
						rangeEnd = e === 7 ? 6 : e;
					} else {
						rangeStart = parseInt(base, 10);
						if (!Number.isFinite(rangeStart)) return;
						if (rangeStart === 7) rangeStart = 0;
						rangeEnd = 6;
					}

					if (rangeStart < 0 || rangeStart > 6 || rangeEnd < 0 || rangeEnd > 6) return;
					for (let v = rangeStart; v <= rangeEnd; v += step) {
						matchingDows.add(v === 7 ? 0 : v);
					}
					return;
				}

				if (t.includes('-')) {
					const [startStr, endStr] = t.split('-');
					const start = parseInt(startStr, 10);
					const end = parseInt(endStr, 10);
					if (!Number.isFinite(start) || !Number.isFinite(end)) return;
					// Bounds check: allow 0-7 input range (7 = Sunday alias), normalise to 0-6
					if (start < 0 || start > 7 || end < 0 || end > 7) return;
					// Normalize endpoints: 7 → 0
					const normStart = start === 7 ? 0 : start;
					const normEnd = end === 7 ? 6 : end; // 5-7 → walk 5,6 then add Sunday(0)
					if (normStart <= normEnd) {
						for (let v = normStart; v <= normEnd; v++) {
							matchingDows.add(v);
						}
					} else {
						// wrap-around (e.g. 6-1 doesn't really appear but handle it)
						for (let v = normStart; v <= 6; v++) matchingDows.add(v);
						for (let v = 0; v <= normEnd; v++) matchingDows.add(v);
					}
					// If the original end was 7, also add Sunday (0) explicitly
					if (end === 7) matchingDows.add(0);
					return;
				}

				// single value
				const num = parseInt(t, 10);
				if (!Number.isFinite(num)) return;
				if (num < 0 || num > 7) return; // 8+ is invalid
				matchingDows.add(num === 7 ? 0 : num);
			};

			dowField.split(',').forEach(part => addDowToken(part));

			return matchingDows.has(dow);
		})();

		return (
			matchesField(minuteField, minute, 0, 59) &&
			matchesField(hourField, hour, 0, 23) &&
			matchesField(domField, dom, 1, 31) &&
			matchesField(monthField, month, 1, 12) &&
			dowMatches
		);
	} catch {
		return false;
	}
}

/** @deprecated Use cronMatchesTime() for time-based evaluation instead of string comparison */
export function matchesCron(cronExpression: string, eventCron: string): boolean {
	if (!cronExpression || !eventCron) return false;
	return cronExpression.trim() === eventCron.trim();
}

export function filterJobsByCron(
	jobs: SyncJobConfig[],
	scheduledTime: Date
): SyncJobConfig[] {
	return jobs.filter(job => {
		if (!job.enabled) return false;

		const jobCron = job.cronSchedule || '0 */6 * * *';
		return cronMatchesTime(jobCron, scheduledTime);
	});
}

export async function createQueueState(
	jobIds: string[],
	jobs: SyncJobConfig[]
): Promise<QueueState> {
	const jobMap = new Map(jobs.map(j => [j.id, j]));
	
	return {
		jobs: jobIds.map(id => ({
			jobId: id,
			jobName: jobMap.get(id)?.name || id,
			status: 'pending'
		})),
		isRunning: true,
		currentJobIndex: 0,
		startedAt: new Date().toISOString()
	};
}

export async function saveQueueState(
	kv: KVNamespace,
	queueId: string,
	state: QueueState
): Promise<void> {
	await kv.put(`${QUEUE_KEY_PREFIX}${queueId}`, JSON.stringify(state), {
		expirationTtl: 86400
	});
}

export async function getQueueState(
	kv: KVNamespace,
	queueId: string
): Promise<QueueState | null> {
	return await kv.get<QueueState>(`${QUEUE_KEY_PREFIX}${queueId}`, 'json');
}

export async function updateJobInQueue(
	kv: KVNamespace,
	queueId: string,
	jobId: string,
	updates: Partial<QueuedJob>
): Promise<void> {
	const state = await getQueueState(kv, queueId);
	if (!state) return;
	
	const jobIndex = state.jobs.findIndex(j => j.jobId === jobId);
	if (jobIndex === -1) return;
	
	state.jobs[jobIndex] = { ...state.jobs[jobIndex], ...updates };
	await saveQueueState(kv, queueId, state);
}

export async function completeQueue(
	kv: KVNamespace,
	queueId: string
): Promise<void> {
	const state = await getQueueState(kv, queueId);
	if (!state) return;
	
	state.isRunning = false;
	state.completedAt = new Date().toISOString();
	await saveQueueState(kv, queueId, state);
}

export async function getCronSchedules(
	kv: KVNamespace
): Promise<CronSchedule[]> {
	const schedules = await kv.get<CronSchedule[]>(CRON_CONFIG_KEY, 'json');
	return schedules || DEFAULT_CRON_SCHEDULES;
}

export async function saveCronSchedules(
	kv: KVNamespace,
	schedules: CronSchedule[]
): Promise<void> {
	await kv.put(CRON_CONFIG_KEY, JSON.stringify(schedules));
}

export function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

export function estimateJobDuration(job: SyncJobConfig): number {
	const baseTime = 5000;
	const perRowTime = 0.5;
	const estimatedRows = 10000;
	return baseTime + (estimatedRows * perRowTime);
}

export function calculateDelayBetweenJobs(
	completedJob: SyncJobConfig,
	nextJob: SyncJobConfig
): number {
	const estimatedDuration = estimateJobDuration(completedJob);
	const bufferTime = 2000;
	return Math.max(DEFAULT_JOB_DELAY_MS, estimatedDuration + bufferTime);
}
