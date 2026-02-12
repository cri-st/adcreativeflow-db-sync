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

export function matchesCron(cronExpression: string, eventCron: string): boolean {
	if (!cronExpression || !eventCron) return false;
	return cronExpression.trim() === eventCron.trim();
}

export function filterJobsByCron(
	jobs: SyncJobConfig[],
	eventCron: string
): SyncJobConfig[] {
	return jobs.filter(job => {
		if (!job.enabled) return false;
		
		const jobCron = job.cronSchedule || '0 */6 * * *';
		return matchesCron(jobCron, eventCron);
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
