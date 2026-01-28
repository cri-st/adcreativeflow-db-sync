import { Hono } from 'hono';
import { BigQueryClient } from './bigquery/client';
import { handleSync, SyncJobConfig, SyncResult } from './sync/handler';
import { handleSheetsToBigQuerySync } from './sync/sheets-to-bq-handler';
import { SheetsClient } from './sheets/client';
import { SHEETS_WHITELIST, SheetsSyncConfig } from './types/funnel';
import { Logger } from './logger';

export interface Env {
	GOOGLE_SERVICE_ACCOUNT_JSON: string;
	GOOGLE_PROJECT_ID: string;
	SUPABASE_URL: string;
	SUPABASE_SERVICE_KEY: string;
	SYNC_API_KEY: string;
	SYNC_CONFIGS: KVNamespace;
	SYNC_LOGS: KVNamespace;
	ASSETS: Fetcher;
	WORKER_URL?: string;
}

const app = new Hono<{ Bindings: Env }>();

app.use('/api/*', async (c, next) => {
	if (c.req.path === '/api/auth' && c.req.method === 'POST') {
		return next();
	}

	const authHeader = c.req.header('Authorization');
	if (!authHeader || authHeader !== `Bearer ${c.env.SYNC_API_KEY}`) {
		return c.json({ error: 'Unauthorized' }, 401);
	}
	await next();
});

app.post('/api/auth', async (c) => {
	const { key } = await c.req.json();
	if (key === c.env.SYNC_API_KEY) {
		return c.json({ success: true, token: c.env.SYNC_API_KEY });
	}
	return c.json({ success: false }, 401);
});

app.get('/api/configs', async (c) => {
	const list = await c.env.SYNC_CONFIGS.list({ prefix: 'job:' });
	const configs = await Promise.all(
		list.keys.map(async (k) => {
			const data = await c.env.SYNC_CONFIGS.get(k.name, 'json');
			return data;
		})
	);
	return c.json(configs);
});

app.post('/api/configs', async (c) => {
	const job: SyncJobConfig = await c.req.json();
	
	if (job.type === 'sheets-to-bq') {
		const sheetsJob = job as any;
		if (sheetsJob.sheets?.spreadsheetUrl) {
			const match = sheetsJob.sheets.spreadsheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
			if (!match || !match[1]) {
				return c.json({ error: 'Invalid spreadsheet URL' }, 400);
			}
			if (!sheetsJob.sheets) sheetsJob.sheets = {};
			sheetsJob.sheets.spreadsheetId = match[1];
		}

		if (sheetsJob.sheets?.sheetName) {
			if (!sheetsJob.sheets.range) {
				sheetsJob.sheets.range = sheetsJob.sheets.sheetName;
			}
		}
	}

	if (!job.id) job.id = crypto.randomUUID();
	await c.env.SYNC_CONFIGS.put(`job:${job.id}`, JSON.stringify(job));
	return c.json({ success: true, job });
});

app.put('/api/configs/:id', async (c) => {
	const id = c.req.param('id');
	const job: SyncJobConfig = await c.req.json();

	if (job.type === 'sheets-to-bq') {
		const sheetsJob = job as any;
		if (sheetsJob.sheets?.spreadsheetUrl) {
			const match = sheetsJob.sheets.spreadsheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
			if (!match || !match[1]) {
				return c.json({ error: 'Invalid spreadsheet URL' }, 400);
			}
			if (!sheetsJob.sheets) sheetsJob.sheets = {};
			sheetsJob.sheets.spreadsheetId = match[1];
		}

		if (sheetsJob.sheets?.sheetName) {
			if (!sheetsJob.sheets.range) {
				sheetsJob.sheets.range = sheetsJob.sheets.sheetName;
			}
		}
	}

	await c.env.SYNC_CONFIGS.put(`job:${id}`, JSON.stringify(job));
	return c.json({ success: true });
});

app.delete('/api/configs/:id', async (c) => {
	const id = c.req.param('id');
	await c.env.SYNC_CONFIGS.delete(`job:${id}`);
	return c.json({ success: true });
});

app.post('/api/diagnostics/sheets', async (c) => {
	const body = await c.req.json().catch(() => ({}));
	const { spreadsheetUrl, sheetName } = body;
	
	if (!spreadsheetUrl) return c.json({ error: 'Missing spreadsheetUrl' }, 400);

	let spreadsheetId = spreadsheetUrl;
	const match = spreadsheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
	if (match && match[1]) {
		spreadsheetId = match[1];
	}

	try {
		if (!c.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
			console.error('Missing GOOGLE_SERVICE_ACCOUNT_JSON env var');
			return c.json({ error: 'Server configuration error: Missing Google Credentials' }, 500);
		}

		const client = new SheetsClient(c.env.GOOGLE_SERVICE_ACCOUNT_JSON);
		const range = sheetName ? `${sheetName}!1:1` : 'A1:Z1';
		const rows = await client.getSheetRange(spreadsheetId, range);
		
		return c.json({ 
			success: true, 
			message: 'Sheet accessible', 
			preview: rows ? rows[0] : [] 
		});
	} catch (err: any) {
		console.error('Diagnostic error:', err);
		return c.json({ error: err.message || 'Unknown error during connection test' }, 500);
	}
});

app.get('/api/logs/:jobId', async (c) => {
	const jobId = c.req.param('jobId');
	const runId = c.req.query('runId');
	const limit = parseInt(c.req.query('limit') || '500', 10);

	const job = await c.env.SYNC_CONFIGS.get(`job:${jobId}`);
	if (!job) {
		return c.json({ exists: false, logs: [], runs: [] });
	}

	if (!runId) {
		const runs = await Logger.getJobRuns(c.env.SYNC_LOGS, jobId);
		const logs = limit <= 5 ? await Logger.getRecentLogs(c.env.SYNC_LOGS, jobId, undefined, limit) : [];
		return c.json({ exists: true, logs, runs });
	}

	const logs = await Logger.getRecentLogs(c.env.SYNC_LOGS, jobId, runId, limit);
	return c.json({ exists: true, logs, runs: [] });
});

app.delete('/api/logs/:jobId', async (c) => {
	const jobId = c.req.param('jobId');
	const runId = c.req.query('runId');

	const deleted = await Logger.clearLogs(c.env.SYNC_LOGS, jobId, runId);
	return c.json({ success: true, deleted });
});

app.post('/api/sync/:id', async (c) => {
	const id = c.req.param('id');
	const body = await c.req.json().catch(() => ({}));
	const runId = body.runId;
	const batchNumber = body.batchNumber || 1;

	const job = await c.env.SYNC_CONFIGS.get<SyncJobConfig>(`job:${id}`, 'json');
	if (!job) return c.json({ error: 'Job not found' }, 404);

	try {
		const origin = new URL(c.req.url).origin;
		const result = await runJobWithAutoContinuation(c.env, job, c.executionCtx, runId, batchNumber, origin);
		return c.json({ success: true, ...result });
	} catch (err: any) {
		return c.json({ error: err.message }, 500);
	}
});

	app.post('/api/sync/all', async (c) => {
	const list = await c.env.SYNC_CONFIGS.list({ prefix: 'job:' });
	const results = [];
	const origin = new URL(c.req.url).origin;

	for (const k of list.keys) {
		const job = await c.env.SYNC_CONFIGS.get<SyncJobConfig>(k.name, 'json');
		if (job && job.enabled) {
			try {
				await runJobWithAutoContinuation(c.env, job, c.executionCtx, undefined, 1, origin);
				results.push({ id: job.id, status: 'success' });
			} catch (err: any) {
				results.push({ id: job.id, status: 'error', message: err.message });
			}
		}
	}
	return c.json(results);
});

app.get('/api/scheduled/sheets', async (c) => {
	const list = await c.env.SYNC_CONFIGS.list({ prefix: 'job:' });
	const results = [];
	const origin = new URL(c.req.url).origin;

	for (const k of list.keys) {
		const job = await c.env.SYNC_CONFIGS.get<SyncJobConfig>(k.name, 'json');
		if (job && job.enabled && job.type === 'sheets-to-bq') {
			try {
				c.executionCtx.waitUntil(runJobWithAutoContinuation(c.env, job, c.executionCtx, undefined, 1, origin));
				results.push({ id: job.id, status: 'triggered' });
			} catch (err: any) {
				results.push({ id: job.id, status: 'error', message: err.message });
			}
		}
	}
	return c.json({ success: true, results });
});




app.get('*', async (c) => {
	return await c.env.ASSETS.fetch(c.req.raw);
});

async function runJobWithAutoContinuation(
	env: Env, 
	job: SyncJobConfig, 
	ctx: ExecutionContext, 
	runId?: string, 
	batchNumber: number = 1,
	originUrl?: string
) {
	const currentRunId = runId || crypto.randomUUID();

	try {
		if (!env.SUPABASE_URL) throw new Error('SUPABASE_URL is missing.');
		if (!env.SUPABASE_SERVICE_KEY) throw new Error('SUPABASE_SERVICE_KEY is missing.');
		if (!env.GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON is missing.');

		const auth = {
			googleServiceAccount: env.GOOGLE_SERVICE_ACCOUNT_JSON,
			supabaseUrl: env.SUPABASE_URL,
			supabaseKey: env.SUPABASE_SERVICE_KEY
		};

		let result: SyncResult;

		if (job.type === 'sheets-to-bq') {
			const sheetJob = job as any;
			const rawSheetName = sheetJob.sheets.sheetName || sheetJob.sheets.range || '';
			const actualSheetName = rawSheetName.includes('!') ? rawSheetName.split('!')[0] : rawSheetName;

			const handlerJob = {
				id: sheetJob.id,
				name: sheetJob.name,
				enabled: sheetJob.enabled,
				type: 'sheets-to-bq',
				sheets: {
					spreadsheetId: sheetJob.sheets.spreadsheetId,
					range: actualSheetName,
					append: sheetJob.sheets.append
				},
				bigquery: {
					projectId: sheetJob.bigquery.projectId,
					datasetId: sheetJob.bigquery.datasetId,
					tableId: sheetJob.bigquery.tableId
				}
			};

			result = await handleSheetsToBigQuerySync(
				auth,
				handlerJob as any,
				currentRunId,
				env.SYNC_LOGS,
				batchNumber
			);
		} else {
			result = await handleSync(
				auth,
				job,
				currentRunId,
				env.SYNC_LOGS,
				batchNumber
			);
		}

		if (result.hasMore && originUrl) {
			const nextBatch = batchNumber + 1;
			const nextUrl = `${originUrl}/api/sync/${job.id}`;
			
			ctx.waitUntil(
				fetch(nextUrl, {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json',
						'Authorization': `Bearer ${env.SYNC_API_KEY}`
					},
					body: JSON.stringify({ runId: currentRunId, batchNumber: nextBatch })
				}).catch(err => console.error(`Failed to trigger next batch ${nextBatch}:`, err))
			);
		} else if (!result.hasMore) {
			job.lastRun = new Date().toISOString();
			job.lastStatus = 'success';
			delete job.lastError;
            
			if (result.stats) {
				const { totalRows, totalBatches, durationMs } = result.stats;
				const minutes = Math.floor(durationMs / 60000);
				const seconds = Math.floor((durationMs % 60000) / 1000);
				const deletePart = result.rowsDeleted > 0 ? `, ${result.rowsDeleted.toLocaleString()} deleted` : '';
				job.lastSummary = `${totalRows.toLocaleString()} rows synced${deletePart} in ${minutes}m ${seconds}s`;
			}
            
			await env.SYNC_CONFIGS.put(`job:${job.id}`, JSON.stringify(job));
		}

		return { ...result, runId: currentRunId };

	} catch (err: any) {
		job.lastRun = new Date().toISOString();
		job.lastStatus = 'error';
		job.lastError = err.message;
		await env.SYNC_CONFIGS.put(`job:${job.id}`, JSON.stringify(job));
		throw err;
	}
}

export default {
	fetch: app.fetch,
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const list = await env.SYNC_CONFIGS.list({ prefix: 'job:' });
		const cron = event.cron;

		for (const k of list.keys) {
			const job = await env.SYNC_CONFIGS.get<SyncJobConfig>(k.name, 'json');
			if (job && job.enabled) {
				const isSheetsJob = job.type === 'sheets-to-bq';
				
				if (cron === "30 */6 * * *" && isSheetsJob) {
					ctx.waitUntil(runJobWithAutoContinuation(env, job, ctx));
				} else if (cron === "0 */6 * * *" && !isSheetsJob) {
					ctx.waitUntil(runJobWithAutoContinuation(env, job, ctx));
				} else if (!cron) {
					ctx.waitUntil(runJobWithAutoContinuation(env, job, ctx));
				}
			}
		}
	}
};
