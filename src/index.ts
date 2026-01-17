import { Hono } from 'hono';
import { handleSync, SyncJobConfig, SyncResult } from './sync/handler';
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
	if (!job.id) job.id = crypto.randomUUID();
	await c.env.SYNC_CONFIGS.put(`job:${job.id}`, JSON.stringify(job));
	return c.json({ success: true, job });
});

app.put('/api/configs/:id', async (c) => {
	const id = c.req.param('id');
	const job: SyncJobConfig = await c.req.json();
	await c.env.SYNC_CONFIGS.put(`job:${id}`, JSON.stringify(job));
	return c.json({ success: true });
});

app.delete('/api/configs/:id', async (c) => {
	const id = c.req.param('id');
	await c.env.SYNC_CONFIGS.delete(`job:${id}`);
	return c.json({ success: true });
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
		return c.json({ exists: true, logs: [], runs });
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

	// DIAGNOSTIC: Log inbound request
	console.log(`[API_INBOUND] Received sync request:`, {
		jobId: id,
		runId,
		batchNumber,
		isInternalCall: c.req.header('X-Internal-Call') === 'true',
		hasAuth: !!c.req.header('Authorization')
	});

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

		const result: SyncResult = await handleSync({
			googleServiceAccount: env.GOOGLE_SERVICE_ACCOUNT_JSON,
			supabaseUrl: env.SUPABASE_URL,
			supabaseKey: env.SUPABASE_SERVICE_KEY
		}, job, currentRunId, env.SYNC_LOGS, batchNumber);

		// DIAGNOSTIC: Log what handler returned
		console.log(`[HANDLER_RESULT] Batch ${batchNumber} handler returned:`, {
			hasMore: result.hasMore,
			nextBatch: result.nextBatch,
			rowsProcessed: result.rowsProcessed,
			stats: result.stats
		});

		if (!result.hasMore) {
			job.lastRun = new Date().toISOString();
			job.lastStatus = 'success';
			delete job.lastError;
            
            if (result.stats) {
                const { totalRows, totalBatches, durationMs } = result.stats;
                const minutes = Math.floor(durationMs / 60000);
                const seconds = Math.floor((durationMs % 60000) / 1000);
                job.lastSummary = `${totalRows.toLocaleString()} rows in ${totalBatches} batches (${minutes}m ${seconds}s)`;
            }
            
			await env.SYNC_CONFIGS.put(`job:${job.id}`, JSON.stringify(job));
		} else {
			// DIAGNOSTIC: Entering auto-continuation branch
			console.log(`[AUTO_CONTINUATION_START] Spawning next batch:`, {
				currentBatch: batchNumber,
				nextBatch: result.nextBatch,
				originUrl,
				envWorkerUrl: env.WORKER_URL,
				runId: currentRunId
			});
			
			const workerUrl = originUrl || env.WORKER_URL;
			
			// DIAGNOSTIC: Log URL decision
			console.log(`[AUTO_CONTINUATION_URL] Worker URL selected:`, {
				workerUrl,
				usingOrigin: !!originUrl,
				usingEnvVar: !originUrl && !!env.WORKER_URL
			});
			
			if (workerUrl) {
				const nextUrl = `${workerUrl}/api/sync/${job.id}`;
				console.log(`[Auto-Continuation] Spawning Batch ${result.nextBatch} via ${nextUrl}`);
				
				const nextBatchPromise = (async () => {
					try {
						console.log(`[AUTO_CONTINUATION_FETCH] Starting fetch to:`, {
							url: nextUrl,
							batchNumber: result.nextBatch,
							runId: currentRunId
						});
						
						const response = await fetch(nextUrl, {
							method: 'POST',
							headers: {
								'Content-Type': 'application/json',
								'Authorization': `Bearer ${env.SYNC_API_KEY}`,
								'X-Internal-Call': 'true'
							},
							body: JSON.stringify({
								runId: currentRunId,
								batchNumber: result.nextBatch
							})
						});
						
						console.log(`[AUTO_CONTINUATION_RESPONSE] Batch ${result.nextBatch} fetch completed:`, {
							status: response.status,
							statusText: response.statusText,
							ok: response.ok
						});
						
						if (!response.ok) {
							const body = await response.text().catch(() => 'Unable to read body');
							console.error(`[AUTO_CONTINUATION_ERROR] Batch ${result.nextBatch} fetch failed:`, {
								status: response.status,
								body: body.substring(0, 200)
							});
						}
						
						return response;
					} catch (error: any) {
						console.error(`[AUTO_CONTINUATION_EXCEPTION] Batch ${result.nextBatch} fetch threw:`, {
							error: error.message,
							stack: error.stack?.substring(0, 300)
						});
						throw error;
					}
				})();
				
				ctx.waitUntil(nextBatchPromise);
			} else {
				console.warn('[Auto-Continuation] Cannot spawn next batch: WORKER_URL not set and no origin URL available.');
			}
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
		for (const k of list.keys) {
			const job = await env.SYNC_CONFIGS.get<SyncJobConfig>(k.name, 'json');
			if (job && job.enabled) {
				ctx.waitUntil(runJobWithAutoContinuation(env, job, ctx));
			}
		}
	}
};
