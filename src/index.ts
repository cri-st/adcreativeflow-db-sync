import { Hono } from 'hono';
import { handleSync, SyncJobConfig } from './sync/handler';

export interface Env {
	GOOGLE_SERVICE_ACCOUNT_JSON: string;
	GOOGLE_PROJECT_ID: string;
	SUPABASE_URL: string;
	SUPABASE_SERVICE_KEY: string;
	SUPABASE_POSTGRES_URL: string;
	SYNC_API_KEY: string;
	SYNC_CONFIGS: KVNamespace;
	ASSETS: Fetcher;
}

const app = new Hono<{ Bindings: Env }>();

// --- MIDDLEWARE: Auth ---
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

// --- API: Auth ---
app.post('/api/auth', async (c) => {
	const { key } = await c.req.json();
	if (key === c.env.SYNC_API_KEY) {
		return c.json({ success: true, token: c.env.SYNC_API_KEY });
	}
	return c.json({ success: false }, 401);
});

// --- API: Configs ---
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

// --- API: Sync Control ---
app.post('/api/sync/:id', async (c) => {
	const id = c.req.param('id');
	const job = await c.env.SYNC_CONFIGS.get<SyncJobConfig>(`job:${id}`, 'json');
	if (!job) return c.json({ error: 'Job not found' }, 404);

	try {
		await executeJob(c.env, job);
		return c.json({ success: true });
	} catch (err: any) {
		return c.json({ error: err.message }, 500);
	}
});

app.post('/api/sync/all', async (c) => {
	const list = await c.env.SYNC_CONFIGS.list({ prefix: 'job:' });
	const results = [];
	for (const k of list.keys) {
		const job = await c.env.SYNC_CONFIGS.get<SyncJobConfig>(k.name, 'json');
		if (job && job.enabled) {
			try {
				await executeJob(c.env, job);
				results.push({ id: job.id, status: 'success' });
			} catch (err: any) {
				results.push({ id: job.id, status: 'error', message: err.message });
			}
		}
	}
	return c.json(results);
});

// --- STATIC ASSETS ---
app.get('*', async (c) => {
	// Try to fetch from assets binding
	return await c.env.ASSETS.fetch(c.req.raw);
});

// Helper to execute job and update KV with status
async function executeJob(env: Env, job: SyncJobConfig) {
	try {
		if (!env.SUPABASE_URL) throw new Error('SUPABASE_URL is missing. Check your .dev.vars (local) or secrets (prod).');
		if (!env.SUPABASE_SERVICE_KEY) throw new Error('SUPABASE_SERVICE_KEY is missing.');
		if (!env.GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON is missing.');

		await handleSync({
			googleServiceAccount: env.GOOGLE_SERVICE_ACCOUNT_JSON,
			supabaseUrl: env.SUPABASE_URL,
			supabaseKey: env.SUPABASE_SERVICE_KEY,
			supabasePostgresUrl: env.SUPABASE_POSTGRES_URL
		}, job);

		job.lastRun = new Date().toISOString();
		job.lastStatus = 'success';
		delete job.lastError;
		await env.SYNC_CONFIGS.put(`job:${job.id}`, JSON.stringify(job));
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
				ctx.waitUntil(executeJob(env, job));
			}
		}
	}
};
