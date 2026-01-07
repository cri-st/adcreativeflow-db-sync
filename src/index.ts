/**
 * Cloudflare Worker - BigQuery to Supabase Sync
 */

import { handleSync } from './sync/handler';

export interface Env {
	// Secrets
	GOOGLE_SERVICE_ACCOUNT_JSON: string;
	GOOGLE_PROJECT_ID: string;
	SUPABASE_URL: string;
	SUPABASE_SERVICE_KEY: string;
	SYNC_API_KEY: string;
}

export default {
	// This will run on the configured schedule
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
		ctx.waitUntil(
			handleSync({
				googleServiceAccount: env.GOOGLE_SERVICE_ACCOUNT_JSON,
				googleProjectId: env.GOOGLE_PROJECT_ID,
				supabaseUrl: env.SUPABASE_URL,
				supabaseKey: env.SUPABASE_SERVICE_KEY,
			})
		);
	},

	// Security: Require POST and valid SYNC_API_KEY in Authorization header
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		if (request.method !== 'POST') {
			return new Response('Method Not Allowed. Use POST.', { status: 405 });
		}

		const authHeader = request.headers.get('Authorization');
		const expectedHeader = `Bearer ${env.SYNC_API_KEY}`;

		if (!authHeader || authHeader !== expectedHeader) {
			console.error('Unauthorized sync attempt');
			return new Response('Unauthorized', { status: 401 });
		}

		try {
			await handleSync({
				googleServiceAccount: env.GOOGLE_SERVICE_ACCOUNT_JSON,
				googleProjectId: env.GOOGLE_PROJECT_ID,
				supabaseUrl: env.SUPABASE_URL,
				supabaseKey: env.SUPABASE_SERVICE_KEY,
			});
			return new Response('Sync triggered successfully', { status: 200 });
		} catch (err: any) {
			console.error(`Sync failed: ${err.message}`);
			return new Response(`Sync failed: ${err.message}`, { status: 500 });
		}
	},
};
