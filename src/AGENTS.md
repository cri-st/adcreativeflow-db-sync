# Cloudflare Worker - AI Agent Ruleset

## Skills Reference

<!-- SKILLS_TABLE_START -->
| Skill | Description | Link |
|-------|-------------|------|
<!-- SKILLS_TABLE_END -->

### Auto-invoke Skills

<!-- AUTO_INVOKE_START -->
| Action | Skill |
|--------|-------|
| Creating Worker handlers | cloudflare-workers |
| Using KV namespace | cloudflare-workers |
| Implementing scheduled triggers | cloudflare-workers |
| Working with Hono in Workers | cloudflare-workers |
<!-- AUTO_INVOKE_END -->

---

## Critical Rules

### ALWAYS
- Use typed Hono app: `new Hono<{ Bindings: Env }>()`
- Define bindings in `Env` interface for KV, secrets, and assets
- Use `ctx.waitUntil()` for background tasks in scheduled handlers
- Use KV `.get(key, 'json')` for typed JSON retrieval

### NEVER
- Access `process.env` (use `c.env` or `env` parameter)
- Use Node.js-specific APIs without `nodejs_compat` flag
- Store secrets in code (use Wrangler secrets or .dev.vars)

---

## Patterns

### Env Interface
```typescript
export interface Env {
  SYNC_API_KEY: string;
  SYNC_CONFIGS: KVNamespace;
  ASSETS: Fetcher;
}
```

### Hono App
```typescript
const app = new Hono<{ Bindings: Env }>();
```

### KV Operations
```typescript
const list = await env.SYNC_CONFIGS.list({ prefix: 'job:' });
const job = await env.SYNC_CONFIGS.get<SyncJobConfig>('job:123', 'json');
await env.SYNC_CONFIGS.put('job:123', JSON.stringify(job));
await env.SYNC_CONFIGS.delete('job:123');
```

### Scheduled Handler
```typescript
export default {
  fetch: app.fetch,
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(doBackgroundWork(env));
  }
};
```

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Hono | Web framework with typed bindings |
| KVNamespace | Configuration storage |
| jose | JWT signing for BigQuery auth |
