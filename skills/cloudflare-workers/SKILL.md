---
name: cloudflare-workers
description: Cloudflare Workers patterns for adcreativeflow-db-sync
license: MIT
metadata:
  scope:
    - src
  auto_invoke:
    - "Creating Worker handlers"
    - "Using KV namespace"
    - "Implementing scheduled triggers"
    - "Working with Hono in Workers"
---

# Cloudflare Workers Patterns

## Critical Rules

### ALWAYS
- Use typed Hono: `new Hono<{ Bindings: Env }>()`
- Define all bindings in `Env` interface
- Use `ctx.waitUntil()` for background work in scheduled handlers
- Use KV `.get(key, 'json')` for typed retrieval
- Prefix KV keys: `job:${id}`

### NEVER
- Use `process.env` (use `c.env` or `env` parameter)
- Block main thread in scheduled handlers
- Store secrets in code

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
const job = await env.SYNC_CONFIGS.get<Config>('job:123', 'json');
await env.SYNC_CONFIGS.put('job:123', JSON.stringify(job));
await env.SYNC_CONFIGS.delete('job:123');
```

### Scheduled Handler
```typescript
export default {
  fetch: app.fetch,
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(runJobs(env));
  }
};
```

### Assets Binding
```typescript
app.get('*', async (c) => {
  return await c.env.ASSETS.fetch(c.req.raw);
});
```

## References

- [Cloudflare Workers Docs](https://developers.cloudflare.com/workers/)
- [Hono Cloudflare Workers](https://hono.dev/docs/getting-started/cloudflare-workers)
