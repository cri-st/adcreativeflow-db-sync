# Repository Guidelines

## Project Overview

AdCreativeFlow DB Sync is a Cloudflare Worker that synchronizes data from Google BigQuery to Supabase on a scheduled 6-hour cron cycle. The worker includes a configuration dashboard for managing sync settings.

**Key Features:**
- Scheduled sync every 6 hours (`0 */6 * * *`)
- KV-based configuration storage (SYNC_CONFIGS)
- Static bearer token authentication (SYNC_API_KEY)
- Vanilla HTML/CSS/JS dashboard for config management

---

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `src/` | Cloudflare Worker source (Hono API, sync logic, BigQuery/Supabase clients) |
| `ui/` | Dashboard UI (vanilla HTML/CSS/JS, served as static assets) |
| `skills/` | AI Agent skills for development assistance |

---

## Skills Reference

<!-- SKILLS_TABLE_START -->
| Skill | Description | Link |
|-------|-------------|------|
<!-- SKILLS_TABLE_END -->

### Auto-invoke Skills

When performing these actions, invoke the corresponding skill FIRST:

<!-- AUTO_INVOKE_START -->
| Action | Skill |
|--------|-------|
<!-- AUTO_INVOKE_END -->

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Cloudflare Workers | Serverless runtime with cron triggers |
| Hono | Lightweight web framework for API routes |
| TypeScript | Type-safe JavaScript |
| Google BigQuery | Source database (via `jose` for JWT auth) |
| Supabase | Target database (via `@supabase/supabase-js`) |
| Vitest | Unit testing framework |
| Wrangler | Cloudflare CLI for dev/deploy |

---

## Commands

```bash
npm run dev       # Development (with scheduled trigger testing)
npm run deploy    # Deploy to Cloudflare
npm test          # Run tests
npm run cf-typegen # Generate Cloudflare types
```

---

## Commit Guidelines

Follow conventional commit format: `<type>[scope]: <description>`

**Types:**
- `feat` - New feature
- `fix` - Bug fix
- `chore` - Maintenance tasks
- `docs` - Documentation changes
- `refactor` - Code restructuring
- `test` - Adding or updating tests
