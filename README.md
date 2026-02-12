# AdCreativeFlow DB Sync

Serverless synchronization service between Google BigQuery/Sheets and Supabase/PostgreSQL.

For detailed documentation in Spanish, see [DOCUMENTACION.md](./DOCUMENTACION.md).

## Setup & Verification

### 1. Prerequisites

- Node.js (v18+)
- Cloudflare Wrangler CLI (`npm install -g wrangler`)
- Google Cloud Service Account (JSON key)
- Supabase Project

### 2. Environment Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. Create a `.dev.vars` file in the root directory for local testing:

   ```bash
   GOOGLE_SERVICE_ACCOUNT_JSON="..."
   GOOGLE_PROJECT_ID="your-project-id"
   SUPABASE_URL="https://your-project.supabase.co"
   SUPABASE_SERVICE_KEY="your-service-role-key"
   SYNC_API_KEY="your-secret-api-key"
   ```

### 3. Verification

To verify that your Google Service Account has the necessary permissions (specifically for BigQuery and Google Sheets), follow these steps:

#### Method A: Run the Verification Script (Recommended)

We have provided a script `test-auth.js` that locally signs a JWT using your Service Account key and attempts to authenticate with Google APIs requesting the required scopes.

1. Ensure `.dev.vars` is populated (as above).
2. Run the script:
   ```bash
   node test-auth.js
   ```
3. If successful, you will see:
   ```
   ✅ Successfully obtained Access Token!
   Type: Bearer
   Expires in: 3599 seconds
   Scopes: https://www.googleapis.com/auth/spreadsheets.readonly, https://www.googleapis.com/auth/bigquery
   Verification Complete: Service Account is valid and can request required scopes.
   ```

#### Method B: Check via Worker Endpoint

If the Worker is already running (locally or deployed), you can use the diagnostic endpoint:

**Request:**
```http
POST /api/diagnostics/sheets
Authorization: Bearer YOUR_SYNC_API_KEY
Content-Type: application/json

{
  "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Sheet accessible",
  "preview": [...]
}
```

#### Method C: Manual Verification in Google Cloud Console

1. Go to [Google Cloud Console > IAM & Admin > IAM](https://console.cloud.google.com/iam-admin/iam).
2. Find your Service Account (`...@...iam.gserviceaccount.com`).
3. Ensure it has the necessary Roles (e.g., BigQuery Data Viewer, BigQuery Job User).
4. **For Sheets**: The Service Account must be **shared** on the Google Sheet itself.
   - Open your Google Sheet.
   - Click "Share".
   - Add the Service Account email as a "Viewer".

### 4. Deployment

```bash
npm run deploy
```

## Cron Schedule Configuration

Each sync job can have its own cron schedule. The system supports flexible scheduling with a queue-based execution model.

### Available Presets

- **Every 6 Hours** (default): `0 */6 * * *`
- **Hourly**: `0 * * * *`
- **Daily (Midnight)**: `0 0 * * *`
- **Daily (8 AM UTC)**: `0 8 * * *`
- **Twice Daily**: `0 0,12 * * *`
- **Weekly (Monday)**: `0 0 * * 1`
- **Every 30 Minutes**: `*/30 * * * *`
- **Every 15 Minutes**: `*/15 * * * *`
- **Custom**: Any valid cron expression

### Queue-Based Execution

Jobs are executed sequentially (one at a time) to respect Cloudflare Free Tier limits:

1. **Sequential Processing**: Jobs run one after another, not in parallel
2. **Smart Delays**: The system waits between jobs based on the previous job's duration
3. **Error Handling**: If a job fails, the queue continues with the next job
4. **State Tracking**: Queue state is persisted in KV for monitoring

### Cloudflare Free Tier Limits

| Resource | Free Tier Limit | Strategy |
|----------|----------------|----------|
| CPU Time | 50ms/request | Jobs run sequentially with delays |
| KV Reads | 100,000/day | Minimal reads, batch operations |
| KV Writes | 1,000/day | State updates only when necessary |
| Cron Triggers | 3 triggers | 4 triggers configured (0, 15, 30, 45 min) |

### API Endpoints

#### Validate Cron Expression
```http
POST /api/cron/validate
Authorization: Bearer YOUR_SYNC_API_KEY
Content-Type: application/json

{
  "expression": "0 */6 * * *"
}
```

#### Get Cron Schedules
```http
GET /api/cron/schedules
Authorization: Bearer YOUR_SYNC_API_KEY
```

#### Update Cron Schedules
```http
POST /api/cron/schedules
Authorization: Bearer YOUR_SYNC_API_KEY
Content-Type: application/json

[
  {
    "id": "custom",
    "name": "My Schedule",
    "expression": "0 */6 * * *",
    "description": "Every 6 hours",
    "enabled": true
  }
]
```

#### Get Queue Status
```http
GET /api/queue
Authorization: Bearer YOUR_SYNC_API_KEY
```

#### Get Specific Queue
```http
GET /api/queue/{queueId}
Authorization: Bearer YOUR_SYNC_API_KEY
```
