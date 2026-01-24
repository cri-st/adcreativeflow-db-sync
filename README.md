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
