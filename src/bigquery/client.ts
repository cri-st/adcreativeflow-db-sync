import * as jose from 'jose';

export class BigQueryClient {
    private accessToken: string | null = null;
    private tokenExpiry: number = 0;

    constructor(private serviceAccountJson: string) { }

    private async getAccessToken() {
        const now = Math.floor(Date.now() / 1000);
        if (this.accessToken && now < this.tokenExpiry) {
            return this.accessToken;
        }

        const sa = JSON.parse(this.serviceAccountJson);
        const iat = now;
        const exp = iat + 3600;

        const jwt = await new jose.SignJWT({
            iss: sa.client_email,
            sub: sa.client_email,
            aud: 'https://oauth2.googleapis.com/token',
            scope: 'https://www.googleapis.com/auth/bigquery.readonly',
        })
            .setProtectedHeader({ alg: 'RS256', kid: sa.private_key_id })
            .setIssuedAt(iat)
            .setExpirationTime(exp)
            .sign(await jose.importPKCS8(sa.private_key, 'RS256'));

        const response = await fetch('https://oauth2.googleapis.com/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                assertion: jwt,
            }),
        });

        const data: any = await response.json();
        if (data.error) {
            throw new Error(`Google Auth Error: ${data.error_description || data.error}`);
        }

        this.accessToken = data.access_token;
        this.tokenExpiry = now + data.expires_in - 60; // 1 min buffer
        return this.accessToken;
    }

    private parseRows<T>(schema: { fields: any[] }, rows: any[] | undefined): T[] {
        if (!rows) return [];

        return rows.map((row: any) => {
            const obj: any = {};
            schema.fields.forEach((field: any, i: number) => {
                const val = row.f[i].v;
                if (val === null) {
                    obj[field.name] = null;
                } else {
                    switch (field.type) {
                        case 'INTEGER':
                        case 'INT64':
                            obj[field.name] = parseInt(val, 10);
                            break;
                        case 'FLOAT':
                        case 'FLOAT64':
                            obj[field.name] = parseFloat(val);
                            break;
                        default:
                            obj[field.name] = val;
                    }
                }
            });
            return obj;
        });
    }

    async query<T>(projectId: string, sql: string): Promise<T[]> {
        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/queries`;

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: sql,
                useLegacySql: false,
            }),
        });

        const data: any = await response.json();
        if (data.error) {
            throw new Error(`BigQuery Error: ${data.error.message}`);
        }

        return this.parseRows<T>(data.schema, data.rows);
    }

    async queryPaginated<T>(projectId: string, sql: string): Promise<T[]> {
        const token = await this.getAccessToken();
        const baseUrl = `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/queries`;

        const initialResponse = await fetch(baseUrl, {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: sql,
                useLegacySql: false,
                maxResults: 5000,
            }),
        });

        const initialData: any = await initialResponse.json();
        if (initialData.error) {
            throw new Error(`BigQuery Error: ${initialData.error.message}`);
        }

        if (!initialData.jobComplete) {
            throw new Error('BigQuery Error: Query did not complete. Consider increasing timeout.');
        }

        const schema = initialData.schema;
        const jobId = initialData.jobReference.jobId;
        const totalRows = parseInt(initialData.totalRows || '0', 10);
        const estimatedPages = totalRows > 0 ? Math.ceil(totalRows / 5000) : 1;

        const allRows: T[] = this.parseRows<T>(schema, initialData.rows);
        let pageToken = initialData.pageToken;
        let pageNumber = 1;

        console.log(`[BQ PAGINATION] Page ${pageNumber}/${estimatedPages} (${allRows.length} rows fetched)`);

        while (pageToken && pageToken !== '') {
            pageNumber++;
            const pageUrl = `${baseUrl}/${jobId}?pageToken=${encodeURIComponent(pageToken)}&maxResults=5000`;

            const pageResponse = await fetch(pageUrl, {
                headers: {
                    Authorization: `Bearer ${token}`,
                },
            });

            const pageData: any = await pageResponse.json();
            if (pageData.error) {
                throw new Error(`BigQuery Pagination Error: ${pageData.error.message}`);
            }

            const pageRows = this.parseRows<T>(schema, pageData.rows);
            allRows.push(...pageRows);
            pageToken = pageData.pageToken;

            console.log(`[BQ PAGINATION] Page ${pageNumber}/${estimatedPages} (${allRows.length} rows fetched)`);
        }

        return allRows;
    }

    async getTableMetadata(projectId: string, datasetId: string, tableId: string): Promise<any> {
        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/datasets/${datasetId}/tables/${tableId}`;

        const response = await fetch(url, {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        });

        const data: any = await response.json();
        if (data.error) {
            throw new Error(`BigQuery Metadata Error: ${data.error.message}`);
        }

        return data;
    }
}
