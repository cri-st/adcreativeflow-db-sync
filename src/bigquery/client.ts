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
            scope: 'https://www.googleapis.com/auth/bigquery',
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

    private parseRows<T>(schema: { fields: any[] }, rows: any[] | undefined, forceStringFields?: string[]): T[] {
        if (!rows) return [];
        const forceSet = new Set(forceStringFields || []);

        return rows.map((row: any) => {
            const obj: any = {};
            schema.fields.forEach((field: any, i: number) => {
                const val = row.f[i].v;
                if (val === null) {
                    obj[field.name] = null;
                } else if (forceSet.has(field.name)) {
                    obj[field.name] = val;
                } else {
                    switch (field.type) {
                        case 'INTEGER':
                        case 'INT64':
                            // Keep large integers as strings to avoid JavaScript precision loss
                            // Numbers > MAX_SAFE_INTEGER (9007199254740991) lose precision with parseInt
                            const numVal = BigInt(val);
                            obj[field.name] = numVal > Number.MAX_SAFE_INTEGER || numVal < Number.MIN_SAFE_INTEGER
                                ? val  // Keep as string
                                : parseInt(val, 10);  // Safe to convert
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

    async query<T>(projectId: string, sql: string, forceStringFields?: string[]): Promise<T[]> {
        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(projectId)}/queries`;

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

        return this.parseRows<T>(data.schema, data.rows, forceStringFields);
    }

    async queryPaginated<T>(projectId: string, sql: string, forceStringFields?: string[]): Promise<T[]> {
        const token = await this.getAccessToken();
        const baseUrl = `https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(projectId)}/queries`;

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

        const allRows: T[] = this.parseRows<T>(schema, initialData.rows, forceStringFields);
        let pageToken = initialData.pageToken;
        let pageNumber = 1;

        console.log(`[BQ PAGINATION] Page ${pageNumber}/${estimatedPages} (${allRows.length} rows fetched)`);

        while (pageToken && pageToken !== '') {
            pageNumber++;
            const pageUrl = `${baseUrl}/${encodeURIComponent(jobId)}?pageToken=${encodeURIComponent(pageToken)}&maxResults=5000`;

            const pageResponse = await fetch(pageUrl, {
                headers: {
                    Authorization: `Bearer ${token}`,
                },
            });

            const pageData: any = await pageResponse.json();
            if (pageData.error) {
                throw new Error(`BigQuery Pagination Error: ${pageData.error.message}`);
            }

            const pageRows = this.parseRows<T>(schema, pageData.rows, forceStringFields);
            allRows.push(...pageRows);
            pageToken = pageData.pageToken;

            console.log(`[BQ PAGINATION] Page ${pageNumber}/${estimatedPages} (${allRows.length} rows fetched)`);
        }

        return allRows;
    }

    async getTableMetadata(projectId: string, datasetId: string, tableId: string): Promise<any> {
        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}`;

        const response = await fetch(url, {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        });

        if (!response.ok) {
            const statusCode = response.status;
            let errorMessage: string;
            
            try {
                const errorData: any = await response.json();
                errorMessage = errorData.error?.message || `HTTP ${statusCode}: ${response.statusText}`;
            } catch {
                errorMessage = `HTTP ${statusCode}: ${response.statusText}`;
            }
            
            throw new Error(`BigQuery Metadata Error: ${errorMessage}`);
        }

        const contentType = response.headers.get('content-type');
        const contentLength = response.headers.get('content-length');
        
        if (contentLength === '0' || !contentType?.includes('application/json')) {
            throw new Error('BigQuery Metadata Error: Empty or invalid response from API');
        }

        const data: any = await response.json();
        if (data.error) {
            throw new Error(`BigQuery Metadata Error: ${data.error.message}`);
        }

        return data;
    }

    async updateTableSchema(
        projectId: string, 
        datasetId: string, 
        tableId: string, 
        newColumns: string[]
    ): Promise<void> {
        if (newColumns.length === 0) return;

        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}`;

        // First, get current schema
        const currentMetadata = await this.getTableMetadata(projectId, datasetId, tableId);
        const currentFields = currentMetadata.schema?.fields || [];

        // Add new columns as STRING type (nullable)
        const newFields = newColumns.map(col => ({
            name: col,
            type: 'STRING',
            mode: 'NULLABLE'
        }));

        const updatedSchema = {
            fields: [...currentFields, ...newFields]
        };

        const response = await fetch(url, {
            method: 'PATCH',
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                schema: updatedSchema
            }),
        });

        const data: any = await response.json();
        if (data.error) {
            throw new Error(`BigQuery Schema Update Error: ${data.error.message}`);
        }
    }

    async loadFromJson(projectId: string, datasetId: string, tableId: string, ndjson: string, append: boolean = true, schema?: { fields: any[] }): Promise<any> {
        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/upload/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs?uploadType=multipart`;
        const boundary = 'XXXXXXXXXX';

        const metadata: any = {
            configuration: {
                load: {
                    destinationTable: {
                        projectId,
                        datasetId,
                        tableId
                    },
                    sourceFormat: 'NEWLINE_DELIMITED_JSON',
                    writeDisposition: append ? 'WRITE_APPEND' : 'WRITE_TRUNCATE'
                }
            }
        };

        if (schema) {
            metadata.configuration.load.schema = schema;
            metadata.configuration.load.autodetect = false;
        }

        const body = `--${boundary}
Content-Type: application/json; charset=UTF-8

${JSON.stringify(metadata)}

--${boundary}
Content-Type: application/json

${ndjson}

--${boundary}--`;

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': `multipart/related; boundary=${boundary}`
            },
            body: body
        });

        const data: any = await response.json();
        if (data.error) {
            throw new Error(`BigQuery Load Error: ${data.error.message}`);
        }

        const jobId = data.jobReference.jobId;
        return this.pollJob(projectId, jobId);
    }

    private async pollJob(projectId: string, jobId: string): Promise<any> {
        const token = await this.getAccessToken();
        const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}`;

        while (true) {
            const response = await fetch(url, {
                headers: { Authorization: `Bearer ${token}` }
            });
            const data: any = await response.json();

            if (data.status?.state === 'DONE') {
                if (data.status.errorResult) {
                    const errorDetails = data.status.errors?.map((e: any) => e.message).join('; ') || '';
                    const fullMessage = errorDetails 
                        ? `${data.status.errorResult.message}. Details: ${errorDetails}`
                        : data.status.errorResult.message;
                    throw new Error(`BigQuery Job Failed: ${fullMessage}`);
                }
                return data;
            }

            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}
