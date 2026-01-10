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

        if (!data.rows) return [];

        return data.rows.map((row: any) => {
            const obj: any = {};
            data.schema.fields.forEach((field: any, i: number) => {
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
