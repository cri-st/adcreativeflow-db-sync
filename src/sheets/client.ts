import { GoogleSheetsAuth } from '../google/auth';

export class SheetsClient {
    private auth: GoogleSheetsAuth;

    constructor(serviceAccountJson: string) {
        this.auth = new GoogleSheetsAuth(serviceAccountJson);
    }

    private async fetchWithRetry(url: string, options: RequestInit, retries = 3): Promise<Response> {
        for (let i = 0; i < retries; i++) {
            const response = await fetch(url, options);

            if (response.status === 429 || (response.status >= 500 && response.status < 600)) {
                const delay = Math.pow(2, i) * 1000 + Math.random() * 1000;
                console.warn(`Request failed with status ${response.status}. Retrying in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
                continue;
            }
            
            return response;
        }
        throw new Error(`Request failed after ${retries} retries`);
    }

    /**
     * Fetches a specific range of values from a spreadsheet.
     * @param spreadsheetId The ID of the spreadsheet.
     * @param range The A1 notation of the values to retrieve.
     * @returns A 2D array of values.
     */
    async getSheetRange(spreadsheetId: string, range: string): Promise<any[][]> {
        const token = await this.auth.getSheetsAccessToken();
        const url = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(range)}`;

        const response = await this.fetchWithRetry(url, {
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
            },
        });

        if (!response.ok) {
             if (response.status === 403) throw new Error(`Permission denied for spreadsheet ${spreadsheetId}`);
             if (response.status === 404) throw new Error(`Spreadsheet ${spreadsheetId} not found`);
             
             const errorText = await response.text();
             throw new Error(`Sheets API Error: ${response.status} ${errorText}`);
        }

        const data: any = await response.json();
        return data.values || [];
    }

    /**
     * Fetches all values from a sheet using batching to avoid timeouts and rate limits.
     * @param spreadsheetId The ID of the spreadsheet.
     * @param sheetName The name of the sheet (e.g., 'Sheet1').
     * @returns A 2D array of all values in the sheet.
     */
    async getSheetValues(spreadsheetId: string, sheetName: string): Promise<any[][]> {
        const batchSize = 100;
        let allValues: any[][] = [];
        let startRow = 1;
        let hasMore = true;

        while (hasMore) {
            // Construct range: SheetName!StartRow:EndRow
            // This fetches all columns for the specified rows
            const endRow = startRow + batchSize - 1;
            const range = `${sheetName}!${startRow}:${endRow}`;
            
            console.log(`[SheetsClient] Fetching batch: ${range}`);
            const values = await this.getSheetRange(spreadsheetId, range);
            
            if (values.length === 0) {
                hasMore = false;
            } else {
                allValues = allValues.concat(values);
                
                // If we got fewer rows than requested, we've reached the end
                // However, empty rows at the end might be trimmed by the API
                if (values.length < batchSize) {
                    hasMore = false;
                } else {
                    startRow += batchSize;
                }
            }
        }
        return allValues;
    }
}
