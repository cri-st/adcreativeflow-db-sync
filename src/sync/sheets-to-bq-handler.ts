import { BigQueryClient } from '../bigquery/client';
import { SheetsClient } from '../sheets/client';
import { Logger, LogEntry } from '../logger';
import { GlobalAuth, SyncResult } from './handler';
import { SheetsSyncConfig } from '../types/funnel';

interface SheetsSyncState {
    lastProcessedRow: number;
    totalRows: number;
    headers: string[];
    startTime: number;
    tableExists?: boolean;
    tableSchema?: string[];
}

function sanitizeColumnName(name: string): string {
    return name
        .trim()
        .replace(/[^a-zA-Z0-9_]/g, '_')
        .replace(/^{0-9}/, '_$&');
}

function cleanValue(val: any): any {
    if (val === undefined || val === '') return null;
    
    if (typeof val === 'string') {
        // Escape special JSON characters to prevent malformed JSON
        return val
            .replace(/\\/g, '\\\\')  // Escape backslashes first
            .replace(/"/g, '\\"')      // Escape double quotes
            .replace(/\n/g, '\\n')      // Escape newlines
            .replace(/\r/g, '\\r')      // Escape carriage returns
            .replace(/\t/g, '\\t');     // Escape tabs
    }
    return val;
}

export async function handleSheetsToBigQuerySync(
    auth: GlobalAuth,
    job: SheetsSyncConfig,
    runId: string,
    kvNamespace: KVNamespace,
    batchNumber: number = 1
): Promise<SyncResult> {
    const logger = new Logger(job.id, job.name, runId);
    
    if (batchNumber === 1) {
        await logger.startRun(kvNamespace);
    }

    try {
        const sheets = new SheetsClient(auth.googleServiceAccount);
        const bq = new BigQueryClient(auth.googleServiceAccount);
        const stateKey = `sheets_sync_state:${job.id}:${runId}`;
        
        let startRow = 2;
        let headers: string[] = [];
        let startTime = Date.now();
        let totalRows = 0;

        const sheetName = job.sheets.range;

        let tableExists = false;
        let tableSchema: string[] | undefined;

        if (batchNumber === 1) {
            logger.info('SYNC_START', 'Starting Sheets to BigQuery sync', { 
                source: job.sheets, 
                destination: job.bigquery 
            });

            logger.info('HEADERS', 'Fetching sheet headers');
            const headerRows = await sheets.getSheetRange(job.sheets.spreadsheetId, `${sheetName}!1:1`);
            
            if (!headerRows || headerRows.length === 0 || !headerRows[0] || headerRows[0].length === 0) {
                throw new Error('Sheet is empty or missing headers in row 1');
            }

            headers = headerRows[0].map(h => sanitizeColumnName(h.toString()));
            logger.info('HEADERS', 'Headers found', { count: headers.length, headers });

            try {
                const metadata = await bq.getTableMetadata(job.bigquery.projectId, job.bigquery.datasetId, job.bigquery.tableId);
                tableExists = true;
                tableSchema = metadata.schema?.fields?.map((f: any) => f.name) || [];
                logger.info('TABLE_CHECK', 'Table exists, will use schema evolution', { 
                    tableColumns: tableSchema!.length,
                    sheetColumns: headers.length 
                });
            } catch (err: any) {
                if (err.message?.includes('Not found')) {
                    tableExists = false;
                    logger.info('TABLE_CHECK', 'Table does not exist, will create with schema');
                } else {
                    throw err;
                }
            }

            await kvNamespace.put(stateKey, JSON.stringify({
                lastProcessedRow: 1,
                totalRows: 0,
                headers,
                startTime,
                tableExists,
                tableSchema
            }), { expirationTtl: 86400 });

        } else {
            logger.info('BATCH_START', `Starting batch ${batchNumber}`);
            const state = await kvNamespace.get<SheetsSyncState & { tableExists: boolean }>(stateKey, 'json');
            
            if (!state) {
                throw new Error(`Sync state not found for runId ${runId} (batch ${batchNumber}). The run may have expired.`);
            }

            startRow = state.lastProcessedRow + 1;
            headers = state.headers;
            startTime = state.startTime;
            totalRows = state.totalRows;
            tableExists = state.tableExists ?? false;
            tableSchema = state.tableSchema;
        }

        const BATCH_SIZE = 5000;
        const endRow = startRow + BATCH_SIZE - 1;
        const range = `${sheetName}!${startRow}:${endRow}`;

        logger.info('DATA_FETCH', `Fetching batch rows ${startRow} to ${endRow}`);
        const rows = await sheets.getSheetRange(job.sheets.spreadsheetId, range);
        
        const rowCount = rows ? rows.length : 0;
        logger.info('DATA_FETCH', 'Rows fetched', { count: rowCount });

        let rowsProcessedInBatch = 0;

        if (rowCount > 0) {
            const shouldPreserveExistingData = job.sheets.append === true;
            const isFirstBatchOfNewSync = batchNumber === 1;
            const shouldTruncate = isFirstBatchOfNewSync && !shouldPreserveExistingData;
            const writeDisposition = shouldTruncate ? 'WRITE_TRUNCATE' : 'WRITE_APPEND';
            const isNewTable = isFirstBatchOfNewSync && !tableExists;
            
            let effectiveHeaders = headers;
            
            if (!isNewTable && tableSchema) {
                const newColumns = headers.filter(h => !tableSchema!.includes(h));
                
                if (newColumns.length > 0) {
                    logger.info('SCHEMA_UPDATE', 'Detected new columns in Sheet, updating BigQuery schema', {
                        newColumns,
                        existingColumns: tableSchema.length,
                        totalColumns: headers.length
                    });
                    
                    await bq.updateTableSchema(
                        job.bigquery.projectId,
                        job.bigquery.datasetId,
                        job.bigquery.tableId,
                        newColumns
                    );
                    
                    tableSchema = [...tableSchema, ...newColumns];
                    
                    logger.success('SCHEMA_UPDATE', 'BigQuery schema updated successfully', {
                        addedColumns: newColumns.length
                    });
                }
            }
            
            const ndjsonLines = rows.map(row => {
                const obj: any = {};
                effectiveHeaders.forEach((header) => {
                    const originalIndex = headers.indexOf(header);
                    const val = row[originalIndex];
                    obj[header] = (val === undefined || val === '') ? null : cleanValue(val);
                });
                return JSON.stringify(obj);
            });

            const ndjson = ndjsonLines.join('\n');
            const shouldProvideSchema = isNewTable;

            const schema = shouldProvideSchema ? {
                fields: headers.map(h => ({
                    name: h,
                    type: 'STRING',
                    mode: 'NULLABLE'
                }))
            } : undefined;

            logger.info('BQ_LOAD', `Loading ${rowCount} rows to BigQuery`, { 
                writeDisposition,
                shouldPreserveExistingData,
                isNewTable,
                shouldProvideSchema,
                effectiveColumns: effectiveHeaders.length,
                batchNumber 
            });
            
            await bq.loadFromJson(
                job.bigquery.projectId,
                job.bigquery.datasetId,
                job.bigquery.tableId,
                ndjson,
                writeDisposition === 'WRITE_APPEND',
                schema
            );

            rowsProcessedInBatch = rowCount;
            totalRows += rowCount;
        }

        const hasMore = rowCount === BATCH_SIZE;
        const nextStartRow = startRow + rowCount;

        if (hasMore) {
            await kvNamespace.put(stateKey, JSON.stringify({
                lastProcessedRow: nextStartRow - 1,
                totalRows,
                headers,
                startTime,
                tableExists,
                tableSchema
            }), { expirationTtl: 86400 });
            
            logger.success('BATCH_COMPLETE', `Batch ${batchNumber} complete`, { 
                rowsProcessed: rowsProcessedInBatch,
                totalRowsSoFar: totalRows,
                nextBatch: batchNumber + 1 
            });
        } else {
            const durationMs = Date.now() - startTime;
            const minutes = Math.floor(durationMs / 60000);
            const seconds = Math.floor((durationMs % 60000) / 1000);
            const durationStr = `${minutes}m ${seconds}s`;

            logger.success('SYNC_COMPLETE', 'Sheets sync job finished successfully', {
                totalBatches: batchNumber,
                totalRows,
                duration: durationStr
            });

            await logger.endRun(kvNamespace, 'success');
            await kvNamespace.delete(stateKey);
        }

        return {
            hasMore,
            nextBatch: batchNumber + 1,
            rowsProcessed: rowsProcessedInBatch,
            rowsDeleted: 0,
            stats: {
                totalRows,
                totalBatches: batchNumber,
                durationMs: Date.now() - startTime
            },
            logs: logger.getLogs()
        };

    } catch (err: any) {
        logger.error('SYNC_ERROR', 'Sheets sync failed', { error: err.message, stack: err.stack?.substring(0, 500) });
        await logger.endRun(kvNamespace, 'error');
        throw err;
    }
}
