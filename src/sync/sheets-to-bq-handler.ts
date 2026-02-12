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
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, '_')
        .replace(/^[0-9]/, '_$&');
}

function cleanValue(val: any): any {
    if (val === undefined || val === '') return null;
    return val;
}

function convertTimestampToBigQueryFormat(val: string): string | null {
    if (!val || val === '') return null;
    
    const isoMatch = val.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:[+-]\d{2}:?\d{2})?$/);
    if (isoMatch) {
        const [, year, month, day, hour, minute, second] = isoMatch;
        return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
    }
    
    return val;
}

function inferBigQueryType(values: any[]): string {
    let hasFloat = false;
    let hasInteger = true;
    let hasDate = false;
    let hasTimestamp = false;
    
    for (const val of values) {
        if (val === null || val === undefined || val === '') continue;
        
        const strVal = String(val).trim();
        if (strVal === '') continue;
        
        if (/^\d{4}-\d{2}-\d{2}$/.test(strVal)) {
            hasDate = true;
            continue;
        }
        
        if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/.test(strVal)) {
            hasTimestamp = true;
            continue;
        }
        
        if (/^-?\d+\.\d+$/.test(strVal)) {
            hasFloat = true;
            hasInteger = false;
            continue;
        }
        
        if (/^-?\d+$/.test(strVal)) {
            continue;
        }
        
        return 'STRING';
    }
    
    if (hasTimestamp) return 'TIMESTAMP';
    if (hasDate) return 'DATE';
    if (hasFloat) return 'FLOAT';
    if (hasInteger) return 'INTEGER';
    return 'STRING';
}

function generateSchemaFromData(headers: string[], rows: any[][]): { fields: { name: string; type: string; mode: string }[] } {
    const fields = headers.map((header, colIndex) => {
        const columnValues = rows.map(row => row[colIndex]).filter(v => v !== undefined && v !== null && v !== '');
        const bqType = inferBigQueryType(columnValues);
        
        return {
            name: header,
            type: bqType,
            mode: 'NULLABLE'
        };
    });
    
    return { fields };
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
                    sheetColumns: headers.length,
                    tableSchema: tableSchema,
                    sheetHeaders: headers
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
                const tableSchemaLower = tableSchema.map(s => s.toLowerCase());
                const newColumns = headers.filter(h => !tableSchemaLower.includes(h.toLowerCase()));
                
                logger.info('SCHEMA_COMPARE', 'Comparing Sheet headers with BigQuery schema', {
                    sheetHeaders: headers,
                    tableSchema: tableSchema,
                    tableSchemaLower: tableSchemaLower,
                    newColumnsDetected: newColumns
                });
                
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
                    let cleanVal = (val === undefined || val === '') ? null : cleanValue(val);
                    
                    if (typeof cleanVal === 'string' && (header.includes('time') || header.includes('date'))) {
                        cleanVal = convertTimestampToBigQueryFormat(cleanVal);
                    }
                    
                    if (!isNewTable && tableSchema) {
                        const bqColumnName = tableSchema.find(s => s.toLowerCase() === header.toLowerCase());
                        if (bqColumnName) {
                            obj[bqColumnName] = cleanVal;
                        } else {
                            obj[header] = cleanVal;
                        }
                    } else {
                        obj[header] = cleanVal;
                    }
                });
                return JSON.stringify(obj);
            });

            const ndjson = ndjsonLines.join('\n');
            
            if (ndjsonLines.length > 0) {
                logger.info('NDJSON_SAMPLE', 'First row of NDJSON data', { 
                    sample: ndjsonLines[0],
                    totalRows: ndjsonLines.length
                });
            }
            
            const shouldProvideSchema = isNewTable;

            let schema: { fields: { name: string; type: string; mode: string }[] } | undefined;
            if (shouldProvideSchema) {
                schema = generateSchemaFromData(headers, rows);
                logger.info('SCHEMA_INFERENCE', 'Inferred BigQuery schema from data', {
                    fields: schema.fields.map(f => ({ name: f.name, type: f.type }))
                });
            }

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
