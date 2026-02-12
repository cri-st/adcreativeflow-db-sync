export interface FunnelData {
    date_monday: string;
    month_year: string | null;
    week_number: number | null;
    campaign_id: number;
    campaign_name: string | null;
    impressions: number | null;
    clicks: number | null;
    landing_page_views: number | null;
    add_to_cart: number | null;
    initiate_checkout: number | null;
    add_payment_info: number | null;
    purchases: number | null;
    spend: number | null;
    revenue: number | null;
    cpm: number | null;
    cpc: number | null;
    cost_per_landing_view: number | null;
    cac: number | null;
    roas: number | null;
    aov: number | null;
    profit: number | null;
    ctr: number | null;
    click_to_landing_rate: number | null;
    cvr_landing_to_atc: number | null;
    cvr_atc_to_ic: number | null;
    cvr_ic_to_payment: number | null;
    cvr_payment_to_purchase: number | null;
    cvr_global: number | null;
}

export interface BigQuerySyncConfig {
    type?: 'bq-to-supabase';
    id: string;
    name: string;
    enabled: boolean;

    bigquery: {
        projectId: string;
        datasetId: string;
        tableOrView: string;
        incrementalColumn?: string;
        forceStringFields?: string[];
    };

    supabase: {
        tableName: string;
        upsertColumns: string[];
    };

    lastRun?: string;
    lastStatus?: 'success' | 'error';
    lastError?: string;
    lastSummary?: string;
    cronSchedule?: string;
}

export interface SheetsSyncConfig {
    type: 'sheets-to-bq';
    id: string;
    name: string;
    enabled: boolean;

    sheets: {
        spreadsheetId: string;
        range: string;
        append?: boolean;
    };

    bigquery: {
        projectId: string;
        datasetId: string;
        tableId: string;
        writeDisposition?: 'WRITE_TRUNCATE' | 'WRITE_APPEND' | 'WRITE_EMPTY';
    };

    lastRun?: string;
    lastStatus?: 'success' | 'error';
    lastError?: string;
    lastSummary?: string;
    cronSchedule?: string;
}

export type SyncJobConfig = BigQuerySyncConfig | SheetsSyncConfig;

export const SHEETS_WHITELIST: string[] = [];
