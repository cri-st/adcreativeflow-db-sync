const state = {
    apiKey: localStorage.getItem('sync_key') || '',
    jobs: [],
    logs: [],
    runs: [],
    currentRunId: null,
    jobLogs: {},
    logPollingInterval: null,
    currentLogJobId: null,
    currentLogJobName: null,
    isAutoScrollEnabled: true,
    activeFilters: ['INFO', 'SUCCESS', 'WARNING', 'ERROR', 'DEBUG'],
    retryCount: 0,
    jobDeleted: false,
    syncCompleted: false,
    lastLogCount: 0,
    stablePolls: 0,
    activeSyncs: new Map(),
    cronSchedules: [],
    selectedCronExpression: '0 */6 * * *',
};

const loginView = document.getElementById('login-view');
const dashboardView = document.getElementById('dashboard-view');
const apiKeyInput = document.getElementById('api-key-input');
const loginBtn = document.getElementById('login-btn');
const loginError = document.getElementById('login-error');
const bqJobsContainer = document.getElementById('bq-jobs-container');
const sheetsJobsContainer = document.getElementById('sheets-jobs-container');
const logoutBtn = document.getElementById('logout-btn');
const runAllBtn = document.getElementById('run-all-btn');
const createBqBtn = document.getElementById('create-bq-btn');
const createSheetsBtn = document.getElementById('create-sheets-btn');

// Tab elements
const tabBtns = document.querySelectorAll('.tab-btn');
const tabContents = document.querySelectorAll('.tab-content');

// Tab logic
tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        const tabId = btn.dataset.tab;
        
        tabBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        
        tabContents.forEach(c => {
            c.classList.remove('active');
            if (c.id === `tab-${tabId}`) {
                c.classList.add('active');
            }
        });
    });
});

const jobModal = document.getElementById('job-modal');
const jobForm = document.getElementById('job-form');
const jobTypeInput = document.getElementById('job-type');
const bqSection = document.getElementById('bq-supabase-section');
const sheetsSection = document.getElementById('sheets-bq-section');
const cancelModal = document.getElementById('cancel-modal');
const modalTitle = document.getElementById('modal-title');
const testSheetBtn = document.getElementById('test-sheet-btn');

const logModal = document.getElementById('log-modal');
const logModalTitle = document.getElementById('log-modal-title');
const logModalSubtitle = document.getElementById('log-modal-subtitle');
const logModalClose = document.getElementById('log-modal-close');
const logEntries = document.getElementById('log-entries');
const logClearBtn = document.getElementById('log-clear-btn');
const logDownloadBtn = document.getElementById('log-download-btn');
const logFilters = document.querySelectorAll('.log-filter');
const logDeletedBanner = document.getElementById('log-deleted-banner');
const logErrorBanner = document.getElementById('log-error-banner');
const logCompleteBanner = document.getElementById('log-complete-banner');

if (state.apiKey) {
    showDashboard();
}

function showToast(message, type = 'error', duration = 5000) {
    const container = document.getElementById('toast-container');
    const icons = { error: '❌', success: '✅', warning: '⚠️' };
    
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.error}</span>
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;
    
    container.appendChild(toast);
    
    setTimeout(() => {
        toast.classList.add('toast-fade-out');
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

async function login() {
    const key = apiKeyInput.value.trim();
    if (!key) return;

    loginBtn.disabled = true;
    loginBtn.innerText = 'AUTHENTICATING...';

    try {
        const res = await fetch('/api/auth', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key })
        });

        if (res.ok) {
            state.apiKey = key;
            localStorage.setItem('sync_key', key);
            showDashboard();
        } else {
            loginError.classList.remove('hidden');
        }
    } catch (err) {
        console.error(err);
        loginError.innerText = 'CONNECTION ERROR';
        loginError.classList.remove('hidden');
    } finally {
        loginBtn.disabled = false;
        loginBtn.innerText = 'INITIALIZE SESSION';
    }
}

function logout() {
    localStorage.removeItem('sync_key');
    location.reload();
}

function showDashboard() {
    loginView.classList.add('hidden');
    dashboardView.classList.remove('hidden');
    fetchJobs();
}

async function fetchJobLogs(jobId) {
    try {
        const res = await fetch(`/api/logs/${jobId}?limit=3`, {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        if (!res.ok) return;
        const data = await res.json();
        if (data.exists) {
            state.jobLogs[jobId] = data.logs || [];
        }
    } catch (err) {
        console.error(`Failed to fetch logs for ${jobId}:`, err);
    }
}

async function fetchJobs() {
    try {
        const res = await fetch('/api/configs', {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        if (res.status === 401) return logout();
        state.jobs = await res.json();

        await Promise.all(state.jobs.map(job => fetchJobLogs(job.id)));

        renderJobs();
    } catch (err) {
        console.error('Failed to fetch jobs', err);
    }
}

function renderMiniLogs(jobId) {
    const logs = state.jobLogs[jobId] || [];
    
    if (logs.length === 0) {
        return '<div class="mini-log-no-activity">No recent activity</div>';
    }
    
    const sortedLogs = [...logs].sort((a, b) => 
        new Date(b.timestamp) - new Date(a.timestamp)
    ).slice(0, 3);
    
    return sortedLogs.map(log => {
        const time = new Date(log.timestamp).toLocaleTimeString('en-US', { 
            hour12: false, 
            hour: '2-digit', 
            minute: '2-digit' 
        });
        const truncatedMsg = log.message.length > 40 
            ? log.message.substring(0, 40) + '...' 
            : log.message;
        return `
            <div class="mini-log-entry log-${log.level}">
                <span class="mini-log-time">[${time}]</span>
                ${log.level}: ${truncatedMsg}
            </div>
        `;
    }).join('');
}

function hasRecentErrors(jobId) {
    const logs = state.jobLogs[jobId] || [];
    const twoHoursAgo = Date.now() - (2 * 60 * 60 * 1000);
    return logs.some(log => 
        log.level === 'ERROR' && 
        new Date(log.timestamp).getTime() > twoHoursAgo
    );
}

function toggleJobLogs(jobId) {
    const logsContainer = document.getElementById(`logs-container-${jobId}`);
    const toggleBtn = document.getElementById(`toggle-logs-${jobId}`);
    
    if (logsContainer && toggleBtn) {
        logsContainer.classList.toggle('expanded');
        toggleBtn.classList.toggle('active');
        
        // Hide mini logs summary if expanded
        const miniLogs = document.getElementById(`mini-logs-${jobId}`);
        if (miniLogs) {
            miniLogs.style.opacity = logsContainer.classList.contains('expanded') ? '0' : '1';
        }
    }
}

function renderJobs() {
    // Filter jobs by tab logic (handled by CSS via container IDs, but we fetch all)
    const bqJobs = state.jobs.filter(j => !j.type || j.type === 'bq-to-supabase');
    const sheetsJobs = state.jobs.filter(j => j.type === 'sheets-to-bq');

    // Update global counter (active jobs total)
    document.getElementById('stat-active').innerText = state.jobs.length;

    const renderJobCard = (job) => {
        const isSheets = job.type === 'sheets-to-bq';
        const sourceName = isSheets ? 'Google Sheets' : job.bigquery.tableOrView;
        const targetName = isSheets ? job.bigquery.tableId : job.supabase.tableName;
        const isSyncing = state.activeSyncs.has(job.id);
        
        return `
            <div class="job-card stagger-in ${isSyncing ? 'syncing' : ''}" id="card-${job.id}">
                <div class="job-info">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <h3 style="margin: 0;">${job.name}</h3>
                        <span class="type-badge ${job.type || 'bq-to-supabase'}">
                            ${(job.type || 'bq-to-supabase').replace(/-/g, ' ')}
                        </span>
                    </div>
                    <div class="id">${job.id}</div>
                </div>
                
                <div class="job-path">
                    <span class="mono">${sourceName}</span>
                    <span class="arrow">→</span>
                    <span class="mono">${targetName}</span>
                </div>
                
                <div class="job-status">
                    <div class="status-badge ${job.lastStatus || ''}" id="status-badge-${job.id}">
                        ${isSyncing ? '<span class="spinner" style="margin-right: 8px;"></span>SYNCING' : (job.lastStatus ? job.lastStatus.toUpperCase() : 'PENDING')}
                    </div>
                    <div class="mono" style="font-size: 0.65rem; color: var(--text-secondary); margin-top: 4px;">
                        ${job.lastRun ? new Date(job.lastRun).toLocaleString() : 'NEVER RUN'}
                    </div>
                    ${job.lastSummary ? `<div class="mono" style="font-size: 0.65rem; color: var(--text-secondary); margin-top: 2px;">${job.lastSummary}</div>` : ''}
                </div>

                <div class="job-actions">
                    <button class="btn btn-ghost" onclick="syncJob('${job.id}')" id="sync-${job.id}" ${isSyncing ? 'disabled' : ''}>
                        ${isSyncing ? '<span class="spinner"></span>' : 'RUN'}
                    </button>
                    <button class="btn btn-ghost" onclick="viewLogs('${job.id}', '${job.name}')">LOGS</button>
                    
                    <button class="btn-toggle-logs" id="toggle-logs-${job.id}" onclick="toggleJobLogs('${job.id}')" title="Toggle Recent Logs">
                        <span style="font-size: 10px;">▼</span>
                    </button>
                    
                    <button class="btn btn-ghost" onclick="editJob('${job.id}')" ${isSyncing ? 'disabled' : ''}>EDIT</button>
                    <button class="btn btn-danger" onclick="deleteJob('${job.id}')" ${isSyncing ? 'disabled' : ''}>DELETE</button>
                </div>

                <div class="job-recent-logs" id="logs-container-${job.id}">
                    <div class="job-recent-logs-title">
                        RECENT LOGS STREAM
                        ${hasRecentErrors(job.id) ? '<span class="job-error-badge">ERRORS</span>' : ''}
                    </div>
                    <div id="mini-logs-${job.id}">
                        ${renderMiniLogs(job.id)}
                    </div>
                    <div class="progress-container">
                        <div class="progress-bar indeterminate" id="progress-${job.id}"></div>
                    </div>
                </div>
            </div>
        `;
    };

    bqJobsContainer.innerHTML = bqJobs.length > 0 
        ? bqJobs.map(renderJobCard).join('')
        : '<p class="mono" style="color: var(--text-secondary); text-align: center; padding: 20px;">NO BIGQUERY SYNC JOBS.</p>';

    sheetsJobsContainer.innerHTML = sheetsJobs.length > 0
        ? sheetsJobs.map(renderJobCard).join('')
        : '<p class="mono" style="color: var(--text-secondary); text-align: center; padding: 20px;">NO SHEETS SYNC JOBS.</p>';
}

function extractSpreadsheetId(url) {
    if (!url) return null;
    const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    return match ? match[1] : null;
}

async function testSheetConnection() {
    const url = document.getElementById('sheet-url').value;
    const name = document.getElementById('sheet-tab-name').value;
    
    if (!url || !name) {
        showToast('Spreadsheet URL and Sheet Tab Name are required');
        return;
    }

    testSheetBtn.disabled = true;
    testSheetBtn.innerText = 'TESTING...';

    try {
        const res = await fetch('/api/diagnostics/sheets', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${state.apiKey}`
            },
            body: JSON.stringify({ spreadsheetUrl: url, sheetName: name })
        });

        const data = await res.json();
        if (res.ok && data.success) {
            showToast('Connection Successful: Sheet is accessible', 'success');
        } else {
            showToast(data.message || 'Connection Failed');
        }
    } catch (err) {
        showToast('Connection Failed: Network Error');
    } finally {
        testSheetBtn.disabled = false;
        testSheetBtn.innerText = 'TEST CONNECTION';
    }
}

async function runAllSyncs() {
    if (!confirm('INITIATE SYNC FOR ALL ENABLED JOBS?')) return;
    
    const enabledJobs = state.jobs.filter(j => j.enabled);
    if (enabledJobs.length === 0) {
        showToast('No enabled jobs to run');
        return;
    }

    showToast(`Initiating ${enabledJobs.length} sync jobs`, 'success');

    // Sort jobs: Sheets -> BigQuery first, then BQ -> Supabase
    // This ensures data flows in the correct dependency order
    const sheetsJobs = enabledJobs.filter(j => j.type === 'sheets-to-bq');
    const bqJobs = enabledJobs.filter(j => !j.type || j.type === 'bq-to-supabase');

    const orderedJobs = [...sheetsJobs, ...bqJobs];

    // Execute strictly sequentially to respect dependencies
    // Sheets needs to finish writing to BQ before BQ sends to Supabase
    for (const job of orderedJobs) {
        await syncJob(job.id);
    }
}

async function saveJob(e) {
    e.preventDefault();
    const id = document.getElementById('job-id').value;
    const type = jobTypeInput.value;
    const isNew = !id;

    const cronSelect = document.getElementById('job-cron-schedule');
    const cronCustom = document.getElementById('job-cron-custom');
    let cronSchedule = cronSelect.value;
    if (cronSchedule === 'custom') {
        cronSchedule = cronCustom.value.trim() || '0 */6 * * *';
    }

    let job = {
        id: id || undefined,
        name: document.getElementById('job-name').value,
        enabled: document.getElementById('job-enabled').checked,
        type: type,
        cronSchedule: cronSchedule
    };

    if (type === 'sheets-to-bq') {
        const url = document.getElementById('sheet-url').value;
        const spreadsheetId = extractSpreadsheetId(url);
        
        if (!spreadsheetId) {
            showToast('Invalid Google Sheets URL');
            return;
        }

        const tabName = document.getElementById('sheet-tab-name').value;
        if (!tabName) {
            showToast('Please enter the Sheet Tab Name (Source)');
            return;
        }

        const tableName = document.getElementById('sheet-table-name').value;
        if (!tableName) {
            showToast('Please enter the Target Table Name');
            return;
        }

        const datasetId = document.getElementById('sheet-bq-dataset').value;
        if (!datasetId) {
            showToast('Please enter the Dataset ID');
            return;
        }

        job.sheets = {
            spreadsheetUrl: url,
            spreadsheetId: spreadsheetId,
            range: tabName,
            sheetName: tabName,
            projectId: document.getElementById('sheet-bq-project').value,
            datasetId: datasetId,
            append: document.getElementById('sheet-append').checked
        };
        job.bigquery = {
            projectId: job.sheets.projectId,
            datasetId: job.sheets.datasetId || 'auto',
            tableId: tableName
        };
    } else {
        job.bigquery = {
            projectId: document.getElementById('bq-project').value,
            datasetId: document.getElementById('bq-dataset').value,
            tableOrView: document.getElementById('bq-table').value,
            incrementalColumn: document.getElementById('bq-column').value || undefined,
            forceStringFields: document.getElementById('bq-force-string').value
                ? document.getElementById('bq-force-string').value.split(',').map(s => s.trim()).filter(Boolean)
                : undefined,
        };
        job.supabase = {
            tableName: document.getElementById('sb-table').value,
            upsertColumns: document.getElementById('sb-columns').value.split(',').map(s => s.trim()),
        };
    }

    try {
        const method = isNew ? 'POST' : 'PUT';
        const url = isNew ? '/api/configs' : `/api/configs/${id}`;

        const res = await fetch(url, {
            method,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${state.apiKey}`
            },
            body: JSON.stringify(job)
        });

        if (!res.ok) {
            const data = await res.json();
            throw new Error(data.error || 'Failed to save');
        }

        jobModal.classList.add('hidden');
        fetchJobs();
        showToast('Job Configuration Saved', 'success');
    } catch (err) {
        showToast(err.message || 'Failed to save job configuration');
    }
}

async function syncJob(id) {
    if (state.activeSyncs.has(id)) return;
    
    state.activeSyncs.set(id, { batchNumber: 1, runId: null });
    renderJobs();

    const job = state.jobs.find(j => j.id === id);
    let batchNumber = 1;
    let runId = null;

    const pollInterval = setInterval(async () => {
        const syncState = state.activeSyncs.get(id);
        if (!syncState || !syncState.runId) return;
        await fetchJobLogs(id);
        const miniLogsContainer = document.getElementById(`mini-logs-${id}`);
        if (miniLogsContainer) {
            miniLogsContainer.innerHTML = renderMiniLogs(id);
        }
    }, 2000);

    try {
        while (true) {
            const res = await fetch(`/api/sync/${id}`, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${state.apiKey}` 
                },
                body: JSON.stringify({ batchNumber, runId })
            });

            if (!res.ok) {
                const data = await res.json();
                showToast(`Sync Failed: ${data.error}`);
                break;
            }

            const result = await res.json();
            runId = result.runId;
            state.activeSyncs.set(id, { batchNumber, runId });
            
            const statusBadge = document.getElementById(`status-badge-${id}`);
            if (statusBadge && result.rowsProcessed !== undefined) {
                const totalSoFar = (result.stats?.totalRows || result.rowsProcessed);
                const summaryEl = statusBadge.parentElement.querySelector('.mono:last-child');
                if (summaryEl) {
                    summaryEl.innerText = `Synced ${totalSoFar.toLocaleString()} rows so far...`;
                }
            }

            if (!result.hasMore) {
                showToast(`Sync completed for ${job?.name || id}`, 'success');
                break;
            }

            batchNumber = result.nextBatch;
        }
    } catch (err) {
        console.error(err);
        showToast('Network Error during sync');
    } finally {
        clearInterval(pollInterval);
        state.activeSyncs.delete(id);
        fetchJobs();
    }
}

async function deleteJob(id) {
    if (!confirm('ARE YOU SURE YOU WANT TO TERMINATE THIS SYNC JOB?')) return;

    try {
        const res = await fetch(`/api/configs/${id}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        if (res.ok) {
            fetchJobs();
        } else {
            showToast('Failed to delete job');
        }
    } catch (err) {
        showToast('Network Error');
    }
}

function editJob(id) {
    const job = state.jobs.find(j => j.id === id);
    if (!job) return;

    const type = job.type || 'bq-to-supabase';
    jobTypeInput.value = type;
    
    modalTitle.innerText = `Update ${type === 'sheets-to-bq' ? 'Sheets' : 'BigQuery'} Sync Job`;
    document.getElementById('job-id').value = job.id;
    document.getElementById('job-name').value = job.name;
    document.getElementById('job-enabled').checked = job.enabled;

    const cronSchedule = job.cronSchedule || '0 */6 * * *';
    const cronSelect = document.getElementById('job-cron-schedule');
    const cronCustom = document.getElementById('job-cron-custom');
    
    const presetValues = Array.from(cronSelect.options).map(o => o.value);
    if (presetValues.includes(cronSchedule)) {
        cronSelect.value = cronSchedule;
        cronCustom.classList.add('hidden');
    } else {
        cronSelect.value = 'custom';
        cronCustom.value = cronSchedule;
        cronCustom.classList.remove('hidden');
    }

    if (type === 'sheets-to-bq') {
        bqSection.classList.remove('active');
        sheetsSection.classList.add('active');
        
        document.getElementById('sheet-url').value = job.sheets.spreadsheetUrl;
        document.getElementById('sheet-tab-name').value = job.sheets.range || job.sheets.sheetName;
        document.getElementById('sheet-table-name').value = job.bigquery.tableId;
        document.getElementById('sheet-bq-project').value = job.sheets.projectId;
        document.getElementById('sheet-bq-dataset').value = job.sheets.datasetId || '';
        document.getElementById('sheet-append').checked = job.sheets.append;
    } else {
        bqSection.classList.add('active');
        sheetsSection.classList.remove('active');

        document.getElementById('bq-project').value = job.bigquery.projectId;
        document.getElementById('bq-dataset').value = job.bigquery.datasetId;
        document.getElementById('bq-table').value = job.bigquery.tableOrView;
        document.getElementById('bq-column').value = job.bigquery.incrementalColumn || '';
        document.getElementById('bq-force-string').value = (job.bigquery.forceStringFields || []).join(', ');
        
        const warning = document.getElementById('incremental-warning');
        if (job.bigquery.incrementalColumn) {
            warning.classList.add('hidden');
        } else {
            warning.classList.remove('hidden');
        }
        document.getElementById('sb-table').value = job.supabase.tableName;
        document.getElementById('sb-columns').value = job.supabase.upsertColumns.join(', ');
    }

    jobModal.classList.remove('hidden');
}

function viewLogs(jobId, jobName) {
    openLogModal(jobId, jobName);
}

function createRunSelector(runs, jobId) {
    const existingSelector = document.getElementById('run-selector');
    if (existingSelector) existingSelector.remove();
    
    if (!runs || runs.length === 0) return null;
    
    const select = document.createElement('select');
    select.id = 'run-selector';
    select.className = 'run-selector';
    select.style.cssText = 'margin-bottom: 10px; padding: 8px; background: #1a1a2e; color: #0ff; border: 1px solid #0ff; border-radius: 4px; width: 100%;';
    
    runs.forEach((run, i) => {
        const option = document.createElement('option');
        option.value = run.runId;
        const date = new Date(run.startedAt).toLocaleString();
        const statusIcon = run.status === 'success' ? '✓' : run.status === 'error' ? '✗' : '⟳';
        option.textContent = `${statusIcon} ${date} - ${run.status}`;
        if (i === 0) option.selected = true;
        select.appendChild(option);
    });
    
    select.addEventListener('change', async (e) => {
        state.currentRunId = e.target.value;
        state.logs = [];
        logEntries.innerHTML = '<div class="log-loading"></div>';
        await fetchLogsForRun(jobId, e.target.value);
    });
    
    return select;
}

async function fetchLogsForRun(jobId, runId) {
    try {
        const res = await fetch(`/api/logs/${jobId}?runId=${runId}`, {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        
        const data = await res.json();
        
        if (data.logs && data.logs.length > 0) {
            const existingKeys = new Set(state.logs.map(l => `${l.timestamp}|${l.phase}`));
            const newLogs = data.logs.filter(l => !existingKeys.has(`${l.timestamp}|${l.phase}`));
            
            if (newLogs.length > 0) {
                state.logs = state.logs.concat(newLogs);
                renderLogs();
                updateLogStats();
            }
        }
    } catch (err) {
        console.error('Failed to fetch logs for run:', err);
    }
}

async function openLogModal(jobId, jobName, isNewSync = false) {
    state.currentLogJobId = jobId;
    state.currentLogJobName = jobName;
    state.logs = [];
    state.runs = [];
    state.currentRunId = null;
    state.isAutoScrollEnabled = true;
    state.retryCount = 0;
    state.jobDeleted = false;
    state.syncCompleted = false;
    state.lastLogCount = 0;
    state.stablePolls = 0;
    
    document.getElementById('log-deleted-banner')?.classList.add('hidden');
    document.getElementById('log-error-banner')?.classList.add('hidden');
    document.getElementById('log-complete-banner')?.classList.add('hidden');
    
    const existingSelector = document.getElementById('run-selector');
    if (existingSelector) existingSelector.remove();
    
    logClearBtn.disabled = false;
    
    logModalTitle.innerText = `SYSTEM LOGS`;
    logModalSubtitle.innerText = jobName;
    
    logEntries.innerHTML = '<div class="log-loading"></div>';
    logModal.classList.remove('hidden');
    
    const activeSync = state.activeSyncs.get(jobId);
    if (isNewSync || activeSync) {
        state.currentRunId = activeSync?.runId || null;
        logEntries.innerHTML = '<div class="log-empty-state"><span class="mono">AWAITING LOG STREAM...</span></div>';
        state.logPollingInterval = setInterval(() => {
            const currentSync = state.activeSyncs.get(jobId);
            const rId = state.currentRunId || currentSync?.runId;
            if (rId) {
                state.currentRunId = rId;
                fetchLogsForRun(jobId, rId);
            }
        }, 1000);
        return;
    }
    
    try {
        const runsRes = await fetch(`/api/logs/${jobId}`, {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        
        if (runsRes.ok) {
            const runsData = await runsRes.json();
            
            if (!runsData.exists) {
                state.jobDeleted = true;
                document.getElementById('log-deleted-banner')?.classList.remove('hidden');
                logClearBtn.disabled = true;
                return;
            }
            
            state.runs = runsData.runs || [];
            
            if (state.runs.length > 0) {
                const selector = createRunSelector(state.runs, jobId);
                if (selector) {
                    const logHeader = logModalSubtitle.parentElement;
                    logHeader.insertAdjacentElement('afterend', selector);
                }
                state.currentRunId = state.runs[0].runId;
                await fetchLogsForRun(jobId, state.currentRunId);
            } else {
                await fetchLogs(jobId);
            }
        }
    } catch (err) {
        console.error('Failed to fetch runs:', err);
        await fetchLogs(jobId);
    }
    
    state.logPollingInterval = setInterval(() => {
        if (state.currentRunId) {
            fetchLogsForRun(jobId, state.currentRunId);
        } else {
            fetchLogs(jobId);
        }
    }, 2000);
}

function closeLogModal() {
    if (state.logPollingInterval) {
        clearInterval(state.logPollingInterval);
        state.logPollingInterval = null;
    }
    
    logModal.classList.add('hidden');
    
    document.getElementById('log-deleted-banner')?.classList.add('hidden');
    document.getElementById('log-error-banner')?.classList.add('hidden');
    document.getElementById('log-complete-banner')?.classList.add('hidden');
    
    const existingSelector = document.getElementById('run-selector');
    if (existingSelector) existingSelector.remove();
    
    state.currentLogJobId = null;
    state.currentLogJobName = null;
    state.logs = [];
    state.runs = [];
    state.currentRunId = null;
    state.retryCount = 0;
    state.jobDeleted = false;
    state.syncCompleted = false;
    state.lastLogCount = 0;
    state.stablePolls = 0;
}

async function fetchLogs(jobId) {
    if (state.jobDeleted) return;
    
    try {
        const res = await fetch(`/api/logs/${jobId}`, {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        
        const data = await res.json();
        
        if (state.retryCount > 0) {
            state.retryCount = 0;
            document.getElementById('log-error-banner')?.classList.add('hidden');
        }
        
        if (!data.exists) {
            state.jobDeleted = true;
            
            if (state.logPollingInterval) {
                clearInterval(state.logPollingInterval);
                state.logPollingInterval = null;
            }
            
            document.getElementById('log-deleted-banner')?.classList.remove('hidden');
            logClearBtn.disabled = true;
            
            fetchJobs();
            return;
        }
        
        const newLogCount = data.logs?.length || 0;
        if (newLogCount === state.lastLogCount && newLogCount > 0) {
            state.stablePolls++;
            if (state.stablePolls >= 3 && !state.syncCompleted) {
                state.syncCompleted = true;
                document.getElementById('log-complete-banner')?.classList.remove('hidden');
                
                if (state.logPollingInterval) {
                    clearInterval(state.logPollingInterval);
                    state.logPollingInterval = null;
                }
            }
        } else {
            state.stablePolls = 0;
            state.syncCompleted = false;
            document.getElementById('log-complete-banner')?.classList.add('hidden');
        }
        state.lastLogCount = newLogCount;
        
        const pendingFrontendErrors = state.logs.filter(log => log.runId === 'frontend-error');
        const backendLogs = data.logs || [];
        
        const mergedLogs = [...backendLogs];
        for (const pendingError of pendingFrontendErrors) {
            const alreadyPersisted = mergedLogs.some(
                log => log.timestamp === pendingError.timestamp && log.phase === pendingError.phase
            );
            if (!alreadyPersisted) {
                mergedLogs.push(pendingError);
            }
        }
        
        if (mergedLogs.length > 0) {
            state.logs = state.logs.concat(mergedLogs);
            renderLogs();
            updateLogStats();
        }
        
    } catch (err) {
        console.error('Failed to fetch logs:', err);
        
        document.getElementById('log-error-banner')?.classList.remove('hidden');
        
        state.retryCount++;
        
        if (state.retryCount >= 10) {
            if (state.logPollingInterval) {
                clearInterval(state.logPollingInterval);
                state.logPollingInterval = null;
            }
            
            const banner = document.getElementById('log-error-banner');
            if (banner) {
                banner.innerHTML = '❌ CONNECTION LOST - Please close and reopen the modal';
            }
        } else {
            const backoffDelay = Math.min(2000 * Math.pow(2, state.retryCount), 30000);
            
            if (state.logPollingInterval) {
                clearInterval(state.logPollingInterval);
                state.logPollingInterval = setInterval(() => fetchLogs(jobId), backoffDelay);
            }
        }
    }
}

function renderLogs() {
    const filteredLogs = state.logs.filter(log => state.activeFilters.includes(log.level));
    
    if (filteredLogs.length === 0) {
        logEntries.innerHTML = '<div class="log-empty-state"><span class="mono">AWAITING DATA STREAM...</span></div>';
        return;
    }
    
    const sortedLogs = [...filteredLogs].sort((a, b) => 
        new Date(a.timestamp) - new Date(b.timestamp)
    );
    
    const wasAtBottom = logEntries.scrollHeight - logEntries.clientHeight <= logEntries.scrollTop + 50;
    
    logEntries.innerHTML = sortedLogs.slice(-500).map(log => {
        const time = new Date(log.timestamp).toLocaleTimeString('en-US', { 
            hour12: false, 
            hour: '2-digit', 
            minute: '2-digit', 
            second: '2-digit' 
        });
        return `
            <div class="log-entry log-${log.level}">
                <span class="log-entry-time">${time}</span>
                <span class="log-entry-level">${log.level}</span>
                <span class="log-entry-phase">[${log.phase}]</span>
                <span class="log-entry-message">${log.message}${log.metadata ? ' ' + JSON.stringify(log.metadata) : ''}</span>
            </div>
        `;
    }).join('');
    
    if (state.isAutoScrollEnabled && wasAtBottom) {
        logEntries.scrollTop = logEntries.scrollHeight;
    }
}

function updateLogStats() {
    const counts = {
        total: state.logs.length,
        info: state.logs.filter(l => l.level === 'INFO').length,
        success: state.logs.filter(l => l.level === 'SUCCESS').length,
        warning: state.logs.filter(l => l.level === 'WARNING').length,
        error: state.logs.filter(l => l.level === 'ERROR').length,
    };
    
    const totalEl = document.getElementById('log-stat-total');
    const infoEl = document.getElementById('log-stat-info');
    const successEl = document.getElementById('log-stat-success');
    const warningEl = document.getElementById('log-stat-warning');
    const errorEl = document.getElementById('log-stat-error');

    if (totalEl) totalEl.innerText = counts.total;
    if (infoEl) infoEl.innerText = counts.info;
    if (successEl) successEl.innerText = counts.success;
    if (warningEl) warningEl.innerText = counts.warning;
    if (errorEl) errorEl.innerText = counts.error;
}

function toggleLogFilter(level) {
    const index = state.activeFilters.indexOf(level);
    if (index > -1) {
        state.activeFilters.splice(index, 1);
    } else {
        state.activeFilters.push(level);
    }
    
    logFilters.forEach(btn => {
        if (btn.dataset.level === level) {
            btn.classList.toggle('active', state.activeFilters.includes(level));
        }
    });
    
    renderLogs();
}

async function clearLogs() {
    if (!state.currentLogJobId) return;
    if (!confirm('CLEAR ALL LOG ENTRIES FOR THIS JOB?')) return;
    
    try {
        const res = await fetch(`/api/logs/${state.currentLogJobId}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        
        if (res.ok) {
            state.logs = [];
            renderLogs();
            updateLogStats();
        }
    } catch (err) {
        console.error('Failed to clear logs:', err);
    }
}

function downloadLogs() {
    if (state.logs.length === 0) return;
    
    const content = state.logs.map(log => {
        const time = new Date(log.timestamp).toISOString();
        return `[${time}] [${log.level}] [${log.phase}] ${log.message}`;
    }).join('\n');
    
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${state.currentLogJobName || 'logs'}-${new Date().toISOString().split('T')[0]}.log`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

loginBtn.addEventListener('click', login);
apiKeyInput.addEventListener('keypress', (e) => e.key === 'Enter' && login());
logoutBtn.addEventListener('click', logout);
runAllBtn.addEventListener('click', runAllSyncs);

createBqBtn.addEventListener('click', () => {
    jobTypeInput.value = 'bq-to-supabase';
    bqSection.classList.add('active');
    sheetsSection.classList.remove('active');
    modalTitle.innerText = 'Configure New BigQuery Sync';
    jobForm.reset();
    document.getElementById('job-id').value = '';
    document.getElementById('incremental-warning').classList.remove('hidden');
    jobModal.classList.remove('hidden');
});

createSheetsBtn.addEventListener('click', () => {
    jobTypeInput.value = 'sheets-to-bq';
    bqSection.classList.remove('active');
    sheetsSection.classList.add('active');
    modalTitle.innerText = 'Configure New Sheets Sync';
    jobForm.reset();
    document.getElementById('job-id').value = '';
    jobModal.classList.remove('hidden');
});

cancelModal.addEventListener('click', () => jobModal.classList.add('hidden'));
jobForm.addEventListener('submit', saveJob);
testSheetBtn.addEventListener('click', testSheetConnection);
window.addEventListener('click', (e) => e.target === jobModal && jobModal.classList.add('hidden'));

document.getElementById('bq-column').addEventListener('input', function() {
    const warning = document.getElementById('incremental-warning');
    if (this.value.trim() === '') {
        warning.classList.remove('hidden');
    } else {
        warning.classList.add('hidden');
    }
});

logModalClose.addEventListener('click', closeLogModal);
logClearBtn.addEventListener('click', clearLogs);
logDownloadBtn.addEventListener('click', downloadLogs);

logFilters.forEach(btn => {
    btn.addEventListener('click', () => toggleLogFilter(btn.dataset.level));
});

window.addEventListener('click', (e) => e.target === logModal && closeLogModal());

logEntries.addEventListener('scroll', () => {
    const isAtBottom = logEntries.scrollHeight - logEntries.clientHeight <= logEntries.scrollTop + 50;
    state.isAutoScrollEnabled = isAtBottom;
});

const CRON_PRESETS = [
    { id: 'default', name: 'Every 6 Hours', expression: '0 */6 * * *', description: 'Runs at minute 0 of every 6th hour' },
    { id: 'hourly', name: 'Hourly', expression: '0 * * * *', description: 'Runs at the start of every hour' },
    { id: 'daily', name: 'Daily (Midnight)', expression: '0 0 * * *', description: 'Runs every day at midnight UTC' },
    { id: 'daily-morning', name: 'Daily (8 AM)', expression: '0 8 * * *', description: 'Runs every day at 8:00 AM UTC' },
    { id: 'twice-daily', name: 'Twice Daily', expression: '0 0,12 * * *', description: 'Runs at midnight and noon UTC' },
    { id: 'weekly', name: 'Weekly (Monday)', expression: '0 0 * * 1', description: 'Runs every Monday at midnight UTC' },
    { id: '30min', name: 'Every 30 Minutes', expression: '*/30 * * * *', description: 'Runs every 30 minutes' },
    { id: '15min', name: 'Every 15 Minutes', expression: '*/15 * * * *', description: 'Runs every 15 minutes' },
];

function renderCronPresets() {
    const grid = document.getElementById('preset-grid');
    if (!grid) return;

    grid.innerHTML = CRON_PRESETS.map(preset => `
        <div class="preset-card ${state.selectedCronExpression === preset.expression ? 'selected' : ''}" 
             data-expression="${preset.expression}"
             onclick="selectCronPreset('${preset.expression}')">
            <div class="preset-name">${preset.name}</div>
            <div class="preset-expression">${preset.expression}</div>
            <div class="preset-description">${preset.description}</div>
        </div>
    `).join('');
}

function selectCronPreset(expression) {
    state.selectedCronExpression = expression;
    document.getElementById('custom-cron-input').value = expression;
    renderCronPresets();
    validateCronExpression();
}

async function validateCronExpression() {
    const input = document.getElementById('custom-cron-input');
    const resultEl = document.getElementById('cron-validation-result');
    const expression = input.value.trim();

    if (!expression) {
        resultEl.textContent = '';
        resultEl.className = 'mono';
        return;
    }

    try {
        const res = await fetch('/api/cron/validate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${state.apiKey}`
            },
            body: JSON.stringify({ expression })
        });

        const data = await res.json();

        if (data.valid) {
            resultEl.textContent = `✓ ${data.description}`;
            resultEl.className = 'mono cron-validation-success';
        } else {
            resultEl.textContent = '✗ Invalid cron expression';
            resultEl.className = 'mono cron-validation-error';
        }
    } catch (err) {
        resultEl.textContent = '⚠ Validation failed';
        resultEl.className = 'mono cron-validation-error';
    }
}

function renderCronJobAssignments() {
    const container = document.getElementById('cron-job-list');
    if (!container) return;

    if (state.jobs.length === 0) {
        container.innerHTML = '<p class="mono" style="color: var(--text-secondary); text-align: center; padding: 20px;">No jobs configured</p>';
        return;
    }

    container.innerHTML = state.jobs.map(job => {
        const currentSchedule = job.cronSchedule || '0 */6 * * *';
        const preset = CRON_PRESETS.find(p => p.expression === currentSchedule);
        const displayText = preset ? preset.name : currentSchedule;

        return `
            <div class="cron-job-item">
                <div class="job-info">
                    <div class="job-name">${job.name}</div>
                    <div class="job-type">${(job.type || 'bq-to-supabase').replace(/-/g, ' ')}</div>
                </div>
                <div class="job-schedule">
                    <span class="schedule-display">${displayText}</span>
                    <select onchange="updateJobCronSchedule('${job.id}', this.value)">
                        ${CRON_PRESETS.map(p => `
                            <option value="${p.expression}" ${currentSchedule === p.expression ? 'selected' : ''}>
                                ${p.name}
                            </option>
                        `).join('')}
                        <option value="custom" ${!CRON_PRESETS.some(p => p.expression === currentSchedule) ? 'selected' : ''}>
                            Custom
                        </option>
                    </select>
                </div>
            </div>
        `;
    }).join('');
}

async function updateJobCronSchedule(jobId, expression) {
    const job = state.jobs.find(j => j.id === jobId);
    if (!job) return;

    if (expression === 'custom') {
        const customExpression = prompt('Enter custom cron expression (e.g., 0 */6 * * *):', job.cronSchedule || '0 */6 * * *');
        if (!customExpression) return;
        expression = customExpression;
    }

    try {
        const res = await fetch(`/api/configs/${jobId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${state.apiKey}`
            },
            body: JSON.stringify({ ...job, cronSchedule: expression })
        });

        if (res.ok) {
            job.cronSchedule = expression;
            renderCronJobAssignments();
            showToast(`Schedule updated for ${job.name}`, 'success');
        } else {
            throw new Error('Failed to update schedule');
        }
    } catch (err) {
        showToast('Failed to update schedule');
    }
}

async function loadCronSchedules() {
    try {
        const res = await fetch('/api/cron/schedules', {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });

        if (res.ok) {
            state.cronSchedules = await res.json();
        }
    } catch (err) {
        console.error('Failed to load cron schedules:', err);
    }
}

const validateCronBtn = document.getElementById('validate-cron-btn');
const customCronInput = document.getElementById('custom-cron-input');

if (validateCronBtn) {
    validateCronBtn.addEventListener('click', validateCronExpression);
}

if (customCronInput) {
    customCronInput.addEventListener('input', () => {
        state.selectedCronExpression = customCronInput.value.trim();
        renderCronPresets();
    });
    customCronInput.addEventListener('blur', validateCronExpression);
}

const jobCronSchedule = document.getElementById('job-cron-schedule');
const jobCronCustom = document.getElementById('job-cron-custom');

if (jobCronSchedule) {
    jobCronSchedule.addEventListener('change', () => {
        if (jobCronSchedule.value === 'custom') {
            jobCronCustom.classList.remove('hidden');
            jobCronCustom.focus();
        } else {
            jobCronCustom.classList.add('hidden');
        }
    });
}

const originalShowDashboard = showDashboard;
showDashboard = async function() {
    await originalShowDashboard();
    await loadCronSchedules();
    renderCronPresets();
    renderCronJobAssignments();
};

const originalRenderJobs = renderJobs;
renderJobs = function() {
    originalRenderJobs();
    renderCronJobAssignments();
};
