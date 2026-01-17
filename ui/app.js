const state = {
    apiKey: localStorage.getItem('sync_key') || '',
    jobs: [],
    logs: [],
    jobLogs: {},  // { [jobId]: LogEntry[] }
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
};

// --- DOM Elements ---
const loginView = document.getElementById('login-view');
const dashboardView = document.getElementById('dashboard-view');
const apiKeyInput = document.getElementById('api-key-input');
const loginBtn = document.getElementById('login-btn');
const loginError = document.getElementById('login-error');
const jobsContainer = document.getElementById('jobs-container');
const logoutBtn = document.getElementById('logout-btn');
const createBtn = document.getElementById('create-btn');

const jobModal = document.getElementById('job-modal');
const jobForm = document.getElementById('job-form');
const cancelModal = document.getElementById('cancel-modal');
const modalTitle = document.getElementById('modal-title');

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

// --- Initialization ---
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

// --- Auth Functions ---
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

// --- Job Functions ---
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

        // Fetch logs for each job
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
    
    // Sort by timestamp descending (newest first)
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

function renderJobs() {
    if (state.jobs.length === 0) {
        jobsContainer.innerHTML = '<p class="mono" style="color: var(--text-secondary); text-align: center; padding: 40px;">NO SYNC JOBS CONFIGURED. CLICK ABOVE TO START.</p>';
        return;
    }

    document.getElementById('stat-active').innerText = state.jobs.length;

    jobsContainer.innerHTML = state.jobs.map(job => `
        <div class="job-card stagger-in">
            <div class="job-info">
                <h3>${job.name}</h3>
                <div class="id">${job.id}</div>
            </div>
            <div class="job-path">
                <span class="mono">${job.bigquery.tableOrView}</span>
                <span class="arrow">→</span>
                <span class="mono">${job.supabase.tableName}</span>
            </div>
            <div class="job-status">
                <div class="status-badge ${job.lastStatus || ''}">
                    ${job.lastStatus ? job.lastStatus.toUpperCase() : 'PENDING'}
                </div>
                <div class="mono" style="font-size: 0.65rem; color: var(--text-secondary); margin-top: 4px;">
                    ${job.lastRun ? new Date(job.lastRun).toLocaleString() : 'NEVER RUN'}
                </div>
            </div>
            <div class="job-recent-logs">
                <div class="job-recent-logs-title">
                    RECENT LOGS
                    ${hasRecentErrors(job.id) ? '<span class="job-error-badge">ERRORS</span>' : ''}
                </div>
                ${renderMiniLogs(job.id)}
            </div>
            <div class="job-actions">
                <button class="btn btn-ghost" onclick="syncJob('${job.id}')" id="sync-${job.id}">RUN</button>
                <button class="btn btn-ghost" onclick="editJob('${job.id}')">EDIT</button>
                <button class="btn btn-danger" onclick="deleteJob('${job.id}')">DELETE</button>
            </div>
        </div>
    `).join('');
}

async function saveJob(e) {
    e.preventDefault();
    const id = document.getElementById('job-id').value;
    const isNew = !id;

    const job = {
        id: id || undefined,
        name: document.getElementById('job-name').value,
        enabled: document.getElementById('job-enabled').checked,
        bigquery: {
            projectId: document.getElementById('bq-project').value,
            datasetId: document.getElementById('bq-dataset').value,
            tableOrView: document.getElementById('bq-table').value,
            incrementalColumn: document.getElementById('bq-column').value || undefined,
        },
        supabase: {
            tableName: document.getElementById('sb-table').value,
            upsertColumns: document.getElementById('sb-columns').value.split(',').map(s => s.trim()),
        }
    };

    try {
        const method = isNew ? 'POST' : 'PUT';
        const url = isNew ? '/api/configs' : `/api/configs/${id}`;

        await fetch(url, {
            method,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${state.apiKey}`
            },
            body: JSON.stringify(job)
        });

        jobModal.classList.add('hidden');
        fetchJobs();
    } catch (err) {
        showToast('Failed to save job configuration');
    }
}

async function syncJob(id) {
    const btn = document.getElementById(`sync-${id}`);
    const job = state.jobs.find(j => j.id === id);
    const originalText = btn.innerText;
    btn.innerText = 'SYNCING...';
    btn.disabled = true;

    openLogModal(id, job?.name || id);

    try {
        const res = await fetch(`/api/sync/${id}`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        if (res.ok) {
            fetchJobs();
        } else {
            const data = await res.json();
            
            state.logs.push({
                timestamp: new Date().toISOString(),
                level: 'ERROR',
                jobId: id,
                jobName: job?.name || id,
                runId: 'frontend-error',
                phase: 'SYNC_ERROR',
                message: data.error || 'Sync failed'
            });
            renderLogs();
            
            showToast(`Sync Failed: ${data.error}`);
            fetchJobs();
        }
    } catch (err) {
        state.logs.push({
            timestamp: new Date().toISOString(),
            level: 'ERROR',
            jobId: id,
            jobName: job?.name || id,
            runId: 'frontend-error',
            phase: 'NETWORK_ERROR',
            message: 'Network error during sync request'
        });
        renderLogs();
        
        showToast('Network Error during sync');
    } finally {
        btn.innerText = originalText;
        btn.disabled = false;
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

    modalTitle.innerText = 'Update Sync Job';
    document.getElementById('job-id').value = job.id;
    document.getElementById('job-name').value = job.name;
    document.getElementById('job-enabled').checked = job.enabled;
    document.getElementById('bq-project').value = job.bigquery.projectId;
    document.getElementById('bq-dataset').value = job.bigquery.datasetId;
    document.getElementById('bq-table').value = job.bigquery.tableOrView;
    document.getElementById('bq-column').value = job.bigquery.incrementalColumn || '';
    document.getElementById('sb-table').value = job.supabase.tableName;
    document.getElementById('sb-columns').value = job.supabase.upsertColumns.join(', ');

    jobModal.classList.remove('hidden');
}

async function openLogModal(jobId, jobName) {
    state.currentLogJobId = jobId;
    state.currentLogJobName = jobName;
    state.logs = [];
    state.isAutoScrollEnabled = true;
    state.retryCount = 0;
    state.jobDeleted = false;
    state.syncCompleted = false;
    state.lastLogCount = 0;
    state.stablePolls = 0;
    
    document.getElementById('log-deleted-banner')?.classList.add('hidden');
    document.getElementById('log-error-banner')?.classList.add('hidden');
    document.getElementById('log-complete-banner')?.classList.add('hidden');
    
    logClearBtn.disabled = false;
    
    logModalTitle.innerText = `SYSTEM LOGS`;
    logModalSubtitle.innerText = jobName;
    
    logEntries.innerHTML = '<div class="log-loading"></div>';
    logModal.classList.remove('hidden');
    
    await fetchLogs(jobId);
    
    state.logPollingInterval = setInterval(() => fetchLogs(jobId), 2000);
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
    
    state.currentLogJobId = null;
    state.currentLogJobName = null;
    state.logs = [];
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
        
        state.logs = mergedLogs;
        renderLogs();
        updateLogStats();
        
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
                <span class="log-entry-message">${log.message}</span>
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

// --- Event Listeners ---
loginBtn.addEventListener('click', login);
apiKeyInput.addEventListener('keypress', (e) => e.key === 'Enter' && login());
logoutBtn.addEventListener('click', logout);
createBtn.addEventListener('click', () => {
    modalTitle.innerText = 'Configure New Sync Job';
    jobForm.reset();
    document.getElementById('job-id').value = '';
    jobModal.classList.remove('hidden');
});
cancelModal.addEventListener('click', () => jobModal.classList.add('hidden'));
jobForm.addEventListener('submit', saveJob);
window.addEventListener('click', (e) => e.target === jobModal && jobModal.classList.add('hidden'));

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
