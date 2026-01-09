const state = {
    apiKey: localStorage.getItem('sync_key') || '',
    jobs: [],
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

// --- Initialization ---
if (state.apiKey) {
    showDashboard();
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
async function fetchJobs() {
    try {
        const res = await fetch('/api/configs', {
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        if (res.status === 401) return logout();
        state.jobs = await res.json();
        renderJobs();
    } catch (err) {
        console.error('Failed to fetch jobs', err);
    }
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
        alert('Failed to save job configuration');
    }
}

async function syncJob(id) {
    const btn = document.getElementById(`sync-${id}`);
    const originalText = btn.innerText;
    btn.innerText = 'SYNCING...';
    btn.disabled = true;

    try {
        const res = await fetch(`/api/sync/${id}`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${state.apiKey}` }
        });
        if (res.ok) {
            fetchJobs();
        } else {
            const data = await res.json();
            alert(`Sync Failed: ${data.error}`);
            fetchJobs();
        }
    } catch (err) {
        alert('Network Error during sync');
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
            alert('Failed to delete job');
        }
    } catch (err) {
        alert('Network Error');
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
