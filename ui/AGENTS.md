# Dashboard UI - AI Agent Ruleset

## Skills Reference

<!-- SKILLS_TABLE_START -->
| Skill | Description | Link |
|-------|-------------|------|
<!-- SKILLS_TABLE_END -->

### Auto-invoke Skills

<!-- AUTO_INVOKE_START -->
| Action | Skill |
|--------|-------|
<!-- AUTO_INVOKE_END -->

---

## Critical Rules

### ALWAYS
- Use vanilla JavaScript (no frameworks)
- Include Bearer token in API requests: `Authorization: Bearer ${token}`
- Handle loading/error states for all API calls
- Store auth token in localStorage

### NEVER
- Use React, Vue, or other frameworks
- Make API calls without Authorization header (except /api/auth)
- Leave unhandled promise rejections

---

## Patterns

### API Request with Auth
```javascript
async function fetchWithAuth(url, options = {}) {
    const res = await fetch(url, {
        ...options,
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${state.apiKey}`,
            ...options.headers
        }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
}
```

### DOM Element Selection
```javascript
const element = document.getElementById('element-id');
```

### Event Handling
```javascript
button.addEventListener('click', async () => {
    button.disabled = true;
    try {
        await doAction();
    } finally {
        button.disabled = false;
    }
});
```

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Vanilla JavaScript | UI logic |
| CSS | Styling |
| localStorage | Token persistence |
