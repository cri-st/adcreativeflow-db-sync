export type LogLevel = 'INFO' | 'SUCCESS' | 'WARNING' | 'ERROR' | 'DEBUG';

export interface LogEntry {
  timestamp: string;
  level: LogLevel;
  jobId: string;
  jobName: string;
  runId: string;
  phase: string;
  message: string;
  metadata?: Record<string, unknown>;
}

interface RunInfo {
  runId: string;
  timestamp: string;
  status: 'running' | 'success' | 'error';
}

export function truncateSql(sql: string, maxLen: number = 1000): string {
  if (sql.length <= maxLen) return sql;
  return sql.substring(0, maxLen) + '...(truncated)';
}

export class Logger {
  private readonly jobId: string;
  private readonly jobName: string;
  private readonly runId: string;
  private readonly logs: LogEntry[] = [];
  private kv: KVNamespace | null = null;
  private logCount: number = 0;

  constructor(jobId: string, jobName: string, runId: string) {
    this.jobId = jobId;
    this.jobName = jobName;
    this.runId = runId;
  }

  private getLevelEmoji(level: LogLevel): string {
    const emojis: Record<LogLevel, string> = {
      INFO: 'ℹ️',
      SUCCESS: '✅',
      WARNING: '⚠️',
      ERROR: '❌',
      DEBUG: '🔍'
    };
    return emojis[level];
  }

  private sanitize(metadata?: Record<string, unknown>): Record<string, unknown> | undefined {
    if (!metadata) return undefined;

    const sensitiveKeys = ['key', 'token', 'password', 'secret', 'credential', 'auth'];
    const sanitized: Record<string, unknown> = {};

    for (const [k, v] of Object.entries(metadata)) {
      if (sensitiveKeys.some(s => k.toLowerCase().includes(s))) {
        sanitized[k] = '***REDACTED***';
      } else if (typeof v === 'string' && v.length > 1000) {
        sanitized[k] = v.substring(0, 1000) + '...(truncated)';
      } else {
        sanitized[k] = v;
      }
    }

    try {
      JSON.stringify(sanitized);
    } catch {
      return { error: '[Circular reference detected]' };
    }

    return sanitized;
  }

  private createEntry(level: LogLevel, phase: string, message: string, metadata?: Record<string, unknown>): LogEntry {
    return {
      timestamp: new Date().toISOString(),
      level,
      jobId: this.jobId,
      jobName: this.jobName,
      runId: this.runId,
      phase,
      message,
      metadata
    };
  }

  private writeLog(entry: LogEntry): void {
    if (!this.kv) {
      console.error('Logger: KV not initialized. Call startRun() first.');
      return;
    }

    const paddedTs = Date.now().toString().padStart(16, '0');
    const key = `logs:${entry.jobId}:${entry.runId}:${paddedTs}`;

    // Fire and forget - DO NOT await
    void this.kv.put(key, JSON.stringify(entry), { expirationTtl: 86400 })
      .catch(err => console.error('Log write failed:', err));

    const emoji = this.getLevelEmoji(entry.level);
    console.log(`${emoji} [${entry.timestamp}] [${entry.jobName}] [${entry.phase}] ${entry.message}`);

    this.logs.push(entry);
  }

  private log(level: LogLevel, phase: string, message: string, metadata?: Record<string, unknown>): void {
    const entry = this.createEntry(level, phase, message, this.sanitize(metadata));

    if (this.logCount >= 500) {
      console.warn('Log limit reached (500), subsequent logs in-memory only');
      const emoji = this.getLevelEmoji(entry.level);
      console.log(`${emoji} [${entry.timestamp}] [${this.jobName}] [${phase}] ${message}`);
      this.logs.push(entry);
      return;
    }

    this.writeLog(entry);
    this.logCount++;
  }

  info(phase: string, message: string, metadata?: Record<string, unknown>): void {
    this.log('INFO', phase, message, metadata);
  }

  success(phase: string, message: string, metadata?: Record<string, unknown>): void {
    this.log('SUCCESS', phase, message, metadata);
  }

  warning(phase: string, message: string, metadata?: Record<string, unknown>): void {
    this.log('WARNING', phase, message, metadata);
  }

  error(phase: string, message: string, metadata?: Record<string, unknown>): void {
    this.log('ERROR', phase, message, metadata);
  }

  /**
   * Awaitable error logging for critical errors that MUST be persisted before continuing.
   * Use this in catch blocks where you need to ensure the error is written to KV.
   */
  async errorSync(phase: string, message: string, metadata?: Record<string, unknown>): Promise<void> {
    if (!this.kv) {
      console.error('Logger: KV not initialized. Call startRun() first.');
      return;
    }

    const entry = this.createEntry('ERROR', phase, message, this.sanitize(metadata));

    if (this.logCount >= 500) {
      console.warn('Log limit reached (500), subsequent logs in-memory only');
      const emoji = this.getLevelEmoji(entry.level);
      console.log(`${emoji} [${entry.timestamp}] [${this.jobName}] [${phase}] ${message}`);
      this.logs.push(entry);
      return;
    }

    const paddedTs = Date.now().toString().padStart(16, '0');
    const key = `logs:${entry.jobId}:${entry.runId}:${paddedTs}`;

    try {
      await this.kv.put(key, JSON.stringify(entry), { expirationTtl: 86400 });
    } catch (err) {
      console.error('Critical error log write failed:', err);
    }

    const emoji = this.getLevelEmoji(entry.level);
    console.log(`${emoji} [${entry.timestamp}] [${this.jobName}] [${phase}] ${message}`);

    this.logs.push(entry);
    this.logCount++;
  }

  debug(phase: string, message: string, metadata?: Record<string, unknown>): void {
    this.log('DEBUG', phase, message, metadata);
  }

  getLogs(): LogEntry[] {
    return [...this.logs];
  }

  async startRun(kv: KVNamespace): Promise<void> {
    this.kv = kv;

    await kv.put(`logs:${this.jobId}:latest`, JSON.stringify({
      runId: this.runId,
      timestamp: new Date().toISOString()
    }), { expirationTtl: 86400 });

    const runsKey = `logs:${this.jobId}:runs`;
    const existingRuns = await kv.get<RunInfo[]>(runsKey, 'json') || [];
    const newRun: RunInfo = { runId: this.runId, timestamp: new Date().toISOString(), status: 'running' };
    const updatedRuns = [newRun, ...existingRuns].slice(0, 10);
    await kv.put(runsKey, JSON.stringify(updatedRuns), { expirationTtl: 86400 });
  }

  async endRun(kv: KVNamespace, status: 'success' | 'error'): Promise<void> {
    const runsKey = `logs:${this.jobId}:runs`;
    const runs = await kv.get<RunInfo[]>(runsKey, 'json') || [];
    const updated = runs.map(r => r.runId === this.runId ? { ...r, status } : r);
    await kv.put(runsKey, JSON.stringify(updated), { expirationTtl: 86400 });
  }

  static async getRecentLogs(
    kv: KVNamespace,
    jobId: string,
    runId?: string,
    limit: number = 500
  ): Promise<LogEntry[]> {
    if (!runId) {
      const latest = await kv.get<{ runId: string }>(`logs:${jobId}:latest`, 'json');
      if (!latest) return [];
      runId = latest.runId;
    }

    // List all keys with cursor pagination
    const prefix = `logs:${jobId}:${runId}:`;
    const allKeys: string[] = [];
    let cursor: string | undefined;

    do {
      const result = await kv.list({ prefix, cursor });
      allKeys.push(...result.keys.map(k => k.name));
      cursor = result.list_complete ? undefined : result.cursor;
    } while (cursor);

    const entries: LogEntry[] = [];
    for (let i = 0; i < allKeys.length && entries.length < limit; i += 50) {
      const batch = allKeys.slice(i, i + 50);
      const results = await Promise.all(batch.map(k => kv.get<LogEntry>(k, 'json')));
      entries.push(...results.filter((e): e is LogEntry => e !== null));
    }

    return entries
      .sort((a, b) => b.timestamp.localeCompare(a.timestamp))
      .slice(0, limit);
  }

  static async clearLogs(
    kv: KVNamespace,
    jobId: string,
    runId?: string
  ): Promise<number> {
    const prefix = runId ? `logs:${jobId}:${runId}:` : `logs:${jobId}:`;
    let deleted = 0;
    let cursor: string | undefined;

    do {
      const result = await kv.list({ prefix, cursor });
      await Promise.all(result.keys.map(k => kv.delete(k.name)));
      deleted += result.keys.length;
      cursor = result.list_complete ? undefined : result.cursor;
    } while (cursor);

    if (runId) {
      const runsKey = `logs:${jobId}:runs`;
      const runs = await kv.get<RunInfo[]>(runsKey, 'json') || [];
      const updated = runs.filter(r => r.runId !== runId);
      if (updated.length > 0) {
        await kv.put(runsKey, JSON.stringify(updated), { expirationTtl: 86400 });
      } else {
        await kv.delete(runsKey);
      }

      const latest = await kv.get<{ runId: string }>(`logs:${jobId}:latest`, 'json');
      if (latest?.runId === runId) {
        if (updated.length > 0) {
          await kv.put(`logs:${jobId}:latest`, JSON.stringify({
            runId: updated[0].runId,
            timestamp: updated[0].timestamp
          }), { expirationTtl: 86400 });
        } else {
          await kv.delete(`logs:${jobId}:latest`);
        }
      }
    } else {
      await kv.delete(`logs:${jobId}:latest`);
      await kv.delete(`logs:${jobId}:runs`);
    }

    return deleted;
  }
}
