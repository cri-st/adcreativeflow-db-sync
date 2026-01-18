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
  startedAt: string;
  status: 'running' | 'success' | 'error';
  endedAt?: string;
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
    try { JSON.stringify(sanitized); } catch { return { error: '[Circular reference detected]' }; }
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

  private log(level: LogLevel, phase: string, message: string, metadata?: Record<string, unknown>): void {
    const entry = this.createEntry(level, phase, message, this.sanitize(metadata));
    const emoji = this.getLevelEmoji(entry.level);
    console.log(`${emoji} [${entry.timestamp}] [${this.jobName}] [${phase}] ${message}`);
    if (this.logs.length < 500) {
      this.logs.push(entry);
    }
  }

  info(phase: string, message: string, metadata?: Record<string, unknown>): void { this.log('INFO', phase, message, metadata); }
  success(phase: string, message: string, metadata?: Record<string, unknown>): void { this.log('SUCCESS', phase, message, metadata); }
  warning(phase: string, message: string, metadata?: Record<string, unknown>): void { this.log('WARNING', phase, message, metadata); }
  error(phase: string, message: string, metadata?: Record<string, unknown>): void { this.log('ERROR', phase, message, metadata); }
  debug(phase: string, message: string, metadata?: Record<string, unknown>): void { this.log('DEBUG', phase, message, metadata); }
  getLogs(): LogEntry[] { return [...this.logs]; }

  async startRun(kv: KVNamespace): Promise<void> {
    this.kv = kv;
    await kv.put(`logs:${this.jobId}:latest`, JSON.stringify({ runId: this.runId, timestamp: new Date().toISOString() }), { expirationTtl: 86400 });
    const indexKey = `jobRuns:${this.jobId}`;
    const existingRuns = await kv.get<RunInfo[]>(indexKey, 'json') || [];
    existingRuns.unshift({ runId: this.runId, startedAt: new Date().toISOString(), status: 'running' });
    await kv.put(indexKey, JSON.stringify(existingRuns.slice(0, 50)), { expirationTtl: 2592000 });
  }

  async endRun(kv: KVNamespace, status: 'success' | 'error'): Promise<void> {
    if (this.logs.length > 0) {
      await kv.put(`logs:${this.jobId}:${this.runId}`, JSON.stringify(this.logs), { expirationTtl: 86400 });
    }
    const indexKey = `jobRuns:${this.jobId}`;
    const runs = await kv.get<RunInfo[]>(indexKey, 'json') || [];
    const runIndex = runs.findIndex(r => r.runId === this.runId);
    if (runIndex >= 0) {
      runs[runIndex].status = status;
      runs[runIndex].endedAt = new Date().toISOString();
      await kv.put(indexKey, JSON.stringify(runs), { expirationTtl: 2592000 });
    }
  }

  static async getRecentLogs(kv: KVNamespace, jobId: string, runId?: string, limit: number = 500): Promise<LogEntry[]> {
    if (!runId) {
      const latest = await kv.get<{ runId: string }>(`logs:${jobId}:latest`, 'json');
      if (!latest) return [];
      runId = latest.runId;
    }
    const logs = await kv.get<LogEntry[]>(`logs:${jobId}:${runId}`, 'json') || [];
    return logs.sort((a, b) => b.timestamp.localeCompare(a.timestamp)).slice(0, limit);
  }

  static async getJobRuns(kv: KVNamespace, jobId: string): Promise<RunInfo[]> {
    return await kv.get<RunInfo[]>(`jobRuns:${jobId}`, 'json') || [];
  }

  static async clearLogs(kv: KVNamespace, jobId: string, runId?: string): Promise<number> {
    let deleted = 0;
    if (runId) {
      await kv.delete(`logs:${jobId}:${runId}`);
      deleted = 1;
      const indexKey = `jobRuns:${jobId}`;
      const runs = await kv.get<RunInfo[]>(indexKey, 'json') || [];
      const updated = runs.filter(r => r.runId !== runId);
      if (updated.length > 0) {
        await kv.put(indexKey, JSON.stringify(updated), { expirationTtl: 2592000 });
      } else {
        await kv.delete(indexKey);
      }
      const latest = await kv.get<{ runId: string }>(`logs:${jobId}:latest`, 'json');
      if (latest?.runId === runId) {
        if (updated.length > 0) {
          await kv.put(`logs:${jobId}:latest`, JSON.stringify({ runId: updated[0].runId, timestamp: updated[0].startedAt }), { expirationTtl: 86400 });
        } else {
          await kv.delete(`logs:${jobId}:latest`);
        }
      }
    } else {
      const runs = await kv.get<RunInfo[]>(`jobRuns:${jobId}`, 'json') || [];
      for (const run of runs) { await kv.delete(`logs:${jobId}:${run.runId}`); deleted++; }
      await kv.delete(`logs:${jobId}:latest`);
      await kv.delete(`jobRuns:${jobId}`);
    }
    return deleted;
  }
}
