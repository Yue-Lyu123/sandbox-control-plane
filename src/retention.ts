import { getPool } from "./db.js";
import { ensureSandboxObserveSchema } from "./observer-store.js";
import { registerIntervalTask, stopIntervalTask } from "./scheduler.js";

/**
 * 观测表保留期清理（从主服务 `retention/observability.ts` 搬来：观测表跟着控制面走，清理也一起走）。
 * `OBSERVABILITY_RETENTION_DAYS` 默认 90 天；`OBSERVABILITY_RETENTION_INTERVAL_S` 默认每 6h 一次；
 * `OBSERVABILITY_RETENTION_ENABLED=0` 关掉。
 */
export interface RetentionReport {
  ranAt: string;
  days: number;
  deleted: Record<string, number>;
  total: number;
  errors: Array<{ table: string; error: string }>;
}

export const OBSERVABILITY_RETENTION_DEFAULT_DAYS = 90;
export const OBSERVABILITY_RETENTION_DEFAULT_INTERVAL_S = 6 * 60 * 60;

export const PURGE_TARGETS = [
  { table: "sandbox_pod_logs", column: "observed_at" },
  { table: "sandbox_pod_events", column: "observed_at" },
] as const;

let lastReport: RetentionReport | null = null;

function positiveNumberFromEnv(name: string): number | null {
  const raw = process.env[name];
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export function retentionDays(): number {
  return positiveNumberFromEnv("OBSERVABILITY_RETENTION_DAYS") ?? OBSERVABILITY_RETENTION_DEFAULT_DAYS;
}

export async function purgeExpiredObservabilityRows(
  days: number = retentionDays(),
): Promise<RetentionReport> {
  const report: RetentionReport = {
    ranAt: new Date().toISOString(),
    days,
    deleted: {},
    total: 0,
    errors: [],
  };

  try {
    const pool = getPool();
    await ensureSandboxObserveSchema(pool);
    for (const target of PURGE_TARGETS) {
      try {

        const { rowCount } = await pool.query(
          `DELETE FROM ${target.table}
           WHERE ${target.column} < now() - ($1::text || ' days')::interval`,
          [String(days)],
        );
        report.deleted[target.table] = rowCount ?? 0;
        report.total += rowCount ?? 0;
      } catch (err) {
        report.deleted[target.table] = 0;
        report.errors.push({
          table: target.table,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  } catch (err) {

    report.errors.push({
      table: "*",
      error: err instanceof Error ? err.message : String(err),
    });
  }

  lastReport = report;

  const summary = PURGE_TARGETS.map((t) => `${t.table}=${report.deleted[t.table] ?? 0}`).join(" ");
  if (report.errors.length > 0) {
    console.error(
      `[retention] pass finished with errors (days=${days}) ${summary}`,
      report.errors,
    );
  } else {
    console.log(`[retention] pass ok (days=${days}) ${summary} total=${report.total}`);
  }
  return report;
}

export function getLastRetentionReport(): RetentionReport | null {
  return lastReport;
}

export function maybeStartObservabilityRetentionInterval(): void {
  if (process.env.OBSERVABILITY_RETENTION_ENABLED === "0") return;
  registerIntervalTask({
    key: "retention",
    intervalEnvVar: "OBSERVABILITY_RETENTION_INTERVAL_S",
    defaultIntervalS: OBSERVABILITY_RETENTION_DEFAULT_INTERVAL_S,
    getTask: () => () => purgeExpiredObservabilityRows(),
    logPrefix: "[retention]",
  });
}

export function _stopObservabilityRetentionIntervalForTests(): void {
  stopIntervalTask("retention");
}
