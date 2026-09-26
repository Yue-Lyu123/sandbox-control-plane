import type { Pool } from "pg";
import { getPool } from "./db.js";
import { schemaEnsurer } from "./ensure-once.js";

export const SANDBOX_ACTIVITY_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS sandbox_activity (
    pod_name         text PRIMARY KEY,
    tenant_id        text,
    session_id       text,
    last_activity_at timestamptz NOT NULL
);
-- 声明式保活 (SAR msg303/305, 2026-07-30)。**故意是独立列，不是往
-- last_activity_at 里写一个未来时间戳**：那等于伪造一次从未发生的活动，把
-- 事实降级成猜测——正是 SAR 那句「声明是事实，活跃度是猜」反对的东西。
-- 分开存之后，「这个 pod 为什么没被收」在数据里是可回答的：是真的有人在用，
-- 还是有人声明了在等人批。
ALTER TABLE sandbox_activity ADD COLUMN IF NOT EXISTS held_until  timestamptz;
ALTER TABLE sandbox_activity ADD COLUMN IF NOT EXISTS hold_reason text;
`;

const sandboxActivitySchema = schemaEnsurer((pool: Pool) => pool.query(SANDBOX_ACTIVITY_SCHEMA_SQL));

export function ensureSandboxActivitySchema(pool: Pool): Promise<void> {
  return sandboxActivitySchema.ensure(pool);
}

export async function recordSandboxActivity(
  podName: string,
  tenantId: string,
  sessionId: string,
  at: Date = new Date(),
): Promise<void> {
  const pool = getPool();
  await ensureSandboxActivitySchema(pool);
  await pool.query(
    `INSERT INTO sandbox_activity (pod_name, tenant_id, session_id, last_activity_at)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (pod_name) DO UPDATE
       SET tenant_id        = EXCLUDED.tenant_id,
           session_id       = EXCLUDED.session_id,
           last_activity_at = GREATEST(sandbox_activity.last_activity_at, EXCLUDED.last_activity_at)`,
    [podName, tenantId, sessionId, at],
  );
}

export async function lastActivityFor(podNames: string[]): Promise<Map<string, Date>> {
  if (podNames.length === 0) return new Map();
  const pool = getPool();
  await ensureSandboxActivitySchema(pool);
  const { rows } = await pool.query<{ pod_name: string; last_activity_at: Date }>(
    "SELECT pod_name, last_activity_at FROM sandbox_activity WHERE pod_name = ANY($1)",
    [podNames],
  );
  return new Map(rows.map((r) => [r.pod_name, r.last_activity_at]));
}

export const SANDBOX_HOLD_MAX_MS = 2 * 60 * 60 * 1000;

export const SANDBOX_HOLD_DEFAULT_MS = 30 * 60 * 1000;

export interface SandboxHold {
  until: Date;
  reason: string | null;
}

export async function recordSandboxHold(
  podName: string,
  tenantId: string,
  sessionId: string,
  ttlMs: number = SANDBOX_HOLD_DEFAULT_MS,
  reason: string | null = null,
  now: Date = new Date(),
): Promise<Date> {
  const clamped = Math.max(0, Math.min(ttlMs, SANDBOX_HOLD_MAX_MS));
  const until = new Date(now.getTime() + clamped);
  const pool = getPool();
  await ensureSandboxActivitySchema(pool);
  await pool.query(
    `INSERT INTO sandbox_activity (pod_name, tenant_id, session_id, last_activity_at, held_until, hold_reason)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (pod_name) DO UPDATE
       SET tenant_id   = EXCLUDED.tenant_id,
           session_id  = EXCLUDED.session_id,
           held_until  = GREATEST(COALESCE(sandbox_activity.held_until, EXCLUDED.held_until), EXCLUDED.held_until),
           hold_reason = EXCLUDED.hold_reason`,

    [podName, tenantId, sessionId, now, until, reason],
  );
  return until;
}

export async function heldUntilFor(podNames: string[], now: Date = new Date()): Promise<Map<string, SandboxHold>> {
  if (podNames.length === 0) return new Map();
  const pool = getPool();
  await ensureSandboxActivitySchema(pool);
  const { rows } = await pool.query<{ pod_name: string; held_until: Date; hold_reason: string | null }>(
    "SELECT pod_name, held_until, hold_reason FROM sandbox_activity WHERE pod_name = ANY($1) AND held_until IS NOT NULL AND held_until > $2",
    [podNames, now],
  );
  return new Map(rows.map((r) => [r.pod_name, { until: r.held_until, reason: r.hold_reason }]));
}

export async function deleteSandboxActivity(podName: string): Promise<void> {
  const pool = getPool();
  await ensureSandboxActivitySchema(pool);
  await pool.query("DELETE FROM sandbox_activity WHERE pod_name = $1", [podName]);
}
