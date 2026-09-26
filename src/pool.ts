import type { Pool } from "pg";
import { getPool } from "./db.js";
import { schemaEnsurer } from "./ensure-once.js";

export const SANDBOX_POOL_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS sandbox_pool (
    pod_name        text PRIMARY KEY,
    created_at      timestamptz NOT NULL,
    claimed_tenant  text,
    claimed_session text,
    claimed_at      timestamptz
);
-- 同一个 (租户, 会话) 最多认领一个池子 pod。重复 acquire 必须拿到同一个，
-- 而不是各认领一个然后泄漏掉前一个。
CREATE UNIQUE INDEX IF NOT EXISTS sandbox_pool_claim_uniq
    ON sandbox_pool (claimed_tenant, claimed_session)
    WHERE claimed_tenant IS NOT NULL;
`;

const sandboxPoolSchema = schemaEnsurer((pool: Pool) => pool.query(SANDBOX_POOL_SCHEMA_SQL));

export function ensureSandboxPoolSchema(pool: Pool): Promise<void> {
  return sandboxPoolSchema.ensure(pool);
}

export function poolPodName(suffix: string): string {
  return `sbx-pool-${suffix}`;
}

export async function registerPoolPod(podName: string, at: Date = new Date()): Promise<void> {
  const pool = getPool();
  await ensureSandboxPoolSchema(pool);
  await pool.query(
    `INSERT INTO sandbox_pool (pod_name, created_at) VALUES ($1, $2)
     ON CONFLICT (pod_name) DO NOTHING`,
    [podName, at],
  );
}

export async function claimedPodName(tenant: string, sessionId: string): Promise<string | null> {
  const pool = getPool();
  await ensureSandboxPoolSchema(pool);
  const { rows } = await pool.query<{ pod_name: string }>(
    "SELECT pod_name FROM sandbox_pool WHERE claimed_tenant = $1 AND claimed_session = $2",
    [tenant, sessionId],
  );
  return rows[0]?.pod_name ?? null;
}

export async function claimPoolPod(
  tenant: string,
  sessionId: string,
  candidates: string[],
  at: Date = new Date(),
): Promise<string | null> {
  const existing = await claimedPodName(tenant, sessionId);
  if (existing !== null) return existing;
  if (candidates.length === 0) return null;

  const pool = getPool();
  const { rows } = await pool.query<{ pod_name: string }>(
    `UPDATE sandbox_pool
        SET claimed_tenant = $1, claimed_session = $2, claimed_at = $3
      WHERE pod_name = (
            SELECT pod_name FROM sandbox_pool
             WHERE claimed_tenant IS NULL AND pod_name = ANY($4)
             ORDER BY created_at
             FOR UPDATE SKIP LOCKED
             LIMIT 1)
      RETURNING pod_name`,
    [tenant, sessionId, at, candidates],
  );
  return rows[0]?.pod_name ?? null;
}

export async function unclaimedPoolPods(): Promise<string[]> {
  const pool = getPool();
  await ensureSandboxPoolSchema(pool);
  const { rows } = await pool.query<{ pod_name: string }>(
    "SELECT pod_name FROM sandbox_pool WHERE claimed_tenant IS NULL ORDER BY created_at",
  );
  return rows.map((r) => r.pod_name);
}

export async function allPoolPods(): Promise<{ podName: string; createdAt: Date; claimed: boolean }[]> {
  const pool = getPool();
  await ensureSandboxPoolSchema(pool);
  const { rows } = await pool.query<{ pod_name: string; created_at: Date; claimed_tenant: string | null }>(
    "SELECT pod_name, created_at, claimed_tenant FROM sandbox_pool",
  );
  return rows.map((r) => ({ podName: r.pod_name, createdAt: r.created_at, claimed: r.claimed_tenant !== null }));
}

export async function forgetPoolPod(podName: string): Promise<void> {
  const pool = getPool();
  await ensureSandboxPoolSchema(pool);
  await pool.query("DELETE FROM sandbox_pool WHERE pod_name = $1", [podName]);
}
