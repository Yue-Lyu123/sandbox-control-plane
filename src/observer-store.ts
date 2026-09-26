import type { Pool } from "pg";
import { getPool } from "./db.js";
import { ensureSandboxActivitySchema } from "./activity.js";
import type { PodEventRecord } from "./observer.js";
import { schemaEnsurer } from "./ensure-once.js";

export const SANDBOX_OBSERVE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS sandbox_pod_logs (
    id          bigserial PRIMARY KEY,
    pod_name    text NOT NULL,
    line        text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sandbox_pod_logs_pod_idx ON sandbox_pod_logs (pod_name, id);

CREATE TABLE IF NOT EXISTS sandbox_pod_events (
    id          bigserial PRIMARY KEY,
    pod_name    text NOT NULL,
    source      text NOT NULL,
    type        text NOT NULL,
    reason      text NOT NULL DEFAULT '',
    message     text NOT NULL DEFAULT '',
    payload     jsonb,
    dedup_key   text,
    observed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sandbox_pod_events_pod_idx ON sandbox_pod_events (pod_name, id);
CREATE UNIQUE INDEX IF NOT EXISTS sandbox_pod_events_dedup_idx
    ON sandbox_pod_events (dedup_key) WHERE dedup_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS sandbox_pod_meta (
    pod_name   text PRIMARY KEY,
    tenant_id  text,
    session_id text,
    first_seen timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE sandbox_pod_meta ADD COLUMN IF NOT EXISTS username text;
ALTER TABLE sandbox_pod_meta ADD COLUMN IF NOT EXISTS agent_id text;
`;

const sandboxObserveSchema = schemaEnsurer((pool: Pool) => pool.query(SANDBOX_OBSERVE_SCHEMA_SQL));

export function ensureSandboxObserveSchema(pool: Pool): Promise<void> {
  return sandboxObserveSchema.ensure(pool);
}

export async function appendSandboxPodLogLines(podName: string, lines: string[]): Promise<void> {
  if (lines.length === 0) return;
  const pool = getPool();
  await ensureSandboxObserveSchema(pool);
  await pool.query(
    "INSERT INTO sandbox_pod_logs (pod_name, line) SELECT $1, unnest($2::text[])",
    [podName, lines],
  );
}

export async function countSandboxPodLogLines(podName: string): Promise<number> {
  const pool = getPool();
  await ensureSandboxObserveSchema(pool);
  const { rows } = await pool.query<{ n: string }>(
    "SELECT count(*)::text AS n FROM sandbox_pod_logs WHERE pod_name = $1",
    [podName],
  );
  return Number(rows[0]?.n ?? 0);
}

export async function insertSandboxPodEvent(event: PodEventRecord): Promise<void> {
  const pool = getPool();
  await ensureSandboxObserveSchema(pool);
  await pool.query(
    `INSERT INTO sandbox_pod_events (pod_name, source, type, reason, message, payload, dedup_key)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`,
    [
      event.podName,
      event.source,
      event.type,
      event.reason,
      event.message,
      event.payload === undefined ? null : JSON.stringify(event.payload),
      event.dedupKey ?? null,
    ],
  );
}

export async function upsertSandboxPodMeta(podName: string, sessionId: string | null): Promise<void> {
  const pool = getPool();
  await ensureSandboxObserveSchema(pool);
  await pool.query(
    `INSERT INTO sandbox_pod_meta (pod_name, tenant_id, session_id)
     VALUES (
       $1,
       (SELECT tenant_id FROM sandbox_activity WHERE pod_name = $1),
       COALESCE($2, (SELECT session_id FROM sandbox_activity WHERE pod_name = $1))
     )
     ON CONFLICT (pod_name) DO UPDATE SET
       tenant_id  = COALESCE(sandbox_pod_meta.tenant_id,  EXCLUDED.tenant_id),
       session_id = COALESCE(sandbox_pod_meta.session_id, EXCLUDED.session_id)`,
    [podName, sessionId],
  );

  // sandbox_session_identity 是主服务的表（身份归因留主服务）。第 2 步与主服务共库时它在，
  // 照旧回填 username / agent_id；控制面独立库（第 3 步 / 本目录测试）里没有这张表，跳过。
  const { rows: present } = await pool.query<{ ok: boolean }>(
    "SELECT to_regclass('sandbox_session_identity') IS NOT NULL AS ok",
  );
  if (present[0]?.ok !== true) return;

  await pool.query(
    `UPDATE sandbox_pod_meta m
     SET tenant_id = COALESCE(m.tenant_id, i.tenant_id),
         username = COALESCE(m.username, i.username),
         agent_id = COALESCE(m.agent_id, i.agent_id)
     FROM sandbox_session_identity i
     WHERE m.pod_name = $1 AND i.session_id = m.session_id`,
    [podName],
  );
}


export interface SandboxPodLogRow {
  id: string;
  pod_name: string;
  line: string;
  observed_at: Date;
  tenant_id: string | null;
  session_id: string | null;
}

export interface SandboxPodEventRow {
  id: string;
  pod_name: string;
  source: string;
  type: string;
  reason: string;
  message: string;
  payload: unknown;
  observed_at: Date;
  tenant_id: string | null;
  session_id: string | null;
}

// 只 join 沙箱自己的两张表；username / agent_id 由主服务按 session_id 补（session-identity.ts），
// 这里不再 join sandbox_session_identity——拆库后它不在这个库里。
const CORRELATION_JOIN = `
  LEFT JOIN sandbox_pod_meta m ON m.pod_name = x.pod_name
  LEFT JOIN sandbox_activity a ON a.pod_name = x.pod_name`;
const CORRELATED_COLUMNS = `
  COALESCE(m.tenant_id,  a.tenant_id)  AS tenant_id,
  COALESCE(m.session_id, a.session_id) AS session_id`;

const TENANT_GATE = `(COALESCE(m.tenant_id, a.tenant_id) IS NULL
                      OR COALESCE(m.tenant_id, a.tenant_id) = $2)`;

export async function readSandboxPodLogs(podName: string, limit: number): Promise<SandboxPodLogRow[]> {
  const pool = getPool();
  await ensureSandboxObserveSchema(pool);
  const { rows } = await pool.query<SandboxPodLogRow>(
    `SELECT * FROM (
       SELECT x.id::text, x.pod_name, x.line, x.observed_at, ${CORRELATED_COLUMNS}
       FROM sandbox_pod_logs x ${CORRELATION_JOIN}
       WHERE x.pod_name = $1
       ORDER BY x.id DESC
       LIMIT $2
     ) t ORDER BY t.id::bigint ASC`,
    [podName, limit],
  );
  return rows;
}

async function ensureSessionReadSchemas(pool: Pool): Promise<void> {
  await Promise.all([ensureSandboxObserveSchema(pool), ensureSandboxActivitySchema(pool)]);
}

export async function readSessionPodNames(tenantId: string, sessionId: string): Promise<string[]> {
  const pool = getPool();
  await ensureSessionReadSchemas(pool);
  const { rows } = await pool.query<{ pod_name: string }>(
    `SELECT pod_name FROM sandbox_pod_meta
      WHERE session_id = $1 AND (tenant_id IS NULL OR tenant_id = $2)
     UNION
     SELECT pod_name FROM sandbox_activity
      WHERE session_id = $1 AND (tenant_id IS NULL OR tenant_id = $2)`,
    [sessionId, tenantId],
  );
  return rows.map((r) => r.pod_name).sort();
}

export async function readSessionPodLogs(
  tenantId: string,
  sessionId: string,
  limit: number,
): Promise<SandboxPodLogRow[]> {
  const pool = getPool();
  await ensureSessionReadSchemas(pool);
  const { rows } = await pool.query<SandboxPodLogRow>(
    `SELECT * FROM (
       SELECT x.id::text, x.pod_name, x.line, x.observed_at, ${CORRELATED_COLUMNS}
       FROM sandbox_pod_logs x ${CORRELATION_JOIN}
       WHERE COALESCE(m.session_id, a.session_id) = $1
         AND ${TENANT_GATE}
       ORDER BY x.id DESC
       LIMIT $3
     ) t ORDER BY t.id::bigint ASC`,
    [sessionId, tenantId, limit],
  );
  return rows;
}

export async function readSessionLogMarkers(
  tenantId: string,
  sessionId: string,
  limit: number,
): Promise<SandboxPodEventRow[]> {
  const pool = getPool();
  await ensureSessionReadSchemas(pool);
  const { rows } = await pool.query<SandboxPodEventRow>(
    `SELECT x.id::text, x.pod_name, x.source, x.type, x.reason, x.message, x.payload, x.observed_at,
            ${CORRELATED_COLUMNS}
     FROM sandbox_pod_events x ${CORRELATION_JOIN}
     WHERE COALESCE(m.session_id, a.session_id) = $1
       AND ${TENANT_GATE}
       AND x.type = ANY($3::text[])
     ORDER BY x.id DESC
     LIMIT $4`,
    [sessionId, tenantId, LOG_MARKER_EVENT_TYPES, limit],
  );
  return rows;
}

export const LOG_MARKER_EVENT_TYPES = ["LOG_UNSALVAGEABLE", "LOG_SALVAGED"] as const;

export async function readSandboxPodEvents(podName: string | null, limit: number): Promise<SandboxPodEventRow[]> {
  const pool = getPool();
  await ensureSandboxObserveSchema(pool);
  const { rows } = await pool.query<SandboxPodEventRow>(
    `SELECT x.id::text, x.pod_name, x.source, x.type, x.reason, x.message, x.payload, x.observed_at,
            ${CORRELATED_COLUMNS}
     FROM sandbox_pod_events x ${CORRELATION_JOIN}
     WHERE ($1::text IS NULL OR x.pod_name = $1)
     ORDER BY x.id DESC
     LIMIT $2`,
    [podName, limit],
  );
  return rows;
}
