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
-- 保留期清理按 observed_at 削（2026-09-26）。生产上这条索引是脚本 CONCURRENTLY 先建好的
-- （scripts/purge-sandbox-logs-2026-09-26.ts），这里只是新库建表时补齐；已存在即 no-op。
CREATE INDEX IF NOT EXISTS sandbox_pod_logs_observed_at_idx ON sandbox_pod_logs (observed_at);

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
CREATE INDEX IF NOT EXISTS sandbox_pod_events_observed_at_idx ON sandbox_pod_events (observed_at);
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
-- 运行日志页按 session 找 pod（2026-09-26）；表小，启动时建索引是瞬时的。
CREATE INDEX IF NOT EXISTS sandbox_pod_meta_session_idx ON sandbox_pod_meta (session_id);
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
  pods?: string[],
): Promise<SandboxPodLogRow[]> {
  const pool = getPool();
  await ensureSessionReadSchemas(pool);
  // 2026-09-26：原来按 COALESCE(m.session_id, a.session_id) 过滤，谓词跨两张 join 表，索引用不上，
  // 每次打开运行日志页都全表扫 sandbox_pod_logs（生产 7000 万行）。现在先按 session 取 pod 名
  // （meta/activity 各有 session_id 索引），再走 (pod_name, id) 索引反向扫。租户门在取 pod 那一步已经过了。
  const podNames = pods ?? (await readSessionPodNames(tenantId, sessionId));
  if (podNames.length === 0) return [];
  const { rows } = await pool.query<SandboxPodLogRow>(
    `SELECT * FROM (
       SELECT x.id::text, x.pod_name, x.line, x.observed_at, ${CORRELATED_COLUMNS}
       FROM sandbox_pod_logs x ${CORRELATION_JOIN}
       WHERE x.pod_name = ANY($1::text[])
       ORDER BY x.id DESC
       LIMIT $2
     ) t ORDER BY t.id::bigint ASC`,
    [podNames, limit],
  );
  return rows;
}

export async function readSessionLogMarkers(
  tenantId: string,
  sessionId: string,
  limit: number,
  pods?: string[],
): Promise<SandboxPodEventRow[]> {
  const pool = getPool();
  await ensureSessionReadSchemas(pool);
  const podNames = pods ?? (await readSessionPodNames(tenantId, sessionId));
  if (podNames.length === 0) return [];
  const { rows } = await pool.query<SandboxPodEventRow>(
    `SELECT x.id::text, x.pod_name, x.source, x.type, x.reason, x.message, x.payload, x.observed_at,
            ${CORRELATED_COLUMNS}
     FROM sandbox_pod_events x ${CORRELATION_JOIN}
     WHERE x.pod_name = ANY($1::text[])
       AND x.type = ANY($2::text[])
     ORDER BY x.id DESC
     LIMIT $3`,
    [podNames, LOG_MARKER_EVENT_TYPES, limit],
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
