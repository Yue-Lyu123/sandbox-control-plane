import http from "node:http";
import { timingSafeEqual } from "node:crypto";
import { recordSandboxHold, SANDBOX_HOLD_DEFAULT_MS, SANDBOX_HOLD_MAX_MS } from "./activity.js";
import { sandboxPodName, type SandboxControlPlane } from "./control-plane.js";
import { errorBody, SandboxNotConfiguredError, statusForError } from "./errors.js";
import { sandboxControlPlaneFromEnv, sandboxReaperFromEnv } from "./from-env.js";
import { loadControlPlaneHealth, type ControlPlaneHealthLoad } from "./load-control-plane-health.js";
import {
  readSandboxPodEvents,
  readSandboxPodLogs,
  readSessionLogMarkers,
  readSessionPodLogs,
  readSessionPodNames,
  type SandboxPodEventRow,
  type SandboxPodLogRow,
} from "./observer-store.js";
import type { ReapPassResult } from "./reaper.js";

/**
 * 控制面 HTTP 面（纯 node:http，JSON，蛇形）。主服务 `lib/sandbox/client.ts` 的 HTTP 实现对的就是这张表：
 *
 *   POST /internal/sandboxes/{acquire,execute,mount-files,hold,release,reap}
 *   GET  /internal/sandboxes/{logs,events,session-logs}
 *   GET  /internal/control-plane/health
 *   GET  /healthz（不鉴权，给 k8s 探针）
 *
 * 除 `/healthz` 外每个请求要 `Authorization: Bearer <SANDBOX_CP_INBOUND_SECRET>`。
 * 控制面抛的错按 `err.name` 映射状态码，body `{detail, error}`，`error` 是类名原文——
 * 主服务按它重建同名 Error（跨进程不能靠 instanceof）。
 *
 * 执行面（acquire…reap）要 K8s 凭据，没配时 503 SandboxNotConfiguredError；
 * 观测面（logs / events / session-logs）只读观测库，没有 K8s 也照常。
 */
export interface ServerDeps {
  /** 入站共享密钥。 */
  secret: string;
  /** 每个请求现建（与主服务拆分前每请求 `sandboxControlPlaneFromEnv()` 一致，测试按用例改 env）。 */
  controlPlane?: () => SandboxControlPlane | null;
  reaper?: () => (() => Promise<ReapPassResult>) | null;
  health?: () => Promise<ControlPlaneHealthLoad>;
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly body: Record<string, unknown>,
  ) {
    super(String(body.detail ?? status));
  }
}

const MAX_BODY_BYTES = 64 * 1024 * 1024;

/** 查询参数 `limit` 收口到 [1, max]，非数字回落到 def。（与主服务 `lib/http/clamp-limit.ts` 同一实现） */
export function clampLimit(raw: string | null, def: number, max: number): number {
  const n = Number(raw ?? def);
  return Number.isFinite(n) ? Math.min(Math.max(Math.trunc(n), 1), max) : def;
}

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
  const buf = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Content-Length": buf.length });
  res.end(buf);
}

function bearerMatches(header: string | undefined, secret: string): boolean {
  if (!header || !header.startsWith("Bearer ")) return false;
  const got = Buffer.from(header.slice("Bearer ".length));
  const want = Buffer.from(secret);
  return got.length === want.length && timingSafeEqual(got, want);
}

async function readJson(req: http.IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = chunk as Buffer;
    size += buf.length;
    if (size > MAX_BODY_BYTES) throw new HttpError(413, { detail: "request body too large" });
    chunks.push(buf);
  }
  const text = Buffer.concat(chunks).toString("utf-8");
  if (text.trim() === "") return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new HttpError(400, { detail: "invalid json" });
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new HttpError(400, { detail: "invalid json" });
  }
  return parsed as Record<string, unknown>;
}

function requireString(body: Record<string, unknown>, key: string): string {
  const v = body[key];
  if (typeof v !== "string" || v === "") throw new HttpError(400, { detail: `missing ${key}` });
  return v;
}

function requireQuery(params: URLSearchParams, key: string): string {
  const v = params.get(key);
  if (v === null || v === "") throw new HttpError(400, { detail: `query param '${key}' is required` });
  return v;
}

function logRowWire(r: SandboxPodLogRow) {
  return {
    id: r.id,
    pod_name: r.pod_name,
    line: r.line,
    observed_at: new Date(r.observed_at).toISOString(),
    tenant_id: r.tenant_id,
    session_id: r.session_id,
  };
}

function eventRowWire(e: SandboxPodEventRow) {
  return {
    id: e.id,
    pod_name: e.pod_name,
    source: e.source,
    type: e.type,
    reason: e.reason,
    message: e.message,
    payload: e.payload,
    observed_at: new Date(e.observed_at).toISOString(),
    tenant_id: e.tenant_id,
    session_id: e.session_id,
  };
}

type Handler = (ctx: {
  req: http.IncomingMessage;
  url: URL;
  requireCp: () => SandboxControlPlane;
}) => Promise<unknown>;

export function createControlPlaneServer(deps: ServerDeps): http.Server {
  if (!deps.secret) throw new Error("SANDBOX_CP_INBOUND_SECRET is required");
  const getCp = deps.controlPlane ?? sandboxControlPlaneFromEnv;
  const getReaper = deps.reaper ?? sandboxReaperFromEnv;
  const getHealth = deps.health ?? loadControlPlaneHealth;

  const routes: Record<string, Handler> = {
    "POST /internal/sandboxes/acquire": async ({ req, requireCp }) => {
      const cp = requireCp();
      const body = await readJson(req);
      const tenant = requireString(body, "tenant");
      const sessionId = requireString(body, "session_id");
      const handle = await cp.acquire(tenant, sessionId);
      return { pod_name: handle.podName, base_url: handle.baseUrl, via: handle.via, expires_at: handle.expiresAt };
    },

    "POST /internal/sandboxes/execute": async ({ req, requireCp }) => {
      const cp = requireCp();
      const body = await readJson(req);
      const tenant = requireString(body, "tenant");
      const sessionId = requireString(body, "session_id");
      if (typeof body.command !== "string") throw new HttpError(400, { detail: "missing command" });
      const timeoutMs =
        typeof body.timeout_ms === "number" && Number.isFinite(body.timeout_ms) && body.timeout_ms > 0
          ? body.timeout_ms
          : undefined;
      const r = await cp.execute(tenant, sessionId, body.command, { timeoutMs });
      return { exit_code: r.exitCode, output: r.output, success: r.success, error: r.error };
    },

    "POST /internal/sandboxes/mount-files": async ({ req, requireCp }) => {
      const cp = requireCp();
      const body = await readJson(req);
      const tenant = requireString(body, "tenant");
      const sessionId = requireString(body, "session_id");
      const destRoot = typeof body.dest_root === "string" && body.dest_root !== "" ? body.dest_root : undefined;
      if (!Array.isArray(body.files)) throw new HttpError(400, { detail: "missing files" });
      const files: Array<[string, Buffer]> = [];
      for (const f of body.files as unknown[]) {
        const rec = (f ?? {}) as { path?: unknown; content_base64?: unknown };
        if (typeof rec.path !== "string" || rec.path === "" || typeof rec.content_base64 !== "string") {
          throw new HttpError(400, { detail: "invalid files: each entry needs path and content_base64" });
        }
        files.push([rec.path, Buffer.from(rec.content_base64, "base64")]);
      }
      const mounted = await cp.mountSkills(tenant, sessionId, files, { destRoot });
      return { mounted };
    },

    "POST /internal/sandboxes/hold": async ({ req, requireCp }) => {
      const cp = requireCp();
      const body = await readJson(req);
      const tenant = requireString(body, "tenant");
      const sessionId = requireString(body, "session_id");
      // 坏 ttl 退回默认值而不是 400（与主服务 hold 路由同一取舍：一个坏参数不该换回「沙箱被误收」）。
      const ttlMs =
        typeof body.ttl_ms === "number" && Number.isFinite(body.ttl_ms) && body.ttl_ms > 0
          ? body.ttl_ms
          : SANDBOX_HOLD_DEFAULT_MS;
      const reason = typeof body.reason === "string" ? body.reason : null;
      // 与主服务拆分前 `inProcessSandboxClient().hold` 逐行同义：按哈希 pod 名记 hold，
      // expires_at 取不到就 null（hold 本身已落库，不因为读 pod 失败而报错）。
      const podName = sandboxPodName(tenant, sessionId);
      const heldUntil = await recordSandboxHold(podName, tenant, sessionId, ttlMs, reason);
      let expiresAt: string | null = null;
      try {
        expiresAt = await cp.expiresAtFor(tenant, sessionId);
      } catch {
        expiresAt = null;
      }
      return {
        held_until: heldUntil.toISOString(),
        ttl_s: Math.round(Math.min(ttlMs, SANDBOX_HOLD_MAX_MS) / 1000),
        expires_at: expiresAt,
      };
    },

    "POST /internal/sandboxes/release": async ({ req, requireCp }) => {
      const cp = requireCp();
      const body = await readJson(req);
      const tenant = requireString(body, "tenant");
      const sessionId = requireString(body, "session_id");
      return { released: await cp.release(tenant, sessionId) };
    },

    "POST /internal/sandboxes/reap": async ({ req, requireCp }) => {
      requireCp();
      await readJson(req);
      const reap = getReaper();
      if (reap === null) {
        throw new HttpError(503, { detail: "sandbox reaper not configured (SANDBOX_IDLE_TTL_S unset or invalid)" });
      }
      return reap();
    },

    "GET /internal/sandboxes/logs": async ({ url }) => {
      const pod = requireQuery(url.searchParams, "pod");
      const limit = clampLimit(url.searchParams.get("limit"), 500, 2000);
      const rows = await readSandboxPodLogs(pod, limit);
      return { lines: rows.map(logRowWire) };
    },

    "GET /internal/sandboxes/events": async ({ url }) => {
      const pod = url.searchParams.get("pod") || null;
      const limit = clampLimit(url.searchParams.get("limit"), 200, 1000);
      const rows = await readSandboxPodEvents(pod, limit);
      return { events: rows.map(eventRowWire) };
    },

    "GET /internal/sandboxes/session-logs": async ({ url }) => {
      const tenant = requireQuery(url.searchParams, "tenant");
      const sessionId = requireQuery(url.searchParams, "session_id");
      // 默认值与主服务 run-logs.ts 一致（RUN_LOGS_DEFAULT_LIMIT=500、MARKER_LIMIT=20）。
      const lines = clampLimit(url.searchParams.get("lines"), 500, 2000);
      const markers = clampLimit(url.searchParams.get("markers"), 20, 1000);
      const [pods, lineRows, markerRows] = await Promise.all([
        readSessionPodNames(tenant, sessionId),
        readSessionPodLogs(tenant, sessionId, lines),
        readSessionLogMarkers(tenant, sessionId, markers),
      ]);
      return { pods, lines: lineRows.map(logRowWire), markers: markerRows.map(eventRowWire) };
    },

    "GET /internal/control-plane/health": async () => getHealth(),
  };

  return http.createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url ?? "/", "http://localhost");
      const method = req.method ?? "GET";

      if (url.pathname === "/healthz" && method === "GET") {
        sendJson(res, 200, { ok: true });
        return;
      }
      if (!bearerMatches(req.headers.authorization, deps.secret)) {
        sendJson(res, 401, { detail: "unauthorized" });
        return;
      }
      const handler = routes[`${method} ${url.pathname}`];
      if (!handler) {
        sendJson(res, 404, { detail: "not found" });
        return;
      }
      const requireCp = (): SandboxControlPlane => {
        const cp = getCp();
        if (!cp) throw new SandboxNotConfiguredError();
        return cp;
      };
      try {
        const result = await handler({ req, url, requireCp });
        sendJson(res, 200, result);
      } catch (err) {
        if (err instanceof HttpError) {
          sendJson(res, err.status, err.body);
          return;
        }
        const status = statusForError(err);
        if (status === 500) console.error(`[sandbox-control-plane] ${method} ${url.pathname} failed:`, err);
        sendJson(res, status, errorBody(err));
      }
    })().catch((err) => {
      console.error("[sandbox-control-plane] response write failed:", err);
      if (!res.headersSent) sendJson(res, 500, { detail: "internal error", error: "Error" });
      else res.destroy();
    });
  });
}
