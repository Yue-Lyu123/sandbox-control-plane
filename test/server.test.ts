import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { recordSandboxActivity } from "../src/activity.js";
import { sandboxPodName, SandboxProvisionError, type SandboxControlPlane } from "../src/control-plane.js";
import { statusForError } from "../src/errors.js";
import { appendSandboxPodLogLines, insertSandboxPodEvent } from "../src/observer-store.js";
import { createControlPlaneServer } from "../src/server.js";
import { TEST_DATABASE_URL } from "./pglite-test-db.js";
import { startCpServer, TEST_CP_SECRET, type CpServer } from "./support/cp-server.js";
import { startFakeAioServer, type FakeAioServer } from "./support/fake-aio-server.js";
import { startFakeK8sServer, type FakeK8sServer } from "./support/fake-k8s-server.js";

/**
 * 控制面 HTTP 面的契约测试：起真 server（随机端口）+ fake k8s / aio，
 * 断言主服务 HTTP 客户端要对的字段名、状态码与错误体。
 */

const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

let workspace: string;
let aio: FakeAioServer;
let k8sServer: FakeK8sServer;
let cp: CpServer;

type Json = Record<string, unknown>;
const json = async (res: Response) => (await res.json()) as Json;

function configureK8s(ns: string): void {
  process.env.SANDBOX_K8S_SERVER_URL = k8sServer.url;
  process.env.SANDBOX_K8S_TOKEN = "fake-token";
  process.env.SANDBOX_K8S_NAMESPACE = ns;
  process.env.SANDBOX_AIO_PORT = String(aio.port);
}

function unconfigureK8s(): void {
  delete process.env.SANDBOX_K8S_SERVER_URL;
  delete process.env.SANDBOX_K8S_TOKEN;
  delete process.env.SANDBOX_K8S_NAMESPACE;
  delete process.env.SANDBOX_AIO_PORT;
}

beforeAll(async () => {
  process.env.DATABASE_URL = TEST_DATABASE_URL;
  workspace = fs.mkdtempSync(path.join(os.tmpdir(), "cp-server-"));
  aio = await startFakeAioServer(workspace);
  k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });
  cp = await startCpServer();
});

afterAll(async () => {
  await cp.close();
  await aio.close();
  await k8sServer.close();
  fs.rmSync(workspace, { recursive: true, force: true });
  unconfigureK8s();
});

afterEach(() => {
  unconfigureK8s();
  delete process.env.SANDBOX_IDLE_TTL_S;
  delete process.env.SANDBOX_MAX_AGE_S;
});

describe("auth", () => {
  it("GET /healthz is open (k8s probe) and returns {ok:true}", async () => {
    const res = await fetch(`${cp.url}/healthz`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });

  it("every other path needs the inbound bearer: missing / wrong -> 401 {detail:'unauthorized'}", async () => {
    for (const p of ["/internal/sandboxes/logs?pod=x", "/internal/control-plane/health", "/nope"]) {
      const none = await fetch(`${cp.url}${p}`);
      expect(none.status).toBe(401);
      expect(await none.json()).toEqual({ detail: "unauthorized" });
      const wrong = await fetch(`${cp.url}${p}`, { headers: { authorization: `Bearer ${TEST_CP_SECRET}x` } });
      expect(wrong.status).toBe(401);
      const raw = await fetch(`${cp.url}${p}`, { headers: { authorization: TEST_CP_SECRET } });
      expect(raw.status).toBe(401);
    }
    const post = await fetch(`${cp.url}/internal/sandboxes/acquire`, { method: "POST", body: "{}" });
    expect(post.status).toBe(401);
  });

  it("refuses to be constructed without a secret", () => {
    expect(() => createControlPlaneServer({ secret: "" })).toThrow(/SANDBOX_CP_INBOUND_SECRET/);
  });

  it("unknown path with valid auth -> 404", async () => {
    const res = await cp.get("/internal/nope");
    expect(res.status).toBe(404);
  });
});

describe("execution endpoints — 200 shapes", () => {
  it("acquire -> {pod_name, base_url, via, expires_at} (no namespace / aio)", async () => {
    configureK8s(`srv-acq-${randomUUID().slice(0, 8)}`);
    const tenant = `t-${randomUUID()}`;
    const res = await cp.post("/internal/sandboxes/acquire", { tenant, session_id: "s1" });
    expect(res.status).toBe(200);
    const body = await json(res);
    expect(Object.keys(body).sort()).toEqual(["base_url", "expires_at", "pod_name", "via"]);
    expect(body.pod_name).toBe(sandboxPodName(tenant, "s1"));
    expect(body.via).toBe("pod-ip");
    expect(body.base_url).toBe(`http://127.0.0.1:${aio.port}`);
    expect(body.expires_at).toBeNull();
  });

  it("acquire with SANDBOX_MAX_AGE_S -> expires_at is an ISO string", async () => {
    configureK8s(`srv-acq-exp-${randomUUID().slice(0, 8)}`);
    process.env.SANDBOX_MAX_AGE_S = "3600";
    const res = await cp.post("/internal/sandboxes/acquire", { tenant: `t-${randomUUID()}`, session_id: "s1" });
    expect(res.status).toBe(200);
    expect((await json(res)).expires_at).toMatch(ISO_RE);
  });

  it("execute -> {exit_code, output, success, error}", async () => {
    configureK8s(`srv-exec-${randomUUID().slice(0, 8)}`);
    const tenant = `t-${randomUUID()}`;
    expect((await cp.post("/internal/sandboxes/acquire", { tenant, session_id: "s1" })).status).toBe(200);
    const res = await cp.post("/internal/sandboxes/execute", {
      tenant,
      session_id: "s1",
      command: "echo hello",
      timeout_ms: 10_000,
    });
    expect(res.status).toBe(200);
    const body = await json(res);
    expect(Object.keys(body).sort()).toEqual(["error", "exit_code", "output", "success"]);
    expect(body.exit_code).toBe(0);
    expect(body.success).toBe(true);
    expect(String(body.output)).toContain("hello");
  });

  it("mount-files -> {mounted}; relative paths land under dest_root, absolute paths are kept as-is", async () => {
    configureK8s(`srv-mount-${randomUUID().slice(0, 8)}`);
    const tenant = `t-${randomUUID()}`;
    expect((await cp.post("/internal/sandboxes/acquire", { tenant, session_id: "s1" })).status).toBe(200);
    const res = await cp.post("/internal/sandboxes/mount-files", {
      tenant,
      session_id: "s1",
      dest_root: "/home/gem/custom/",
      files: [
        { path: "hello/SKILL.md", content_base64: Buffer.from("# hi").toString("base64") },
        { path: "/tmp/abs.txt", content_base64: Buffer.from("abs").toString("base64") },
      ],
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ mounted: 2 });
    expect(fs.readFileSync(path.join(workspace, "home/gem/custom/hello/SKILL.md"), "utf-8")).toBe("# hi");
    expect(fs.readFileSync(path.join(workspace, "tmp/abs.txt"), "utf-8")).toBe("abs");
  });

  it("mount-files without dest_root uses /home/gem/skills", async () => {
    configureK8s(`srv-mount-def-${randomUUID().slice(0, 8)}`);
    const tenant = `t-${randomUUID()}`;
    expect((await cp.post("/internal/sandboxes/acquire", { tenant, session_id: "s1" })).status).toBe(200);
    const res = await cp.post("/internal/sandboxes/mount-files", {
      tenant,
      session_id: "s1",
      files: [{ path: "x/a.txt", content_base64: Buffer.from("A").toString("base64") }],
    });
    expect(await res.json()).toEqual({ mounted: 1 });
    expect(fs.readFileSync(path.join(workspace, "home/gem/skills/x/a.txt"), "utf-8")).toBe("A");
  });

  it("hold -> {held_until (ISO), ttl_s, expires_at}", async () => {
    configureK8s(`srv-hold-${randomUUID().slice(0, 8)}`);
    const res = await cp.post("/internal/sandboxes/hold", {
      tenant: `t-${randomUUID()}`,
      session_id: "s1",
      ttl_ms: 60_000,
      reason: "waiting for approval",
    });
    expect(res.status).toBe(200);
    const body = await json(res);
    expect(Object.keys(body).sort()).toEqual(["expires_at", "held_until", "ttl_s"]);
    expect(body.held_until).toMatch(ISO_RE);
    expect(body.ttl_s).toBe(60);
    expect(body.expires_at).toBeNull();
  });

  it("release -> {released: boolean}", async () => {
    const ns = `srv-rel-${randomUUID().slice(0, 8)}`;
    configureK8s(ns);
    const tenant = `t-${randomUUID()}`;
    await cp.post("/internal/sandboxes/acquire", { tenant, session_id: "s1" });
    const res = await cp.post("/internal/sandboxes/release", { tenant, session_id: "s1" });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ released: true });
  });

  it("reap -> {checked, reaped, skipped, held}; 503 with the exact detail when SANDBOX_IDLE_TTL_S is unset", async () => {
    configureK8s(`srv-reap-${randomUUID().slice(0, 8)}`);
    const off = await cp.post("/internal/sandboxes/reap", {});
    expect(off.status).toBe(503);
    expect(await off.json()).toEqual({
      detail: "sandbox reaper not configured (SANDBOX_IDLE_TTL_S unset or invalid)",
    });

    process.env.SANDBOX_IDLE_TTL_S = "600";
    const res = await cp.post("/internal/sandboxes/reap", {});
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ checked: 0, reaped: [], skipped: [], held: [] });
  });
});

describe("execution endpoints — error bodies carry the class name", () => {
  it("410 SandboxNotFoundError for a never-acquired session", async () => {
    configureK8s(`srv-410-${randomUUID().slice(0, 8)}`);
    const tenant = `t-${randomUUID()}`;
    const res = await cp.post("/internal/sandboxes/execute", { tenant, session_id: "ghost", command: "true" });
    expect(res.status).toBe(410);
    expect(await res.json()).toEqual({
      detail: `sandbox not found: ${sandboxPodName(tenant, "ghost")}`,
      error: "SandboxNotFoundError",
    });
  });

  it("502 SandboxUnreachableError when the pod exists but its AIO port is dead", async () => {
    const ns = `srv-502-${randomUUID().slice(0, 8)}`;
    configureK8s(ns);
    const tenant = `t-${randomUUID()}`;
    expect((await cp.post("/internal/sandboxes/acquire", { tenant, session_id: "s1" })).status).toBe(200);
    process.env.SANDBOX_AIO_PORT = "1";
    const res = await cp.post("/internal/sandboxes/execute", { tenant, session_id: "s1", command: "true" });
    expect(res.status).toBe(502);
    const body = await json(res);
    expect(body.error).toBe("SandboxUnreachableError");
    expect(String(body.detail)).toMatch(/^sandbox unreachable: /);
  });

  it("503 SandboxNotConfiguredError on every execution endpoint when K8s is not configured", async () => {
    unconfigureK8s();
    for (const p of ["acquire", "execute", "mount-files", "hold", "release", "reap"]) {
      const res = await cp.post(`/internal/sandboxes/${p}`, { tenant: "t", session_id: "s", command: "x", files: [] });
      expect(res.status, p).toBe(503);
      expect(await res.json()).toEqual({
        detail: "sandbox control plane not configured",
        error: "SandboxNotConfiguredError",
      });
    }
  });

  it("any other Error -> 500 with {detail, error: err.name}", async () => {
    const stub = {
      acquire: async () => {
        throw new SandboxProvisionError("create pod failed: 500 boom");
      },
    } as unknown as SandboxControlPlane;
    const server = await startCpServer({ controlPlane: () => stub });
    try {
      const res = await server.post("/internal/sandboxes/acquire", { tenant: "t", session_id: "s" });
      expect(res.status).toBe(500);
      expect(await res.json()).toEqual({ detail: "create pod failed: 500 boom", error: "SandboxProvisionError" });
    } finally {
      await server.close();
    }
  });

  it("statusForError maps by name, not instanceof", () => {
    const fake = (name: string) => Object.assign(new Error("x"), { name });
    expect(statusForError(fake("SandboxNotFoundError"))).toBe(410);
    expect(statusForError(fake("SandboxUnreachableError"))).toBe(502);
    expect(statusForError(fake("SandboxNotConfiguredError"))).toBe(503);
    expect(statusForError(fake("SandboxProvisionError"))).toBe(500);
    expect(statusForError("not an error")).toBe(500);
  });
});

describe("request validation", () => {
  it("invalid json -> 400 {detail:'invalid json'}", async () => {
    configureK8s("srv-400");
    const res = await cp.post("/internal/sandboxes/acquire", "{not json");
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ detail: "invalid json" });
  });

  it("missing tenant / session_id -> 400", async () => {
    configureK8s("srv-400");
    let res = await cp.post("/internal/sandboxes/acquire", { session_id: "s" });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ detail: "missing tenant" });
    res = await cp.post("/internal/sandboxes/release", { tenant: "t" });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ detail: "missing session_id" });
  });

  it("mount-files with a malformed file entry -> 400", async () => {
    configureK8s("srv-400");
    const res = await cp.post("/internal/sandboxes/mount-files", {
      tenant: "t",
      session_id: "s",
      files: [{ path: "a" }],
    });
    expect(res.status).toBe(400);
  });
});

describe("observation endpoints (work without K8s credentials)", () => {
  it("logs -> {lines:[{id, pod_name, line, observed_at(ISO), tenant_id, session_id}]}", async () => {
    unconfigureK8s();
    const pod = `sbx-srvlog-${randomUUID().slice(0, 6)}`;
    await recordSandboxActivity(pod, "tenant-l", "session-l");
    await appendSandboxPodLogLines(pod, ["one", "two"]);
    const res = await cp.get(`/internal/sandboxes/logs?pod=${pod}&limit=1`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { lines: Json[] };
    expect(body.lines).toHaveLength(1);
    const [row] = body.lines;
    expect(Object.keys(row).sort()).toEqual(["id", "line", "observed_at", "pod_name", "session_id", "tenant_id"]);
    expect(row).toMatchObject({ pod_name: pod, line: "two", tenant_id: "tenant-l", session_id: "session-l" });
    expect(typeof row.id).toBe("string");
    expect(row.observed_at).toMatch(ISO_RE);
  });

  it("events -> {events:[...]} with observed_at as ISO string; pod may be omitted", async () => {
    unconfigureK8s();
    const pod = `sbx-srvev-${randomUUID().slice(0, 6)}`;
    await insertSandboxPodEvent({
      podName: pod,
      source: "watch",
      type: "ADDED",
      reason: "Running",
      message: "hi",
      payload: { a: 1 },
    });
    const res = await cp.get(`/internal/sandboxes/events?pod=${pod}`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { events: Json[] };
    expect(body.events).toHaveLength(1);
    const [ev] = body.events;
    expect(Object.keys(ev).sort()).toEqual([
      "id",
      "message",
      "observed_at",
      "payload",
      "pod_name",
      "reason",
      "session_id",
      "source",
      "tenant_id",
      "type",
    ]);
    expect(ev).toMatchObject({ pod_name: pod, source: "watch", type: "ADDED", payload: { a: 1 } });
    expect(ev.observed_at).toMatch(ISO_RE);

    const all = await cp.get("/internal/sandboxes/events?limit=5");
    expect(all.status).toBe(200);
    expect(((await all.json()) as { events: Json[] }).events.length).toBeGreaterThanOrEqual(1);
  });

  it("session-logs -> {pods, lines, markers} scoped to (tenant, session_id)", async () => {
    unconfigureK8s();
    const tenant = `t-${randomUUID()}`;
    const session = `s-${randomUUID()}`;
    const pod = `sbx-srvsess-${randomUUID().slice(0, 6)}`;
    await recordSandboxActivity(pod, tenant, session);
    await appendSandboxPodLogLines(pod, ["run line"]);
    await insertSandboxPodEvent({ podName: pod, source: "observer", type: "LOG_SALVAGED", reason: "", message: "" });
    await insertSandboxPodEvent({ podName: pod, source: "watch", type: "ADDED", reason: "", message: "" });

    const res = await cp.get(
      `/internal/sandboxes/session-logs?tenant=${encodeURIComponent(tenant)}&session_id=${encodeURIComponent(session)}&lines=10&markers=5`,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { pods: string[]; lines: Json[]; markers: Json[] };
    expect(body.pods).toEqual([pod]);
    expect(body.lines.map((l) => l.line)).toEqual(["run line"]);
    expect(body.lines[0].observed_at).toMatch(ISO_RE);
    expect(body.markers.map((m) => m.type)).toEqual(["LOG_SALVAGED"]);
    expect(body.markers[0].observed_at).toMatch(ISO_RE);

    const other = await cp.get(
      `/internal/sandboxes/session-logs?tenant=other&session_id=${encodeURIComponent(session)}`,
    );
    expect(await other.json()).toEqual({ pods: [], lines: [], markers: [] });

    const missing = await cp.get(`/internal/sandboxes/session-logs?tenant=${tenant}`);
    expect(missing.status).toBe(400);
  });

  it("control-plane/health passes loadControlPlaneHealth() through verbatim", async () => {
    unconfigureK8s();
    const off = await cp.get("/internal/control-plane/health");
    expect(off.status).toBe(200);
    expect(await off.json()).toEqual({ ok: false, kind: "not_configured" });

    configureK8s(`srv-health-${randomUUID().slice(0, 8)}`);
    const on = await cp.get("/internal/control-plane/health");
    expect(on.status).toBe(200);
    const body = await json(on);
    expect(body.ok).toBe(true);
    expect(body.health).toBeTypeOf("object");
  });
});
