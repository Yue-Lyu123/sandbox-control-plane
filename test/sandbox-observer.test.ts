import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { getPool } from "../src/db.js";
import { K8sClient } from "../src/k8s.js";
import { SandboxObserver } from "../src/observer.js";
import {
  appendSandboxPodLogLines,
  countSandboxPodLogLines,
  insertSandboxPodEvent,
  upsertSandboxPodMeta,
} from "../src/observer-store.js";
import { deleteSandboxActivity, recordSandboxActivity } from "../src/activity.js";
import { TEST_DATABASE_URL } from "./pglite-test-db.js";
import { startFakeK8sServer, type FakeK8sServer } from "./support/fake-k8s-server.js";
import { startCpServer, type CpServer } from "./support/cp-server.js";

const NS = "observer-e2e";
const SELECTOR = "app=community-sandbox";
const pgStore = {
  appendLogLines: appendSandboxPodLogLines,
  countLogLines: countSandboxPodLogLines,
  insertEvent: insertSandboxPodEvent,
  upsertPodMeta: upsertSandboxPodMeta,
};

let k8sServer: FakeK8sServer;
let client: K8sClient;
let cp: CpServer;

/**
 * `sandbox_session_identity` 是主服务的表（身份归因留主服务）。第 2 步共库时它在，
 * `upsertSandboxPodMeta` 会顺手从它回填 tenant / username / agent_id。这里用主服务的 DDL
 * 原样建一份当夹具，只为验证「共库时回填照旧」；表不在时的行为另有一条用例覆盖。
 */
const MAIN_SERVICE_SESSION_IDENTITY_DDL = `
CREATE TABLE IF NOT EXISTS sandbox_session_identity (
    session_id text PRIMARY KEY,
    tenant_id  text,
    username   text,
    agent_id   text,
    minted_at  timestamptz NOT NULL DEFAULT now()
);
`;

async function recordSandboxSessionIdentity(
  sessionId: string,
  tenantId: string,
  username: string,
  agentId: string,
): Promise<void> {
  await getPool().query(MAIN_SERVICE_SESSION_IDENTITY_DDL);
  await getPool().query(
    `INSERT INTO sandbox_session_identity (session_id, tenant_id, username, agent_id)
     VALUES ($1, $2, $3, $4) ON CONFLICT (session_id) DO NOTHING`,
    [sessionId, tenantId, username, agentId],
  );
}
const observers: SandboxObserver[] = [];

function startObserver(opts: { namespace?: string; maxLogLinesPerPod?: number } = {}): SandboxObserver {
  const observer = new SandboxObserver(client, pgStore, {
    namespace: opts.namespace ?? NS,
    reconnectDelayMs: 15,
    eventsPollMs: 25,
    logFlushMs: 10,
    maxLogLinesPerPod: opts.maxLogLinesPerPod,
  });
  observer.start();
  observers.push(observer);
  return observer;
}

async function logLines(pod: string): Promise<string[]> {
  const { rows } = await getPool().query<{ line: string }>(
    "SELECT line FROM sandbox_pod_logs WHERE pod_name = $1 ORDER BY id",
    [pod],
  );
  return rows.map((r) => r.line);
}

async function eventRows(pod: string): Promise<Array<{ source: string; type: string; reason: string }>> {
  const { rows } = await getPool().query<{ source: string; type: string; reason: string }>(
    "SELECT source, type, reason FROM sandbox_pod_events WHERE pod_name = $1 ORDER BY id",
    [pod],
  );
  return rows;
}

beforeAll(async () => {
  process.env.DATABASE_URL = TEST_DATABASE_URL;
  k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });
  client = new K8sClient({ server: k8sServer.url, token: "fake-token", namespace: NS });
  cp = await startCpServer();
});

afterAll(async () => {
  await cp.close();
  await k8sServer.close();
  await getPool().query("DROP TABLE IF EXISTS sandbox_session_identity");
});

afterEach(async () => {
  for (const o of observers.splice(0)) await o.stop();
});

describe("SandboxObserver", () => {
  it("records lifecycle events and captures log lines while the pod runs", async () => {
    startObserver();
    k8sServer.seedPod({ namespace: NS, name: "sbx-live", labels: { app: "community-sandbox" } });

    await expect.poll(async () => (await eventRows("sbx-live")).length).toBeGreaterThanOrEqual(1);
    k8sServer.appendPodLog(NS, "sbx-live", "hello from sandbox");
    k8sServer.appendPodLog(NS, "sbx-live", "second line");
    await expect.poll(() => logLines("sbx-live")).toEqual(["hello from sandbox", "second line"]);

    await client.delete(NS, "pods", "sbx-live");
    await expect.poll(async () => (await eventRows("sbx-live")).map((e) => e.type)).toContain("DELETED");
  });

  it("ignores pods outside the sandbox label selector", async () => {
    startObserver();
    k8sServer.seedPod({ namespace: NS, name: "foreign-pod", labels: { app: "unrelated" } });
    k8sServer.seedPod({ namespace: NS, name: "sbx-mine", labels: { app: "community-sandbox" } });
    await expect.poll(async () => (await eventRows("sbx-mine")).length).toBeGreaterThanOrEqual(1);
    expect(await eventRows("foreign-pod")).toEqual([]);
  });

  it("ingests namespace v1 Events for sbx- pods with durable dedup across polls", async () => {
    startObserver();
    k8sServer.seedEvent(NS, {
      metadata: { uid: "uid-evict-1" },
      type: "Warning",
      reason: "Evicted",
      message: "ephemeral storage exceeded",
      involvedObject: { kind: "Pod", name: "sbx-evicted" },
      count: 1,
    });
    k8sServer.seedEvent(NS, {
      metadata: { uid: "uid-other" },
      type: "Normal",
      reason: "Scheduled",
      message: "not ours",
      involvedObject: { kind: "Pod", name: "not-a-sandbox" },
    });

    await expect.poll(async () => (await eventRows("sbx-evicted")).length).toBe(1);

    await new Promise((r) => setTimeout(r, 120));
    expect((await eventRows("sbx-evicted")).length).toBe(1);
    expect(await eventRows("not-a-sandbox")).toEqual([]);
  });

  it("reattaches the log follower after a server-side stream drop (边跑边收 heartbeat)", async () => {
    startObserver();
    k8sServer.seedPod({ namespace: NS, name: "sbx-drop", labels: { app: "community-sandbox" } });
    await expect.poll(async () => (await eventRows("sbx-drop")).length).toBeGreaterThanOrEqual(1);
    k8sServer.appendPodLog(NS, "sbx-drop", "before drop");
    await expect.poll(() => logLines("sbx-drop")).toEqual(["before drop"]);

    k8sServer.dropLogFollowers(NS, "sbx-drop");

    await expect.poll(async () => {
      k8sServer.appendPodLog(NS, "sbx-drop", "after drop");
      return (await logLines("sbx-drop")).includes("after drop");
    }).toBe(true);
  });

  it("pins tenant/session correlation at observation time — surviving the reaper's activity cleanup", async () => {
    await recordSandboxActivity("sbx-pinned", "tenant-pin", "session-pin");
    startObserver();
    k8sServer.seedPod({
      namespace: NS,
      name: "sbx-pinned",
      labels: { app: "community-sandbox" },
      annotations: { "community/session-id": "session-pin" },
    });
    await expect.poll(async () => (await eventRows("sbx-pinned")).length).toBeGreaterThanOrEqual(1);
    k8sServer.appendPodLog(NS, "sbx-pinned", "attributable line");
    await expect.poll(() => logLines("sbx-pinned")).toEqual(["attributable line"]);

    await deleteSandboxActivity("sbx-pinned");

    // 控制面读口按行出 tenant_id / session_id（主服务再把 lines[0] 的抬到顶层）。
    const res = await cp.get("/internal/sandboxes/logs?pod=sbx-pinned");
    const body = (await res.json()) as { lines: Array<{ tenant_id: string | null; session_id: string | null }> };
    expect(body.lines[0].tenant_id).toBe("tenant-pin");
    expect(body.lines[0].session_id).toBe("session-pin");
  });

  // 删掉原「归因补丁: a mint-time session identity resolves pods to (user, agent) on the read side」：
  // 读侧补 username / agent_id 是主服务 withSessionIdentity 的活（控制面读口不 join 身份表）。

  it("sandbox_session_identity 不在库里（控制面独立库）时，upsertSandboxPodMeta 照常落 meta、不抛", async () => {
    await getPool().query("DROP TABLE IF EXISTS sandbox_session_identity");
    await recordSandboxActivity("sbx-noident", "tenant-noident", "session-noident");
    await upsertSandboxPodMeta("sbx-noident", "session-noident");
    const { rows } = await getPool().query<{ tenant_id: string | null; session_id: string | null }>(
      "SELECT tenant_id, session_id FROM sandbox_pod_meta WHERE pod_name = $1",
      ["sbx-noident"],
    );
    expect(rows).toEqual([{ tenant_id: "tenant-noident", session_id: "session-noident" }]);
  });

  it("归因补丁: activity 行已被回收时，tenant 仍从 mint 身份行落上（2026-07-29 生产实证）", async () => {

    await recordSandboxSessionIdentity("session-noact", "tenant-noact", "observer-noact@corp", "agent-sar");
    await upsertSandboxPodMeta("sbx-noact", "session-noact");

    const { rows } = await getPool().query<{ tenant_id: string | null; username: string | null; agent_id: string | null }>(
      "SELECT tenant_id, username, agent_id FROM sandbox_pod_meta WHERE pod_name = $1",
      ["sbx-noact"],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].tenant_id, "activity 没了就丢租户 —— 正是本次修的 bug").toBe("tenant-noact");

    expect(rows[0].username).toBe("observer-noact@corp");
    expect(rows[0].agent_id).toBe("agent-sar");
  });

  it("归因补丁: 已有的 tenant 不会被身份行覆盖（COALESCE 只补空，不改写）", async () => {

    await recordSandboxActivity("sbx-keep", "tenant-from-activity", "session-keep");
    await recordSandboxSessionIdentity("session-keep", "tenant-from-identity", "gina@corp", "agent-x");
    await upsertSandboxPodMeta("sbx-keep", "session-keep");

    const { rows } = await getPool().query<{ tenant_id: string | null }>(
      "SELECT tenant_id FROM sandbox_pod_meta WHERE pod_name = $1",
      ["sbx-keep"],
    );
    expect(rows[0].tenant_id).toBe("tenant-from-activity");
  });

  it("salvages the log of a pod first seen already dead (crashed before any follower attached)", async () => {

    k8sServer.seedPod({ namespace: NS, name: "sbx-earlycrash", labels: { app: "community-sandbox" } });
    k8sServer.setPodLog(NS, "sbx-earlycrash", ["boot", "panic: died instantly"]);
    k8sServer.terminatePod(NS, "sbx-earlycrash", { exitCode: 2, reason: "Error" });
    startObserver();

    await expect.poll(() => logLines("sbx-earlycrash")).toEqual(["boot", "panic: died instantly"]);
    const rows = await eventRows("sbx-earlycrash");
    expect(rows.filter((e) => e.type === "LOG_SALVAGED").length).toBe(1);
  });

  it("records the CONTAINER_TERMINATED 遗言 and does NOT double-store an already-streamed log", async () => {
    startObserver();
    k8sServer.seedPod({ namespace: NS, name: "sbx-lastwords", labels: { app: "community-sandbox" } });
    await expect.poll(async () => (await eventRows("sbx-lastwords")).length).toBeGreaterThanOrEqual(1);
    k8sServer.appendPodLog(NS, "sbx-lastwords", "streamed line");
    await expect.poll(() => logLines("sbx-lastwords")).toEqual(["streamed line"]);

    k8sServer.terminatePod(NS, "sbx-lastwords", { exitCode: 137, reason: "OOMKilled", message: "tail: out of memory" });
    await expect.poll(async () =>
      (await eventRows("sbx-lastwords")).filter((e) => e.type === "CONTAINER_TERMINATED").length,
    ).toBe(1);
    const { rows } = await getPool().query<{ message: string; reason: string }>(
      "SELECT reason, message FROM sandbox_pod_events WHERE pod_name = $1 AND type = 'CONTAINER_TERMINATED'",
      ["sbx-lastwords"],
    );
    expect(rows[0].reason).toBe("OOMKilled");
    expect(rows[0].message).toBe("tail: out of memory");

    await new Promise((r) => setTimeout(r, 80));
    expect(await logLines("sbx-lastwords")).toEqual(["streamed line"]);
  });

  it("records LOG_UNSALVAGEABLE when the kubelet already GC'd a dead pod's log", async () => {
    k8sServer.seedPod({ namespace: NS, name: "sbx-gcd", labels: { app: "community-sandbox" } });
    k8sServer.terminatePod(NS, "sbx-gcd", { exitCode: 1 });
    k8sServer.setPodLogError(NS, "sbx-gcd", 400, 'container "sandbox" in pod "sbx-gcd" is terminated');
    startObserver();

    await expect.poll(async () =>
      (await eventRows("sbx-gcd")).filter((e) => e.type === "LOG_UNSALVAGEABLE").length,
    ).toBe(1);
    expect(await logLines("sbx-gcd")).toEqual([]);
  });

  it("caps stored lines per pod and marks the truncation once", async () => {
    startObserver({ maxLogLinesPerPod: 3 });
    k8sServer.seedPod({ namespace: NS, name: "sbx-chatty", labels: { app: "community-sandbox" } });
    await expect.poll(async () => (await eventRows("sbx-chatty")).length).toBeGreaterThanOrEqual(1);
    for (let i = 1; i <= 6; i++) k8sServer.appendPodLog(NS, "sbx-chatty", `line ${i}`);

    await expect.poll(() => logLines("sbx-chatty")).toEqual(["line 1", "line 2", "line 3"]);
    await expect.poll(async () =>
      (await eventRows("sbx-chatty")).filter((e) => e.type === "LOG_TRUNCATED").length,
    ).toBe(1);

    k8sServer.appendPodLog(NS, "sbx-chatty", "line 7");
    await new Promise((r) => setTimeout(r, 60));
    expect((await eventRows("sbx-chatty")).filter((e) => e.type === "LOG_TRUNCATED").length).toBe(1);
  });
});

describe("read routes", () => {
  it("GET /internal/sandboxes/logs returns captured lines with tenant/session correlation", async () => {
    startObserver();
    k8sServer.seedPod({ namespace: NS, name: "sbx-read", labels: { app: "community-sandbox" } });
    await recordSandboxActivity("sbx-read", "tenant-a", "session-42");
    await expect.poll(async () => (await eventRows("sbx-read")).length).toBeGreaterThanOrEqual(1);
    k8sServer.appendPodLog(NS, "sbx-read", "captured!");
    await expect.poll(() => logLines("sbx-read")).toEqual(["captured!"]);

    const res = await cp.get("/internal/sandboxes/logs?pod=sbx-read");
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      lines: Array<{ pod_name: string; tenant_id: string | null; session_id: string | null; line: string }>;
    };
    expect(body.lines[0].pod_name).toBe("sbx-read");
    expect(body.lines[0].tenant_id).toBe("tenant-a");
    expect(body.lines[0].session_id).toBe("session-42");
    expect(body.lines.map((l) => l.line)).toEqual(["captured!"]);
  });

  it("GET /internal/sandboxes/logs without pod is a 400", async () => {
    const res = await cp.get("/internal/sandboxes/logs");
    expect(res.status).toBe(400);
  });

  it("GET /internal/sandboxes/events filters by pod and returns newest first", async () => {
    await insertSandboxPodEvent({
      podName: "sbx-evfeed",
      source: "watch",
      type: "ADDED",
      reason: "Running",
      message: "first",
    });
    await insertSandboxPodEvent({
      podName: "sbx-evfeed",
      source: "watch",
      type: "DELETED",
      reason: "Deleted",
      message: "second",
    });
    const res = await cp.get("/internal/sandboxes/events?pod=sbx-evfeed");
    expect(res.status).toBe(200);
    const body = (await res.json()) as { events: Array<{ type: string }> };
    expect(body.events.map((e) => e.type)).toEqual(["DELETED", "ADDED"]);
  });
});
