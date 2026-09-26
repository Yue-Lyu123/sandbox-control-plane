import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { getPool } from "../src/db.js";
import { sandboxPodName } from "../src/control-plane.js";
import { runReapPass, SANDBOX_POD_LABEL_SELECTOR, type SandboxActivitySource } from "../src/reaper.js";
import type { PodJson } from "../src/k8s.js";
import { TEST_DATABASE_URL } from "./pglite-test-db.js";
import { startFakeAioServer, type FakeAioServer } from "./support/fake-aio-server.js";
import { startFakeK8sServer, type FakeK8sServer } from "./support/fake-k8s-server.js";
import { startCpServer, type CpServer } from "./support/cp-server.js";

const HOUR_MS = 3_600_000;
const isoAgo = (ms: number) => new Date(Date.now() - ms).toISOString();

let workspace: string;
let aio: FakeAioServer;
let k8sServer: FakeK8sServer;
let cp: CpServer;

beforeAll(async () => {
  workspace = fs.mkdtempSync(path.join(os.tmpdir(), "sandbox-reaper-"));
  aio = await startFakeAioServer(workspace);

  k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });

  process.env.SANDBOX_K8S_SERVER_URL = k8sServer.url;
  process.env.SANDBOX_K8S_TOKEN = "fake-token";
  process.env.SANDBOX_AIO_PORT = String(aio.port);
  delete process.env.SANDBOX_K8S_CA;
  process.env.DATABASE_URL = TEST_DATABASE_URL;
  cp = await startCpServer();
});

afterAll(async () => {
  await cp.close();
  await aio.close();
  await k8sServer.close();
  fs.rmSync(workspace, { recursive: true, force: true });

  delete process.env.SANDBOX_K8S_SERVER_URL;
  delete process.env.SANDBOX_K8S_TOKEN;
  delete process.env.SANDBOX_K8S_NAMESPACE;
  delete process.env.SANDBOX_K8S_CA;
  delete process.env.SANDBOX_AIO_PORT;
});

afterEach(() => {
  delete process.env.SANDBOX_IDLE_TTL_S;
  delete process.env.SANDBOX_MAX_AGE_S;
  delete process.env.SANDBOX_REAPER_INTERVAL_S;
});

// 原测试打 Next 路由 + 机器令牌解析租户；这里打控制面 HTTP 面，租户直接在 body 里（主服务已解析好）。
async function acquireViaRoute(tenant: string, session: string): Promise<string> {
  const res = await cp.post("/internal/sandboxes/acquire", { tenant, session_id: session });
  expect(res.status).toBe(200);
  return ((await res.json()) as { pod_name: string }).pod_name;
}

async function reapViaRoute(): Promise<{ checked: number; reaped: string[]; skipped: string[]; held: string[] }> {
  const res = await cp.post("/internal/sandboxes/reap", {});
  expect(res.status).toBe(200);
  const body = (await res.json()) as { checked: number; reaped: string[]; skipped: string[]; held: string[] };

  expect(Object.keys(body).sort()).toEqual(["checked", "held", "reaped", "skipped"]);
  expect(body.reaped.length + body.skipped.length).toBe(body.checked);
  return body;
}

async function activityRow(podName: string): Promise<{ tenant_id: string; session_id: string } | null> {
  const { rows } = await getPool().query<{ tenant_id: string; session_id: string }>(
    "SELECT tenant_id, session_id FROM sandbox_activity WHERE pod_name = $1",
    [podName],
  );
  return rows[0] ?? null;
}

async function backdateActivity(podName: string, msAgo: number): Promise<void> {
  const r = await getPool().query("UPDATE sandbox_activity SET last_activity_at = $2 WHERE pod_name = $1", [
    podName,
    new Date(Date.now() - msAgo),
  ]);
  expect(r.rowCount).toBe(1);
}

describe("POST /internal/sandboxes/reap — idle-TTL reaping", () => {
  it("reaps a pod idle beyond SANDBOX_IDLE_TTL_S, keeps a recently-active one, and cleans up only the reaped pod's sandbox_activity row", async () => {
    const ns = `reap-idle-${randomUUID().slice(0, 8)}`;
    process.env.SANDBOX_K8S_NAMESPACE = ns;

    const tenantOld = `t-old-${randomUUID()}`;
    const tenantNew = `t-new-${randomUUID()}`;
    const podOld = await acquireViaRoute(tenantOld, "s-old");
    const podNew = await acquireViaRoute(tenantNew, "s-new");
    expect(podOld).toBe(sandboxPodName(tenantOld, "s-old"));

    expect(await activityRow(podOld)).toEqual({ tenant_id: tenantOld, session_id: "s-old" });

    k8sServer.setCreationTimestamp(ns, podOld, isoAgo(2 * HOUR_MS));
    await backdateActivity(podOld, 2 * HOUR_MS);

    process.env.SANDBOX_IDLE_TTL_S = "600"; 
    const result = await reapViaRoute();

    expect(result.checked).toBe(2);
    expect(result.reaped).toEqual([podOld]);
    expect(result.skipped).toEqual([podNew]);
    expect(k8sServer.hasPod(ns, podOld)).toBe(false);
    expect(k8sServer.hasPod(ns, podNew)).toBe(true);

    expect(await activityRow(podOld)).toBeNull();
    expect(await activityRow(podNew)).not.toBeNull();
  });

  it("activity recorded by execute genuinely postpones reaping", async () => {
    const ns = `reap-postpone-${randomUUID().slice(0, 8)}`;
    process.env.SANDBOX_K8S_NAMESPACE = ns;

    const tenant = `t-${randomUUID()}`;
    const pod = await acquireViaRoute(tenant, "s-exec");

    k8sServer.setCreationTimestamp(ns, pod, isoAgo(2 * HOUR_MS));
    await backdateActivity(pod, 2 * HOUR_MS);

    const execRes = await cp.post("/internal/sandboxes/execute", {
      tenant,
      session_id: "s-exec",
      command: "echo alive",
    });
    expect(execRes.status).toBe(200);

    process.env.SANDBOX_IDLE_TTL_S = "600";
    const first = await reapViaRoute();
    expect(first.reaped).toEqual([]); 
    expect(first.skipped).toEqual([pod]);
    expect(k8sServer.hasPod(ns, pod)).toBe(true);

    await backdateActivity(pod, 2 * HOUR_MS);
    const second = await reapViaRoute();
    expect(second.reaped).toEqual([pod]);
    expect(k8sServer.hasPod(ns, pod)).toBe(false);
  });

  it("a pod with NO sandbox_activity row falls back to creationTimestamp: reaped when old, kept when fresh", async () => {
    const ns = `reap-fallback-${randomUUID().slice(0, 8)}`;
    process.env.SANDBOX_K8S_NAMESPACE = ns;

    const labels = { app: "community-sandbox", "sandbox-name": "irrelevant" };
    k8sServer.seedPod({ namespace: ns, name: "sbx-orphan000a", labels, creationTimestamp: isoAgo(2 * HOUR_MS) });
    k8sServer.seedPod({ namespace: ns, name: "sbx-young000b", labels, creationTimestamp: isoAgo(60_000) });
    expect(await activityRow("sbx-orphan000a")).toBeNull();

    process.env.SANDBOX_IDLE_TTL_S = "600";
    const result = await reapViaRoute();

    expect(result.checked).toBe(2);
    expect(result.reaped).toEqual(["sbx-orphan000a"]);
    expect(result.skipped).toEqual(["sbx-young000b"]);
    expect(k8sServer.hasPod(ns, "sbx-orphan000a")).toBe(false);
    expect(k8sServer.hasPod(ns, "sbx-young000b")).toBe(true);
  });

  it("SANDBOX_MAX_AGE_S reaps even a recently-active pod once its absolute age exceeds it", async () => {
    const ns = `reap-maxage-${randomUUID().slice(0, 8)}`;
    process.env.SANDBOX_K8S_NAMESPACE = ns;

    const tenant = `t-${randomUUID()}`;
    const pod = await acquireViaRoute(tenant, "s-aged"); 
    k8sServer.setCreationTimestamp(ns, pod, isoAgo(2 * HOUR_MS)); 

    process.env.SANDBOX_IDLE_TTL_S = "999999";
    process.env.SANDBOX_MAX_AGE_S = "3600"; 
    const result = await reapViaRoute();

    expect(result.reaped).toEqual([pod]);
    expect(k8sServer.hasPod(ns, pod)).toBe(false);
    expect(await activityRow(pod)).toBeNull();
  });
});

describe("double guard: only sbx-* pods WITH the app=community-sandbox label are ever deleted", () => {
  it("never touches unlabeled or non-sbx pods in the namespace, no matter how ancient (route-level)", async () => {
    const ns = `reap-guard-${randomUUID().slice(0, 8)}`;
    process.env.SANDBOX_K8S_NAMESPACE = ns;

    k8sServer.seedPod({
      namespace: ns,
      name: "legacy-ancient",
      labels: { app: "community-sandbox" },
      creationTimestamp: isoAgo(100 * HOUR_MS),
    });

    k8sServer.seedPod({ namespace: ns, name: "sbx-nolabel001", labels: {}, creationTimestamp: isoAgo(100 * HOUR_MS) });
    k8sServer.seedPod({
      namespace: ns,
      name: "sbx-otherapp02",
      labels: { app: "someone-elses-app" },
      creationTimestamp: isoAgo(100 * HOUR_MS),
    });

    process.env.SANDBOX_IDLE_TTL_S = "600";
    const result = await reapViaRoute();

    expect(result.checked).toBe(1); 
    expect(result.reaped).toEqual([]);
    expect(result.skipped).toEqual(["legacy-ancient"]);
    expect(k8sServer.hasPod(ns, "legacy-ancient")).toBe(true);
    expect(k8sServer.hasPod(ns, "sbx-nolabel001")).toBe(true);
    expect(k8sServer.hasPod(ns, "sbx-otherapp02")).toBe(true);
  });

  it("re-checks BOTH guard conditions in code even if the API server ignores the label selector (unit-level, stubbed list)", async () => {

    const ancient = isoAgo(100 * HOUR_MS);
    const pods: PodJson[] = [
      { metadata: { name: "sbx-nolabel001", labels: {}, creationTimestamp: ancient } },
      { metadata: { name: "critical-db-0", labels: { app: "community-sandbox" }, creationTimestamp: ancient } },
      { metadata: { name: "sbx-legit00000", labels: { app: "community-sandbox" }, creationTimestamp: ancient } },
    ];
    const deleteCalls: string[] = [];
    const listSelectors: Array<string | undefined> = [];
    const stubK8s = {
      listPods: async (_ns: string, selector?: string) => {
        listSelectors.push(selector);
        return pods;
      },
      delete: async (_ns: string, _plural: string, name: string) => {
        deleteCalls.push(name);
        return { status: 200, body: "" };
      },
    };
    const cleared: string[] = [];
    const activity: SandboxActivitySource = {
      lastActivityFor: async () => new Map(),
      clear: async (name) => {
        cleared.push(name);
      },
    };

    const result = await runReapPass(stubK8s, "any-ns", activity, { idleTtlS: 600 });

    expect(listSelectors).toEqual([SANDBOX_POD_LABEL_SELECTOR]); 
    expect(deleteCalls).toEqual(["sbx-legit00000"]); 
    expect(cleared).toEqual(["sbx-legit00000"]);
    expect(result).toEqual({
      checked: 3,
      reaped: ["sbx-legit00000"],
      skipped: ["sbx-nolabel001", "critical-db-0"],

      held: [],
    });
  });
});

describe("reap endpoint configuration + auth gating", () => {
  it("503s when SANDBOX_IDLE_TTL_S is unset or invalid (reaping is strictly opt-in)", async () => {
    process.env.SANDBOX_K8S_NAMESPACE = "reap-unconfigured";

    delete process.env.SANDBOX_IDLE_TTL_S;
    let res = await cp.post("/internal/sandboxes/reap", {});
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({
      detail: "sandbox reaper not configured (SANDBOX_IDLE_TTL_S unset or invalid)",
    });

    for (const invalid of ["not-a-number", "0", "-30"]) {
      process.env.SANDBOX_IDLE_TTL_S = invalid;
      res = await cp.post("/internal/sandboxes/reap", {});
      expect(res.status).toBe(503);
    }
  });

  it("503s with the standard control-plane message when the SANDBOX_K8S_* env is unset", async () => {
    const saved = process.env.SANDBOX_K8S_SERVER_URL;
    delete process.env.SANDBOX_K8S_SERVER_URL;
    try {
      process.env.SANDBOX_IDLE_TTL_S = "600";
      const res = await cp.post("/internal/sandboxes/reap", {});
      expect(res.status).toBe(503);
      // 控制面出线多一个 error（类名原文），主服务客户端据此重建 SandboxNotConfiguredError。
      expect(await res.json()).toEqual({
        detail: "sandbox control plane not configured",
        error: "SandboxNotConfiguredError",
      });
    } finally {
      process.env.SANDBOX_K8S_SERVER_URL = saved;
    }
  });

  it("sits behind the inbound bearer gate (401 without/with wrong token, pass-through with the right one)", async () => {
    process.env.SANDBOX_IDLE_TTL_S = "600";
    process.env.SANDBOX_K8S_NAMESPACE = `reap-gate-${randomUUID().slice(0, 8)}`;

    const noAuth = await fetch(`${cp.url}/internal/sandboxes/reap`, { method: "POST", body: "{}" });
    expect(noAuth.status).toBe(401);
    expect(await noAuth.json()).toEqual({ detail: "unauthorized" });

    const wrong = await cp.post("/internal/sandboxes/reap", {}, { authorization: "Bearer nope" });
    expect(wrong.status).toBe(401);

    const right = await cp.post("/internal/sandboxes/reap", {});
    expect(right.status).toBe(200);
  });
});
