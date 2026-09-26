import { describe, expect, test } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { K8sClient } from "../src/k8s.js";
import { SandboxControlPlane, sandboxPodName } from "../src/control-plane.js";
import { startFakeAioServer, type FakeAioServer } from "./support/fake-aio-server.js";
import { startFakeK8sServer, type FakeK8sServer } from "./support/fake-k8s-server.js";

const NS = "default";

async function setup(activityHeartbeatMs?: number) {
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "fake-aio-hb-"));
  const aio = await startFakeAioServer(workspace);
  const k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });
  const k8s = new K8sClient({ server: k8sServer.url, token: "fake-token", namespace: NS });

  const stamps: { podName: string; tenant: string; sessionId: string; at: number }[] = [];
  const plane = new SandboxControlPlane(k8s, {
    port: aio.port,
    activityRecorder: (info) => {
      stamps.push({ ...info, at: Date.now() });
    },
    activityHeartbeatMs,
  });
  return { workspace, aio, k8sServer, plane, stamps };
}

async function teardown(ctx: { aio: FakeAioServer; k8sServer: FakeK8sServer; workspace: string }) {
  await ctx.aio.close();
  await ctx.k8sServer.close();
  fs.rmSync(ctx.workspace, { recursive: true, force: true });
}

describe("执行中活动心跳", () => {
  test("长命令执行期间持续打点 —— 不再出现「跑着跑着被当成闲置」的空窗", async () => {
    const ctx = await setup(50);
    try {
      const tenant = "acme";
      const session = "sess-heartbeat";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      const afterAcquire = ctx.stamps.length;
      expect(afterAcquire).toBeGreaterThanOrEqual(1); 

      const startedAt = Date.now();
      const result = await ctx.plane.execute(tenant, session, "sleep 0.6");
      expect(result.exitCode).toBe(0);

      const during = ctx.stamps.filter((s) => s.at > startedAt && s.at < startedAt + 600);
      expect(during.length).toBeGreaterThanOrEqual(2);

      const podName = sandboxPodName(tenant, session);
      expect(ctx.stamps.every((s) => s.podName === podName && s.sessionId === session)).toBe(true);

      const times = ctx.stamps.map((s) => s.at).sort((a, b) => a - b);
      const maxGap = times.slice(1).reduce((m, t, i) => Math.max(m, t - times[i]), 0);
      expect(maxGap).toBeLessThan(500);
    } finally {
      await teardown(ctx);
    }
  });

  test("心跳关闭时（间隔 0）退回原行为：执行期间零打点，只有终点戳", async () => {
    const ctx = await setup(0);
    try {
      const tenant = "acme";
      const session = "sess-no-heartbeat";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      const before = ctx.stamps.length;

      const startedAt = Date.now();
      await ctx.plane.execute(tenant, session, "sleep 0.4");

      expect(ctx.stamps.length).toBe(before + 1);
      expect(ctx.stamps[ctx.stamps.length - 1].at).toBeGreaterThanOrEqual(startedAt + 400);
    } finally {
      await teardown(ctx);
    }
  });

  test("打点函数抛异常，命令照常成功", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "fake-aio-hb-throw-"));
    const aio = await startFakeAioServer(workspace);
    const k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });
    const k8s = new K8sClient({ server: k8sServer.url, token: "fake-token", namespace: NS });
    let calls = 0;
    const plane = new SandboxControlPlane(k8s, {
      port: aio.port,
      activityRecorder: () => {
        calls += 1;
        throw new Error("activity DB down");
      },
      activityHeartbeatMs: 50,
    });
    try {
      await plane.acquire("acme", "sess-hb-throw", { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      const result = await plane.execute("acme", "sess-hb-throw", "sleep 0.3");
      expect(result.exitCode).toBe(0);
      expect(calls).toBeGreaterThanOrEqual(2); 
    } finally {
      await teardown({ aio, k8sServer, workspace });
    }
  });
});
