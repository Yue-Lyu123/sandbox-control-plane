import { describe, expect, test } from "vitest";
import {
  computeCapacity,
  summarizeControlPlaneHealth,
  type ReaperConfig,
} from "../src/control-plane-health.js";
import type { PodJson, ResourceQuotaJson } from "../src/k8s.js";

const NOW = new Date("2026-07-30T12:00:00Z");
const REAPER: ReaperConfig = { idleTtlS: 1800, maxAgeS: 21600, intervalS: 300 };

function pod(name: string, opts: { ageS?: number; phase?: string } = {}): PodJson {
  const created = new Date(NOW.getTime() - (opts.ageS ?? 60) * 1000).toISOString();
  return {
    metadata: { name, creationTimestamp: created, labels: { app: "community-sandbox" } },
    status: { phase: opts.phase ?? "Running" },
  } as unknown as PodJson;
}

function prodQuota(usedMem = "48384Mi", usedCpu = "19700m"): ResourceQuotaJson {
  return {
    metadata: { name: "quota-ns-mracs8jx" },
    status: {
      hard: { "limits.cpu": "32", "limits.memory": "64Gi", "limits.ephemeral-storage": "100Gi" },
      used: { "limits.cpu": usedCpu, "limits.memory": usedMem, "limits.ephemeral-storage": "42212Mi" },
    },
  } as unknown as ResourceQuotaJson;
}

describe("容量推算", () => {
  test("按 limits 算，不是按 requests —— 配额算的是上限", () => {

    const cap = computeCapacity([prodQuota()], []);
    expect(cap.remainingSandboxes).toBe(8);
    expect(cap.boundBy).toBe("limits.memory");
  });

  test("取最紧的那个维度，不是第一个能算的", () => {

    const cap = computeCapacity([prodQuota("10Gi", "31000m")], []);
    expect(cap.remainingSandboxes).toBe(1);
    expect(cap.boundBy).toBe("limits.cpu");
  });

  test("带 scope 的配额一律跳过（未必落在我们这种 pod 上，拿它算会低报）", () => {
    const scoped = {
      metadata: { name: "scoped" },
      spec: { scopes: ["BestEffort"] },
      status: { hard: { "limits.memory": "1Gi" }, used: { "limits.memory": "1Gi" } },
    } as unknown as ResourceQuotaJson;
    expect(computeCapacity([scoped], []).remainingSandboxes).toBeNull();
  });

  test("配额读不到时容量是 null（未知），不是 0（开不出来）", () => {
    expect(computeCapacity([], []).remainingSandboxes).toBeNull();
  });
});

describe("待命 vs 在用：不能混成一个数", () => {

  function poolPod(name: string): PodJson {
    const p = pod(name) as unknown as { metadata: { labels: Record<string, string> } };
    p.metadata.labels["sandbox-role"] = "pool";
    return p as unknown as PodJson;
  }
  function sessionPod(name: string): PodJson {
    const p = pod(name) as unknown as { metadata: { labels: Record<string, string> } };
    p.metadata.labels["sandbox-role"] = "session";
    return p as unknown as PodJson;
  }

  test("未认领的池子 pod 计进「待命」，不计进「在用」", () => {
    const cap = computeCapacity([prodQuota()], [poolPod("sbx-pool-a"), poolPod("sbx-pool-b")]);
    expect(cap.idlePool).toBe(2);
    expect(cap.inUse).toBe(0);
    expect(cap.liveSandboxes).toBe(2);
  });

  test("被认领的（role=session）计进「在用」", () => {
    const cap = computeCapacity([prodQuota()], [poolPod("sbx-pool-a"), sessionPod("sbx-pool-b")]);
    expect(cap.idlePool).toBe(1);
    expect(cap.inUse).toBe(1);
  });

  test("没有 role 标签的老 pod（直接创建的会话）计进「在用」", () => {
    const cap = computeCapacity([prodQuota()], [pod("sbx-legacy00001")]);
    expect(cap.idlePool).toBe(0);
    expect(cap.inUse).toBe(1);
  });

  test("两个数相加恒等于总数 —— 不允许有 pod 两边都不算", () => {
    const pods = [poolPod("sbx-pool-a"), sessionPod("sbx-pool-b"), pod("sbx-legacy00001")];
    const cap = computeCapacity([prodQuota()], pods);
    expect(cap.idlePool + cap.inUse).toBe(cap.liveSandboxes);
  });
});

describe("回收器活性：用后果推断，不靠心跳", () => {
  test("超过绝对寿命 + 两个扫描周期仍在的 pod => critical", () => {
    const stale = pod("sbx-stale", { ageS: 21600 + 601 }); 
    const h = summarizeControlPlaneHealth([stale], [prodQuota()], REAPER, { now: NOW });
    const c = h.checks.find((x) => x.id === "reaper-overdue");
    expect(c?.severity).toBe("critical");
    expect(c?.evidence).toEqual(["sbx-stale"]);
    expect(h.severity).toBe("critical");
  });

  test("刚过寿命但还在宽限期内 => 不报（还没轮到它扫）", () => {
    const fresh = pod("sbx-justover", { ageS: 21600 + 60 });
    const h = summarizeControlPlaneHealth([fresh], [prodQuota()], REAPER, { now: NOW });
    expect(h.checks.find((x) => x.id === "reaper-overdue")).toBeUndefined();
  });

  test("回收器没配全 => warn（沙箱永远不会被回收）", () => {
    const h = summarizeControlPlaneHealth([], [prodQuota()], { idleTtlS: 1800, maxAgeS: null, intervalS: 300 }, {
      now: NOW,
    });
    expect(h.checks.find((x) => x.id === "reaper-configured")?.severity).toBe("warn");
  });
});

describe("尸体与卡住", () => {
  test("Failed/Succeeded 的沙箱 => warn，且列出是哪几个", () => {
    const h = summarizeControlPlaneHealth(
      [pod("sbx-dead", { phase: "Failed" }), pod("sbx-done", { phase: "Succeeded" }), pod("sbx-ok")],
      [prodQuota()],
      REAPER,
      { now: NOW },
    );
    const c = h.checks.find((x) => x.id === "terminal-corpses");
    expect(c?.severity).toBe("warn");
    expect(c?.evidence).toEqual(["sbx-dead (Failed)", "sbx-done (Succeeded)"]);
  });

  test("Pending 超过就绪预算 => warn（冷镜像拉取是最常见原因）", () => {
    const h = summarizeControlPlaneHealth([pod("sbx-pending", { ageS: 400, phase: "Pending" })], [prodQuota()], REAPER, {
      now: NOW,
    });
    const c = h.checks.find((x) => x.id === "stuck-pending");
    expect(c?.severity).toBe("warn");
    expect(c?.detail).toContain("冷拉");
  });

  test("Pending 但还在预算内 => 不报（正常启动过程）", () => {
    const h = summarizeControlPlaneHealth([pod("sbx-starting", { ageS: 30, phase: "Pending" })], [prodQuota()], REAPER, {
      now: NOW,
    });
    expect(h.checks.find((x) => x.id === "stuck-pending")).toBeUndefined();
  });
});

describe("容量告警", () => {
  test("开不出新沙箱 => critical", () => {
    const h = summarizeControlPlaneHealth([], [prodQuota("64Gi")], REAPER, { now: NOW });
    expect(h.checks.find((x) => x.id === "capacity-exhausted")?.severity).toBe("critical");
  });

  test("只够再开 2 个 => warn", () => {

    const h = summarizeControlPlaneHealth([], [prodQuota("61440Mi")], REAPER, { now: NOW });
    expect(h.checks.find((x) => x.id === "capacity-low")?.severity).toBe("warn");
  });
});

describe("元检查：预检有没有变回空转", () => {

  test("配额约束的维度与预检会看的完全不相交 => warn", () => {
    const exotic = {
      metadata: { name: "exotic" },
      status: {
        hard: { "requests.storage": "100Gi", "services.nodeports": "10" },
        used: { "requests.storage": "36Gi", "services.nodeports": "3" },
      },
    } as unknown as ResourceQuotaJson;
    const h = summarizeControlPlaneHealth([], [exotic], REAPER, { now: NOW });
    const c = h.checks.find((x) => x.id === "precheck-blind");
    expect(c?.severity).toBe("warn");
    expect(c?.evidence).toEqual(["requests.storage", "services.nodeports"]);
  });

  test("修复后的现网配额形状（卡 limits.*）不再算盲区", () => {
    const h = summarizeControlPlaneHealth([], [prodQuota()], REAPER, { now: NOW });
    expect(h.checks.find((x) => x.id === "precheck-blind")).toBeUndefined();
  });
});

describe("一切正常", () => {
  test("没有任何检查项，总体 ok —— 绿灯本身也是信息", () => {
    const h = summarizeControlPlaneHealth([pod("sbx-fine")], [prodQuota()], REAPER, { now: NOW });
    expect(h.checks).toEqual([]);
    expect(h.severity).toBe("ok");
    expect(h.capacity.liveSandboxes).toBe(1);
    expect(h.checkedAt).toBe(NOW.toISOString());
  });
});
