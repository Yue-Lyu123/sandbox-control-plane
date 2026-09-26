import { describe, expect, it } from "vitest";
import { CLAIMED_AT_ANNOTATION, lifetimeStartMs } from "../src/control-plane.js";
import type { PodJson } from "../src/k8s.js";
import { runReapPass, type SandboxActivitySource } from "../src/reaper.js";

const NOW = new Date("2026-07-31T12:00:00Z");
const MIN = 60_000;

function pod(opts: { createdMinAgo: number; claimedMinAgo?: number; name?: string }): PodJson {
  const annotations: Record<string, string> = {};
  if (opts.claimedMinAgo !== undefined) {
    annotations[CLAIMED_AT_ANNOTATION] = new Date(NOW.getTime() - opts.claimedMinAgo * MIN).toISOString();
  }
  return {
    metadata: {
      name: opts.name ?? "sbx-pool-abc",
      creationTimestamp: new Date(NOW.getTime() - opts.createdMinAgo * MIN).toISOString(),
      labels: { app: "community-sandbox", "sandbox-role": "session" },
      annotations,
    },
    status: { phase: "Running" },
  } as unknown as PodJson;
}

describe("lifetimeStartMs：认领时间优先，创建时间兜底", () => {
  it("有认领注解 -> 用认领时间（池子 pod 的既往年龄不侵蚀会话预算）", () => {
    const p = pod({ createdMinAgo: 25, claimedMinAgo: 2 });
    expect(lifetimeStartMs(p)).toBe(NOW.getTime() - 2 * MIN);
  });

  it("没有注解 -> 用创建时间（直接创建的 pod，行为与引入池子之前一致）", () => {
    const p = pod({ createdMinAgo: 25 });
    expect(lifetimeStartMs(p)).toBe(NOW.getTime() - 25 * MIN);
  });

  it("注解是垃圾 -> 退回创建时间，不抛也不返回 NaN", () => {
    const p = pod({ createdMinAgo: 10 });
    (p.metadata as { annotations: Record<string, string> }).annotations[CLAIMED_AT_ANNOTATION] = "不是时间";
    expect(lifetimeStartMs(p)).toBe(NOW.getTime() - 10 * MIN);
  });

  it("两个都读不出来 -> null（调用方据此 fail-safe：年龄未知绝不删）", () => {
    expect(lifetimeStartMs({ metadata: {} })).toBeNull();
  });
});

describe("回收器按认领时间算绝对寿命", () => {
  const activity: SandboxActivitySource = {

    lastActivityFor: async (names) => new Map(names.map((n) => [n, new Date(NOW.getTime() - MIN)])),
    clear: async () => undefined,
  };
  function k8s(pods: PodJson[], deleted: string[]) {
    return {
      listPods: async () => pods,
      delete: async (_ns: string, _p: string, n: string) => {
        deleted.push(n);
        return { status: 200, body: "" };
      },
    };
  }

  it("pod 已活 80 分钟但刚被认领 -> **不收**（用户拿到完整预算，这就是那个 bug 的修复）", async () => {
    const deleted: string[] = [];
    await runReapPass(k8s([pod({ createdMinAgo: 80, claimedMinAgo: 1 })], deleted), "ns", activity, {
      idleTtlS: 1800,
      maxAgeS: 90 * 60,
      now: NOW,
    });
    expect(deleted).toEqual([]);
  });

  it("认领至今已超过绝对寿命 -> 照收（认领时间不是免死金牌）", async () => {
    const deleted: string[] = [];
    await runReapPass(k8s([pod({ createdMinAgo: 200, claimedMinAgo: 95 })], deleted), "ns", activity, {
      idleTtlS: 1800,
      maxAgeS: 90 * 60,
      now: NOW,
    });
    expect(deleted).toEqual(["sbx-pool-abc"]);
  });

  it("没有认领注解的老 pod -> 按创建时间收，行为与引入池子之前逐字节一致", async () => {
    const deleted: string[] = [];
    await runReapPass(
      k8s([pod({ createdMinAgo: 200, name: "sbx-legacy00001" })], deleted),
      "ns",
      activity,
      { idleTtlS: 1800, maxAgeS: 90 * 60, now: NOW },
    );
    expect(deleted).toEqual(["sbx-legacy00001"]);
  });

  it("回收器的到期时刻 === lifetimeStartMs + maxAge（与 expires_at 同一个公式）", async () => {
    const maxAgeS = 90 * 60;
    const p = pod({ createdMinAgo: 200, claimedMinAgo: 90 }); 
    const predicted = lifetimeStartMs(p)! + maxAgeS * 1000;

    const before: string[] = [];
    await runReapPass(k8s([p], before), "ns", activity, {
      idleTtlS: 1800,
      maxAgeS,
      now: new Date(predicted - 1000),
    });
    expect(before).toEqual([]);

    const after: string[] = [];
    await runReapPass(k8s([p], after), "ns", activity, {
      idleTtlS: 1800,
      maxAgeS,
      now: new Date(predicted + 1000),
    });
    expect(after).toEqual(["sbx-pool-abc"]);
  });
});
