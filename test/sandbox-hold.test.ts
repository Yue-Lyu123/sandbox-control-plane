import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { getPool } from "../src/db.js";
import {
  SANDBOX_HOLD_MAX_MS,
  heldUntilFor,
  lastActivityFor,
  recordSandboxActivity,
  recordSandboxHold,
} from "../src/activity.js";
import { sandboxPodName } from "../src/control-plane.js";
import type { PodJson } from "../src/k8s.js";
import { runReapPass, type SandboxActivitySource } from "../src/reaper.js";
import { TEST_DATABASE_URL } from "./pglite-test-db.js";
import { startCpServer, type CpServer } from "./support/cp-server.js";

const MIN = 60_000;
const HOUR = 3_600_000;

const SALT = randomUUID().slice(0, 8);
const podFor = (s: string) => sandboxPodName("acme", `${SALT}-${s}`);

let cp: CpServer;

beforeAll(async () => {
  process.env.DATABASE_URL = TEST_DATABASE_URL;
  cp = await startCpServer();

  process.env.SANDBOX_K8S_SERVER_URL = "http://127.0.0.1:1";
  process.env.SANDBOX_K8S_TOKEN = "fake-token";
  process.env.SANDBOX_K8S_NAMESPACE = "ns-hold-test";
});

afterAll(async () => {
  await cp.close();
  delete process.env.SANDBOX_K8S_SERVER_URL;
  delete process.env.SANDBOX_K8S_TOKEN;
  delete process.env.SANDBOX_K8S_NAMESPACE;
  const pool = getPool();
  await pool.query("DELETE FROM sandbox_activity WHERE pod_name LIKE $1", ["sbx-%"]).catch(() => undefined);
});

function pod(name: string, ageMs: number): PodJson {
  return {
    metadata: {
      name,
      creationTimestamp: new Date(Date.now() - ageMs).toISOString(),
      labels: { app: "community-sandbox" },
    },
    status: { phase: "Running" },
  } as unknown as PodJson;
}

function source(cleared: string[]): SandboxActivitySource {
  return {
    lastActivityFor,
    heldUntilFor: (names) => heldUntilFor(names),
    clear: async (name) => {
      cleared.push(name);
    },
  };
}

describe("声明式保活：存储语义", () => {
  it("hold 落在独立列上，绝不改 last_activity_at —— 声明不是伪造的活动", async () => {
    const name = podFor("sep");
    const activityAt = new Date(Date.now() - 90 * MIN);
    await recordSandboxActivity(name, "acme", "s-sep", activityAt);
    await recordSandboxHold(name, "acme", "s-sep", 30 * MIN, "approval_id=abc");

    const { rows } = await getPool().query<{
      last_activity_at: Date;
      held_until: Date | null;
      hold_reason: string | null;
    }>("SELECT last_activity_at, held_until, hold_reason FROM sandbox_activity WHERE pod_name = $1", [name]);

    expect(rows[0].last_activity_at.getTime()).toBe(activityAt.getTime());
    expect(rows[0].held_until).not.toBeNull();
    expect(rows[0].held_until!.getTime()).toBeGreaterThan(Date.now());
    expect(rows[0].hold_reason).toBe("approval_id=abc");
  });

  it("ttl 超过上限被 clamp 到 2 小时（clamp 在数据层，绕过路由也逃不掉）", async () => {
    const name = podFor("clamp");
    const before = Date.now();
    const until = await recordSandboxHold(name, "acme", "s-clamp", 999 * HOUR);
    expect(until.getTime() - before).toBeLessThanOrEqual(SANDBOX_HOLD_MAX_MS + 1000);
    expect(until.getTime() - before).toBeGreaterThan(SANDBOX_HOLD_MAX_MS - 5000);
  });

  it("重复调用只延后、绝不缩短已有窗口（要提前结束请调 release）", async () => {
    const name = podFor("refresh");
    const long = await recordSandboxHold(name, "acme", "s-refresh", 90 * MIN);
    const short = await recordSandboxHold(name, "acme", "s-refresh", 1 * MIN);

    expect(short.getTime()).toBeLessThan(long.getTime()); 
    const held = await heldUntilFor([name]);
    expect(held.get(name)!.until.getTime()).toBe(long.getTime()); 
  });

  it("过期的声明不出现在读取结果里（过期判定在数据层做完）", async () => {
    const name = podFor("expired");
    await recordSandboxHold(name, "acme", "s-expired", 60 * MIN);
    await getPool().query("UPDATE sandbox_activity SET held_until = $1 WHERE pod_name = $2", [
      new Date(Date.now() - MIN),
      name,
    ]);
    expect((await heldUntilFor([name])).has(name)).toBe(false);
  });
});

describe("声明式保活：回收器行为", () => {
  const IDLE_TTL_S = 1800; 
  const MAX_AGE_S = 6 * 3600; 

  function k8s(pods: PodJson[], deleted: string[]) {
    return {
      listPods: async () => pods,
      delete: async (_ns: string, _plural: string, name: string) => {
        deleted.push(name);
        return { status: 200, body: "" };
      },
    };
  }

  it("闲置超时但有有效声明 -> 不收，且计进 held", async () => {
    const name = podFor("keep");
    await recordSandboxActivity(name, "acme", "s-keep", new Date(Date.now() - 60 * MIN)); 
    await recordSandboxHold(name, "acme", "s-keep", 30 * MIN, "waiting_approval");

    const deleted: string[] = [];
    const r = await runReapPass(k8s([pod(name, 60 * MIN)], deleted), "ns", source([]), {
      idleTtlS: IDLE_TTL_S,
      maxAgeS: MAX_AGE_S,
    });

    expect(deleted).toEqual([]);
    expect(r.held).toEqual([name]);
    expect(r.skipped).toContain(name); 
    expect(r.reaped.length + r.skipped.length).toBe(r.checked); 
  });

  it("**绝对寿命照杀** —— 声明挡不住它（hold 泄漏时的唯一防线）", async () => {
    const name = podFor("maxage");
    await recordSandboxActivity(name, "acme", "s-maxage", new Date(Date.now() - 60 * MIN));
    await recordSandboxHold(name, "acme", "s-maxage", 2 * HOUR, "waiting_approval");

    const deleted: string[] = [];
    const r = await runReapPass(k8s([pod(name, 7 * HOUR)], deleted), "ns", source([]), {
      idleTtlS: IDLE_TTL_S,
      maxAgeS: MAX_AGE_S,
    });

    expect(deleted).toEqual([name]);
    expect(r.reaped).toEqual([name]);
    expect(r.held).toEqual([]);
  });

  it("声明过期后恢复正常回收", async () => {
    const name = podFor("lapsed");
    await recordSandboxActivity(name, "acme", "s-lapsed", new Date(Date.now() - 60 * MIN));
    await recordSandboxHold(name, "acme", "s-lapsed", 60 * MIN);
    await getPool().query("UPDATE sandbox_activity SET held_until = $1 WHERE pod_name = $2", [
      new Date(Date.now() - MIN),
      name,
    ]);

    const deleted: string[] = [];
    const r = await runReapPass(k8s([pod(name, 60 * MIN)], deleted), "ns", source([]), {
      idleTtlS: IDLE_TTL_S,
      maxAgeS: MAX_AGE_S,
    });
    expect(r.reaped).toEqual([name]);
  });

  it("保活读取抛错时 fail-open 到「继续回收」，不是「谁都别收」", async () => {
    const name = podFor("dberr");
    await recordSandboxActivity(name, "acme", "s-dberr", new Date(Date.now() - 60 * MIN));

    const deleted: string[] = [];
    const broken: SandboxActivitySource = {
      lastActivityFor,
      heldUntilFor: async () => {
        throw new Error("hold table unreadable");
      },
      clear: async () => undefined,
    };
    const r = await runReapPass(k8s([pod(name, 60 * MIN)], deleted), "ns", broken, {
      idleTtlS: IDLE_TTL_S,
      maxAgeS: MAX_AGE_S,
    });

    expect(r.reaped).toEqual([name]);
  });
});

// 原测试打 Next 路由（SAR 面：ttl_s 秒、租户走机器令牌）；这里打控制面内部面：tenant 在 body、ttl_ms 毫秒。
describe("POST /internal/sandboxes/hold", () => {
  async function post(body: Record<string, unknown>, tenant = "acme") {
    return cp.post("/internal/sandboxes/hold", { tenant, reason: null, ...body });
  }

  it("缺 session_id -> 400", async () => {
    const res = await post({});
    expect(res.status).toBe(400);
  });

  it("pod 不存在也成功 —— 声明未来的意图不需要现在就有 pod", async () => {
    const res = await post({ session_id: `${SALT}-never-acquired` });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { held_until: string };
    expect(typeof body.held_until).toBe("string");
    expect(new Date(body.held_until).getTime()).toBeGreaterThan(Date.now());
  });

  it("坏 ttl 退回默认值而不是 400 —— 一个坏参数不该换回「沙箱被误收」这个更贵的后果", async () => {
    for (const ttl of [undefined, -5, "abc", 0]) {
      const res = await post({ session_id: `${SALT}-badttl-${String(ttl)}`, ttl_ms: ttl });
      expect(res.status).toBe(200);
      expect(((await res.json()) as { ttl_s: number }).ttl_s).toBe(1800);
    }
  });

  it("ttl 超上限时回给调用方的也是 clamp 后的值，不谎报", async () => {
    const res = await post({ session_id: `${SALT}-bigttl`, ttl_ms: 999999 * 1000 });
    expect(((await res.json()) as { ttl_s: number }).ttl_s).toBe(SANDBOX_HOLD_MAX_MS / 1000);
  });
});
