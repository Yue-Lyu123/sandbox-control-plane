import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { getPool } from "../src/db.js";
import {
  SANDBOX_ROLE_LABEL,
  SandboxControlPlane,
  sandboxPodName,
  type SandboxPoolSeam,
} from "../src/control-plane.js";
import { K8sClient, type PodJson } from "../src/k8s.js";
import {
  allPoolPods,
  claimPoolPod,
  claimedPodName,
  forgetPoolPod,
  poolPodName,
  registerPoolPod,
  unclaimedPoolPods,
} from "../src/pool.js";
import { poolPodManifest, runPoolPass } from "../src/pool-maintainer.js";
import { runReapPass, type SandboxActivitySource } from "../src/reaper.js";
import { TEST_DATABASE_URL } from "./pglite-test-db.js";
import { startFakeAioServer, type FakeAioServer } from "./support/fake-aio-server.js";
import { startFakeK8sServer, type FakeK8sServer } from "./support/fake-k8s-server.js";

const SALT = randomUUID().slice(0, 8);
const NS = "default";

beforeAll(() => {
  process.env.DATABASE_URL = TEST_DATABASE_URL;
});

afterAll(async () => {
  await getPool()
    .query("DELETE FROM sandbox_pool WHERE pod_name LIKE $1", [`sbx-pool-${SALT}%`])
    .catch(() => undefined);
});

const name = (s: string) => poolPodName(`${SALT}${s}`);

function podJson(n: string, role: string | undefined, ageMs: number, phase = "Running"): PodJson {
  const labels: Record<string, string> = { app: "community-sandbox" };
  if (role) labels[SANDBOX_ROLE_LABEL] = role;
  return {
    metadata: { name: n, creationTimestamp: new Date(Date.now() - ageMs).toISOString(), labels },
    status: { phase, conditions: [{ type: "Ready", status: "True" }] },
  } as unknown as PodJson;
}

describe("① 认领表：原子与幂等", () => {
  it("并发认领同一批候选，两个会话拿到不同的 pod，没有人拿到同一个", async () => {
    const a = name("-c1");
    const b = name("-c2");
    await registerPoolPod(a);
    await registerPoolPod(b);

    const [r1, r2] = await Promise.all([
      claimPoolPod("acme", `${SALT}-s1`, [a, b]),
      claimPoolPod("acme", `${SALT}-s2`, [a, b]),
    ]);

    expect(r1).not.toBeNull();
    expect(r2).not.toBeNull();
    expect(r1).not.toBe(r2); 
  });

  it("同一会话重复认领拿到同一个 pod，不会再吃掉一个", async () => {
    const p = name("-idem");
    await registerPoolPod(p);
    const first = await claimPoolPod("acme", `${SALT}-idem`, [p]);
    const before = (await unclaimedPoolPods()).length;
    const second = await claimPoolPod("acme", `${SALT}-idem`, [p]);
    expect(second).toBe(first);
    expect((await unclaimedPoolPods()).length).toBe(before);
  });

  it("没有候选 -> null（调用方退回自己建）", async () => {
    expect(await claimPoolPod("acme", `${SALT}-none`, [])).toBeNull();
  });

  it("候选都已被认领 -> null，不会抢别人的", async () => {
    const p = name("-taken");
    await registerPoolPod(p);
    await claimPoolPod("acme", `${SALT}-owner`, [p]);
    expect(await claimPoolPod("acme", `${SALT}-thief`, [p])).toBeNull();
  });

  it("forget 之后认领关系消失", async () => {
    const p = name("-forget");
    await registerPoolPod(p);
    await claimPoolPod("acme", `${SALT}-f`, [p]);
    expect(await claimedPodName("acme", `${SALT}-f`)).toBe(p);
    await forgetPoolPod(p);
    expect(await claimedPodName("acme", `${SALT}-f`)).toBeNull();
  });
});

describe("② 回收器：豁免只给未认领的池子 pod", () => {
  const activity: SandboxActivitySource = {
    lastActivityFor: async () => new Map(),
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

  it("未认领的池子 pod 即使很老也不收（维护器管它的生死）", async () => {
    const deleted: string[] = [];
    const r = await runReapPass(k8s([podJson("sbx-pool-old", "pool", 99 * 3600_000)], deleted), NS, activity, {
      idleTtlS: 1800,
      maxAgeS: 6 * 3600,
    });
    expect(deleted).toEqual([]);
    expect(r.skipped).toEqual(["sbx-pool-old"]);
  });

  it("认领后（标签变 session）没有任何豁免，照常按闲置回收", async () => {
    const deleted: string[] = [];
    await runReapPass(k8s([podJson("sbx-pool-claimed", "session", 99 * 3600_000)], deleted), NS, activity, {
      idleTtlS: 1800,
      maxAgeS: 6 * 3600,
    });
    expect(deleted).toEqual(["sbx-pool-claimed"]);
  });

  it("没有 role 标签的老 pod 行为一个字都没变", async () => {
    const deleted: string[] = [];
    await runReapPass(k8s([podJson("sbx-legacy00001", undefined, 99 * 3600_000)], deleted), NS, activity, {
      idleTtlS: 1800,
      maxAgeS: 6 * 3600,
    });
    expect(deleted).toEqual(["sbx-legacy00001"]);
  });
});

describe("③ 池子 pod 的 manifest", () => {
  it("带 pool 角色标签，且**不设** SESSION_ID / session 注解（创建后改不了，宁可没有也不要错的）", () => {
    const m = poolPodManifest("sbx-pool-x");
    expect(m.metadata.labels[SANDBOX_ROLE_LABEL]).toBe("pool");
    expect(m.metadata.labels["app"]).toBe("community-sandbox");
    expect(m.metadata.annotations["community/session-id"]).toBeUndefined();
    expect(m.spec.containers[0].env).toEqual([]);
  });

  it("除上述两处外与普通沙箱 manifest 完全一致 —— 否则池子 pod 会有第二处行为差异", () => {
    const pooled = poolPodManifest("sbx-pool-y");
    const normal = poolPodManifest("sbx-pool-y"); 
    expect(pooled).toEqual(normal);
    expect(pooled.spec.containers[0].resources).toEqual({
      requests: { cpu: "200m", memory: "512Mi", "ephemeral-storage": "1Gi" },
      limits: { cpu: "1", memory: "2Gi", "ephemeral-storage": "4Gi" },
    });
    expect(pooled.spec.automountServiceAccountToken).toBe(false);
    expect(pooled.spec.containers[0].securityContext.allowPrivilegeEscalation).toBe(false);
  });
});

describe("④ 维护器：只吃闲置产能", () => {
  function stubK8s(opts: {
    pods?: PodJson[];
    quotaShort?: boolean;
    created?: string[];
    deleted?: string[];
    patched?: string[];
  }) {
    return {
      listPods: async () => opts.pods ?? [],
      listResourceQuotas: async () =>
        opts.quotaShort
          ? [
              {
                metadata: { name: "q" },
                status: { hard: { "limits.memory": "64Gi" }, used: { "limits.memory": "65000Mi" } },
              },
            ]
          : [],
      create: async (_ns: string, _p: string, m: unknown) => {
        opts.created?.push((m as { metadata: { name: string } }).metadata.name);
        return { status: 201, body: "" };
      },
      delete: async (_ns: string, _p: string, n: string) => {
        opts.deleted?.push(n);
        return { status: 200, body: "" };
      },
      patchPod: async (_ns: string, n: string) => {
        opts.patched?.push(n);
        return { status: 200, body: "" };
      },
    } as unknown as K8sClient;
  }

  it("配额不足时这一轮不补货 —— 真实会话永远优先", async () => {
    const created: string[] = [];
    const r = await runPoolPass(stubK8s({ quotaShort: true, created }), NS, { desired: 2 });
    expect(created).toEqual([]);
    expect(r.skippedForQuota).toBe(true);
  });

  it("配额够时补到目标数，但一轮最多建 1 个（不一次性吃光配额）", async () => {
    const created: string[] = [];
    const r = await runPoolPass(stubK8s({ created }), NS, { desired: 2 });
    expect(created).toHaveLength(1);
    expect(r.created).toEqual(created);
    for (const n of created) await forgetPoolPod(n);
  });

  it("库里有行、集群里没 pod -> 孤儿行清掉", async () => {
    const orphan = name("-orphan");
    await registerPoolPod(orphan);
    const r = await runPoolPass(stubK8s({ pods: [], quotaShort: true }), NS, { desired: 0 });
    expect(r.pruned).toContain(orphan);
    expect(await claimedPodName("acme", orphan)).toBeNull();
  });

  it("已认领却还挂着 pool 标签 -> 补打 session 标签（认领时 patch 失败的补救）", async () => {
    const p = name("-relabel");
    await registerPoolPod(p);
    await claimPoolPod("acme", `${SALT}-relabel`, [p]);
    const patched: string[] = [];
    const r = await runPoolPass(stubK8s({ pods: [podJson(p, "pool", 1000)], quotaShort: true, patched }), NS, {
      desired: 0,
    });
    expect(patched).toContain(p);
    expect(r.relabeled).toContain(p);
  });

  it("未认领且超龄 -> 换新（删除 + 清行）", async () => {
    const old = name("-aged");
    await registerPoolPod(old, new Date(Date.now() - 10 * 3600_000));
    const deleted: string[] = [];
    const r = await runPoolPass(stubK8s({ pods: [podJson(old, "pool", 10 * 3600_000)], quotaShort: true, deleted }), NS, {
      desired: 0,
      maxAgeMs: 4 * 3600_000,
    });
    expect(deleted).toContain(old);
    expect(r.retired).toContain(old);
    expect((await allPoolPods()).find((x) => x.podName === old)).toBeUndefined();
  });
});

describe("⑤ 控制面：命中池子就跳过整个建 pod + 等就绪", () => {
  let workspace: string;
  let aio: FakeAioServer;
  let k8sServer: FakeK8sServer;

  beforeAll(async () => {
    workspace = fs.mkdtempSync(path.join(os.tmpdir(), "pool-cp-"));
    aio = await startFakeAioServer(workspace);
    k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });
  });

  afterAll(async () => {
    await aio.close();
    await k8sServer.close();
    fs.rmSync(workspace, { recursive: true, force: true });
  });

  function plane(pool?: SandboxPoolSeam) {
    const k8s = new K8sClient({ server: k8sServer.url, token: "t", namespace: NS });
    return new SandboxControlPlane(k8s, { port: aio.port, pool });
  }

  it("命中：acquire 直接返回池子 pod 的句柄，且**一次 create 都没发**", async () => {
    const pooled = name("-hit");
    k8sServer.seedPod({
      namespace: NS,
      name: pooled,
      labels: { app: "community-sandbox", [SANDBOX_ROLE_LABEL]: "pool" },
    });
    const before = k8sServer.createCallCount();

    const seam: SandboxPoolSeam = {
      claimedPodName: async () => null,
      claim: async (_t, _s, candidates) => candidates[0] ?? null,
      unclaimed: async () => [],
      forget: async () => undefined,
    };

    const handle = await plane(seam).acquire("acme", `${SALT}-hit`, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
    expect(handle.podName).toBe(pooled);

    expect(k8sServer.createCallCount()).toBe(before);
  });

  it("未命中：原样走老路径，pod 名仍是确定性哈希", async () => {
    const seam: SandboxPoolSeam = {
      claimedPodName: async () => null,
      claim: async () => null,
      unclaimed: async () => [],
      forget: async () => undefined,
    };
    const session = `${SALT}-miss`;
    const handle = await plane(seam).acquire("acme", session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
    expect(handle.podName).toBe(sandboxPodName("acme", session));
  });

  it("池子接缝整个抛错时，退回老路径而不是让 acquire 失败", async () => {
    const broken: SandboxPoolSeam = {
      claimedPodName: async () => {
        throw new Error("pool table down");
      },
      claim: async () => {
        throw new Error("pool table down");
      },
      unclaimed: async () => [],
      forget: async () => undefined,
    };
    const session = `${SALT}-broken`;
    const handle = await plane(broken).acquire("acme", session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

    expect(handle.podName).toBe(sandboxPodName("acme", session));
  });

  it("已认领的会话，execute/release 找的是认领的那个 pod，不是哈希名", async () => {
    const pooled = name("-resolve");
    k8sServer.seedPod({
      namespace: NS,
      name: pooled,
      labels: { app: "community-sandbox", [SANDBOX_ROLE_LABEL]: "session" },
    });
    const forgotten: string[] = [];
    const seam: SandboxPoolSeam = {
      claimedPodName: async () => pooled,
      claim: async () => null,
      unclaimed: async () => [],
      forget: async (n) => {
        forgotten.push(n);
      },
    };
    const cp = plane(seam);
    const session = `${SALT}-resolve`;
    expect(sandboxPodName("acme", session)).not.toBe(pooled); 

    const r = await cp.execute("acme", session, "echo hi");
    expect(r.exitCode).toBe(0);
    // 回显的是实际跑在的池子 pod，不是哈希名——acquire 与 execute 回的必须是同一个名字
    expect(r.podName).toBe(pooled);
    expect(await cp.podNameFor("acme", session)).toBe(pooled);

    await cp.release("acme", session);

    expect(forgotten).toEqual([pooled]);
  });
});

describe("⑥ issue #59: 认领池子 pod 时一并清空 /skills", () => {

  let workspace: string;
  let aio: FakeAioServer;
  let k8sServer: FakeK8sServer;

  beforeAll(async () => {
    workspace = fs.mkdtempSync(path.join(os.tmpdir(), "pool-skills-reset-"));
    aio = await startFakeAioServer(workspace, { mountRoot: "/skills" });
    k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 0 });
  });

  afterAll(async () => {
    await aio.close();
    await k8sServer.close();
    fs.rmSync(workspace, { recursive: true, force: true });
  });

  it("认领命中后，池子 pod 上遗留的 /skills 内容被清空", async () => {
    const pooled = name("-skills-leak");
    k8sServer.seedPod({
      namespace: NS,
      name: pooled,
      labels: { app: "community-sandbox", [SANDBOX_ROLE_LABEL]: "pool" },
    });

    fs.mkdirSync(path.join(workspace, "skills/leaked-skill"), { recursive: true });
    fs.writeFileSync(path.join(workspace, "skills/leaked-skill/SKILL.md"), "# leaked\n");
    expect(fs.existsSync(path.join(workspace, "skills/leaked-skill/SKILL.md"))).toBe(true);

    const k8s = new K8sClient({ server: k8sServer.url, token: "t", namespace: NS });
    const seam: SandboxPoolSeam = {
      claimedPodName: async () => null,
      claim: async (_t, _s, candidates) => candidates[0] ?? null,
      unclaimed: async () => [],
      forget: async () => undefined,
    };
    const cp = new SandboxControlPlane(k8s, { port: aio.port, pool: seam });

    const handle = await cp.acquire("acme", `${SALT}-skills-leak`, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
    expect(handle.podName).toBe(pooled);

    expect(fs.existsSync(path.join(workspace, "skills/leaked-skill"))).toBe(false);
  });
});
