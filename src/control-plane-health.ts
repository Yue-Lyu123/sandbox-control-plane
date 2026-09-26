import { parseCpuMillis, parseMemoryBytes, podManifest, SANDBOX_ROLE_LABEL } from "./control-plane.js";
import type { PodJson, ResourceQuotaJson } from "./k8s.js";

export type HealthSeverity = "ok" | "warn" | "critical";

export interface HealthCheck {

  id: string;
  title: string;
  severity: HealthSeverity;

  detail: string;

  evidence?: string[];
}

export interface SandboxCapacity {

  remainingSandboxes: number | null;

  boundBy: string | null;

  liveSandboxes: number;

  idlePool: number;

  inUse: number;
}

export interface ControlPlaneHealth {

  severity: HealthSeverity;
  checks: HealthCheck[];
  capacity: SandboxCapacity;
  checkedAt: string;
}

export interface ReaperConfig {
  idleTtlS: number | null;
  maxAgeS: number | null;
  intervalS: number | null;
}

const PRECHECKED_QUOTA_KEYS = [
  "pods",
  "count/pods",
  "requests.cpu",
  "cpu",
  "requests.memory",
  "memory",
  "limits.cpu",
  "limits.memory",
];

const worst = (a: HealthSeverity, b: HealthSeverity): HealthSeverity =>
  a === "critical" || b === "critical" ? "critical" : a === "warn" || b === "warn" ? "warn" : "ok";

function sandboxLimits(): { cpuMillis: number | null; memBytes: number | null } {
  const limits = podManifest("sbx-probe", "probe").spec.containers[0].resources.limits;
  return {
    cpuMillis: parseCpuMillis(limits.cpu),
    memBytes: parseMemoryBytes(limits.memory),
  };
}

export function computeCapacity(quotas: ResourceQuotaJson[], pods: PodJson[]): SandboxCapacity {
  const { cpuMillis, memBytes } = sandboxLimits();
  let best: { n: number; key: string } | null = null;

  for (const quota of quotas) {
    if ((quota.spec?.scopes?.length ?? 0) > 0 || quota.spec?.scopeSelector !== undefined) continue;
    const hard = quota.status?.hard;
    const used = quota.status?.used;
    if (!hard || !used) continue;

    const dims: { key: string; parse: (q: string) => number | null; need: number | null }[] = [
      { key: "limits.memory", parse: parseMemoryBytes, need: memBytes },
      { key: "limits.cpu", parse: parseCpuMillis, need: cpuMillis },
      { key: "requests.memory", parse: parseMemoryBytes, need: memBytes },
      { key: "requests.cpu", parse: parseCpuMillis, need: cpuMillis },
    ];
    for (const d of dims) {
      if (d.need === null || d.need <= 0) continue;
      if (hard[d.key] === undefined || used[d.key] === undefined) continue;
      const h = d.parse(hard[d.key]);
      const u = d.parse(used[d.key]);
      if (h === null || u === null) continue;
      const n = Math.max(0, Math.floor((h - u) / d.need));
      if (best === null || n < best.n) best = { n, key: d.key };
    }
    for (const key of ["pods", "count/pods"]) {
      if (hard[key] === undefined || used[key] === undefined) continue;
      const h = Number(hard[key]);
      const u = Number(used[key]);
      if (!Number.isFinite(h) || !Number.isFinite(u)) continue;
      const n = Math.max(0, Math.floor(h - u));
      if (best === null || n < best.n) best = { n, key };
    }
  }

  const idlePool = pods.filter((p) => p.metadata?.labels?.[SANDBOX_ROLE_LABEL] === "pool").length;

  return {
    remainingSandboxes: best?.n ?? null,
    boundBy: best?.key ?? null,
    liveSandboxes: pods.length,
    idlePool,
    inUse: pods.length - idlePool,
  };
}

export function summarizeControlPlaneHealth(
  pods: PodJson[],
  quotas: ResourceQuotaJson[],
  reaper: ReaperConfig,
  opts: { now?: Date; readyBudgetMs?: number } = {},
): ControlPlaneHealth {
  const now = opts.now ?? new Date();
  const nowMs = now.getTime();
  const readyBudgetMs = opts.readyBudgetMs ?? 360_000;
  const checks: HealthCheck[] = [];

  const ageMs = (p: PodJson): number | null => {
    const t = p.metadata?.creationTimestamp ? Date.parse(p.metadata.creationTimestamp) : NaN;
    return Number.isFinite(t) ? nowMs - t : null;
  };
  const nameOf = (p: PodJson): string => p.metadata?.name ?? "<unnamed>";

  if (reaper.maxAgeS === null || reaper.intervalS === null) {
    checks.push({
      id: "reaper-configured",
      title: "回收器",
      severity: "warn",
      detail:
        "回收器没有完整配置（缺 SANDBOX_MAX_AGE_S 或 SANDBOX_REAPER_INTERVAL_S），" +
        "沙箱不会被自动回收，配额会一直被占住。",
    });
  } else {

    const graceMs = reaper.intervalS * 2 * 1000;
    const overdue = pods.filter((p) => {
      const a = ageMs(p);
      return a !== null && a > reaper.maxAgeS! * 1000 + graceMs;
    });
    if (overdue.length > 0) {
      checks.push({
        id: "reaper-overdue",
        title: "回收器没在干活",
        severity: "critical",
        detail:
          `有 ${overdue.length} 个沙箱已超过绝对寿命（${Math.round(reaper.maxAgeS / 3600)} 小时）` +
          `且过了两个扫描周期仍然存在。回收器可能没启动、卡住、或删除被拒。` +
          `这正是 #46 的形状（一个 Failed pod 躺了 3 天）——请先看服务日志。`,
        evidence: overdue.map(nameOf),
      });
    }
  }

  const corpses = pods.filter((p) => p.status?.phase === "Failed" || p.status?.phase === "Succeeded");
  if (corpses.length > 0) {
    checks.push({
      id: "terminal-corpses",
      title: "终态沙箱未清理",
      severity: "warn",
      detail:
        `有 ${corpses.length} 个沙箱处于 Failed/Succeeded。restartPolicy 是 Never，` +
        `它们永远不会再跑，但会一直占配额直到被回收。若同时伴随「回收器没在干活」，` +
        `以那条为准。`,
      evidence: corpses.map((p) => `${nameOf(p)} (${p.status?.phase})`),
    });
  }

  const stuck = pods.filter((p) => {
    if (p.status?.phase !== "Pending") return false;
    const a = ageMs(p);
    return a !== null && a > readyBudgetMs;
  });
  if (stuck.length > 0) {
    checks.push({
      id: "stuck-pending",
      title: "沙箱卡在 Pending",
      severity: "warn",
      detail:
        `有 ${stuck.length} 个沙箱 Pending 超过了就绪预算（${Math.round(readyBudgetMs / 1000)} 秒）。` +
        `最常见的原因是**该节点上没有沙箱镜像、正在冷拉**（现网实测可达 4 分 37 秒），` +
        `其次是调度不上。看 pod 事件里的 Pulling/FailedScheduling 就能分辨。`,
      evidence: stuck.map(nameOf),
    });
  }

  const capacity = computeCapacity(quotas, pods);
  if (capacity.remainingSandboxes !== null) {
    const n = capacity.remainingSandboxes;
    if (n <= 0) {
      checks.push({
        id: "capacity-exhausted",
        title: "配额已满",
        severity: "critical",
        detail: `按 ${capacity.boundBy} 推算，现在开不出新沙箱了。下一个用户会直接失败——请先释放沙箱或扩容配额。`,
      });
    } else if (n <= 2) {
      checks.push({
        id: "capacity-low",
        title: "配额余量偏低",
        severity: "warn",
        detail: `按 ${capacity.boundBy} 推算，只够再开 ${n} 个沙箱。`,
      });
    }
  }

  for (const quota of quotas) {
    if ((quota.spec?.scopes?.length ?? 0) > 0 || quota.spec?.scopeSelector !== undefined) continue;
    const hard = quota.status?.hard;
    if (!hard) continue;
    const constrained = Object.keys(hard);
    if (constrained.length === 0) continue;
    const overlap = constrained.filter((k) => PRECHECKED_QUOTA_KEYS.includes(k));
    if (overlap.length === 0) {
      checks.push({
        id: "precheck-blind",
        title: "配额预检存在盲区",
        severity: "warn",
        detail:
          `配额 "${quota.metadata?.name ?? "(unnamed)"}" 约束的维度（${constrained.join(", ")}）` +
          `与配额预检会检查的维度完全不相交，**预检对它恒放行**。撞墙时用户会拿到 ` +
          `K8s 原始 403 而不是可读的提示。请把这些维度加进 quotaShortfall。`,
        evidence: constrained,
      });
    }
  }

  const severity = checks.reduce<HealthSeverity>((acc, c) => worst(acc, c.severity), "ok");
  return { severity, checks, capacity, checkedAt: now.toISOString() };
}
