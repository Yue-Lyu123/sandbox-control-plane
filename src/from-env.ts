import { deleteSandboxActivity, heldUntilFor, lastActivityFor, recordSandboxActivity } from "./activity.js";
import { SandboxControlPlane } from "./control-plane.js";
import { K8sClient, loadInClusterK8sConfig } from "./k8s.js";
import { SandboxObserver } from "./observer.js";
import {
  appendSandboxPodLogLines,
  countSandboxPodLogLines,
  insertSandboxPodEvent,
  upsertSandboxPodMeta,
} from "./observer-store.js";
import { claimPoolPod, claimedPodName, forgetPoolPod, unclaimedPoolPods } from "./pool.js";
import { runPoolPass, type PoolPassResult } from "./pool-maintainer.js";
import { runReapPass, type ReapPassResult } from "./reaper.js";
import { registerIntervalTask, stopIntervalTask } from "./scheduler.js";

function k8sClientFromEnv(): K8sClient | null {
  const server = process.env.SANDBOX_K8S_SERVER_URL;
  const token = process.env.SANDBOX_K8S_TOKEN;
  const namespace = process.env.SANDBOX_K8S_NAMESPACE;
  if (!server || !token || !namespace) {

    const inCluster = loadInClusterK8sConfig();
    return inCluster ? new K8sClient(inCluster) : null;
  }
  const ca = process.env.SANDBOX_K8S_CA || undefined;
  return new K8sClient({ server, token, namespace, ca });
}

function positiveNumberFromEnv(name: string): number | null {
  const raw = process.env[name];
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export function sandboxControlPlaneFromEnv(): SandboxControlPlane | null {
  const k8s = k8sClientFromEnv();
  if (!k8s) return null;

  const portEnv = process.env.SANDBOX_AIO_PORT;
  const port = portEnv ? Number(portEnv) : undefined;

  maybeStartSandboxReaperInterval();

  return new SandboxControlPlane(k8s, {
    port,


    activityRecorder: ({ podName, tenant, sessionId }) => recordSandboxActivity(podName, tenant, sessionId),

    maxAgeMs: (positiveNumberFromEnv("SANDBOX_MAX_AGE_S") ?? 0) * 1000 || undefined,

    pool: {
      claimedPodName,
      claim: (tenant, sessionId, candidates) => claimPoolPod(tenant, sessionId, candidates),
      unclaimed: unclaimedPoolPods,
      forget: forgetPoolPod,
    },
  });
}

export function sandboxReaperFromEnv(): (() => Promise<ReapPassResult>) | null {
  const k8s = k8sClientFromEnv();
  if (!k8s) return null;
  const idleTtlS = positiveNumberFromEnv("SANDBOX_IDLE_TTL_S");
  if (idleTtlS === null) return null;
  const maxAgeS = positiveNumberFromEnv("SANDBOX_MAX_AGE_S") ?? undefined;
  return () =>
    runReapPass(
      k8s,
      k8s.namespace,

      { lastActivityFor, heldUntilFor, clear: deleteSandboxActivity },
      { idleTtlS, maxAgeS },
    );
}

// reaper / pool 的定时器走 `scheduler.ts::registerIntervalTask`——按 key 防重、可停、unref。
//
// ⚠️ observer 不走 scheduler：它是 k8s watch（一条常驻流，不是 setInterval 周期回调），
// 形状不同，硬塞进 registerIntervalTask 会把「长连接实例」和「定时器」混为一谈。

export function maybeStartSandboxReaperInterval(): void {
  registerIntervalTask({
    key: "sandbox-reaper",
    intervalEnvVar: "SANDBOX_REAPER_INTERVAL_S",
    // env-gated：没配 SANDBOX_REAPER_INTERVAL_S 就不起（sandbox 没开就不该有回收）。
    defaultIntervalS: null,
    getTask: () => sandboxReaperFromEnv(),
    logPrefix: "[sandbox-reaper]",
  });
}

export function maybeStartSandboxPoolInterval(): void {
  registerIntervalTask({
    key: "sandbox-pool",
    intervalEnvVar: "SANDBOX_POOL_INTERVAL_S",
    defaultIntervalS: null,
    getTask: () => sandboxPoolMaintainerFromEnv(),
    logPrefix: "[sandbox-pool]",
    // 原实现起来就先补一轮池子（等一个完整周期才补第一批太慢），保留这个行为。
    runImmediately: true,
  });
}

export function sandboxPoolMaintainerFromEnv(): (() => Promise<PoolPassResult>) | null {
  const desired = positiveNumberFromEnv("SANDBOX_POOL_DESIRED");
  if (desired === null) return null;
  const k8s = k8sClientFromEnv();
  if (!k8s) return null;
  const namespace = process.env.SANDBOX_K8S_NAMESPACE ?? k8s.namespace;
  const portEnv = positiveNumberFromEnv("SANDBOX_AIO_PORT");
  return () =>
    runPoolPass(k8s, namespace, {
      desired,
      maxAgeMs: (positiveNumberFromEnv("SANDBOX_POOL_MAX_AGE_S") ?? 30 * 60) * 1000,
      port: portEnv ?? undefined,
    });
}

// observer 实例是「防重建标记」：独立进程，模块级变量即可。
// （它不是定时器——k8s watch 常驻流，所以不进 scheduler；防重只看「在不在」。）
let observerInstance: SandboxObserver | null = null;

export function maybeStartSandboxObserver(): void {
  if (process.env.SANDBOX_OBSERVER_ENABLED === "0") return;
  if (observerInstance !== null) return;
  const k8s = k8sClientFromEnv();
  if (!k8s) return;
  const observer = new SandboxObserver(
    k8s,
    {
      appendLogLines: appendSandboxPodLogLines,
      countLogLines: countSandboxPodLogLines,
      insertEvent: insertSandboxPodEvent,
      upsertPodMeta: upsertSandboxPodMeta,
    },
    {
      namespace: k8s.namespace,
      maxLogLinesPerPod: positiveNumberFromEnv("SANDBOX_OBSERVER_MAX_LOG_LINES") ?? undefined,
    },
  );
  observer.start();
  observerInstance = observer;
}

export async function stopSandboxObserver(): Promise<void> {
  const o = observerInstance;
  observerInstance = null;
  await o?.stop();
}

export function _stopSandboxReaperIntervalForTests(): void {
  stopIntervalTask("sandbox-reaper");
}

export function _stopSandboxPoolIntervalForTests(): void {
  stopIntervalTask("sandbox-pool");
}
