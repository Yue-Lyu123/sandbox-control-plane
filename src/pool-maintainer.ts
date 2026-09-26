import { podManifest, SANDBOX_ROLE_LABEL, quotaShortfall } from "./control-plane.js";
import type { K8sClient, PodJson } from "./k8s.js";
import { allPoolPods, forgetPoolPod, poolPodName, registerPoolPod } from "./pool.js";

export interface PoolMaintainerOptions {

  desired: number;

  maxAgeMs?: number;

  maxCreatePerPass?: number;
  image?: string;
  port?: number;
  now?: Date;
}

export interface PoolPassResult {

  idle: number;
  created: string[];

  retired: string[];

  pruned: string[];

  relabeled: string[];

  skippedForQuota: boolean;
}

function randomSuffix(): string {
  return Math.random().toString(36).slice(2, 10).padEnd(8, "0");
}

function isReadyOrStarting(p: PodJson): boolean {
  const phase = p.status?.phase;
  return phase === "Running" || phase === "Pending";
}

export async function runPoolPass(
  k8s: K8sClient,
  namespace: string,
  opts: PoolMaintainerOptions,
): Promise<PoolPassResult> {
  const now = opts.now ?? new Date();
  const maxAgeMs = opts.maxAgeMs ?? 30 * 60 * 1000;
  const maxCreate = opts.maxCreatePerPass ?? 1;

  const result: PoolPassResult = {
    idle: 0,
    created: [],
    retired: [],
    pruned: [],
    relabeled: [],
    skippedForQuota: false,
  };

  const live = await k8s.listPods(namespace, `app=community-sandbox,${SANDBOX_ROLE_LABEL}=pool`);
  const liveByName = new Map<string, PodJson>();
  for (const p of live) {
    const n = p.metadata?.name;
    if (n) liveByName.set(n, p);
  }

  const rows = await allPoolPods();

  for (const row of rows) {
    const pod = liveByName.get(row.podName);

    if (row.claimed) {

      if (pod !== undefined) {
        try {
          await k8s.patchPod(namespace, row.podName, {
            metadata: { labels: { [SANDBOX_ROLE_LABEL]: "session" } },
          });
          result.relabeled.push(row.podName);
        } catch {
          // 下一轮再试。
        }
      }
      continue;
    }

    if (pod === undefined) {
      await forgetPoolPod(row.podName).catch(() => undefined);
      result.pruned.push(row.podName);
      continue;
    }

    if (pod.status?.phase === "Failed" || pod.status?.phase === "Succeeded") {
      try {
        await k8s.delete(namespace, "pods", row.podName);
        await forgetPoolPod(row.podName);
        result.retired.push(row.podName);
      } catch {
        // 下一轮再试。
      }
      continue;
    }

    if (now.getTime() - row.createdAt.getTime() > maxAgeMs) {
      try {
        await k8s.delete(namespace, "pods", row.podName);
        await forgetPoolPod(row.podName);
        result.retired.push(row.podName);
      } catch {
        // 下一轮再试。
      }
      continue;
    }

    if (isReadyOrStarting(pod)) result.idle += 1;
  }

  const missing = Math.min(opts.desired - result.idle, maxCreate);
  if (missing <= 0) return result;

  const manifestProbe = podManifest("sbx-pool-probe", "", { image: opts.image, port: opts.port });
  const res = manifestProbe.spec.containers[0].resources;
  const quotas = await k8s.listResourceQuotas(namespace);
  if (quotas !== null && quotaShortfall(quotas, res.requests, res.limits) !== null) {

    result.skippedForQuota = true;
    return result;
  }

  for (let i = 0; i < missing; i += 1) {
    const name = poolPodName(randomSuffix());
    const manifest = poolPodManifest(name, { image: opts.image, port: opts.port });
    try {
      const r = await k8s.create(namespace, "pods", manifest);
      if ([200, 201, 202].includes(r.status)) {
        await registerPoolPod(name, now);
        result.created.push(name);
      }
    } catch {
      // 建不出来就下一轮再说，绝不抛。
    }
  }

  return result;
}

export function poolPodManifest(name: string, opts: { image?: string; port?: number } = {}) {
  return podManifest(name, "", {
    ...opts,
    labels: { [SANDBOX_ROLE_LABEL]: "pool" },
    annotations: {},
    env: [],
  });
}
