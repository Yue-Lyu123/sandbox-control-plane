import { lifetimeStartMs, SANDBOX_ROLE_LABEL } from "./control-plane.js";
import type { PodJson } from "./k8s.js";

export const SANDBOX_POD_LABEL_SELECTOR = "app=community-sandbox";

export interface ReaperK8s {
  listPods(ns: string, labelSelector?: string): Promise<PodJson[]>;
  delete(ns: string, plural: string, name: string): Promise<{ status: number; body: string }>;
}

export interface SandboxActivitySource {

  lastActivityFor(podNames: string[]): Promise<Map<string, Date>>;

  heldUntilFor?(podNames: string[]): Promise<Map<string, { until: Date; reason: string | null }>>;

  clear(podName: string): Promise<void>;
}

export interface ReapPassOptions {

  idleTtlS: number;

  maxAgeS?: number;

  now?: Date;
}

export interface ReapPassResult {

  checked: number;

  reaped: string[];

  skipped: string[];

  held: string[];
}

function passesDoubleGuard(name: string, labels: Record<string, string>): boolean {
  return name.startsWith("sbx-") && labels["app"] === "community-sandbox";
}

export async function runReapPass(
  k8s: ReaperK8s,
  namespace: string,
  activity: SandboxActivitySource,
  opts: ReapPassOptions,
): Promise<ReapPassResult> {
  if (!Number.isFinite(opts.idleTtlS) || opts.idleTtlS <= 0) {
    throw new Error(`reaper misconfigured: idleTtlS must be a positive number, got ${opts.idleTtlS}`);
  }
  const nowMs = (opts.now ?? new Date()).getTime();

  const pods = await k8s.listPods(namespace, SANDBOX_POD_LABEL_SELECTOR);
  const names = pods.map((p) => p.metadata?.name).filter((n): n is string => typeof n === "string" && n.length > 0);
  const lastActivity = await activity.lastActivityFor(names);

  let holds = new Map<string, { until: Date; reason: string | null }>();
  if (activity.heldUntilFor) {
    try {
      holds = await activity.heldUntilFor(names);
    } catch {
      holds = new Map();
    }
  }

  const reaped: string[] = [];
  const skipped: string[] = [];
  const held: string[] = [];

  for (const pod of pods) {
    const name = pod.metadata?.name;
    if (!name) {

      skipped.push("<unnamed>");
      continue;
    }
    const labels = pod.metadata?.labels ?? {};

    if (!passesDoubleGuard(name, labels)) {
      skipped.push(name);
      continue;
    }

    if (labels[SANDBOX_ROLE_LABEL] === "pool") {
      skipped.push(name);
      continue;
    }

    const startMs = lifetimeStartMs(pod);
    if (startMs === null) {
      skipped.push(name); 
      continue;
    }
    const creationMs = startMs;

    const activityMs = lastActivity.get(name)?.getTime();
    const effectiveLastMs =
      activityMs !== undefined && Number.isFinite(activityMs) ? Math.max(activityMs, creationMs) : creationMs;

    const idleExpired = nowMs - effectiveLastMs > opts.idleTtlS * 1000;
    const ageExpired = opts.maxAgeS !== undefined && nowMs - creationMs > opts.maxAgeS * 1000;

    const heldNow = holds.get(name);
    if (heldNow !== undefined && idleExpired && !ageExpired) {
      skipped.push(name);
      held.push(name);
      continue;
    }

    if (!idleExpired && !ageExpired) {
      skipped.push(name);
      continue;
    }

    const r = await k8s.delete(namespace, "pods", name);
    if ([200, 202, 404].includes(r.status)) {
      reaped.push(name);
      try {
        await activity.clear(name);
      } catch {
        // Best-effort: a leftover activity row for a deleted pod is harmless
        // (the pod is gone from every future list) and self-heals if the
        // same (tenant, session) pod name is ever recreated.
      }
    } else {
      skipped.push(name);
    }
  }

  return { checked: pods.length, reaped, skipped, held };
}
