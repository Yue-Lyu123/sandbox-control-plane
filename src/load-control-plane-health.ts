import { summarizeControlPlaneHealth, type ControlPlaneHealth, type ReaperConfig } from "./control-plane-health.js";
import { K8sClient, loadInClusterK8sConfig, type K8sClientConfig, type PodJson, type ResourceQuotaJson } from "./k8s.js";
import { SANDBOX_POD_LABEL_SELECTOR } from "./reaper.js";

export type ControlPlaneHealthLoad =
  | { ok: true; health: ControlPlaneHealth }
  | { ok: false; kind: "not_configured" }
  | { ok: false; kind: "unavailable"; detail: string };

/**
 * 沙箱控制面「配没配」的**唯一判据**：显式三件套，否则退回集群内 serviceaccount，
 * 再否则 null（= `not_configured`）。只读 env / 本地文件，不发任何网络请求。
 * `/admin/api/platform/settings` 复用它报 `sandbox.configured` / `sandbox.namespace`，
 * 免得两处各写一份判据然后慢慢分家。
 */
export function sandboxK8sConfigFromEnv(): K8sClientConfig | null {
  const server = process.env.SANDBOX_K8S_SERVER_URL;
  const token = process.env.SANDBOX_K8S_TOKEN;
  const namespace = process.env.SANDBOX_K8S_NAMESPACE;
  if (!server || !token || !namespace) return loadInClusterK8sConfig();
  return { server, token, namespace, ca: process.env.SANDBOX_K8S_CA || undefined };
}

function k8sFromEnv(): K8sClient | null {
  const config = sandboxK8sConfigFromEnv();
  return config === null ? null : new K8sClient(config);
}

function positiveNumberFromEnv(name: string): number | null {
  const raw = process.env[name];
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export function reaperConfigFromEnv(): ReaperConfig {
  return {
    idleTtlS: positiveNumberFromEnv("SANDBOX_IDLE_TTL_S"),
    maxAgeS: positiveNumberFromEnv("SANDBOX_MAX_AGE_S"),
    intervalS: positiveNumberFromEnv("SANDBOX_REAPER_INTERVAL_S"),
  };
}

export async function loadControlPlaneHealth(): Promise<ControlPlaneHealthLoad> {
  const k8s = k8sFromEnv();
  if (k8s === null) return { ok: false, kind: "not_configured" };

  let pods: PodJson[];
  try {
    pods = await k8s.listPods(k8s.namespace, SANDBOX_POD_LABEL_SELECTOR);
  } catch (err) {

    return { ok: false, kind: "unavailable", detail: err instanceof Error ? err.message : String(err) };
  }

  const quotas: ResourceQuotaJson[] = (await k8s.listResourceQuotas(k8s.namespace)) ?? [];

  return { ok: true, health: summarizeControlPlaneHealth(pods, quotas, reaperConfigFromEnv()) };
}
