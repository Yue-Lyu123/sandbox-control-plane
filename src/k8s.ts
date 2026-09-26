import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import { load as yamlLoad } from "js-yaml";
import { httpRequest, sleep } from "./http-client.js";

export interface K8sClientConfig {

  server: string;

  token: string | (() => string);
  namespace: string;

  ca?: string | Buffer;
  timeoutMs?: number;
}

export interface PodCondition {
  type: string;
  status: string;

  reason?: string;

  message?: string;
}

export interface PodJson {
  metadata?: {
    name?: string;

    labels?: Record<string, string>;

    annotations?: Record<string, string>;

    creationTimestamp?: string;

    deletionTimestamp?: string;
  };
  status?: {
    phase?: string;
    podIP?: string;
    conditions?: PodCondition[];

    containerStatuses?: Array<{
      name?: string;
      restartCount?: number;
      state?: { terminated?: { exitCode?: number; reason?: string; message?: string } };
    }>;
  };
}

export interface PodListJson {
  metadata?: { resourceVersion?: string };
  items?: PodJson[];
}

export interface K8sEventJson {
  metadata?: { uid?: string; name?: string };
  type?: string;
  reason?: string;
  message?: string;
  count?: number;
  firstTimestamp?: string | null;
  lastTimestamp?: string | null;
  involvedObject?: { kind?: string; name?: string; uid?: string };
}

export interface PodWatchEvent {
  type: "ADDED" | "MODIFIED" | "DELETED" | "BOOKMARK" | "ERROR";
  object: PodJson & { code?: number; kind?: string };
}

export type WatchEnd = { reason: "closed" } | { reason: "expired" } | { reason: "error"; message: string };

export interface LineStream {
  stop(): void;
  done: Promise<{ status: number; error?: string }>;
}

export interface PodWatch {
  stop(): void;
  done: Promise<WatchEnd>;
}

export interface PodLogOptions {
  container?: string;
  previous?: boolean;
  tailLines?: number;
  sinceTime?: string;
}

export interface ResourceQuotaJson {
  metadata?: { name?: string };
  spec?: {
    hard?: Record<string, string>;
    scopes?: string[];
    scopeSelector?: unknown;
  };
  status?: {
    hard?: Record<string, string>;
    used?: Record<string, string>;
  };
}

export interface ResourceQuotaListJson {
  items?: ResourceQuotaJson[];
}

export type PodReadyOutcome =
  | { outcome: "ready" }
  | { outcome: "timeout" }
  | { outcome: "unschedulable"; message: string };

function podJsonReady(pod: PodJson | null): boolean {
  if (!pod) return false;
  if (pod.metadata?.deletionTimestamp) return false;
  const status = pod.status ?? {};
  if (status.phase !== "Running") return false;
  return (status.conditions ?? []).some((c) => c.type === "Ready" && c.status === "True");
}

function podUnschedulableMessage(pod: PodJson | null): string | null {
  if (!pod || pod.metadata?.deletionTimestamp) return null;
  if (pod.status?.phase !== "Pending") return null;
  const cond = (pod.status.conditions ?? []).find(
    (c) => c.type === "PodScheduled" && c.status === "False" && c.reason === "Unschedulable",
  );
  if (!cond) return null;
  return cond.message ?? "Unschedulable (scheduler gave no message)";
}

/**
 * In-cluster ServiceAccount auth (k8s/community-deployment.yaml 的代码半边):
 * when the pod runs with `automountServiceAccountToken: true`, the kubelet
 * mounts {token, ca.crt, namespace} under this directory and keeps the
 * token file ROTATED — hence `token` is returned as a provider that
 * re-reads the file per request, never a copied string.
 *
 * Returns null when not in a cluster (no KUBERNETES_SERVICE_HOST, or the
 * mount is absent/unreadable) — callers fall through to their other config
 * sources. `SANDBOX_K8S_SA_DIR` overrides the mount path (tests point it
 * at a temp dir; a devbox with a hand-mounted token could too).
 */
export function loadInClusterK8sConfig(): K8sClientConfig | null {
  const host = process.env.KUBERNETES_SERVICE_HOST;
  if (!host) return null;
  const port = process.env.KUBERNETES_SERVICE_PORT || "443";
  const dir = process.env.SANDBOX_K8S_SA_DIR || "/var/run/secrets/kubernetes.io/serviceaccount";
  try {

    fs.readFileSync(`${dir}/token`, "utf-8");
    const namespace = fs.readFileSync(`${dir}/namespace`, "utf-8").trim();
    const ca = fs.readFileSync(`${dir}/ca.crt`);
    return {
      server: `https://${host}:${port}`,
      token: () => fs.readFileSync(`${dir}/token`, "utf-8").trim(),
      namespace,
      ca,
    };
  } catch {
    return null;
  }
}

export function loadKubeconfig(path: string): K8sClientConfig {
  const raw = fs.readFileSync(path, "utf-8");
  const cfg = yamlLoad(raw) as {
    clusters?: Array<{ cluster: { server: string; "certificate-authority-data"?: string } }>;
    users?: Array<{ user: { token?: string } }>;
    contexts?: Array<{ context: { namespace?: string } }>;
  };
  const cluster = cfg.clusters?.[0]?.cluster;
  const user = cfg.users?.[0]?.user;
  if (!cluster) throw new Error("kubeconfig has no clusters[0]");
  const server = cluster.server.replace(/\/$/, "");
  const caData = cluster["certificate-authority-data"];
  if (!caData) {
    throw new Error("kubeconfig has no certificate-authority-data; refusing to run without CA");
  }
  const ca = Buffer.from(caData, "base64");
  const token = user?.token;
  if (!token) throw new Error("kubeconfig has no bearer token");
  const contexts = cfg.contexts ?? [];
  const namespace = contexts.length > 0 ? (contexts[0].context.namespace ?? "default") : "default";
  return { server, token, namespace, ca };
}

export class K8sClient {
  readonly namespace: string;
  private readonly server: string;
  private readonly token: string | (() => string);
  private readonly timeoutMs: number;
  private readonly agent: http.Agent | https.Agent;

  constructor(config: K8sClientConfig) {
    this.server = config.server.replace(/\/$/, "");
    this.token = config.token;
    this.namespace = config.namespace;
    this.timeoutMs = config.timeoutMs ?? 30_000;
    if (this.server.startsWith("https://")) {
      if (!config.ca) {
        throw new Error("https K8s server configured without a CA; refusing to run without CA verification");
      }
      this.agent = new https.Agent({ ca: config.ca });
    } else {
      this.agent = new http.Agent();
    }
  }

  create(ns: string, plural: string, manifest: unknown) {
    return this.request("POST", `/api/v1/namespaces/${ns}/${plural}`, manifest);
  }

  get(ns: string, plural: string, name: string) {
    return this.request("GET", `/api/v1/namespaces/${ns}/${plural}/${name}`);
  }

  delete(ns: string, plural: string, name: string) {
    return this.request("DELETE", `/api/v1/namespaces/${ns}/${plural}/${name}`);
  }

  private async request(method: string, path: string, body?: unknown) {
    return httpRequest(`${this.server}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.bearerToken()}`,
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      agent: this.agent,
      timeoutMs: this.timeoutMs,
    });
  }

  async patchPod(ns: string, name: string, patch: unknown): Promise<{ status: number; body: string }> {
    return httpRequest(`${this.server}/api/v1/namespaces/${ns}/pods/${name}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${this.bearerToken()}`,
        "Content-Type": "application/merge-patch+json",
      },
      body: JSON.stringify(patch),
      agent: this.agent,
      timeoutMs: this.timeoutMs,
    });
  }

  async listPods(ns: string, labelSelector?: string): Promise<PodJson[]> {
    const qs = labelSelector ? `?labelSelector=${encodeURIComponent(labelSelector)}` : "";
    const r = await this.request("GET", `/api/v1/namespaces/${ns}/pods${qs}`);
    if (r.status !== 200) {
      throw new Error(`list pods failed: ${r.status} ${r.body.slice(0, 300)}`);
    }
    return (JSON.parse(r.body) as PodListJson).items ?? [];
  }

  async getPod(ns: string, name: string): Promise<PodJson | null> {
    const r = await this.get(ns, "pods", name);
    return r.status === 200 ? (JSON.parse(r.body) as PodJson) : null;
  }

  async listResourceQuotas(ns: string): Promise<ResourceQuotaJson[] | null> {
    try {
      const r = await this.request("GET", `/api/v1/namespaces/${ns}/resourcequotas`);
      if (r.status !== 200) return null;
      return (JSON.parse(r.body) as ResourceQuotaListJson).items ?? [];
    } catch {
      return null;
    }
  }

  async podIp(ns: string, name: string): Promise<string | null> {
    const pod = await this.getPod(ns, name);
    return pod?.status?.podIP ?? null;
  }

  async podReady(ns: string, name: string): Promise<boolean> {

    return podJsonReady(await this.getPod(ns, name));
  }

  async waitPodReady(
    ns: string,
    name: string,
    opts: { timeoutMs?: number; intervalMs?: number } = {},
  ): Promise<boolean> {
    const r = await this.waitPodReadyOutcome(ns, name, {
      ...opts,
      unschedulableGraceMs: Number.POSITIVE_INFINITY,
    });
    return r.outcome === "ready";
  }

  /**
   * Task #8: ready-wait that FAILS FAST on Unschedulable instead of burning
   * the whole timeout. A pod whose `PodScheduled` condition is
   * `False/Unschedulable` (cluster out of cpu/memory, quota full, …) will
   * not fix itself on acquire's timescale — the first poll already shows
   * the verdict, yet pre-#8 acquire still sat through the full 360s budget.
   *
   * Grace before giving up (`unschedulableGraceMs`, default 2 polling
   * intervals): the condition must PERSIST across the grace window. This
   * tolerates scheduler latency and transient blips — a freshly created pod
   * with no conditions yet, or one whose Unschedulable verdict clears on a
   * later poll (capacity freed, condition flips to PodScheduled=True),
   * RESETS the clock and keeps waiting normally. Only "Pending +
   * PodScheduled=False + reason=Unschedulable" observations count; see
   * `podUnschedulableMessage`.
   *
   * Returns the scheduler's own condition message in the `unschedulable`
   * arm so callers can report the concrete resource reason.
   */
  async waitPodReadyOutcome(
    ns: string,
    name: string,
    opts: { timeoutMs?: number; intervalMs?: number; unschedulableGraceMs?: number } = {},
  ): Promise<PodReadyOutcome> {
    const timeoutMs = opts.timeoutMs ?? 360_000;
    const intervalMs = opts.intervalMs ?? 3_000;
    const graceMs = opts.unschedulableGraceMs ?? 2 * intervalMs;
    const deadline = Date.now() + timeoutMs;
    let unschedulableSince: number | null = null;
    let lastMessage = "";
    while (Date.now() < deadline) {
      const pod = await this.getPod(ns, name);
      if (podJsonReady(pod)) return { outcome: "ready" };
      const message = podUnschedulableMessage(pod);
      if (message !== null) {
        unschedulableSince ??= Date.now();
        lastMessage = message;
        if (Date.now() - unschedulableSince >= graceMs) {
          return { outcome: "unschedulable", message: lastMessage };
        }
      } else {

        unschedulableSince = null;
      }
      await sleep(intervalMs);
    }
    return { outcome: "timeout" };
  }

  async waitPodGone(
    ns: string,
    name: string,
    opts: { timeoutMs?: number; intervalMs?: number } = {},
  ): Promise<boolean> {
    const timeoutMs = opts.timeoutMs ?? 60_000;
    const intervalMs = opts.intervalMs ?? 1_000;
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if ((await this.getPod(ns, name)) === null) return true;
      if (Date.now() >= deadline) return false;
      await sleep(intervalMs);
    }
  }

  async podLog(ns: string, name: string, opts: PodLogOptions = {}): Promise<{ status: number; text: string }> {
    const r = await this.request("GET", this.podLogPath(ns, name, opts));
    return { status: r.status, text: r.body };
  }

  followPodLog(
    ns: string,
    name: string,
    opts: PodLogOptions & { onLine: (line: string) => void },
  ): LineStream {
    return this.lineStream(this.podLogPath(ns, name, opts, { follow: "true" }), opts.onLine);
  }

  async listPodsWithVersion(
    ns: string,
    labelSelector?: string,
  ): Promise<{ items: PodJson[]; resourceVersion: string }> {
    const qs = labelSelector ? `?labelSelector=${encodeURIComponent(labelSelector)}` : "";
    const r = await this.request("GET", `/api/v1/namespaces/${ns}/pods${qs}`);
    if (r.status !== 200) {
      throw new Error(`list pods failed: ${r.status} ${r.body.slice(0, 300)}`);
    }
    const list = JSON.parse(r.body) as PodListJson;
    return { items: list.items ?? [], resourceVersion: list.metadata?.resourceVersion ?? "0" };
  }

  watchPods(
    ns: string,
    opts: {
      labelSelector?: string;
      resourceVersion: string;
      timeoutSeconds?: number;
      onEvent: (event: PodWatchEvent) => void;
    },
  ): PodWatch {
    const params = new URLSearchParams({ watch: "1", allowWatchBookmarks: "true", resourceVersion: opts.resourceVersion });
    if (opts.labelSelector) params.set("labelSelector", opts.labelSelector);
    if (opts.timeoutSeconds) params.set("timeoutSeconds", String(opts.timeoutSeconds));
    let expired = false;
    const stream = this.lineStream(`/api/v1/namespaces/${ns}/pods?${params.toString()}`, (line) => {
      let event: PodWatchEvent;
      try {
        event = JSON.parse(line) as PodWatchEvent;
      } catch {
        return; 
      }
      if (event.type === "ERROR") {
        if (event.object.code === 410) expired = true;
        return;
      }
      opts.onEvent(event);
    });
    return {
      stop: stream.stop,
      done: stream.done.then((end): WatchEnd => {
        if (expired) return { reason: "expired" };
        if (end.error !== undefined || (end.status !== 200 && end.status !== 0)) {
          return { reason: "error", message: end.error ?? `watch HTTP ${end.status}` };
        }
        return { reason: "closed" };
      }),
    };
  }

  async listEvents(ns: string, opts: { fieldSelector?: string } = {}): Promise<K8sEventJson[]> {
    const qs = opts.fieldSelector ? `?fieldSelector=${encodeURIComponent(opts.fieldSelector)}` : "";
    const r = await this.request("GET", `/api/v1/namespaces/${ns}/events${qs}`);
    if (r.status !== 200) {
      throw new Error(`list events failed: ${r.status} ${r.body.slice(0, 300)}`);
    }
    return (JSON.parse(r.body) as { items?: K8sEventJson[] }).items ?? [];
  }

  private podLogPath(ns: string, name: string, opts: PodLogOptions, extra: Record<string, string> = {}): string {
    const params = new URLSearchParams(extra);
    if (opts.container) params.set("container", opts.container);
    if (opts.previous) params.set("previous", "true");
    if (opts.tailLines !== undefined) params.set("tailLines", String(opts.tailLines));
    if (opts.sinceTime) params.set("sinceTime", opts.sinceTime);
    const qs = params.toString();
    return `/api/v1/namespaces/${ns}/pods/${name}/log${qs ? `?${qs}` : ""}`;
  }

  private lineStream(path: string, onLine: (line: string) => void): LineStream {
    const u = new URL(`${this.server}${path}`);
    const lib = u.protocol === "https:" ? https : http;
    let stopped = false;
    let resolveDone!: (r: { status: number; error?: string }) => void;
    const done = new Promise<{ status: number; error?: string }>((resolve) => {
      resolveDone = resolve;
    });
    let settled = false;
    const settle = (r: { status: number; error?: string }) => {
      if (!settled) {
        settled = true;
        resolveDone(r);
      }
    };
    const req = lib.request(
      {
        protocol: u.protocol,
        hostname: u.hostname,
        port: u.port,
        path: `${u.pathname}${u.search}`,
        method: "GET",
        headers: { Authorization: `Bearer ${this.bearerToken()}` },
        agent: this.agent,
      },
      (res) => {
        const status = res.statusCode ?? 0;
        let buffer = "";
        res.on("data", (chunk: Buffer) => {
          if (status !== 200) {
            buffer += chunk.toString("utf-8"); 
            return;
          }
          buffer += chunk.toString("utf-8");
          for (let nl = buffer.indexOf("\n"); nl >= 0; nl = buffer.indexOf("\n")) {
            const line = buffer.slice(0, nl);
            buffer = buffer.slice(nl + 1);
            if (line.length > 0) onLine(line);
          }
        });
        res.on("end", () => {
          if (status === 200 && buffer.length > 0) onLine(buffer); 
          settle(status === 200 ? { status } : { status, error: buffer.slice(0, 500) });
        });
        res.on("aborted", () => settle({ status }));
      },
    );
    req.on("error", (err: Error) => {

      settle(stopped ? { status: 200 } : { status: 0, error: err.message });
    });
    req.end();
    return {
      stop: () => {
        stopped = true;
        req.destroy();
      },
      done,
    };
  }

  podProxyBase(ns: string, name: string, port: number): string {
    return `${this.server}/api/v1/namespaces/${ns}/pods/${name}:${port}/proxy`;
  }

  authedRequestOptions(): { agent: http.Agent | https.Agent; headers: Record<string, string> } {
    return { agent: this.agent, headers: { Authorization: `Bearer ${this.bearerToken()}` } };
  }

  private bearerToken(): string {
    return typeof this.token === "function" ? this.token() : this.token;
  }
}
