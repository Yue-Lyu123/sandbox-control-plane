import { createHash } from "node:crypto";
import { AIOSandboxClient, AIOTransportError, type ExecResult } from "./aio.js";
import type { K8sClient, ResourceQuotaJson } from "./k8s.js";

export const AIO_IMAGE = "ghcr.io/agent-infra/sandbox:1.9.3";
export const AIO_PORT = 8080;
/** 技能落盘根目录——与主服务 `lib/skills/mount-gate.ts` 的同名常量是同一条 wire 约定，两边不互相 import。 */
export const DEFAULT_SKILLS_DEST_ROOT = "/home/gem/skills";
/** pod 里 emptyDir 卷的挂载点（`/home/gem/skills` 是它的软链）；与 `lib/skills/mount-gate.ts` 同值。 */
export const POD_ROOT_SKILLS_DEST_ROOT = "/skills";

export function sandboxPodName(tenant: string, sessionId: string): string {
  const sid = createHash("sha256").update(`${tenant}:${sessionId}`).digest("hex").slice(0, 12);
  return `sbx-${sid}`;
}

export interface PodManifestOptions {
  image?: string;
  labels?: Record<string, string>;

  annotations?: Record<string, string>;
  /**
   * 覆盖容器环境变量。预热池 pod 传空数组：`SESSION_ID` 在创建后**不可修改**，
   * 与其填一个看起来像真会话 id 的错误值（可能导致跨会话串数据），不如不设——
   * 读不到是响亮的失败，读到错的是沉默的错误。见 `pool-maintainer.ts`。
   */
  env?: { name: string; value: string }[];

  port?: number;
}

/**
 * 内网 PyPI 源注入进沙箱 pod（2026-08-30）。
 *
 * 为什么需要：pod 在集群内网，**打不通公网 PyPI**。此前 pod 的环境变量只有
 * `SESSION_ID`，于是任何在沙箱里跑的 `pip install` 都只能由技能作者自己手写
 * `-i <内网源>`——每个人每次都写一遍，写错就是一个装不上的技能。快照构建那条链
 * 早就自动了（`scripts/venv-builder-worker.mjs` 把同样两个变量转成 uv 的
 * `UV_INDEX_URL`/`UV_INSECURE_HOST`），沙箱这条一直没跟上。
 *
 * 四个变量一起给：`pip` 原生读 `PIP_INDEX_URL` / `PIP_TRUSTED_HOST`，`uv` 读
 * `UV_INDEX_URL` / `UV_INSECURE_HOST`，镜像里哪个在都能用上。
 *
 * ⚠️ **URL 里的用户名密码会被剥掉再注入**。pod 里跑的是**技能作者写的代码**，
 * 它读得到自己的环境变量——把带凭据的源地址原样塞进去，等于把 Nexus 账号发给
 * 每一个上传技能的人。现网配的是匿名只读源（`repo.jla.petrotech.cnpc/repository/
 * pypi-public/simple`），剥掉不影响；真哪天换成要认证的源，这里会剥成不可用的
 * 地址而不是静默泄漏，那时该做的是给沙箱单独发一份只读凭据，不是把这段删掉。
 */
export function pipIndexEnv(
  env: NodeJS.ProcessEnv = process.env,
): { name: string; value: string }[] {
  const out: { name: string; value: string }[] = [];
  const raw = env.PIP_INDEX_URL;
  if (raw) {
    let value = raw;
    try {
      const url = new URL(raw);
      if (url.username || url.password) {
        url.username = "";
        url.password = "";
        value = url.toString();
      }
    } catch {
      // 不是合法 URL 就原样透传——判它对错不是这里的职责，pip 自己会报。
    }
    out.push({ name: "PIP_INDEX_URL", value }, { name: "UV_INDEX_URL", value });
  }
  const trusted = env.PIP_TRUSTED_HOST;
  if (trusted) {
    out.push({ name: "PIP_TRUSTED_HOST", value: trusted }, { name: "UV_INSECURE_HOST", value: trusted });
  }
  return out;
}

export function podManifest(name: string, sessionId: string, opts: PodManifestOptions = {}) {
  const image = opts.image ?? AIO_IMAGE;
  const port = opts.port ?? AIO_PORT;
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      name,
      labels: { app: "community-sandbox", "sandbox-name": name, ...(opts.labels ?? {}) } as Record<string, string>,

      annotations: (opts.annotations ?? { "community/session-id": sessionId }) as Record<string, string>,
    },
    spec: {
      restartPolicy: "Never",

      automountServiceAccountToken: false,

      securityContext: { fsGroup: 1000 },

      volumes: [{ name: "skills", emptyDir: {} }],
      containers: [
        {
          name: "sandbox",
          image,
          imagePullPolicy: "IfNotPresent",

          terminationMessagePolicy: "FallbackToLogsOnError",
          ports: [{ containerPort: port, name: "http" }],

          volumeMounts: [{ name: "skills", mountPath: POD_ROOT_SKILLS_DEST_ROOT }],

          securityContext: {
            allowPrivilegeEscalation: false,
            seccompProfile: { type: "RuntimeDefault" },
          },
          readinessProbe: {
            httpGet: { path: "/v1/sandbox", port },

            initialDelaySeconds: 2,
            periodSeconds: 2,
            failureThreshold: 5,
          },
          resources: {

            requests: { cpu: "200m", memory: "512Mi", "ephemeral-storage": "1Gi" },
            limits: { cpu: "1", memory: "2Gi", "ephemeral-storage": "4Gi" },
          },
          // 内网源恒注入：`opts.env` 覆盖的是会话变量（预热池传空数组），
          // 与"这个集群从哪儿装包"无关，两者不该互相顶掉。
          env: [...(opts.env ?? [{ name: "SESSION_ID", value: sessionId }]), ...pipIndexEnv()],
        },
      ],
    },
  };
}

export interface SandboxHandle {
  podName: string;
  namespace: string;
  baseUrl: string;
  via: "pod-ip" | "api-proxy";
  aio: AIOSandboxClient;

  expiresAt: string | null;
}

export const CLAIMED_AT_ANNOTATION = "community/claimed-at";

export function lifetimeStartMs(pod: {
  metadata?: { creationTimestamp?: string; annotations?: Record<string, string> };
}): number | null {
  const claimed = pod.metadata?.annotations?.[CLAIMED_AT_ANNOTATION];
  if (claimed) {
    const t = Date.parse(claimed);
    if (Number.isFinite(t)) return t;
  }
  const created = pod.metadata?.creationTimestamp ? Date.parse(pod.metadata.creationTimestamp) : NaN;
  return Number.isFinite(created) ? created : null;
}

export function parseCpuMillis(q: string): number | null {
  const m = /^([0-9]+(?:\.[0-9]+)?)(n|u|m)?$/.exec(q.trim());
  if (!m) return null;
  const v = Number(m[1]);
  if (!Number.isFinite(v)) return null;
  switch (m[2]) {
    case "n":
      return v / 1e6;
    case "u":
      return v / 1e3;
    case "m":
      return v;
    default:
      return v * 1000;
  }
}

export function parseMemoryBytes(q: string): number | null {
  const m = /^([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E|m)?$/.exec(q.trim());
  if (!m) return null;
  const v = Number(m[1]);
  if (!Number.isFinite(v)) return null;
  const mult: Record<string, number> = {
    Ki: 1024,
    Mi: 1024 ** 2,
    Gi: 1024 ** 3,
    Ti: 1024 ** 4,
    Pi: 1024 ** 5,
    Ei: 1024 ** 6,
    k: 1e3,
    M: 1e6,
    G: 1e9,
    T: 1e12,
    P: 1e15,
    E: 1e18,
    m: 1e-3,
  };
  return v * (m[2] ? mult[m[2]] : 1);
}

export function quotaShortfall(
  quotas: ResourceQuotaJson[],
  req: { cpu?: string; memory?: string },
  lim: { cpu?: string; memory?: string } = {},
): string | null {
  const needCpu = req.cpu !== undefined ? parseCpuMillis(req.cpu) : null;
  const needMem = req.memory !== undefined ? parseMemoryBytes(req.memory) : null;
  const needLimCpu = lim.cpu !== undefined ? parseCpuMillis(lim.cpu) : null;
  const needLimMem = lim.memory !== undefined ? parseMemoryBytes(lim.memory) : null;
  for (const quota of quotas) {
    const qname = quota.metadata?.name ?? "(unnamed)";
    if ((quota.spec?.scopes?.length ?? 0) > 0 || quota.spec?.scopeSelector !== undefined) continue;
    const hard = quota.status?.hard;
    const used = quota.status?.used;
    if (!hard || !used) continue;

    for (const key of ["pods", "count/pods"]) {
      if (hard[key] !== undefined && used[key] !== undefined) {
        const h = Number(hard[key]);
        const u = Number(used[key]);
        if (Number.isFinite(h) && Number.isFinite(u) && u + 1 > h) {
          return `ResourceQuota "${qname}" has no headroom for ${key}: used ${used[key]} of hard ${hard[key]}, need 1 more pod`;
        }
      }
    }
    if (needCpu !== null) {
      for (const key of ["requests.cpu", "cpu"]) {
        if (hard[key] !== undefined && used[key] !== undefined) {
          const h = parseCpuMillis(hard[key]);
          const u = parseCpuMillis(used[key]);
          if (h !== null && u !== null && u + needCpu > h) {
            return `ResourceQuota "${qname}" has no headroom for ${key}: used ${used[key]} of hard ${hard[key]}, need ${req.cpu} more cpu`;
          }
        }
      }
    }
    if (needMem !== null) {
      for (const key of ["requests.memory", "memory"]) {
        if (hard[key] !== undefined && used[key] !== undefined) {
          const h = parseMemoryBytes(hard[key]);
          const u = parseMemoryBytes(used[key]);
          if (h !== null && u !== null && u + needMem > h) {
            return `ResourceQuota "${qname}" has no headroom for ${key}: used ${used[key]} of hard ${hard[key]}, need ${req.memory} more memory`;
          }
        }
      }
    }

    if (needLimCpu !== null && hard["limits.cpu"] !== undefined && used["limits.cpu"] !== undefined) {
      const h = parseCpuMillis(hard["limits.cpu"]);
      const u = parseCpuMillis(used["limits.cpu"]);
      if (h !== null && u !== null && u + needLimCpu > h) {
        return `ResourceQuota "${qname}" has no headroom for limits.cpu: used ${used["limits.cpu"]} of hard ${hard["limits.cpu"]}, need ${lim.cpu} more cpu`;
      }
    }
    if (needLimMem !== null && hard["limits.memory"] !== undefined && used["limits.memory"] !== undefined) {
      const h = parseMemoryBytes(hard["limits.memory"]);
      const u = parseMemoryBytes(used["limits.memory"]);
      if (h !== null && u !== null && u + needLimMem > h) {
        return `ResourceQuota "${qname}" has no headroom for limits.memory: used ${used["limits.memory"]} of hard ${hard["limits.memory"]}, need ${lim.memory} more memory`;
      }
    }
  }
  return null;
}

export class SandboxProvisionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SandboxProvisionError";
  }
}

export class SandboxNotFoundError extends SandboxProvisionError {
  readonly podName: string;
  constructor(podName: string) {
    super(`sandbox not found: ${podName}`);
    this.name = "SandboxNotFoundError";
    this.podName = podName;
  }
}

export class SandboxUnreachableError extends SandboxProvisionError {
  constructor(reason: string) {
    super(`sandbox unreachable: ${reason}`);
    this.name = "SandboxUnreachableError";
  }
}

export type SandboxActivityRecorder = (info: {
  podName: string;
  tenant: string;
  sessionId: string;
}) => void | Promise<void>;

export interface SandboxPoolSeam {

  claimedPodName(tenant: string, sessionId: string): Promise<string | null>;

  claim(tenant: string, sessionId: string, candidates: string[]): Promise<string | null>;

  unclaimed(): Promise<string[]>;

  forget(podName: string): Promise<void>;
}

export const SANDBOX_ROLE_LABEL = "sandbox-role";

export interface SandboxControlPlaneOptions {
  namespace?: string;
  image?: string;

  port?: number;

  activityRecorder?: SandboxActivityRecorder;

  activityHeartbeatMs?: number;

  pool?: SandboxPoolSeam;

  maxAgeMs?: number;
}

export class SandboxControlPlane {
  private readonly k8s: K8sClient;
  private readonly ns: string;
  private readonly image: string;
  private readonly port: number;
  private readonly activityRecorder?: SandboxActivityRecorder;
  private readonly activityHeartbeatMs: number;
  private readonly pool?: SandboxPoolSeam;
  private readonly maxAgeMs?: number;

  constructor(k8s: K8sClient, opts: SandboxControlPlaneOptions = {}) {
    this.k8s = k8s;
    this.ns = opts.namespace ?? k8s.namespace;
    this.image = opts.image ?? AIO_IMAGE;
    this.port = opts.port ?? AIO_PORT;
    this.activityRecorder = opts.activityRecorder;
    this.activityHeartbeatMs = opts.activityHeartbeatMs ?? 60_000;
    this.pool = opts.pool;
    this.maxAgeMs = opts.maxAgeMs;
  }

  /**
   * 这个会话到底该找哪个 pod。**认领表优先，哈希兜底。**
   *
   * 池子 pod 是会话到来之前建的，名字算不出来，所以认领过的会话必须走表；
   * 没认领过的（包括池子没开、或当时池子是空的）仍旧走确定性哈希 —— 那条路径
   * 与引入池子之前一模一样。
   *
   * 表读不到时**退回哈希而不是抛**：DB 抖一下不该让一个本来能跑的会话挂掉。
   * 代价是那一刻可能找错 pod（认领过却查不到），但那会得到一个诚实的
   * `SandboxNotFoundError`(410)，SAR 的语义正是"重新 acquire"，能自愈。
   */
  private async podNameFor(tenant: string, sessionId: string): Promise<string> {
    if (this.pool) {
      try {
        const claimed = await this.pool.claimedPodName(tenant, sessionId);
        if (claimed !== null) return claimed;
      } catch {
        // 见上：退回哈希，不抛。
      }
    }
    return sandboxPodName(tenant, sessionId);
  }

  private async withActivityHeartbeat<T>(
    podName: string,
    tenant: string,
    sessionId: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    if (!this.activityRecorder || !(this.activityHeartbeatMs > 0)) return fn();
    const timer: ReturnType<typeof setInterval> = setInterval(() => {
      void this.recordActivity(podName, tenant, sessionId);
    }, this.activityHeartbeatMs);
    (timer as unknown as { unref?: () => void }).unref?.();
    try {
      return await fn();
    } finally {
      clearInterval(timer);
    }
  }

  private async recordActivity(podName: string, tenant: string, sessionId: string): Promise<void> {
    if (!this.activityRecorder) return;
    try {
      await this.activityRecorder({ podName, tenant, sessionId });
    } catch {
      // Deliberately swallowed: last-activity tracking is advisory (the
      // reaper falls back to creationTimestamp when a row is missing);
      // a DB hiccup must not fail an otherwise-successful sandbox call.
    }
  }

  async acquire(
    tenant: string,
    sessionId: string,
    opts: { readyTimeoutMs?: number; readyIntervalMs?: number; unschedulableGraceMs?: number } = {},
  ): Promise<SandboxHandle> {
    const readyTimeoutMs = opts.readyTimeoutMs ?? 360_000;
    const deadline = Date.now() + readyTimeoutMs;

    const pooled = await this.tryPooledAcquire(tenant, sessionId);
    if (pooled !== null) return pooled;

    const name = await this.podNameFor(tenant, sessionId);
    const existing = await this.k8s.getPod(this.ns, name);
    let mustCreate = existing === null;
    if (existing !== null && existing.metadata?.deletionTimestamp) {

      const gone = await this.k8s.waitPodGone(this.ns, name, {
        timeoutMs: Math.max(0, deadline - Date.now()),
        intervalMs: opts.readyIntervalMs,
      });
      if (!gone) {
        throw new SandboxUnreachableError(
          `pod ${name} is still terminating (previous sandbox for this session not fully deleted after ${readyTimeoutMs}ms); retry acquire shortly`,
        );
      }
      mustCreate = true;
    } else if (
      existing !== null &&
      (existing.status?.phase === "Failed" || existing.status?.phase === "Succeeded")
    ) {

      await this.k8s.delete(this.ns, "pods", name);
      const gone = await this.k8s.waitPodGone(this.ns, name, {
        timeoutMs: Math.max(0, deadline - Date.now()),
        intervalMs: opts.readyIntervalMs,
      });
      if (!gone) {
        throw new SandboxUnreachableError(
          `pod ${name} is ${existing.status.phase} and its corpse is not deleting (still present after ${readyTimeoutMs}ms); retry acquire shortly`,
        );
      }
      mustCreate = true;
    }
    if (mustCreate) {
      const manifest = podManifest(name, sessionId, { image: this.image, port: this.port });
      const declared = manifest.spec.containers[0]?.resources;
      await this.precheckQuota(name, declared?.requests ?? {}, declared?.limits ?? {});
      const r = await this.k8s.create(this.ns, "pods", manifest);
      if (![200, 201, 202].includes(r.status)) {
        throw new SandboxProvisionError(`create pod failed: ${r.status} ${r.body.slice(0, 300)}`);
      }
    }
    const wait = await this.k8s.waitPodReadyOutcome(this.ns, name, {
      timeoutMs: Math.max(0, deadline - Date.now()),
      intervalMs: opts.readyIntervalMs,
      unschedulableGraceMs: opts.unschedulableGraceMs,
    });
    if (wait.outcome === "unschedulable") {

      try {
        await this.k8s.delete(this.ns, "pods", name);
      } catch {
        // Swallowed deliberately: the unschedulable error below is the
        // truth the caller needs; a failed cleanup just means the reaper
        // (or the next acquire) gets a second chance at the corpse.
      }
      throw new SandboxProvisionError(
        `pod ${name} unschedulable: ${wait.message}; deleted the pending pod (best-effort) so it no longer holds ResourceQuota — free capacity or raise the quota, then retry acquire`,
      );
    }
    if (wait.outcome !== "ready") {
      throw new SandboxProvisionError(`pod ${name} not ready within ${readyTimeoutMs}ms`);
    }
    const handle = await this.resolve(name);
    await this.recordActivity(name, tenant, sessionId);
    return handle;
  }

  private async tryPooledAcquire(tenant: string, sessionId: string): Promise<SandboxHandle | null> {
    const pool = this.pool;
    if (!pool) return null;
    try {
      let name = await pool.claimedPodName(tenant, sessionId);

      if (name === null) {

        const ready = await this.readyPoolPods();
        if (ready.length === 0) return null;
        name = await pool.claim(tenant, sessionId, ready);
        if (name === null) return null;

        await this.markPodClaimed(name, sessionId);
      }

      const handle = await this.resolve(name);
      await this.recordActivity(name, tenant, sessionId);
      return handle;
    } catch {

      return null;
    }
  }

  private async readyPoolPods(): Promise<string[]> {
    const pods = await this.k8s.listPods(this.ns, `app=community-sandbox,${SANDBOX_ROLE_LABEL}=pool`);
    return pods
      .filter((p) => p.status?.conditions?.some((c) => c.type === "Ready" && c.status === "True"))
      .map((p) => p.metadata?.name)
      .filter((n): n is string => typeof n === "string" && n.length > 0);
  }

  private async markPodClaimed(podName: string, sessionId: string): Promise<void> {
    try {
      await this.k8s.patchPod(this.ns, podName, {
        metadata: {
          labels: { [SANDBOX_ROLE_LABEL]: "session" },
          annotations: {
            "community/session-id": sessionId,
            [CLAIMED_AT_ANNOTATION]: new Date().toISOString(),
          },
        },
      });
    } catch {
      // 维护器对账时会补打这两处标记。
    }
    await this.resetPooledDisk(podName);
  }

  private async resetPooledDisk(podName: string): Promise<void> {
    try {
      const handle = await this.resolve(podName);
      await handle.aio.execute(
        `rm -rf /home/gem/.config/browser/BrowserMetrics/* ${POD_ROOT_SKILLS_DEST_ROOT}/* 2>/dev/null; true`,
        { timeoutMs: 15_000 },
      );
    } catch {
      // 见方法头部：清不掉就算了，绝对寿命仍然兜底。
    }
  }

  private async precheckQuota(
    name: string,
    requests: { cpu?: string; memory?: string },
    limits: { cpu?: string; memory?: string } = {},
  ): Promise<void> {
    const quotas: ResourceQuotaJson[] | null = await this.k8s.listResourceQuotas(this.ns);
    if (quotas === null || quotas.length === 0) return;
    const shortfall = quotaShortfall(quotas, requests, limits);
    if (!shortfall) return;

    if (this.pool) {
      try {
        const idle = await this.pool.unclaimed();
        if (idle.length > 0) {
          const victim = idle[0];
          await this.k8s.delete(this.ns, "pods", victim);
          await this.pool.forget(victim);

          await this.k8s.waitPodGone(this.ns, victim, { timeoutMs: 15_000 });
          const after = await this.k8s.listResourceQuotas(this.ns);
          if (after !== null && quotaShortfall(after, requests, limits) === null) return;
        }
      } catch {
        // 让位失败就照常抛下面那个错——它至少是句人话。
      }
    }

    throw new SandboxProvisionError(
      `cannot create pod ${name}: ${shortfall}; nothing was created (advisory precheck — free capacity or raise the quota, then retry acquire)`,
    );
  }

  private async resolve(name: string): Promise<SandboxHandle> {
    const pod = await this.k8s.getPod(this.ns, name);
    if (pod === null || pod.metadata?.deletionTimestamp) {
      throw new SandboxNotFoundError(name);
    }

    const startMs = lifetimeStartMs(pod);
    const expiresAt =
      this.maxAgeMs !== undefined && startMs !== null ? new Date(startMs + this.maxAgeMs).toISOString() : null;

    const ip = pod.status?.podIP ?? null;
    if (ip) {
      const base = `http://${ip}:${this.port}`;
      const aio = new AIOSandboxClient(base);
      if (await aio.isReady()) {
        return { podName: name, namespace: this.ns, baseUrl: base, via: "pod-ip", aio, expiresAt };
      }
    }
    const base = this.k8s.podProxyBase(this.ns, name, this.port);
    const { agent, headers } = this.k8s.authedRequestOptions();
    const aio = new AIOSandboxClient(base, { agent, headers });
    if (await aio.isReady()) {
      return { podName: name, namespace: this.ns, baseUrl: base, via: "api-proxy", aio, expiresAt };
    }
    throw new SandboxUnreachableError(
      `pod ${name} exists but its AIO API did not respond (pod-ip and api-proxy both failed)`,
    );
  }

  private async translateTransportErrors<T>(name: string, fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      if (err instanceof AIOTransportError) {
        throw new SandboxUnreachableError(
          `pod ${name} dropped the connection mid-request (${err.message}); the operation may or may not have taken effect — do not assume it ran`,
        );
      }
      throw err;
    }
  }

  async execute(
    tenant: string,
    sessionId: string,
    command: string,
    opts: { timeoutMs?: number } = {},
  ): Promise<ExecResult> {
    const name = await this.podNameFor(tenant, sessionId);
    const handle = await this.resolve(name);

    const result = await this.withActivityHeartbeat(name, tenant, sessionId, () =>
      this.translateTransportErrors(name, () => handle.aio.execute(command, { timeoutMs: opts.timeoutMs })),
    );

    await this.recordActivity(name, tenant, sessionId);
    return result;
  }

  async mountSkills(
    tenant: string,
    sessionId: string,
    files: Array<[string, Buffer]>,
    opts: { destRoot?: string } = {},
  ): Promise<number> {
    const destRoot = (opts.destRoot ?? DEFAULT_SKILLS_DEST_ROOT).replace(/\/$/, "");
    const name = await this.podNameFor(tenant, sessionId);
    const handle = await this.resolve(name);
    let n = 0;
    for (const [rel, content] of files) {
      const path = rel.startsWith("/") ? rel : `${destRoot}/${rel.replace(/^\/+/, "")}`;

      const ok = await this.translateTransportErrors(name, () => handle.aio.writeFile(path, content));
      if (!ok) throw new SandboxProvisionError(`mount failed: ${rel}`);
      n += 1;
    }
    await this.recordActivity(name, tenant, sessionId);
    return n;
  }

  async expiresAtFor(tenant: string, sessionId: string): Promise<string | null> {
    if (this.maxAgeMs === undefined) return null;
    const name = await this.podNameFor(tenant, sessionId);
    const pod = await this.k8s.getPod(this.ns, name);
    if (pod === null) return null;
    const startMs = lifetimeStartMs(pod);
    return startMs === null ? null : new Date(startMs + this.maxAgeMs).toISOString();
  }

  async release(tenant: string, sessionId: string): Promise<boolean> {
    const name = await this.podNameFor(tenant, sessionId);
    const r = await this.k8s.delete(this.ns, "pods", name);
    const released = [200, 202, 404].includes(r.status);
    if (released && this.pool) {

      try {
        await this.pool.forget(name);
      } catch {
        // 尽力而为：忘不掉只是留下一行孤儿，维护器对账时会清掉。
      }
    }
    return released;
  }
}
