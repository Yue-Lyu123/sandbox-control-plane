import http from "node:http";

interface PodState {
  createdAt: number;
  podIp: string;
  labels: Record<string, string>;

  annotations?: Record<string, string>;

  creationTimestamp: string;

  deletionTimestamp?: string;

  deletedAtMs?: number;

  unschedulableMessage?: string;

  scheduledNeverReady?: boolean;

  evictedMessage?: string;

  terminatedStatus?: { exitCode?: number; reason?: string; message?: string; restartCount?: number };
}

export interface FakeK8sServerOptions {

  podIp: string;

  readyDelayMs?: number;

  terminationGraceMs?: number;

  unschedulableMessage?: string;

  scheduledNeverReady?: boolean;

  resourceQuotas?: unknown[];

  resourceQuotasStatus?: number;
}

export interface SeedPodOptions {
  namespace: string;
  name: string;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;

  creationTimestamp?: string;

  unschedulableMessage?: string;
}

export interface FakeK8sServer {
  url: string;

  createCallCount(): number;

  setPodLog(namespace: string, name: string, lines: string[]): void;

  appendPodLog(namespace: string, name: string, line: string): void;

  setPodPreviousLog(namespace: string, name: string, lines: string[]): void;

  setPodLogError(namespace: string, name: string, status: number, message: string): void;

  seedEvent(namespace: string, event: Record<string, unknown>): void;

  openWatchCount(): number;

  dropLogFollowers(namespace: string, name: string): void;

  terminatePod(
    namespace: string,
    name: string,
    opts?: { exitCode?: number; reason?: string; message?: string; restartCount?: number },
  ): void;

  seedPod(opts: SeedPodOptions): void;

  setCreationTimestamp(namespace: string, name: string, iso: string): void;

  hasPod(namespace: string, name: string): boolean;

  isTerminating(namespace: string, name: string): boolean;

  finalizePod(namespace: string, name: string): void;

  deleteCalls(): string[];

  resolveUnschedulable(namespace: string, name: string): void;

  evictPod(namespace: string, name: string, message?: string): void;

  resourceQuotaCallCount(): number;
  close(): Promise<void>;
}

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const buf = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { "Content-Type": "application/json", "Content-Length": String(buf.length) });
  res.end(buf);
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    req.on("error", reject);
  });
}

function matchesLabelSelector(labels: Record<string, string>, selector: string | null): boolean {
  if (!selector) return true;
  return selector.split(",").every((clause) => {
    const eq = clause.indexOf("=");
    if (eq < 0) return false; 
    const key = clause.slice(0, eq).trim();
    const value = clause.slice(eq + 1).trim();
    return labels[key] === value;
  });
}

export async function startFakeK8sServer(opts: FakeK8sServerOptions): Promise<FakeK8sServer> {
  const readyDelayMs = opts.readyDelayMs ?? 150;
  const terminationGraceMs = opts.terminationGraceMs ?? 0;
  const pods = new Map<string, PodState>();
  let createCalls = 0;
  let resourceQuotaCalls = 0;
  const deleteCallLog: string[] = [];

  const podLogs = new Map<string, string[]>();

  const podPreviousLogs = new Map<string, string[]>();

  const podLogErrors = new Map<string, { status: number; message: string }>();

  const logFollowers = new Map<string, Set<http.ServerResponse>>();

  const seededEvents = new Map<string, Record<string, unknown>[]>();

  const watchers = new Set<{ ns: string; selector: string | null; res: http.ServerResponse }>();

  let resourceVersion = 1000;

  function broadcast(type: "ADDED" | "MODIFIED" | "DELETED", ns: string, name: string, state: PodState): void {
    resourceVersion += 1;
    const line = `${JSON.stringify({ type, object: podJson(name, state) })}\n`;
    for (const w of watchers) {
      if (w.ns !== ns) continue;
      if (!matchesLabelSelector(state.labels, w.selector)) continue;
      w.res.write(line);
    }
  }

  function endLogFollowers(key: string): void {
    for (const res of logFollowers.get(key) ?? []) res.end();
    logFollowers.delete(key);
  }

  function sweepTerminated(): void {
    for (const [key, state] of pods) {
      if (state.deletedAtMs !== undefined && Date.now() - state.deletedAtMs >= terminationGraceMs) {
        pods.delete(key);
        const slash = key.indexOf("/");
        broadcast("DELETED", key.slice(0, slash), key.slice(slash + 1), state);
        endLogFollowers(key);
      }
    }
  }

  function podJson(name: string, state: PodState) {
    const metadata = {
      name,
      labels: state.labels,
      ...(state.annotations ? { annotations: state.annotations } : {}),
      creationTimestamp: state.creationTimestamp,

      ...(state.deletionTimestamp ? { deletionTimestamp: state.deletionTimestamp } : {}),
    };

    if (state.evictedMessage !== undefined) {
      return {
        apiVersion: "v1",
        kind: "Pod",
        metadata,
        status: {
          phase: "Failed",
          reason: "Evicted",
          message: state.evictedMessage,
          containerStatuses: [
            {
              state: {
                terminated: { exitCode: 137, reason: "ContainerStatusUnknown" },
              },
            },
          ],
        },
      };
    }

    if (state.terminatedStatus !== undefined) {
      const t = state.terminatedStatus;
      return {
        apiVersion: "v1",
        kind: "Pod",
        metadata,
        status: {
          phase: "Failed",
          containerStatuses: [
            {
              name: "sandbox",
              restartCount: t.restartCount ?? 0,
              state: {
                terminated: {
                  exitCode: t.exitCode ?? 1,
                  ...(t.reason !== undefined ? { reason: t.reason } : {}),
                  ...(t.message !== undefined ? { message: t.message } : {}),
                },
              },
            },
          ],
        },
      };
    }

    if (state.unschedulableMessage !== undefined) {
      return {
        apiVersion: "v1",
        kind: "Pod",
        metadata,
        status: {
          phase: "Pending",
          conditions: [
            {
              type: "PodScheduled",
              status: "False",
              reason: "Unschedulable",
              message: state.unschedulableMessage,
              lastProbeTime: null,
              lastTransitionTime: state.creationTimestamp,
            },
          ],
        },
      };
    }

    if (state.scheduledNeverReady) {
      return {
        apiVersion: "v1",
        kind: "Pod",
        metadata,
        status: {
          phase: "Pending",
          conditions: [
            {
              type: "PodScheduled",
              status: "True",
              lastProbeTime: null,
              lastTransitionTime: state.creationTimestamp,
            },
            {
              type: "Ready",
              status: "False",
              reason: "ContainersNotReady",
              message: "containers with unready status: [sandbox]",
              lastProbeTime: null,
              lastTransitionTime: state.creationTimestamp,
            },
          ],
        },
      };
    }
    const ready = Date.now() - state.createdAt >= readyDelayMs;
    return {
      apiVersion: "v1",
      kind: "Pod",
      metadata,
      status: {
        phase: ready ? "Running" : "Pending",
        podIP: ready ? state.podIp : undefined,
        conditions: ready ? [{ type: "Ready", status: "True" }] : [],
      },
    };
  }

  const server = http.createServer((req, res) => {
    void (async () => {
      sweepTerminated();
      const url = new URL(req.url ?? "/", "http://localhost");

      const rq = /^\/api\/v1\/namespaces\/([^/]+)\/resourcequotas$/.exec(url.pathname);
      if (rq && req.method === "GET") {
        resourceQuotaCalls += 1;
        if (opts.resourceQuotasStatus !== undefined && opts.resourceQuotasStatus !== 200) {
          send(res, opts.resourceQuotasStatus, { message: "resourcequota read exploded (test fixture)" });
          return;
        }
        send(res, 200, { apiVersion: "v1", kind: "ResourceQuotaList", items: opts.resourceQuotas ?? [] });
        return;
      }

      const evm = /^\/api\/v1\/namespaces\/([^/]+)\/events$/.exec(url.pathname);
      if (evm && req.method === "GET") {
        send(res, 200, { apiVersion: "v1", kind: "EventList", items: seededEvents.get(evm[1]) ?? [] });
        return;
      }

      const lm = /^\/api\/v1\/namespaces\/([^/]+)\/pods\/([^/]+)\/log$/.exec(url.pathname);
      if (lm && req.method === "GET") {
        const key = `${lm[1]}/${lm[2]}`;
        if (!pods.has(key)) {
          send(res, 404, { message: "not found" });
          return;
        }
        const forced = podLogErrors.get(key);
        if (forced) {
          send(res, forced.status, { kind: "Status", status: "Failure", message: forced.message, code: forced.status });
          return;
        }
        if (url.searchParams.get("previous") === "true") {
          const prev = podPreviousLogs.get(key);
          if (!prev) {
            send(res, 400, {
              kind: "Status",
              status: "Failure",
              message: `previous terminated container "sandbox" in pod "${lm[2]}" not found`,
              code: 400,
            });
            return;
          }
          res.writeHead(200, { "Content-Type": "text/plain" });
          res.end(prev.map((l) => `${l}\n`).join(""));
          return;
        }
        const lines = podLogs.get(key) ?? [];
        const tail = url.searchParams.get("tailLines");
        const served = tail !== null ? lines.slice(-Number(tail)) : lines;
        if (url.searchParams.get("follow") === "true") {
          res.writeHead(200, { "Content-Type": "text/plain" });
          res.write(served.map((l) => `${l}\n`).join(""));
          let set = logFollowers.get(key);
          if (!set) logFollowers.set(key, (set = new Set()));
          set.add(res);
          req.on("close", () => set.delete(res));
          return; 
        }
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end(served.map((l) => `${l}\n`).join(""));
        return;
      }

      const m = /^\/api\/v1\/namespaces\/([^/]+)\/pods(?:\/([^/]+))?$/.exec(url.pathname);
      if (!m) {
        send(res, 404, { message: "not found" });
        return;
      }
      const ns = m[1];
      const name = m[2];

      if (req.method === "GET" && !name && url.searchParams.get("watch") === "1") {
        res.writeHead(200, { "Content-Type": "application/json" });
        if (url.searchParams.get("resourceVersion") === "EXPIRED") {
          res.end(
            `${JSON.stringify({
              type: "ERROR",
              object: { kind: "Status", status: "Failure", reason: "Expired", code: 410, message: "too old resource version" },
            })}\n`,
          );
          return;
        }
        const watcher = { ns, selector: url.searchParams.get("labelSelector"), res };
        watchers.add(watcher);
        req.on("close", () => watchers.delete(watcher));
        return; 
      }

      if (req.method === "POST" && !name) {
        const bodyStr = await readBody(req);
        const manifest = JSON.parse(bodyStr) as {
          metadata: { name: string; labels?: Record<string, string>; annotations?: Record<string, string> };
        };
        const podName = manifest.metadata.name;
        createCalls += 1;
        const key = `${ns}/${podName}`;
        pods.set(key, {
          createdAt: Date.now(),
          podIp: opts.podIp,
          labels: manifest.metadata.labels ?? {},
          ...(manifest.metadata.annotations ? { annotations: manifest.metadata.annotations } : {}),
          creationTimestamp: new Date().toISOString(),

          ...(opts.unschedulableMessage !== undefined
            ? { unschedulableMessage: opts.unschedulableMessage }
            : {}),
          ...(opts.scheduledNeverReady ? { scheduledNeverReady: true } : {}),
        });
        podLogs.set(key, []);
        send(res, 201, podJson(podName, pods.get(key)!));
        broadcast("ADDED", ns, podName, pods.get(key)!);
        return;
      }
      if (req.method === "GET" && !name) {

        const selector = url.searchParams.get("labelSelector");
        const items = [...pods.entries()]
          .filter(([key]) => key.startsWith(`${ns}/`))
          .filter(([, state]) => matchesLabelSelector(state.labels, selector))
          .map(([key, state]) => podJson(key.slice(ns.length + 1), state));
        send(res, 200, {
          apiVersion: "v1",
          kind: "PodList",
          metadata: { resourceVersion: String(resourceVersion) },
          items,
        });
        return;
      }
      if (req.method === "GET" && name) {
        const state = pods.get(`${ns}/${name}`);
        if (!state) {
          send(res, 404, { message: "not found" });
          return;
        }
        send(res, 200, podJson(name, state));
        return;
      }
      if (req.method === "DELETE" && name) {
        const key = `${ns}/${name}`;
        deleteCallLog.push(key); 
        const state = pods.get(key);
        if (!state) {
          send(res, 404, { message: "not found" });
          return;
        }
        if (terminationGraceMs > 0) {

          if (state.deletedAtMs === undefined) {
            state.deletedAtMs = Date.now();
            state.deletionTimestamp = new Date().toISOString();
            broadcast("MODIFIED", ns, name, state); 
          }
          send(res, 200, { message: "deleted" });
          return;
        }
        pods.delete(key);
        send(res, 200, { message: "deleted" });
        broadcast("DELETED", ns, name, state);
        endLogFollowers(key);
        return;
      }
      send(res, 405, { message: "method not allowed" });
    })();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  return {
    url: `http://127.0.0.1:${port}`,
    createCallCount: () => createCalls,
    setPodLog: (namespace: string, name: string, lines: string[]) => {
      podLogs.set(`${namespace}/${name}`, [...lines]);
    },
    appendPodLog: (namespace: string, name: string, line: string) => {
      const key = `${namespace}/${name}`;
      const lines = podLogs.get(key) ?? [];
      lines.push(line);
      podLogs.set(key, lines);
      for (const follower of logFollowers.get(key) ?? []) follower.write(`${line}\n`);
    },
    setPodPreviousLog: (namespace: string, name: string, lines: string[]) => {
      podPreviousLogs.set(`${namespace}/${name}`, [...lines]);
    },
    setPodLogError: (namespace: string, name: string, status: number, message: string) => {
      podLogErrors.set(`${namespace}/${name}`, { status, message });
    },
    seedEvent: (namespace: string, event: Record<string, unknown>) => {
      const list = seededEvents.get(namespace) ?? [];
      list.push(event);
      seededEvents.set(namespace, list);
    },
    openWatchCount: () => watchers.size,
    dropLogFollowers: (namespace: string, name: string) => endLogFollowers(`${namespace}/${name}`),
    terminatePod: (namespace, name, opts = {}) => {
      const key = `${namespace}/${name}`;
      const state = pods.get(key);
      if (!state) throw new Error(`terminatePod: no such pod ${namespace}/${name}`);
      state.terminatedStatus = { exitCode: 1, ...opts };
      broadcast("MODIFIED", namespace, name, state);
      endLogFollowers(key);
    },
    seedPod: (seed: SeedPodOptions) => {
      pods.set(`${seed.namespace}/${seed.name}`, {

        createdAt: Date.now() - readyDelayMs - 60_000,
        podIp: opts.podIp,
        labels: seed.labels ?? {},
        ...(seed.annotations ? { annotations: seed.annotations } : {}),
        creationTimestamp: seed.creationTimestamp ?? new Date().toISOString(),
        ...(seed.unschedulableMessage !== undefined
          ? { unschedulableMessage: seed.unschedulableMessage }
          : {}),
      });
      if (!podLogs.has(`${seed.namespace}/${seed.name}`)) podLogs.set(`${seed.namespace}/${seed.name}`, []);
      broadcast("ADDED", seed.namespace, seed.name, pods.get(`${seed.namespace}/${seed.name}`)!);
    },
    setCreationTimestamp: (namespace: string, name: string, iso: string) => {
      const state = pods.get(`${namespace}/${name}`);
      if (!state) throw new Error(`setCreationTimestamp: no such pod ${namespace}/${name}`);
      state.creationTimestamp = iso;
    },
    hasPod: (namespace: string, name: string) => {
      sweepTerminated();
      return pods.has(`${namespace}/${name}`);
    },
    isTerminating: (namespace: string, name: string) => {
      sweepTerminated();
      return pods.get(`${namespace}/${name}`)?.deletionTimestamp !== undefined;
    },
    finalizePod: (namespace: string, name: string) => {
      const key = `${namespace}/${name}`;
      const state = pods.get(key);
      if (!state || !pods.delete(key)) {
        throw new Error(`finalizePod: no such pod ${namespace}/${name}`);
      }
      broadcast("DELETED", namespace, name, state);
      endLogFollowers(key);
    },
    deleteCalls: () => [...deleteCallLog],
    resolveUnschedulable: (namespace: string, name: string) => {
      const state = pods.get(`${namespace}/${name}`);
      if (!state) throw new Error(`resolveUnschedulable: no such pod ${namespace}/${name}`);
      delete state.unschedulableMessage;
      // The pod now proceeds through the normal Pending -> Running+Ready
      // transition, clocked from its original createdAt (so with a small
      // readyDelayMs it is ready on the next poll — capacity freed, pod runs).
    },
    evictPod: (namespace: string, name: string, message?: string) => {
      const state = pods.get(`${namespace}/${name}`);
      if (!state) throw new Error(`evictPod: no such pod ${namespace}/${name}`);
      state.evictedMessage =
        message ?? "Pod ephemeral local storage usage exceeds the total limit of containers 100Mi.";
    },
    resourceQuotaCallCount: () => resourceQuotaCalls,
    close: () => {

      for (const w of watchers) w.res.end();
      watchers.clear();
      for (const key of [...logFollowers.keys()]) endLogFollowers(key);
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
