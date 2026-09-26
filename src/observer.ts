import { SANDBOX_POD_LABEL_SELECTOR } from "./reaper.js";
import { sleep } from "./http-client.js";
import type { K8sEventJson, LineStream, PodJson, PodLogOptions, PodWatch, PodWatchEvent } from "./k8s.js";

export interface ObserverK8s {
  listPodsWithVersion(ns: string, labelSelector?: string): Promise<{ items: PodJson[]; resourceVersion: string }>;
  watchPods(
    ns: string,
    opts: {
      labelSelector?: string;
      resourceVersion: string;
      timeoutSeconds?: number;
      onEvent: (event: PodWatchEvent) => void;
    },
  ): PodWatch;
  followPodLog(ns: string, name: string, opts: PodLogOptions & { onLine: (line: string) => void }): LineStream;
  podLog(ns: string, name: string, opts?: PodLogOptions): Promise<{ status: number; text: string }>;
  listEvents(ns: string, opts?: { fieldSelector?: string }): Promise<K8sEventJson[]>;
}

export interface PodEventRecord {
  podName: string;

  source: "watch" | "k8s-event" | "observer";
  type: string;
  reason: string;
  message: string;
  payload?: unknown;

  dedupKey?: string;
}

export interface ObserverStore {
  appendLogLines(podName: string, lines: string[]): Promise<void>;
  countLogLines(podName: string): Promise<number>;
  insertEvent(event: PodEventRecord): Promise<void>;

  upsertPodMeta(podName: string, sessionId: string | null): Promise<void>;
}

export interface SandboxObserverOptions {
  namespace: string;
  labelSelector?: string;

  reconnectDelayMs?: number;

  eventsPollMs?: number;

  maxLogLinesPerPod?: number;

  watchTimeoutSeconds?: number;

  logFlushMs?: number;
}

interface TrackedPod {
  lastPhase: string;
  stream?: LineStream;

  reattachSince?: string;
  reattachTimer?: ReturnType<typeof setTimeout>;

  pending: string[];
  flushTimer?: ReturnType<typeof setTimeout>;
  storedLines: number;
  truncationMarked: boolean;
  countLoaded: boolean;
  metaWritten: boolean;

  salvageAttempted: boolean;

  terminationRecorded: boolean;
}

const phaseOf = (pod: PodJson): string => {
  if (pod.metadata?.deletionTimestamp) return "Terminating";
  return pod.status?.phase ?? "Unknown";
};

export class SandboxObserver {
  private readonly ns: string;
  private readonly selector: string;
  private readonly reconnectDelayMs: number;
  private readonly eventsPollMs: number;
  private readonly maxLogLines: number;
  private readonly watchTimeoutSeconds: number;
  private readonly logFlushMs: number;
  private readonly pods = new Map<string, TrackedPod>();
  private stopped = false;
  private currentWatch: PodWatch | null = null;

  private writeChain: Promise<void> = Promise.resolve();
  private loops: Promise<void>[] = [];

  constructor(
    private readonly k8s: ObserverK8s,
    private readonly store: ObserverStore,
    opts: SandboxObserverOptions,
  ) {
    this.ns = opts.namespace;
    this.selector = opts.labelSelector ?? SANDBOX_POD_LABEL_SELECTOR;
    this.reconnectDelayMs = opts.reconnectDelayMs ?? 5_000;
    this.eventsPollMs = opts.eventsPollMs ?? 60_000;
    this.maxLogLines = opts.maxLogLinesPerPod ?? 5_000;
    this.watchTimeoutSeconds = opts.watchTimeoutSeconds ?? 300;
    this.logFlushMs = opts.logFlushMs ?? 200;
  }

  start(): void {
    this.loops = [this.watchLoop(), this.eventsLoop()];
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.currentWatch?.stop();
    for (const [name, pod] of this.pods) {
      pod.stream?.stop();
      if (pod.reattachTimer) clearTimeout(pod.reattachTimer);
      if (pod.flushTimer) clearTimeout(pod.flushTimer);
      this.flushPod(name, pod);
    }
    await Promise.all(this.loops);
    await this.writeChain;
  }

  private async watchLoop(): Promise<void> {
    let consecutiveFailures = 0;
    while (!this.stopped) {
      try {
        const { items, resourceVersion } = await this.k8s.listPodsWithVersion(this.ns, this.selector);
        consecutiveFailures = 0;
        const listed = new Set<string>();
        for (const pod of items) {
          const name = pod.metadata?.name;
          if (!name) continue;
          listed.add(name);
          this.observePod(name, pod, "LISTED");
        }

        for (const [name] of this.pods) {
          if (!listed.has(name)) this.dropPod(name, "relist found pod gone");
        }
        const watch = this.k8s.watchPods(this.ns, {
          labelSelector: this.selector,
          resourceVersion,
          timeoutSeconds: this.watchTimeoutSeconds,
          onEvent: (event) => this.handleWatchEvent(event),
        });
        this.currentWatch = watch;
        await watch.done; 
        this.currentWatch = null;
        if (!this.stopped) await sleep(this.reconnectDelayMs);
      } catch {

        consecutiveFailures += 1;
        if (!this.stopped) {
          await sleep(this.reconnectDelayMs * Math.min(2 ** (consecutiveFailures - 1), 16));
        }
      }
    }
  }

  private handleWatchEvent(event: PodWatchEvent): void {
    const name = event.object.metadata?.name;
    if (!name) return;
    if (event.type === "DELETED") {
      this.dropPod(name, "watch DELETED");
      return;
    }
    if (event.type === "ADDED" || event.type === "MODIFIED") {
      this.observePod(name, event.object, event.type);
    }
  }

  private observePod(name: string, pod: PodJson, sighting: string): void {
    const phase = phaseOf(pod);
    let tracked = this.pods.get(name);
    if (!tracked) {
      tracked = {
        lastPhase: "",
        pending: [],
        storedLines: 0,
        truncationMarked: false,
        countLoaded: false,
        metaWritten: false,
        salvageAttempted: false,
        terminationRecorded: false,
      };
      this.pods.set(name, tracked);
    }
    if (!tracked.metaWritten) {
      tracked.metaWritten = true;
      const sessionId = pod.metadata?.annotations?.["community/session-id"] ?? null;
      this.enqueue(() => this.store.upsertPodMeta(name, sessionId));
    }
    if (tracked.lastPhase !== phase) {
      tracked.lastPhase = phase;
      this.enqueueEvent({
        podName: name,
        source: "watch",
        type: sighting === "LISTED" ? "SYNCED" : sighting,
        reason: phase,
        message: `pod observed in phase ${phase}`,
        payload: { conditions: pod.status?.conditions ?? [] },
      });
    }

    const terminated = pod.status?.containerStatuses?.find((c) => c.state?.terminated)?.state?.terminated;
    if (terminated && !tracked.terminationRecorded) {
      tracked.terminationRecorded = true;
      this.enqueueEvent({
        podName: name,
        source: "watch",
        type: "CONTAINER_TERMINATED",
        reason: terminated.reason ?? "",
        message: (terminated.message ?? "").slice(0, 4096),
        payload: { exitCode: terminated.exitCode },
      });
    }

    if ((phase === "Failed" || phase === "Succeeded") && !tracked.salvageAttempted) {
      tracked.salvageAttempted = true;
      this.salvageLogIfEmpty(name, tracked);
    }
    this.ensureLogStream(name, tracked);
  }

  private salvageLogIfEmpty(name: string, tracked: TrackedPod): void {
    this.enqueue(async () => {
      if (!tracked.countLoaded) {
        tracked.countLoaded = true;
        tracked.storedLines = await this.store.countLogLines(name);
      }
      if (tracked.storedLines > 0 || tracked.pending.length > 0) return; 
      const r = await this.k8s.podLog(this.ns, name, { tailLines: this.maxLogLines });
      if (r.status === 200) {
        const lines = r.text.split("\n").filter((l) => l.length > 0);
        if (lines.length > 0) {
          tracked.storedLines += lines.length;
          await this.store.appendLogLines(name, lines);
        }
        await this.store.insertEvent({
          podName: name,
          source: "observer",
          type: "LOG_SALVAGED",
          reason: "TerminalPhaseFetch",
          message: `salvaged ${lines.length} lines from terminal pod (no stream had attached)`,
        });
      } else {
        await this.store.insertEvent({
          podName: name,
          source: "observer",
          type: "LOG_UNSALVAGEABLE",
          reason: `HTTP ${r.status}`,
          message: r.text.slice(0, 500),
        });
      }
    });
  }

  private dropPod(name: string, why: string): void {
    const tracked = this.pods.get(name);
    if (!tracked) return;
    this.pods.delete(name);
    tracked.stream?.stop();
    if (tracked.reattachTimer) clearTimeout(tracked.reattachTimer);
    if (tracked.flushTimer) clearTimeout(tracked.flushTimer);
    this.flushPod(name, tracked);

    this.enqueue(() => this.store.upsertPodMeta(name, null));
    this.enqueueEvent({
      podName: name,
      source: "watch",
      type: "DELETED",
      reason: "Deleted",
      message: why,
    });
  }

  private ensureLogStream(name: string, tracked: TrackedPod): void {
    if (tracked.stream || this.stopped) return;
    if (tracked.lastPhase !== "Running" && tracked.lastPhase !== "Terminating") return; 
    const stream = this.k8s.followPodLog(this.ns, name, {
      sinceTime: tracked.reattachSince,
      onLine: (line) => this.bufferLine(name, tracked, line),
    });
    tracked.stream = stream;
    void stream.done.then(() => {
      tracked.stream = undefined;

      tracked.reattachSince = new Date().toISOString();

      if (!this.stopped && this.pods.get(name) === tracked) {
        tracked.reattachTimer = setTimeout(() => {
          tracked.reattachTimer = undefined;
          if (!this.stopped && this.pods.get(name) === tracked) this.ensureLogStream(name, tracked);
        }, this.reconnectDelayMs);
        tracked.reattachTimer.unref?.();
      }
    });
  }

  private bufferLine(name: string, tracked: TrackedPod, line: string): void {
    tracked.pending.push(line);
    if (tracked.flushTimer) return;
    tracked.flushTimer = setTimeout(() => {
      tracked.flushTimer = undefined;
      this.flushPod(name, tracked);
    }, this.logFlushMs);
    tracked.flushTimer.unref?.();
  }

  private flushPod(name: string, tracked: TrackedPod): void {
    if (tracked.pending.length === 0) return;
    const batch = tracked.pending.splice(0);
    this.enqueue(async () => {
      if (!tracked.countLoaded) {
        tracked.countLoaded = true;
        tracked.storedLines = await this.store.countLogLines(name);
      }
      const room = this.maxLogLines - tracked.storedLines;
      const kept = room > 0 ? batch.slice(0, room) : [];
      if (kept.length > 0) {
        tracked.storedLines += kept.length;
        await this.store.appendLogLines(name, kept);
      }
      if (kept.length < batch.length && !tracked.truncationMarked) {
        tracked.truncationMarked = true;
        await this.store.insertEvent({
          podName: name,
          source: "observer",
          type: "LOG_TRUNCATED",
          reason: "MaxLinesReached",
          message: `stored ${this.maxLogLines} lines; further lines dropped (cap SANDBOX_OBSERVER_MAX_LOG_LINES)`,
        });
      }
    });
  }

  private async eventsLoop(): Promise<void> {
    while (!this.stopped) {
      try {
        const events = await this.k8s.listEvents(this.ns);
        for (const ev of events) {
          const podName = ev.involvedObject?.name ?? "";
          if (ev.involvedObject?.kind !== "Pod" || !podName.startsWith("sbx-")) continue;
          const uid = ev.metadata?.uid ?? `${podName}/${ev.reason}/${ev.lastTimestamp ?? ""}`;
          this.enqueueEvent({
            podName,
            source: "k8s-event",
            type: ev.type ?? "Normal",
            reason: ev.reason ?? "",
            message: ev.message ?? "",
            payload: { firstTimestamp: ev.firstTimestamp, lastTimestamp: ev.lastTimestamp, count: ev.count },

            dedupKey: `${uid}:${ev.count ?? 0}`,
          });
        }
      } catch {
        // events list failed — next tick retries
      }
      if (!this.stopped) await sleep(this.eventsPollMs);
    }
  }

  private enqueue(step: () => Promise<void>): void {
    this.writeChain = this.writeChain.then(step).catch(() => {});
  }

  private enqueueEvent(event: PodEventRecord): void {
    this.enqueue(() => this.store.insertEvent(event));
  }
}
