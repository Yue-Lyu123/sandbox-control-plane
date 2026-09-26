import type { AddressInfo } from "node:net";
import { closePool, ensureAllSchemas } from "./db.js";
import {
  maybeStartSandboxObserver,
  maybeStartSandboxPoolInterval,
  maybeStartSandboxReaperInterval,
  sandboxControlPlaneFromEnv,
  stopSandboxObserver,
} from "./from-env.js";
import { maybeStartObservabilityRetentionInterval } from "./retention.js";
import { stopAllIntervalTasks } from "./scheduler.js";
import { createControlPlaneServer } from "./server.js";

/**
 * 进程入口：读 env → ensure 全部表 → 起定时任务（reaper / observer / pool / retention，
 * 与主服务 instrumentation.ts 的起法一致）→ listen。SIGTERM / SIGINT 优雅退出。
 */
async function main(): Promise<void> {
  const secret = process.env.SANDBOX_CP_INBOUND_SECRET;
  if (!secret) {
    console.error("[sandbox-control-plane] SANDBOX_CP_INBOUND_SECRET is not set; refusing to start");
    process.exit(1);
  }
  if (!process.env.DATABASE_URL) {
    console.error("[sandbox-control-plane] DATABASE_URL is not set; refusing to start");
    process.exit(1);
  }

  await ensureAllSchemas();
  console.log("[sandbox-control-plane] schemas ensured");

  if (sandboxControlPlaneFromEnv() === null) {
    console.warn(
      "[sandbox-control-plane] no K8s credentials (SANDBOX_K8S_* unset, not in-cluster): execution endpoints will 503, observation endpoints stay up",
    );
  }
  maybeStartSandboxReaperInterval();
  maybeStartSandboxObserver();
  maybeStartSandboxPoolInterval();
  maybeStartObservabilityRetentionInterval();

  const server = createControlPlaneServer({ secret });
  // acquire 同步等 pod Ready 最长 360s——关掉 node 默认的请求超时，交给主服务侧的代理超时。
  server.requestTimeout = 0;
  server.headersTimeout = 60_000;

  const port = Number(process.env.PORT ?? 8080);
  const host = process.env.HOST ?? "0.0.0.0";
  await new Promise<void>((resolve) => server.listen(port, host, resolve));
  const addr = server.address() as AddressInfo;
  console.log(`[sandbox-control-plane] listening on ${addr.address}:${addr.port}`);

  let shuttingDown = false;
  const shutdown = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[sandbox-control-plane] ${signal} received, shutting down`);
    stopAllIntervalTasks();
    server.close();
    void (async () => {
      await stopSandboxObserver().catch(() => undefined);
      await closePool();
      process.exit(0);
    })();
    setTimeout(() => process.exit(0), 10_000).unref();
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

main().catch((err) => {
  console.error("[sandbox-control-plane] fatal:", err);
  process.exit(1);
});
