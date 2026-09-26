import { defineConfig } from "vitest/config";

/**
 * 全部测试一档：每个 worker 起一份内存 PGlite（test/pglite-worker.ts），端口 = TEST_DB_PORT 基准 + VITEST_POOL_ID。
 * forks + isolate:false：进程跨文件复用，库一 worker 一份（同主服务 integration 档）。
 * 跑法：`TEST_DB_PORT=55700 pnpm test -- --maxWorkers=3`。
 */
export default defineConfig({
  test: {
    environment: "node",
    globals: false,
    testTimeout: 20_000,
    hookTimeout: 20_000,
    include: ["test/**/*.test.ts"],
    setupFiles: ["./test/pglite-worker.ts"],
    pool: "forks",
    maxWorkers: 3,
    fileParallelism: true,
    isolate: false,
  },
});
