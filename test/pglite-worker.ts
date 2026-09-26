/**
 * 每个 vitest worker 起一份自己的内存 PGlite（照主服务 test/pglite-worker.ts）。
 * 端口 = `TEST_DB_PORT` 基准（默认 55700）+ `VITEST_POOL_ID`。内存库，不落盘。
 *
 * `isolate:false` 时 forks 子进程跨文件复用、setupFiles 每个文件仍重新求值，
 * 所以实例挂在 `globalThis` 上只起一次。
 */
import { PGlite } from "@electric-sql/pglite";
import { PGLiteSocketServer } from "@electric-sql/pglite-socket";

const HOST = "127.0.0.1";
const BASE_PORT = Number(process.env.TEST_DB_PORT ?? 55700);
const POOL_ID = Number(process.env.VITEST_POOL_ID ?? 1);
const PORT = BASE_PORT + POOL_ID;

type Holder = { port: number; ready: Promise<void> };
const g = globalThis as unknown as { __pgliteWorker?: Holder };

if (g.__pgliteWorker === undefined) {
  const ready = (async () => {
    const db = await PGlite.create();
    const server = new PGLiteSocketServer({ db, port: PORT, host: HOST, maxConnections: 10 });
    await server.start();
  })();
  g.__pgliteWorker = { port: PORT, ready };
}

process.env.TEST_DB_PORT = String(g.__pgliteWorker.port);
process.env.DATABASE_URL = `postgresql://postgres:postgres@${HOST}:${g.__pgliteWorker.port}/postgres`;

await g.__pgliteWorker.ready;
