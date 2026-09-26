export const TEST_DB_HOST = "127.0.0.1";

// 基准端口 55700（其它会话在用 55434 / 55660）；worker 实际端口 = 基准 + VITEST_POOL_ID，由 pglite-worker.ts 改写。
export const TEST_DB_PORT = Number(process.env.TEST_DB_PORT ?? 55700);
export const TEST_DATABASE_URL = `postgresql://postgres:postgres@${TEST_DB_HOST}:${TEST_DB_PORT}/postgres`;
