import { Pool } from "pg";
import { ensureSandboxActivitySchema } from "./activity.js";
import { ensureSandboxObserveSchema } from "./observer-store.js";
import { ensureSandboxPoolSchema } from "./pool.js";

/**
 * 控制面自己的连接池。读 `DATABASE_URL`；第 2 步与主服务共用同一个库，第 3 步换独立实例。
 * 独立进程（没有 Next 的双模块实例问题），模块级单例就够，不挂 globalThis。
 */
let pool: Pool | null = null;

function poolMax(): number {
  const raw = process.env.PGPOOL_MAX;
  if (raw === undefined) return 4;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 4;
}

export function getPool(): Pool {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error("DATABASE_URL is not set");
    }
    pool = new Pool({ connectionString, max: poolMax() });
  }
  return pool;
}

/** 启动时把控制面名下的全部表 ensure 一遍（DDL 与主服务逐字一致，幂等）。 */
export async function ensureAllSchemas(p: Pool = getPool()): Promise<void> {
  // 顺序：activity 先于 observe（observer-store 的会话读口 join sandbox_activity）。
  await ensureSandboxActivitySchema(p);
  await ensureSandboxObserveSchema(p);
  await ensureSandboxPoolSchema(p);
}

/** 关闭并清空连接池；进程退出与测试换库时用。 */
export async function closePool(): Promise<void> {
  const p = pool;
  pool = null;
  if (p) {
    try {
      await p.end();
    } catch {
      /* 连接可能已经挂了，吞掉 */
    }
  }
}
