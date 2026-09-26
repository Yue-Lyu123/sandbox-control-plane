/**
 * 只建表不起服务：对 DATABASE_URL 跑一遍全部 ensure，然后列出表与索引。
 * 用法：./entrypoint.sh run ensure-schema.ts
 */
import { closePool, ensureAllSchemas, getPool } from "../src/db.js";

async function main(): Promise<void> {
  await ensureAllSchemas();
  const pool = getPool();
  const { rows } = await pool.query<{ relname: string }>("SELECT relname FROM pg_stat_user_tables ORDER BY 1");
  console.log("tables:", rows.map((r) => r.relname).join(", "));
  const { rows: idx } = await pool.query<{ indexrelname: string }>(
    "SELECT indexrelname FROM pg_stat_user_indexes ORDER BY 1",
  );
  console.log("indexes:", idx.map((r) => r.indexrelname).join(", "));
  await closePool();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
