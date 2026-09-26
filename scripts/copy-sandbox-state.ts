/**
 * 切换日搬运：把主服务库里控制面要接管的**小表**拷进控制面库（sandbox_activity / sandbox_pool /
 * sandbox_pod_meta）。观测日志两表不搬（保留期 14 天，从空开始，见 docs §8/§10 G）。
 *
 * 用法：SOURCE_DATABASE_URL=<主库> ./entrypoint.sh run copy-sandbox-state.ts            # 干跑：两边行数
 *       SOURCE_DATABASE_URL=<主库> ./entrypoint.sh run copy-sandbox-state.ts --confirm  # 逐表 upsert（单事务）
 *
 * 幂等：按主键 ON CONFLICT DO UPDATE，可以在主服务停写之后再跑一次收尾。
 */
import { Pool } from "pg";
import { closePool, ensureAllSchemas, getPool } from "../src/db.js";

const TABLES = ["sandbox_activity", "sandbox_pool", "sandbox_pod_meta"] as const;

async function main(): Promise<void> {
  const confirm = process.argv.includes("--confirm");
  const sourceUrl = process.env.SOURCE_DATABASE_URL;
  if (!sourceUrl) throw new Error("SOURCE_DATABASE_URL is required");
  const src = new Pool({ connectionString: sourceUrl, max: 2 });
  await ensureAllSchemas();
  const dst = getPool();
  for (const t of TABLES) {
    const a = (await src.query<{ n: string }>(`SELECT count(*)::text AS n FROM ${t}`)).rows[0]!.n;
    const b = (await dst.query<{ n: string }>(`SELECT count(*)::text AS n FROM ${t}`)).rows[0]!.n;
    console.log(`${t}: source ${a} rows, dest ${b} rows`);
  }
  if (!confirm) {
    console.log("dry-run only; add --confirm to copy");
    await src.end();
    await closePool();
    return;
  }
  const client = await dst.connect();
  try {
    await client.query("BEGIN");
    for (const t of TABLES) {
      const { rows: cols } = await src.query<{ column_name: string }>(
        `SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
        [t],
      );
      const { rows: pk } = await src.query<{ attname: string }>(
        `SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
          WHERE i.indrelid = $1::regclass AND i.indisprimary ORDER BY a.attnum`,
        [t],
      );
      const names = cols.map((c) => c.column_name);
      const { rows } = await src.query(`SELECT ${names.map((n) => `"${n}"`).join(", ")} FROM ${t}`);
      let n = 0;
      for (const row of rows) {
        const values = names.map((c) => (row as Record<string, unknown>)[c]);
        const placeholders = names.map((_, i) => `$${i + 1}`).join(", ");
        const updates = names
          .filter((c) => !pk.some((p) => p.attname === c))
          .map((c) => `"${c}" = EXCLUDED."${c}"`)
          .join(", ");
        await client.query(
          `INSERT INTO ${t} (${names.map((c) => `"${c}"`).join(", ")}) VALUES (${placeholders})
           ON CONFLICT (${pk.map((p) => `"${p.attname}"`).join(", ")}) DO ${updates === "" ? "NOTHING" : `UPDATE SET ${updates}`}`,
          values,
        );
        n += 1;
      }
      console.log(`${t}: upserted ${n}`);
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
  await src.end();
  await closePool();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
