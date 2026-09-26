/**
 * 收敛「`let schemaReady: Promise<void> | null = null` + 首次调用建表、并发调用共享同一个
 * in-flight promise」这个模式——全仓 29 份 `ensureXxxSchema` 手写实现，语义逐份核对后一致：
 * 建表失败会把缓存清掉（`ready = null`），下次调用重新尝试，不缓存失败（少数派：零份，29 份
 * 全部如此）。
 *
 * 各 schema.ts 里很多 `ensureXxxSchema` 还导出配套的 `_resetXxxSchemaCacheForTests`
 * 给测试用（跨用例清缓存），所以这里连 reset 一起收，调用方直接转发即可。
 */
const ALL_RESETS = new Set<() => void>();

/**
 * 清掉本进程里**所有** ensurer 的「已建表」缓存，下一次各自的 ensure 会重跑建表 SQL。
 * 给「同一进程换了一份库」的测试用（`test/schema-registry-fresh-db.test.ts` 自带一份内存
 * PGlite 验证空库全量建表）：缓存指向旧库时，新库上的 ensure 会被短路成「已建」。生产代码不该调它。
 * （`scripts/test-api-server.ts` 的 `/__test/reset` 现在从快照恢复、不再 TRUNCATE 掉 bootstrap 行，不用它了。）
 */
export function _resetAllSchemaEnsurersForTests(): void {
  for (const reset of ALL_RESETS) reset();
}

export function schemaEnsurer<Args extends unknown[]>(
  build: (...args: Args) => Promise<unknown>,
): { ensure: (...args: Args) => Promise<void>; reset: () => void } {
  let ready: Promise<void> | null = null;

  const ensure = (...args: Args): Promise<void> => {
    if (!ready) {
      ready = build(...args).then(() => undefined);
      ready.catch(() => {
        ready = null;
      });
    }
    return ready;
  };

  const reset = (): void => {
    ready = null;
  };
  ALL_RESETS.add(reset);

  return { ensure, reset };
}
