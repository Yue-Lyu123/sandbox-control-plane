/**
 * 定时任务登记（替代主服务的 `scheduling/interval-task.ts::registerIntervalTask`）。
 *
 * 语义：按 key 去重注册 setInterval 任务（同 key 第二次注册是 no-op）、可停、unref
 * （不挡进程退出）。间隔从 env 读，env 没配就用默认值；默认值为 null = env-gated，
 * 没配就不起（sandbox reaper/pool 这类，不配就真的不该跑）。
 * 独立进程没有 Next 的双模块实例问题，登记表用模块级 Map 即可。
 */
const tasks = new Map<string, NodeJS.Timeout>();

function positiveNumberFromEnv(name: string): number | null {
  const raw = process.env[name];
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export interface RegisterIntervalTaskOptions {
  key: string;
  intervalEnvVar: string;
  /** 默认间隔（秒）。**null = env-gated**：没配 env 就不起。 */
  defaultIntervalS: number | null;
  getTask: () => (() => Promise<unknown>) | null;
  logPrefix: string;
  /** 起来后立刻先跑一轮。首跑失败与周期跑同待遇：logPrefix 打错误，不外抛。 */
  runImmediately?: boolean;
}

/** 返回是否真的起了（env-gated 且没配 / 已登记过 → false）。 */
export function registerIntervalTask(opts: RegisterIntervalTaskOptions): boolean {
  if (tasks.has(opts.key)) return false;

  const intervalS = positiveNumberFromEnv(opts.intervalEnvVar) ?? opts.defaultIntervalS;
  if (intervalS === null) {
    console.log(`${opts.logPrefix} ${opts.intervalEnvVar} not set; task is env-gated and stays off`);
    return false;
  }

  const run = () => {
    const task = opts.getTask();
    if (!task) return;
    task().catch((err) => {
      console.error(`${opts.logPrefix} background pass failed:`, err);
    });
  };
  if (opts.runImmediately === true) run();

  const timer = setInterval(run, intervalS * 1000);
  timer.unref?.();
  tasks.set(opts.key, timer);
  console.log(`${opts.logPrefix} started (every ${intervalS}s)`);
  return true;
}

export function stopIntervalTask(key: string): void {
  const timer = tasks.get(key);
  if (timer) clearInterval(timer);
  tasks.delete(key);
}

export function stopAllIntervalTasks(): void {
  for (const key of [...tasks.keys()]) stopIntervalTask(key);
}

export function isIntervalTaskRunning(key: string): boolean {
  return tasks.has(key);
}
