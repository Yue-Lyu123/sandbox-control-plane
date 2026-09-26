/**
 * 控制面错误 → HTTP 状态码。按 `err.name` 判别而不是 `instanceof`：主服务的 HTTP 客户端
 * 会按响应体里的 `error`（类名原文）重建同名 Error，所以这里出线的 `error` 必须是 `err.name`。
 * 与主服务 `lib/sandbox/http.ts::sandboxErrorResponse` 同一张映射表；其它一律 500。
 */
export const ERROR_STATUS: Readonly<Record<string, number>> = {
  SandboxNotFoundError: 410,
  SandboxUnreachableError: 502,
  SandboxNotConfiguredError: 503,
};

export function statusForError(err: unknown): number {
  const name = err instanceof Error ? err.name : "";
  return ERROR_STATUS[name] ?? 500;
}

export function errorBody(err: unknown): { detail: string; error: string } {
  if (err instanceof Error) return { detail: err.message, error: err.name };
  return { detail: String(err), error: "Error" };
}

/** 执行面在没配 K8s 凭据时抛它（与主服务 `client.ts` 的同名类同一条 wire 约定）。 */
export class SandboxNotConfiguredError extends Error {
  constructor() {
    super("sandbox control plane not configured");
    this.name = "SandboxNotConfiguredError";
  }
}
