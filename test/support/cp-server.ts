import type { AddressInfo } from "node:net";
import { createControlPlaneServer, type ServerDeps } from "../../src/server.js";

/**
 * 测试用：起一个真的控制面 HTTP server（随机端口），取代主服务测试里的「import Next 路由 handler 直调」。
 * 默认 deps 与生产一致（每请求按 env 现建控制面 / reaper），测试照旧按用例改 env。
 */
export const TEST_CP_SECRET = "test-inbound-secret";

export interface CpServer {
  url: string;
  post(path: string, body?: unknown, headers?: Record<string, string>): Promise<Response>;
  get(path: string, headers?: Record<string, string>): Promise<Response>;
  close(): Promise<void>;
}

export async function startCpServer(deps: Partial<ServerDeps> = {}): Promise<CpServer> {
  const server = createControlPlaneServer({ secret: TEST_CP_SECRET, ...deps });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  const url = `http://127.0.0.1:${port}`;
  const auth = { authorization: `Bearer ${TEST_CP_SECRET}` };
  return {
    url,
    post: (path, body = {}, headers = {}) =>
      fetch(`${url}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json", ...auth, ...headers },
        body: typeof body === "string" ? body : JSON.stringify(body),
      }),
    get: (path, headers = {}) => fetch(`${url}${path}`, { headers: { ...auth, ...headers } }),
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
