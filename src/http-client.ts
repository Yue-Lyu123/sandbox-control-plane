import http from "node:http";
import https from "node:https";
import { URL } from "node:url";

export interface HttpResult {
  status: number;
  body: string;
}

export interface HttpRequestOptions {
  method?: string;
  headers?: Record<string, string>;
  body?: string | Buffer;
  timeoutMs?: number;
  agent?: http.Agent | https.Agent;
}

export function httpRequest(url: string, opts: HttpRequestOptions = {}): Promise<HttpResult> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === "https:" ? https : http;
    const bodyBuf =
      opts.body === undefined ? undefined : Buffer.isBuffer(opts.body) ? opts.body : Buffer.from(opts.body);
    const headers: Record<string, string> = { ...(opts.headers ?? {}) };
    if (bodyBuf) headers["Content-Length"] = String(bodyBuf.length);

    const req = lib.request(
      {
        protocol: u.protocol,
        hostname: u.hostname,
        port: u.port,
        path: `${u.pathname}${u.search}`,
        method: opts.method ?? "GET",
        headers,
        agent: opts.agent,
        timeout: opts.timeoutMs,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf-8") });
        });
      },
    );
    req.on("timeout", () => {
      req.destroy(new Error(`request timed out after ${opts.timeoutMs}ms`));
    });
    req.on("error", reject);
    if (bodyBuf) req.write(bodyBuf);
    req.end();
  });
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
