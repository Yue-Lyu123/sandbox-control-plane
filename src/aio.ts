import type http from "node:http";
import type https from "node:https";
import { httpRequest } from "./http-client.js";

export interface ExecResult {

  exitCode: number;
  output: string;

  success: boolean;
  error: string | null;
}

export class AIOTransportError extends Error {}

export interface AIOClientOptions {
  timeoutMs?: number;

  agent?: http.Agent | https.Agent;

  headers?: Record<string, string>;
}

interface ExecEnvelope {
  success?: boolean;
  message?: string;
  data?: { exit_code?: number; output?: string; error_type?: string };
}

interface WriteEnvelope {
  success?: boolean;
}

interface ReadEnvelope {
  success?: boolean;
  data?: { content?: string; encoding?: string };
}

export class AIOSandboxClient {
  private readonly base: string;
  private readonly timeoutMs: number;
  private readonly agent?: http.Agent | https.Agent;
  private readonly headers: Record<string, string>;

  constructor(baseUrl: string, options: AIOClientOptions = {}) {
    this.base = baseUrl.replace(/\/$/, "");
    this.timeoutMs = options.timeoutMs ?? 600_000;
    this.agent = options.agent;
    this.headers = options.headers ?? {};
  }

  async isReady(): Promise<boolean> {
    try {
      const r = await httpRequest(`${this.base}/v1/sandbox`, {
        method: "GET",
        agent: this.agent,
        headers: this.headers,
        timeoutMs: 10_000,
      });
      return r.status === 200;
    } catch {
      return false;
    }
  }

  async execute(command: string, opts: { timeoutMs?: number } = {}): Promise<ExecResult> {
    const timeoutMs = opts.timeoutMs ?? this.timeoutMs;
    const wrapped = `(\n${command}\n)`;
    let r;
    try {
      r = await httpRequest(`${this.base}/v1/shell/exec`, {
        method: "POST",
        headers: { ...this.headers, "Content-Type": "application/json" },
        body: JSON.stringify({ command: wrapped }),
        agent: this.agent,
        timeoutMs,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const timedOut = /timed out/i.test(msg);

      if (timedOut) {
        return { exitCode: 124, output: "", success: false, error: `timeout after ${timeoutMs}ms` };
      }
      throw new AIOTransportError(msg);
    }
    if (r.status >= 400) {
      return { exitCode: -1, output: "", success: false, error: `http_${r.status}` };
    }
    const body = JSON.parse(r.body) as ExecEnvelope;
    const data = body.data ?? {};
    const success = Boolean(body.success);
    return {
      exitCode: typeof data.exit_code === "number" ? data.exit_code : -1,
      output: data.output ?? "",
      success,
      error: success ? null : (data.error_type ?? body.message ?? null),
    };
  }

  async writeFile(path: string, content: Buffer): Promise<boolean> {
    let r;
    try {
      r = await httpRequest(`${this.base}/v1/file/write`, {
        method: "POST",
        headers: { ...this.headers, "Content-Type": "application/json" },
        body: JSON.stringify({ file: path, content: content.toString("base64"), encoding: "base64" }),
        agent: this.agent,
        timeoutMs: this.timeoutMs,
      });
    } catch (err) {
      throw new AIOTransportError(err instanceof Error ? err.message : String(err));
    }
    if (r.status >= 400) return false;
    const body = JSON.parse(r.body) as WriteEnvelope;
    return Boolean(body.success);
  }

  async readFile(path: string): Promise<Buffer | null> {
    try {
      const r = await httpRequest(`${this.base}/v1/file/read`, {
        method: "POST",
        headers: { ...this.headers, "Content-Type": "application/json" },
        body: JSON.stringify({ file: path }),
        agent: this.agent,
        timeoutMs: this.timeoutMs,
      });
      if (r.status >= 400) return null;
      const body = JSON.parse(r.body) as ReadEnvelope;
      if (!body.success) return null;
      const data = body.data ?? {};
      const content = data.content ?? "";
      if (data.encoding === "base64") return Buffer.from(content, "base64");
      return Buffer.from(content, "utf-8");
    } catch {
      return null;
    }
  }
}
