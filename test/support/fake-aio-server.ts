import { exec } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";

export interface FakeAioServer {
  url: string;
  port: number;
  close(): Promise<void>;
}

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const buf = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { "Content-Type": "application/json", "Content-Length": String(buf.length) });
  res.end(buf);
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    req.on("error", reject);
  });
}

export async function startFakeAioServer(
  workspace: string,
  opts: { mountRoot?: string } = {},
): Promise<FakeAioServer> {
  const mountRoot = opts.mountRoot ?? "/home/gem";
  const ws = workspace;

  function real(p: string): string {
    return path.join(ws, p.replace(/^\/+/, ""));
  }

  const server = http.createServer((req, res) => {
    void (async () => {
      try {
        if (req.method === "GET" && req.url === "/v1/sandbox") {
          send(res, 200, { success: true, data: { status: "ready" } });
          return;
        }
        const bodyStr = await readBody(req);
        const body = bodyStr ? (JSON.parse(bodyStr) as Record<string, unknown>) : {};

        if (req.method === "POST" && req.url === "/v1/file/write") {
          const filePath = String(body.file);

          if (filePath.includes("__AIO_WRITE_HANGUP__")) {
            res.socket?.destroy();
            return;
          }
          const raw = String(body.content ?? "");
          const encoding = String(body.encoding ?? "utf-8");
          const data = encoding === "base64" ? Buffer.from(raw, "base64") : Buffer.from(raw, "utf-8");
          const target = real(filePath);
          fs.mkdirSync(path.dirname(target), { recursive: true });
          fs.writeFileSync(target, data);
          send(res, 200, { success: true, data: { path: filePath, size: data.length } });
        } else if (req.method === "POST" && req.url === "/v1/file/read") {
          const target = real(String(body.file));
          if (!fs.existsSync(target)) {
            send(res, 200, { success: false, data: { error_type: "not_found" } });
            return;
          }
          const data = fs.readFileSync(target);
          send(res, 200, { success: true, data: { content: data.toString("base64"), encoding: "base64" } });
        } else if (req.method === "POST" && req.url === "/v1/shell/exec") {
          const command = String(body.command);

          if (command.includes("__AIO_BUSINESS_FAILURE__")) {
            send(res, 200, { success: false, data: { error_type: "shell_session_error" } });
            return;
          }

          if (command.includes("__AIO_SOCKET_HANGUP__")) {
            res.socket?.destroy();
            return;
          }

          const runCmd = command.split(mountRoot).join(path.join(ws, mountRoot.replace(/^\/+/, "")));
          exec(runCmd, { shell: "/bin/bash", cwd: ws }, (err, stdout, stderr) => {
            const output = stdout + stderr;
            const exitCode = err && typeof err.code === "number" ? err.code : 0;
            send(res, 200, {
              success: true,
              data: {
                session_id: "fake",
                status: "completed",
                output,
                console: [],
                exit_code: exitCode,
              },
            });
          });
        } else {
          send(res, 404, { success: false, data: { error_type: "not_found_route" } });
        }
      } catch (e) {
        send(res, 500, { success: false, data: { error_type: "internal", message: String(e) } });
      }
    })();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  return {
    url: `http://127.0.0.1:${port}`,
    port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}
