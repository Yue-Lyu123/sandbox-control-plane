import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import { K8sClient } from "../src/k8s.js";
import { AIO_IMAGE, SandboxControlPlane, podManifest, sandboxPodName } from "../src/control-plane.js";
import { POD_ROOT_SKILLS_DEST_ROOT } from "../src/control-plane.js";
import { startFakeAioServer } from "./support/fake-aio-server.js";
import { startFakeK8sServer } from "./support/fake-k8s-server.js";

const NS = "default";

interface CapturedManifest {
  metadata?: { name?: string; labels?: Record<string, string> };
  spec?: {
    automountServiceAccountToken?: unknown;
    securityContext?: { fsGroup?: unknown };
    volumes?: Array<{ name?: string; emptyDir?: unknown }>;
    containers?: Array<{
      image?: string;
      securityContext?: { allowPrivilegeEscalation?: unknown };
      volumeMounts?: Array<{ name?: string; mountPath?: string }>;
    }>;
  };
}

async function startManifestCaptureProxy(targetUrl: string): Promise<{
  url: string;
  manifests: CapturedManifest[];
  close: () => Promise<void>;
}> {
  const manifests: CapturedManifest[] = [];
  const target = new URL(targetUrl);
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const body = Buffer.concat(chunks);
      if (req.method === "POST" && /\/pods$/.test(req.url ?? "")) {
        manifests.push(JSON.parse(body.toString("utf-8")) as CapturedManifest);
      }
      const fwd = http.request(
        {
          host: target.hostname,
          port: target.port,
          path: req.url,
          method: req.method,
          headers: { ...req.headers, host: `${target.hostname}:${target.port}` },
        },
        (tres) => {
          res.writeHead(tres.statusCode ?? 502, tres.headers);
          tres.pipe(res);
        },
      );
      fwd.end(body);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  return {
    url: `http://127.0.0.1:${port}`,
    manifests,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

describe("podManifest (V7 hardening)", () => {
  test("sets spec.automountServiceAccountToken to exactly false (not merely absent)", () => {
    const m = podManifest("sbx-unit", "sess-unit");
    expect(m.spec.automountServiceAccountToken).toBe(false);
  });

  test("keeps the pre-existing manifest shape intact around the new field", () => {
    const m = podManifest("sbx-unit", "sess-unit", { labels: { extra: "y" } });
    expect(m.spec.restartPolicy).toBe("Never");
    expect(m.metadata.labels).toMatchObject({ app: "community-sandbox", "sandbox-name": "sbx-unit", extra: "y" });
    const c = m.spec.containers[0];
    expect(c.image).toBe(AIO_IMAGE);
    expect(c.securityContext).toEqual({
      allowPrivilegeEscalation: false,
      seccompProfile: { type: "RuntimeDefault" },
    });
    expect(c.env).toEqual([{ name: "SESSION_ID", value: "sess-unit" }]);
  });

  test("内网 PyPI 源注入 pod env：pip 与 uv 四个变量一起给，会话变量不被顶掉", () => {
    const before = { pip: process.env.PIP_INDEX_URL, trusted: process.env.PIP_TRUSTED_HOST };
    process.env.PIP_INDEX_URL = "https://nexus.example.internal/repository/pypi/simple";
    process.env.PIP_TRUSTED_HOST = "nexus.example.internal";
    try {
      const env = podManifest("sbx-unit", "sess-unit").spec.containers[0].env;
      expect(env).toEqual([
        { name: "SESSION_ID", value: "sess-unit" },
        { name: "PIP_INDEX_URL", value: "https://nexus.example.internal/repository/pypi/simple" },
        { name: "UV_INDEX_URL", value: "https://nexus.example.internal/repository/pypi/simple" },
        { name: "PIP_TRUSTED_HOST", value: "nexus.example.internal" },
        { name: "UV_INSECURE_HOST", value: "nexus.example.internal" },
      ]);
    } finally {
      if (before.pip === undefined) delete process.env.PIP_INDEX_URL;
      else process.env.PIP_INDEX_URL = before.pip;
      if (before.trusted === undefined) delete process.env.PIP_TRUSTED_HOST;
      else process.env.PIP_TRUSTED_HOST = before.trusted;
    }
  });

  test("源地址里的用户名密码必须剥掉再进 pod——沙箱里跑的是技能作者的代码，它读得到自己的 env", () => {
    const before = process.env.PIP_INDEX_URL;
    process.env.PIP_INDEX_URL = "https://bot:s3cret@nexus.example.internal/repository/pypi/simple";
    try {
      const env = podManifest("sbx-unit", "sess-unit").spec.containers[0].env;
      const injected = env.filter((e) => e.name.endsWith("INDEX_URL"));
      expect(injected).toHaveLength(2);
      for (const e of injected) {
        expect(e.value).not.toContain("s3cret");
        expect(e.value).not.toContain("bot:");
        expect(e.value).toContain("nexus.example.internal/repository/pypi/simple");
      }
    } finally {
      if (before === undefined) delete process.env.PIP_INDEX_URL;
      else process.env.PIP_INDEX_URL = before;
    }
  });

  test("预热池 pod（env 传空数组，没有 SESSION_ID）同样拿得到内网源", async () => {
    const before = process.env.PIP_INDEX_URL;
    process.env.PIP_INDEX_URL = "https://nexus.example.internal/repository/pypi/simple";
    try {
      const { poolPodManifest } = await import("../src/pool-maintainer.js");
      const env = poolPodManifest("sbx-pool-unit").spec.containers[0].env;
      expect(env.map((e) => e.name)).toEqual(["PIP_INDEX_URL", "UV_INDEX_URL"]);
      expect(env.some((e) => e.name === "SESSION_ID")).toBe(false);
    } finally {
      if (before === undefined) delete process.env.PIP_INDEX_URL;
      else process.env.PIP_INDEX_URL = before;
    }
  });

  test("issue #46: 遗言 + 对账键 — terminationMessagePolicy FallbackToLogsOnError and the community/session-id annotation", () => {
    const m = podManifest("sbx-unit", "sess-unit");
    expect(m.spec.containers[0].terminationMessagePolicy).toBe("FallbackToLogsOnError");
    expect(m.metadata.annotations).toEqual({ "community/session-id": "sess-unit" });
  });

  test("issue #59: pod-root /skills emptyDir + fsGroup 1000, mounted into the sandbox container", () => {
    const m = podManifest("sbx-unit", "sess-unit");
    expect(m.spec.securityContext).toEqual({ fsGroup: 1000 });
    expect(m.spec.volumes).toEqual([{ name: "skills", emptyDir: {} }]);
    expect(m.spec.containers[0].volumeMounts).toEqual([
      { name: "skills", mountPath: POD_ROOT_SKILLS_DEST_ROOT },
    ]);
    expect(POD_ROOT_SKILLS_DEST_ROOT).toBe("/skills");
  });
});

describe("wire level: what the fake K8s API server receives on pod create", () => {
  test("acquire() POSTs a manifest with automountServiceAccountToken === false, and the hardened pod still works end to end", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "pod-hardening-"));
    const aio = await startFakeAioServer(workspace);
    const k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs: 20 });
    const proxy = await startManifestCaptureProxy(k8sServer.url);
    const k8s = new K8sClient({ server: proxy.url, token: "fake-token", namespace: NS });
    const plane = new SandboxControlPlane(k8s, { port: aio.port });
    try {
      const tenant = "acme";
      const session = "sess-hardened";
      await plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      expect(proxy.manifests).toHaveLength(1);
      const manifest = proxy.manifests[0];
      expect(manifest.metadata?.name).toBe(sandboxPodName(tenant, session));

      expect(manifest.spec?.automountServiceAccountToken).toBe(false);

      expect(manifest.spec?.containers?.[0]?.securityContext?.allowPrivilegeEscalation).toBe(false);

      expect(manifest.spec?.securityContext?.fsGroup).toBe(1000);
      expect(manifest.spec?.volumes).toEqual([{ name: "skills", emptyDir: {} }]);
      expect(manifest.spec?.containers?.[0]?.volumeMounts).toEqual([{ name: "skills", mountPath: "/skills" }]);

      const result = await plane.execute(tenant, session, "echo hardened-and-alive");
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("hardened-and-alive");

      const written = await plane.mountSkills(
        tenant,
        session,
        [["hello/SKILL.md", Buffer.from("# hello\n")]],
        { destRoot: "/skills" },
      );
      expect(written).toBe(1);
      expect(fs.readFileSync(path.join(workspace, "skills/hello/SKILL.md"), "utf-8")).toBe("# hello\n");

      expect(await plane.release(tenant, session)).toBe(true);
    } finally {
      await proxy.close();
      await aio.close();
      await k8sServer.close();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });
});
