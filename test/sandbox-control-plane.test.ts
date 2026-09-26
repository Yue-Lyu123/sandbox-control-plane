import { describe, expect, test } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { K8sClient } from "../src/k8s.js";
import {
  SandboxControlPlane,
  SandboxNotFoundError,
  SandboxProvisionError,
  SandboxUnreachableError,
  sandboxPodName,
} from "../src/control-plane.js";
import { startFakeAioServer, type FakeAioServer } from "./support/fake-aio-server.js";
import { startFakeK8sServer, type FakeK8sServer } from "./support/fake-k8s-server.js";

const NS = "default";

async function setup(readyDelayMs = 30, k8sOpts: { terminationGraceMs?: number } = {}) {
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "fake-aio-"));
  const aio = await startFakeAioServer(workspace);
  const k8sServer = await startFakeK8sServer({ podIp: "127.0.0.1", readyDelayMs, ...k8sOpts });
  const k8s = new K8sClient({ server: k8sServer.url, token: "fake-token", namespace: NS });
  const plane = new SandboxControlPlane(k8s, { port: aio.port });
  return { workspace, aio, k8sServer, k8s, plane };
}

async function teardown(ctx: { aio: FakeAioServer; k8sServer: FakeK8sServer; workspace: string }) {
  await ctx.aio.close();
  await ctx.k8sServer.close();
  fs.rmSync(ctx.workspace, { recursive: true, force: true });
}

describe("sandboxPodName", () => {
  test("is deterministic per (tenant, session) and differs across pairs", () => {
    const a = sandboxPodName("acme", "sess-1");
    expect(a).toBe(sandboxPodName("acme", "sess-1"));
    expect(a).not.toBe(sandboxPodName("acme", "sess-2"));
    expect(a).not.toBe(sandboxPodName("other", "sess-1"));
    expect(a.startsWith("sbx-")).toBe(true);
    expect(a).toMatch(/^sbx-[0-9a-f]{12}$/);
  });
});

describe("SandboxControlPlane.acquire", () => {
  test("creates a pod when absent, and is idempotent on repeat calls (same name, no duplicate create)", async () => {
    const ctx = await setup();
    try {
      const tenant = "acme";
      const session = "sess-1";
      const expectedName = sandboxPodName(tenant, session);

      const handle1 = await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      expect(handle1.podName).toBe(expectedName);
      expect(handle1.via).toBe("pod-ip");
      expect(ctx.k8sServer.createCallCount()).toBe(1);

      const handle2 = await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      expect(handle2.podName).toBe(expectedName);

      expect(ctx.k8sServer.createCallCount()).toBe(1);
    } finally {
      await teardown(ctx);
    }
  });
});

const SKILL_MD = Buffer.from(
  `---
name: hello
description: Greets the world.
---
# hello
`,
);

const RUN_PY = Buffer.from(`
import os
here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(here, "..", "SKILL.md")) as f:
    name_line = f.readlines()[1].strip()
print("hello, world")
print(name_line)
`);

describe("mountSkills -> execute (real file write + real subprocess through the fake AIO server)", () => {
  test("execute genuinely runs a command that depends on the mounted file's actual content", async () => {
    const ctx = await setup();
    try {
      const tenant = "acme";
      const session = "sess-mount";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const mounted = await ctx.plane.mountSkills(tenant, session, [
        ["hello/SKILL.md", SKILL_MD],
        ["hello/scripts/run.py", RUN_PY],
      ]);
      expect(mounted).toBe(2);

      const onDisk = fs.readFileSync(path.join(ctx.workspace, "home/gem/skills/hello/SKILL.md"), "utf-8");
      expect(onDisk).toContain("name: hello");

      const result = await ctx.plane.execute(
        tenant,
        session,
        "python3 /home/gem/skills/hello/scripts/run.py",
      );
      expect(result.error).toBeNull();
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("hello, world");
      expect(result.output).toContain("name: hello"); 
    } finally {
      await teardown(ctx);
    }
  });

  test("execute propagates a clean nonzero exit code (not a transport error)", async () => {
    const ctx = await setup();
    try {
      const tenant = "acme";
      const session = "sess-nonzero";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const result = await ctx.plane.execute(tenant, session, "python3 -c 'import sys; sys.exit(3)'");
      expect(result.error).toBeNull();
      expect(result.exitCode).toBe(3);
      expect(result.success).toBe(true); 
    } finally {
      await teardown(ctx);
    }
  });

  test("execute surfaces AIO's HTTP-200-but-success:false failure mode as a failed ExecResult, not a thrown exception", async () => {
    const ctx = await setup();
    try {
      const tenant = "acme";
      const session = "sess-biz-fail";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const result = await ctx.plane.execute(tenant, session, "__AIO_BUSINESS_FAILURE__");
      expect(result.success).toBe(false);
      expect(result.error).toBe("shell_session_error");
    } finally {
      await teardown(ctx);
    }
  });
});

describe("SandboxControlPlane.release", () => {
  test("deletes the pod; a subsequent acquire() creates a fresh pod after confirming the delete really happened", async () => {
    const ctx = await setup();
    try {
      const tenant = "acme";
      const session = "sess-release";
      const name = sandboxPodName(tenant, session);

      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      expect(ctx.k8sServer.createCallCount()).toBe(1);

      const released = await ctx.plane.release(tenant, session);
      expect(released).toBe(true);

      const goneCheck = await ctx.k8s.getPod(NS, name);
      expect(goneCheck).toBeNull();

      const handle2 = await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      expect(handle2.podName).toBe(name);
      expect(ctx.k8sServer.createCallCount()).toBe(2); 
    } finally {
      await teardown(ctx);
    }
  });
});

describe("Task #16 BUG 2: subshell wrap — bare `exit N` propagates as the exit code instead of poisoning the timeout classifier", () => {

  test("SAR's exit-code matrix through the wrap: false=1, sh -c 'exit 7'=7, (exit 7)=7, bare exit 7=7, echo still-alive=0", async () => {
    const ctx = await setup(0);
    try {
      const tenant = "acme";
      const session = "sess-exit-matrix";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const matrix: Array<[string, number]> = [
        ["false", 1],
        ["sh -c 'exit 7'", 7],
        ["(exit 7)", 7],
        ["exit 7", 7], // the live bug: unwrapped, this killed AIO's persistent shell -> bogus 124
      ];
      for (const [command, expected] of matrix) {
        const result = await ctx.plane.execute(tenant, session, command);
        expect(result.exitCode, `command: ${command}`).toBe(expected);
        expect(result.success, `command: ${command}`).toBe(true); 
        expect(result.error, `command: ${command}`).toBeNull();
      }

      const alive = await ctx.plane.execute(tenant, session, "echo still-alive");
      expect(alive.exitCode).toBe(0);
      expect(alive.output).toContain("still-alive");
    } finally {
      await teardown(ctx);
    }
  });

  test("trailing comments, quotes, &&-chains, and multi-line commands survive the wrap", async () => {
    const ctx = await setup(0);
    try {
      const tenant = "acme";
      const session = "sess-wrap-shapes";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const commented = await ctx.plane.execute(tenant, session, "echo commented-ok # trailing comment )");
      expect(commented.exitCode).toBe(0);
      expect(commented.output).toContain("commented-ok");

      const quoted = await ctx.plane.execute(
        tenant,
        session,
        `echo 'single "inner" quoted' && echo "double 'inner' quoted"`,
      );
      expect(quoted.exitCode).toBe(0);
      expect(quoted.output).toContain(`single "inner" quoted`);
      expect(quoted.output).toContain(`double 'inner' quoted`);

      const multi = await ctx.plane.execute(
        tenant,
        session,
        ['GREETING="hello multi-line"', 'echo "$GREETING" # inline comment', "exit 5"].join("\n"),
      );
      expect(multi.output).toContain("hello multi-line");
      expect(multi.exitCode).toBe(5);
    } finally {
      await teardown(ctx);
    }
  });
});

describe("Task #16 BUG 1a: Terminating pods (deletionTimestamp set) count as NOT FOUND", () => {
  test("release -> immediate execute throws SandboxNotFoundError while the pod is still GETtable with a podIP", async () => {
    const ctx = await setup(0, { terminationGraceMs: 60_000 });
    try {
      const tenant = "acme";
      const session = "sess-terminating";
      const name = sandboxPodName(tenant, session);
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      expect(await ctx.plane.release(tenant, session)).toBe(true);

      expect(ctx.k8sServer.isTerminating(NS, name)).toBe(true);
      expect((await ctx.k8s.getPod(NS, name))?.status?.podIP).toBe("127.0.0.1");

      const err: unknown = await ctx.plane.execute(tenant, session, "echo hi").catch((e) => e);
      expect(err).toBeInstanceOf(SandboxNotFoundError);
      expect((err as Error).message).toBe(`sandbox not found: ${name}`);
    } finally {
      await teardown(ctx);
    }
  });

  test("mountSkills against a Terminating pod also throws SandboxNotFoundError", async () => {
    const ctx = await setup(0, { terminationGraceMs: 60_000 });
    try {
      const tenant = "acme";
      const session = "sess-terminating-mount";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      await ctx.plane.release(tenant, session);

      const err: unknown = await ctx.plane
        .mountSkills(tenant, session, [["hello/SKILL.md", SKILL_MD]])
        .catch((e) => e);
      expect(err).toBeInstanceOf(SandboxNotFoundError);
    } finally {
      await teardown(ctx);
    }
  });

  test("acquire on a Terminating pod waits for it to be gone, then creates fresh (documented wait-for-gone-then-create)", async () => {
    const ctx = await setup(0, { terminationGraceMs: 250 });
    try {
      const tenant = "acme";
      const session = "sess-reacquire";
      const name = sandboxPodName(tenant, session);
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      expect(ctx.k8sServer.createCallCount()).toBe(1);

      await ctx.plane.release(tenant, session);
      expect(ctx.k8sServer.isTerminating(NS, name)).toBe(true);

      const handle = await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 10_000, readyIntervalMs: 25 });
      expect(handle.podName).toBe(name);
      expect(ctx.k8sServer.createCallCount()).toBe(2);
      expect(ctx.k8sServer.isTerminating(NS, name)).toBe(false);

      const result = await ctx.plane.execute(tenant, session, "echo reacquired");
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("reacquired");
    } finally {
      await teardown(ctx);
    }
  });

  test("acquire on a STUCK Terminating pod fails fast with SandboxUnreachableError once the budget is spent (never hangs on waitPodReady)", async () => {
    const ctx = await setup(0, { terminationGraceMs: 60_000 }); 
    try {
      const tenant = "acme";
      const session = "sess-stuck-terminating";
      const name = sandboxPodName(tenant, session);
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      await ctx.plane.release(tenant, session);

      const err: unknown = await ctx.plane
        .acquire(tenant, session, { readyTimeoutMs: 400, readyIntervalMs: 25 })
        .catch((e) => e);
      expect(err).toBeInstanceOf(SandboxUnreachableError);
      expect((err as Error).message).toContain(`pod ${name} is still terminating`);
      expect(ctx.k8sServer.createCallCount()).toBe(1); 
    } finally {
      await teardown(ctx);
    }
  });

  test("finalizePod test seam ends the window early: execute flips from NotFound(Terminating) to NotFound(gone), acquire creates immediately", async () => {
    const ctx = await setup(0, { terminationGraceMs: 60_000 });
    try {
      const tenant = "acme";
      const session = "sess-finalize-seam";
      const name = sandboxPodName(tenant, session);
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      await ctx.plane.release(tenant, session);
      expect(ctx.k8sServer.hasPod(NS, name)).toBe(true);

      ctx.k8sServer.finalizePod(NS, name);
      expect(ctx.k8sServer.hasPod(NS, name)).toBe(false);
      await expect(ctx.plane.execute(tenant, session, "echo hi")).rejects.toBeInstanceOf(SandboxNotFoundError);

      const handle = await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });
      expect(handle.podName).toBe(name);
      expect(ctx.k8sServer.createCallCount()).toBe(2);
    } finally {
      await teardown(ctx);
    }
  });
});

describe("Task #16 BUG 1b: mid-request transport failures throw SandboxUnreachableError (502), never a fake ExecResult", () => {
  test("exec connection destroyed mid-request (socket hang up) -> SandboxUnreachableError, not an HTTP-200 exit -1", async () => {
    const ctx = await setup(0);
    try {
      const tenant = "acme";
      const session = "sess-hangup";
      const name = sandboxPodName(tenant, session);
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const err: unknown = await ctx.plane.execute(tenant, session, "__AIO_SOCKET_HANGUP__").catch((e) => e);
      expect(err).toBeInstanceOf(SandboxUnreachableError);
      expect((err as Error).message).toMatch(/^sandbox unreachable: /);
      expect((err as Error).message).toContain(name);
      expect((err as Error).message).toContain("may or may not"); 
    } finally {
      await teardown(ctx);
    }
  });

  test("mountSkills write connection destroyed mid-request -> SandboxUnreachableError too (consistent with execute), while an answered-but-refused write stays 'mount failed'", async () => {
    const ctx = await setup(0);
    try {
      const tenant = "acme";
      const session = "sess-write-hangup";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const err: unknown = await ctx.plane
        .mountSkills(tenant, session, [["__AIO_WRITE_HANGUP__/SKILL.md", SKILL_MD]])
        .catch((e) => e);
      expect(err).toBeInstanceOf(SandboxUnreachableError);
      expect((err as Error).message).toMatch(/^sandbox unreachable: /);
    } finally {
      await teardown(ctx);
    }
  });
});

describe("SandboxControlPlane error paths", () => {
  test("acquire() throws SandboxProvisionError if the pod never becomes ready in time", async () => {
    const ctx = await setup(60_000); 
    try {
      await expect(
        ctx.plane.acquire("acme", "sess-timeout", { readyTimeoutMs: 200, readyIntervalMs: 20 }),
      ).rejects.toBeInstanceOf(SandboxProvisionError);
    } finally {
      await teardown(ctx);
    }
  });

  test("execute() against a never-acquired session throws SandboxNotFoundError naming the missing pod", async () => {
    const ctx = await setup(0);
    try {
      const name = sandboxPodName("acme", "sess-never-acquired");
      const err: unknown = await ctx.plane.execute("acme", "sess-never-acquired", "echo hi").catch((e) => e);
      expect(err).toBeInstanceOf(SandboxNotFoundError);
      expect(err).toBeInstanceOf(SandboxProvisionError); 
      expect((err as SandboxNotFoundError).podName).toBe(name);
      expect((err as Error).message).toBe(`sandbox not found: ${name}`);
    } finally {
      await teardown(ctx);
    }
  });

  test("execute() against an existing pod whose AIO port is dead throws SandboxUnreachableError (not the old misleading 'ready but' message)", async () => {
    const ctx = await setup(0);
    try {
      const tenant = "acme";
      const session = "sess-dead-aio";
      await ctx.plane.acquire(tenant, session, { readyTimeoutMs: 5_000, readyIntervalMs: 20 });

      const deadPlane = new SandboxControlPlane(ctx.k8s, { port: 1 });
      const err: unknown = await deadPlane.execute(tenant, session, "echo hi").catch((e) => e);
      expect(err).toBeInstanceOf(SandboxUnreachableError);
      expect(err).toBeInstanceOf(SandboxProvisionError);
      expect((err as Error).message).toMatch(/^sandbox unreachable: /);
      expect((err as Error).message).toContain(sandboxPodName(tenant, session));
      expect((err as Error).message).not.toContain("ready but"); 
    } finally {
      await teardown(ctx);
    }
  });
});
