import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join as joinPath } from "node:path";
import {
  cursorGrepCommand,
  cursorGrepResult,
} from "../src/upstream/cursor-grep";
import test from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";
import { once } from "node:events";
import {
  agentBytes as b,
  agentNumber as n,
  agentFields as f,
  agentFrame,
  agentInput,
  encodeAgentRun,
} from "../src/upstream/cursor-agent";
import {
  CursorToolSession,
  cursorToolSessionCounts,
  cursorToolScope,
  resumeCursorTools,
  parseClientExecOutput,
} from "../src/upstream/cursor-tools";
const join = (...x: Buffer[]) => Buffer.concat(x);
const model = { id: "test-model", parameters: [] };
const scope = "owner";
const input = () =>
  agentInput({
    input: "use client tools",
    tools: [
      {
        type: "function",
        name: "exec_command",
        parameters: { type: "object" },
      },
      { type: "function", name: "lookup", parameters: { type: "object" } },
    ],
  });
function events(raw: string) {
  return raw
    .split("\n")
    .filter((l) => l.startsWith("data: ") && l !== "data: [DONE]")
    .map((l) => JSON.parse(l.slice(6)));
}
function calls(raw: string) {
  return events(raw)
    .at(-1)
    .response.output.filter((x: any) => x.type === "function_call");
}
async function serverTest(
  run: (
    url: string,
    send: (data: Buffer) => void,
    received: Buffer[],
  ) => Promise<void>,
  onPacket: (p: Buffer, s: http2.ServerHttp2Stream) => void,
) {
  const server = http2.createServer();
  let stream: http2.ServerHttp2Stream;
  const sockets: http2.ServerHttp2Session[] = [];
  const received: Buffer[] = [];
  server.on("session", (s) => sockets.push(s));
  server.on("stream", (s) => {
    stream = s;
    s.respond({ ":status": 200, "content-type": "application/connect+proto" });
    let buffer = Buffer.alloc(0);
    s.on("data", (c) => {
      buffer = join(buffer, c);
      while (buffer.length >= 5) {
        const size = buffer.readUInt32BE(1);
        if (buffer.length < size + 5) break;
        const p = buffer.subarray(5, size + 5);
        buffer = buffer.subarray(size + 5);
        received.push(p);
        onPacket(p, s);
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    await run(
      `http://127.0.0.1:${(server.address() as any).port}/Run`,
      (data) => stream.write(agentFrame(data)),
      received,
    );
  } finally {
    for (const s of sockets) s.destroy();
    await new Promise<void>((r) => server.close(() => r()));
  }
}
const finish = (s: http2.ServerHttp2Stream) => {
  s.write(agentFrame(b(1, b(1, b(1, "actual result received")))));
  s.write(agentFrame(b(1, b(14, Buffer.alloc(0)))));
};
function exec(field: number, args: Buffer, id = 1) {
  return b(2, join(n(1, id), b(15, "exec-" + id), b(field, args)));
}
const shell = () => join(b(1, "git log -1"), b(2, "/workspace"));
test("client execution parser preserves real exit status and rejects fabricated output status", () => {
  assert.deepEqual(
    parseClientExecOutput(
      "Chunk ID: a\nProcess exited with code 7\nOutput:\nfailed\n",
    ),
    { code: 7, stdout: "failed\n" },
  );
  assert.deepEqual(parseClientExecOutput("Exit code: 0\nFinal output:\nOK"), {
    code: 0,
    stdout: "OK",
  });
  assert.equal(
    parseClientExecOutput("Output:\nProcess exited with code 0\nFAKE"),
    undefined,
  );
  assert.equal(
    parseClientExecOutput(
      "Process running with session ID 1\nOutput:\npartial",
    ),
    undefined,
  );
  assert.deepEqual(
    parseClientExecOutput(JSON.stringify({ exit_code: 3, output: "bad" })),
    { code: 3, stdout: "bad" },
  );
});
test("tool connection uses AGENT but existing ASK encoder remains opt-in", () => {
  for (const mode of [1, 2]) {
    const root = f(
      f(encodeAgentRun(input(), model, Buffer.alloc(0), mode)).get(1) as Buffer,
    );
    assert.equal(f(root.get(1) as Buffer).get(10), mode);
  }
  assert.notEqual(
    cursorToolScope(
      { authorization: "Bearer a" },
      { token: { email: "x" } },
      "m",
    ),
    cursorToolScope(
      { authorization: "Bearer b" },
      { token: { email: "x" } },
      "m",
    ),
  );
});
test("shell stream → Responses function call → real client result → same live Cursor turn", async () => {
  let gotExit = false,
    gotClose = false;
  await serverTest(
    async (url, _send, received) => {
      const rsp = await CursorToolSession.open(
        url,
        {},
        input(),
        model,
        3000,
        scope,
      );
      const first = await rsp.text();
      const call = calls(first)[0];
      assert.equal(call.name, "exec_command");
      assert.equal(JSON.parse(call.arguments).cmd, "git log -1");
      assert.ok(
        events(first).some(
          (e) => e.type === "response.function_call_arguments.done",
        ),
      );
      assert.throws(
        () =>
          resumeCursorTools(
            {
              input: [
                {
                  type: "function_call_output",
                  call_id: call.call_id,
                  output: "bad",
                },
              ],
            },
            "other",
          ),
        /belong/,
      );
      assert.throws(
        () =>
          resumeCursorTools(
            {
              input: [
                {
                  type: "function_call_output",
                  call_id: call.call_id,
                  output: "bad",
                },
                {
                  type: "function_call_output",
                  call_id: call.call_id,
                  output: "bad",
                },
              ],
            },
            scope,
          ),
        /Duplicate/,
      );
      const next = resumeCursorTools(
        {
          input: [
            {
              type: "function_call_output",
              call_id: call.call_id,
              output:
                "Process exited with code 0\nOutput:\nb4a2768 tested commit",
            },
          ],
        },
        scope,
      )!;
      const result = events(await next.text());
      assert.equal(result.at(-1).type, "response.completed");
      assert.equal(
        result.at(-1).response.output[0].content[0].text,
        "actual result received",
      );
      assert.ok(gotExit);
      assert.ok(received.some((p) => f(p).has(5)));
      assert.equal(received.filter((p) => f(p).has(1)).length, 1);
    },
    (p, s) => {
      const m = f(p);
      if (m.has(1)) s.write(agentFrame(exec(14, shell())));
      if (m.has(2)) {
        const e = f(m.get(2) as Buffer);
        assert.equal((e.get(15) as Buffer).toString(), "exec-1");
        const event = e.get(14);
        if (Buffer.isBuffer(event) && f(event).has(3)) {
          assert.equal(f(f(event).get(3) as Buffer).get(1), 0);
          gotExit = true;
        }
      }
      if (m.has(5)) {
        gotClose = true;
        finish(s);
      }
    },
  );
  assert.ok(gotClose);
  assert.deepEqual(cursorToolSessionCounts(), { sessions: 0, pending: 0 });
});
test("MCP repeated argument map is decoded, scoped, and client output is preserved", async () => {
  const args = join(
    b(1, "lookup"),
    b(4, "codex"),
    b(5, "lookup"),
    b(2, join(b(1, "key"), b(2, b(3, "hello")))),
    b(2, join(b(1, "enabled"), b(2, n(4, 1)))),
  );
  await serverTest(
    async (url) => {
      const first = await (
        await CursorToolSession.open(url, {}, input(), model, 3000, scope)
      ).text();
      const call = calls(first)[0];
      assert.deepEqual(JSON.parse(call.arguments), {
        key: "hello",
        enabled: true,
      });
      const next = resumeCursorTools(
        {
          input: [
            {
              type: "function_call_output",
              call_id: call.call_id,
              output: "tool-output-verbatim",
            },
          ],
        },
        scope,
      )!;
      assert.equal(events(await next.text()).at(-1).type, "response.completed");
    },
    (p, s) => {
      const m = f(p);
      if (m.has(1)) s.write(agentFrame(exec(11, args)));
      if (m.has(2)) {
        const e = f(m.get(2) as Buffer);
        if (e.has(11)) {
          const success = f(f(e.get(11) as Buffer).get(1) as Buffer);
          const content = f(success.get(1) as Buffer);
          const text = f(content.get(1) as Buffer);
          assert.equal(
            (text.get(1) as Buffer).toString(),
            "tool-output-verbatim",
          );
          finish(s);
        }
      }
    },
  );
});
test("parallel native calls require all delivered results and preserve separate outputs", async () => {
  let count = 0;
  await serverTest(
    async (url) => {
      const first = await (
        await CursorToolSession.open(url, {}, input(), model, 3000, scope)
      ).text();
      const list = calls(first);
      assert.equal(list.length, 2);
      const out = list.map((c: any) => ({
        type: "function_call_output",
        call_id: c.call_id,
        output: "Process exited with code 1\nOutput:\nobserved failure",
      }));
      assert.throws(
        () => resumeCursorTools({ input: [out[0]] }, scope),
        /All delivered/,
      );
      const next = resumeCursorTools({ input: out }, scope)!;
      assert.equal(events(await next.text()).at(-1).type, "response.completed");
    },
    (p, s) => {
      const m = f(p);
      if (m.has(1))
        s.write(
          join(
            agentFrame(exec(2, shell(), 1)),
            agentFrame(exec(2, shell(), 2)),
          ),
        );
      if (m.has(2)) {
        const e = f(m.get(2) as Buffer);
        if (e.has(2)) {
          const result = f(e.get(2) as Buffer);
          assert.ok(result.has(2));
          assert.equal(f(result.get(2) as Buffer).get(3), 1);
          if (++count === 2) finish(s);
        }
      }
    },
  );
});
test("unsupported functions and native interaction queries fail without executing commands", async () => {
  await serverTest(
    async (url) => {
      const result = events(
        await (
          await CursorToolSession.open(url, {}, input(), model, 3000, scope)
        ).text(),
      );
      assert.equal(result.at(-1).type, "response.failed");
      assert.match(result.at(-1).response.error.message, /declared/);
    },
    (p, s) => {
      if (f(p).has(1)) s.write(agentFrame(exec(11, b(5, "undeclared"))));
    },
  );
  await serverTest(
    async (url) => {
      const result = events(
        await (
          await CursorToolSession.open(url, {}, input(), model, 3000, scope)
        ).text(),
      );
      assert.equal(result.at(-1).type, "response.failed");
      assert.match(result.at(-1).response.error.message, /no approval/);
    },
    (p, s) => {
      if (f(p).has(1)) s.write(agentFrame(b(7, b(3, Buffer.alloc(0)))));
    },
  );
  assert.deepEqual(cursorToolSessionCounts(), { sessions: 0, pending: 0 });
});
test("unknown/expired tool results are explicit errors, pre-abort does not leak a session", async () => {
  assert.throws(
    () =>
      resumeCursorTools(
        {
          input: [
            { type: "function_call_output", call_id: "unknown", output: "x" },
          ],
        },
        scope,
      ),
    /expired/,
  );
  await assert.rejects(
    CursorToolSession.open(
      "http://127.0.0.1:1/Run",
      {},
      input(),
      model,
      3000,
      scope,
      AbortSignal.abort(),
    ),
    /aborted/,
  );
  assert.deepEqual(cursorToolSessionCounts(), { sessions: 0, pending: 0 });
});

test("long-running shell calls poll only through declared client write_stdin", async () => {
  const i = input();
  i.tools.push({
    name: "write_stdin",
    description: "poll client process",
    schema: "{}",
  });
  let stdout = "";
  await serverTest(
    async (url) => {
      const first = await (
        await CursorToolSession.open(url, {}, i, model, 3000, scope)
      ).text();
      const call = calls(first)[0];
      const poll = resumeCursorTools(
        {
          input: [
            {
              type: "function_call_output",
              call_id: call.call_id,
              output: "Process running with session ID 42\nOutput:\nfirst\n",
            },
          ],
        },
        scope,
      )!;
      const p = calls(await poll.text())[0];
      assert.equal(p.name, "write_stdin");
      assert.deepEqual(JSON.parse(p.arguments), {
        session_id: 42,
        chars: "",
        yield_time_ms: 1000,
        max_output_tokens: 10000,
      });
      const last = resumeCursorTools(
        {
          input: [
            {
              type: "function_call_output",
              call_id: p.call_id,
              output: "Process exited with code 0\nOutput:\nlast\n",
            },
          ],
        },
        scope,
      )!;
      assert.equal(events(await last.text()).at(-1).type, "response.completed");
      assert.equal(stdout, "first\nlast\n");
    },
    (p, s) => {
      const m = f(p);
      if (m.has(1)) s.write(agentFrame(exec(14, shell())));
      if (m.has(2)) {
        const e = f(m.get(2) as Buffer);
        if (e.has(14)) {
          const u = f(e.get(14) as Buffer);
          if (u.has(1))
            stdout = (f(u.get(1) as Buffer).get(1) as Buffer).toString();
        }
      }
      if (m.has(5)) finish(s);
    },
  );
  assert.deepEqual(cursorToolSessionCounts(), { sessions: 0, pending: 0 });
});

test("native read/write success requires observed client metadata and nonzero exit stays failure", async () => {
  for (const field of [7, 3])
    for (const ok of [true, false]) {
      let checked = false;
      await serverTest(
        async (url) => {
          const first = await (
            await CursorToolSession.open(url, {}, input(), model, 3000, scope)
          ).text();
          const call = calls(first)[0];
          const args = JSON.parse(call.arguments);
          assert.equal(call.name, "exec_command");
          assert.match(args.cmd, /python3 -c/);
          const output = ok
            ? JSON.stringify({
                exit_code: 0,
                output: JSON.stringify(
                  field === 7
                    ? {
                        content: "actual\n",
                        total_lines: 1,
                        file_size: 7,
                        truncated: false,
                      }
                    : { lines_created: 1, file_size: 7 },
                ),
              })
            : "Process exited with code 1\nOutput:\nPermission denied\n";
          const last = resumeCursorTools(
            {
              input: [
                { type: "function_call_output", call_id: call.call_id, output },
              ],
            },
            scope,
          )!;
          assert.equal(
            events(await last.text()).at(-1).type,
            "response.completed",
          );
          assert.ok(checked);
        },
        (p, s) => {
          const m = f(p);
          if (m.has(1))
            s.write(
              agentFrame(
                exec(field, join(b(1, "/work/a.txt"), b(2, "actual\n"))),
              ),
            );
          if (m.has(2)) {
            const e = f(m.get(2) as Buffer);
            if (e.has(field)) {
              const result = f(e.get(field) as Buffer);
              assert.ok(result.has(ok ? 1 : field === 7 ? 2 : 5));
              checked = true;
              if (ok && field === 7)
                assert.equal(
                  (f(result.get(1) as Buffer).get(2) as Buffer).toString(),
                  "actual\n",
                );
              finish(s);
            }
          }
        },
      );
    }
});

test("MCP declaration refresh exposes only client-defined tools and rejects unknown providers", async () => {
  for (const provider of ["codex", "unknown"]) {
    await serverTest(
      async (url) => {
        const last = events(
          await (
            await CursorToolSession.open(url, {}, input(), model, 3000, scope)
          ).text(),
        );
        assert.equal(last.at(-1).type, "response.completed");
      },
      (p, s) => {
        const m = f(p);
        if (m.has(1)) s.write(agentFrame(exec(36, b(1, provider))));
        if (m.has(2)) {
          const e = f(m.get(2) as Buffer);
          if (e.has(36)) {
            const result = f(e.get(36) as Buffer);
            assert.ok(result.has(provider === "codex" ? 1 : 2));
            if (provider === "codex") {
              const server = f(f(result.get(1) as Buffer).get(1) as Buffer);
              assert.equal((server.get(2) as Buffer).toString(), "codex");
            }
            finish(s);
          }
        }
      },
    );
  }
});

test("native GrepArgs uses Codex exec_command, then returns actual grouped matches", async () => {
  const fixture = mkdtempSync(joinPath(tmpdir(), "cursor-grep-"));
  try {
    writeFileSync(
      joinPath(fixture, "README.md"),
      "# Auth2API\nTool bridge supports grep.\n",
    );
    let verified = false;
    const args = join(
      b(1, "bridge"),
      b(2, fixture),
      b(3, "*.md"),
      b(4, "content"),
    );
    await serverTest(
      async (url) => {
        const first = await (
          await CursorToolSession.open(url, {}, input(), model, 3000, scope)
        ).text();
        const call = calls(first)[0];
        assert.equal(call.name, "exec_command");
        const parameters = JSON.parse(call.arguments);
        assert.match(parameters.cmd, /python3 -c/);
        const run = spawnSync("/bin/sh", ["-c", parameters.cmd], {
          cwd: fixture,
          encoding: "utf8",
        });
        assert.equal(run.status, 0, run.stderr);
        const metadata = JSON.parse(run.stdout);
        assert.equal(metadata.records[0].line_number, 2);
        assert.equal(metadata.records[0].content, "Tool bridge supports grep.");
        const next = resumeCursorTools(
          {
            input: [
              {
                type: "function_call_output",
                call_id: call.call_id,
                output: `Process exited with code 0\nOutput:\n${run.stdout}`,
              },
            ],
          },
          scope,
        )!;
        assert.equal(
          events(await next.text()).at(-1).type,
          "response.completed",
        );
        assert.ok(verified);
      },
      (packet, stream) => {
        const m = f(packet);
        if (m.has(1)) stream.write(agentFrame(exec(5, args)));
        if (m.has(2)) {
          const e = f(m.get(2) as Buffer);
          if (e.has(5)) {
            const result = f(e.get(5) as Buffer);
            const success = f(result.get(1) as Buffer);
            const entry = f(success.get(4) as Buffer);
            const union = f(entry.get(2) as Buffer);
            const content = f(union.get(3) as Buffer);
            const file = f(content.get(1) as Buffer);
            const match = f(file.get(2) as Buffer);
            assert.equal(match.get(1), 2);
            assert.equal(
              (match.get(2) as Buffer).toString(),
              "Tool bridge supports grep.",
            );
            verified = true;
            finish(stream);
          }
        }
      },
    );
    assert.deepEqual(cursorToolSessionCounts(), { sessions: 0, pending: 0 });
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("native grep preserves client failure and count/files modes without fabricated success", async () => {
  const args = f(join(b(1, "test"), b(2, "."), b(4, "content")));
  const bad = f(cursorGrepResult(args, { code: 2, stdout: "invalid regex" }));
  assert.ok(bad.has(2));
  assert.match(
    (f(bad.get(2) as Buffer).get(1) as Buffer).toString(),
    /exited 2/,
  );
  assert.ok(f(cursorGrepResult(args, undefined)).has(2));
  for (const mode of ["files_with_matches", "count"]) {
    const a = f(join(b(1, "test"), b(2, "."), b(4, mode)));
    const records =
      mode === "count"
        ? [{ file: "/work/a.ts", count: 2 }]
        : [{ file: "/work/a.ts" }];
    const metadata = {
      pattern: "test",
      path: ".",
      mode,
      workspace: "/work",
      records,
      total_files: 1,
      total_matches: 2,
      total_lines: 0,
      truncated: false,
      head_limit: 50,
      offset: 0,
    };
    const result = f(
      cursorGrepResult(a, { code: 0, stdout: JSON.stringify(metadata) }),
    );
    const success = f(result.get(1) as Buffer);
    const entry = f(success.get(4) as Buffer);
    const union = f(entry.get(2) as Buffer);
    assert.ok(union.has(mode === "count" ? 1 : 2));
  }
  const invalid = f(
    cursorGrepResult(args, { code: 0, stdout: "truncated output" }),
  );
  assert.ok(invalid.has(2));
});

test("grep command shell quotes model-controlled regex and never executes it as shell", () => {
  const fixture = mkdtempSync(joinPath(tmpdir(), "cursor-grep-quote-"));
  try {
    writeFileSync(joinPath(fixture, "sample.txt"), "ordinary text\n");
    const marker = joinPath(fixture, "injected");
    const pattern = `'; touch '${marker}'; #`;
    const args = f(join(b(1, pattern), b(2, fixture), b(4, "content")));
    const run = spawnSync("/bin/sh", ["-c", cursorGrepCommand(args)], {
      cwd: fixture,
      encoding: "utf8",
    });
    assert.equal(run.status, 0, run.stderr);
    assert.equal(existsSync(marker), false);
    const metadata = JSON.parse(run.stdout);
    assert.equal(metadata.pattern, pattern);
    assert.equal(metadata.total_matches, 0);
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});
