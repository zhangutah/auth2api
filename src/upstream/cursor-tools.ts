/** Cursor agent ↔ Responses tool handoff. No shell/filesystem execution here.
 * Native wire schema: installed Cursor 3.27.14 workbench bundle, lines 7307/7524.
 * Only declared client functions can be emitted; Codex retains its sandbox and
 * approval policy. Live HTTP/2 state stays private, scoped, bounded and expiring.
 */
import http2 from "node:http2";
import { createHash, randomUUID } from "node:crypto";
import { gunzipSync } from "node:zlib";
import {
  agentBytes as b,
  agentNumber as n,
  agentFields as fields,
  agentFieldList,
  agentFrame,
  encodeAgentRun,
  agentRequestContext,
  type AgentInput,
  type AgentModel,
} from "./cursor-agent";
import { __openaiResponsesGenerator } from "./cursor-api";
import { cursorGrepCommand, cursorGrepResult } from "./cursor-grep";
const join = (...v: Buffer[]) => Buffer.concat(v);
const buf = (
  f: Map<number, Buffer | number>,
  k: number,
): Buffer | undefined => {
  const v = f.get(k);
  return Buffer.isBuffer(v) ? v : undefined;
};
const str = (f: Map<number, Buffer | number>, k: number): string =>
  buf(f, k)?.toString("utf8") || "";
const quote = (s: string) => "'" + s.replace(/'/g, "'\"'\"'") + "'";
interface Call {
  call_id: string;
  name: string;
  arguments: string;
}
interface Pending {
  call: Call;
  field: number;
  exec: Map<number, Buffer | number>;
  args: Map<number, Buffer | number>;
  delivered: boolean;
  stdout?: string;
  polls?: number;
}
type Event =
  | { textDelta: string; reasoningDelta: string; toolCall?: Call }
  | {
      __done: true;
      aggregatedText: string;
      aggregatedReasoning: string;
      error?: string;
    };
class Queue {
  private data: Event[] = [];
  private wake?: () => void;
  private finished = false;
  push(e: Event) {
    if (this.finished) return;
    this.data.push(e);
    if (this.data.length > 4096)
      throw new Error("Cursor event queue exceeds limit");
    this.wake?.();
  }
  close() {
    this.finished = true;
    this.wake?.();
  }
  async *read(): AsyncGenerator<Event> {
    while (true) {
      if (this.data.length) yield this.data.shift()!;
      else if (this.finished) return;
      else
        await new Promise<void>((resolve) => {
          this.wake = resolve;
        });
    }
  }
}
const waiting = new Map<string, CursorToolSession>();
const sessions = new Set<CursorToolSession>();
const LIMIT = 16,
  IDLE_MS = 120_000;
export function cursorToolScope(
  headers: any,
  account: any,
  model: string,
): string {
  return createHash("sha256")
    .update(
      JSON.stringify([
        headers?.authorization || headers?.["x-api-key"] || "internal",
        account.token?.accountUuid || account.token?.email,
        model,
      ]),
    )
    .digest("hex");
}
export function parseClientExecOutput(
  output: string,
): { code: number; stdout: string } | undefined {
  try {
    const o = JSON.parse(output);
    if (
      Number.isInteger(o.exit_code) &&
      o.exit_code >= 0 &&
      typeof o.output === "string"
    )
      return { code: o.exit_code, stdout: o.output };
  } catch {
    /* CLI uses a text envelope */
  }
  const match = /^(?:Final output:|Output:)\s*\r?\n/m.exec(output);
  if (!match) return undefined;
  const marker = match.index;
  const code = output
    .slice(0, marker)
    .match(/^(?:Process exited with code|Exit code:)\s*(-?\d+)\s*$/m);
  if (!code || Number(code[1]) < 0) return undefined;
  return {
    code: Number(code[1]),
    stdout: output.slice(marker + match[0].length),
  };
}
function value(data: Buffer, depth = 0): unknown {
  if (depth > 32) throw new Error("MCP argument nesting exceeds limit");
  const f = fields(data);
  if (f.has(1)) return null;
  if (f.has(2)) {
    const d = buf(f, 2);
    if (!d || d.length !== 8) throw new Error("Invalid MCP number");
    const x = d.readDoubleLE();
    if (!Number.isFinite(x)) throw new Error("Invalid MCP number");
    return x;
  }
  if (f.has(3)) return str(f, 3);
  if (f.has(4)) return !!f.get(4);
  if (f.has(5)) return struct(buf(f, 5)!, depth + 1);
  if (f.has(6))
    return agentFieldList(buf(f, 6)!)
      .filter(([k]) => k === 1)
      .map(([, v]) => {
        if (!Buffer.isBuffer(v)) throw new Error("Invalid MCP list");
        return value(v, depth + 1);
      });
  throw new Error("Unsupported MCP value");
}
function struct(data: Buffer, depth = 0): Record<string, unknown> {
  const result = Object.create(null);
  for (const [k, v] of agentFieldList(data))
    if (k === 1) {
      if (!Buffer.isBuffer(v)) throw new Error("Invalid MCP map");
      const entry = fields(v);
      const val = buf(entry, 2);
      if (!val) throw new Error("Missing MCP argument value");
      result[str(entry, 1)] = value(val, depth + 1);
    }
  return result;
}
function outputText(output: any): string {
  if (typeof output === "string") return output;
  if (
    Array.isArray(output) &&
    output.every(
      (p) =>
        ["input_text", "output_text", "text"].includes(p?.type) &&
        typeof p.text === "string",
    )
  )
    return output.map((p) => p.text).join("\n");
  throw new Error("Tool results must be text; images/files are unsupported");
}
export function resumeCursorTools(
  body: any,
  scope: string,
  signal?: AbortSignal,
): Response | undefined {
  const input = Array.isArray(body.input) ? body.input : [];
  const outputs = input.filter((x: any) => x?.type === "function_call_output");
  const relevant = outputs.filter((x: any) => waiting.has(x.call_id));
  if (!relevant.length) {
    if (outputs.length && input.at(-1)?.type === "function_call_output")
      throw new Error(
        "Pending Cursor tool session expired or is unknown; start a fresh turn",
      );
    return undefined;
  }
  const ids = relevant.map((x: any) => x.call_id);
  if (new Set(ids).size !== ids.length)
    throw new Error("Duplicate tool result");
  const session = waiting.get(ids[0])!;
  if (
    session.scope !== scope ||
    ids.some((id: string) => waiting.get(id) !== session)
  )
    throw new Error(
      "Tool results do not belong to this account/model/API-key session",
    );
  // Validate every output BEFORE consuming any pending call.
  const results = relevant.map((x: any) => ({
    id: x.call_id,
    output: outputText(x.output),
  }));
  if (Buffer.byteLength(JSON.stringify(results)) > 2 * 1024 * 1024)
    throw new Error("Tool results exceed 2 MiB limit");
  const lastOutput = Math.max(...relevant.map((x: any) => input.indexOf(x)));
  if (input.slice(lastOutput + 1).some((x: any) => x?.role === "user"))
    throw new Error(
      "Finish the pending tool round trip before adding a new user message",
    );
  return session.resume(results, signal);
}
export class CursorToolSession {
  readonly scope: string;
  private queue = new Queue();
  private pending = new Map<string, Pending>();
  private backlog: Event[] = [];
  private paused = false;
  private done = false;
  private text = "";
  private reasoning = "";
  private client!: http2.ClientHttp2Session;
  private req!: http2.ClientHttp2Stream;
  private signal?: AbortSignal;
  private timer?: ReturnType<typeof setTimeout>;
  private heartbeat?: ReturnType<typeof setInterval>;
  private handoffScheduled = false;
  private blobs = new Map<string, Buffer>();
  private blobBytes = 0;
  private bytesOut = 0;
  private ctx = agentRequestContext();
  private constructor(
    private input: AgentInput,
    scope: string,
    private apiModel: string,
  ) {
    this.scope = scope;
  }
  static async open(
    url: string,
    headers: Record<string, string>,
    input: AgentInput,
    model: AgentModel,
    timeoutMs: number,
    scope: string,
    signal?: AbortSignal,
    apiModel = model.id,
  ): Promise<Response> {
    if (signal?.aborted) throw new Error("request aborted");
    if (sessions.size >= LIMIT)
      return new Response(
        JSON.stringify({
          error: {
            message: "Cursor tool session capacity reached",
            type: "rate_limit_error",
          },
        }),
        { status: 503, headers: { "Content-Type": "application/json" } },
      );
    const session = new CursorToolSession(input, scope, apiModel);
    sessions.add(session);
    try {
      return await session.start(url, headers, model, timeoutMs, signal);
    } catch (e) {
      session.stop();
      throw e;
    }
  }
  private abort = () => this.stop("client disconnected");
  private attach(signal?: AbortSignal) {
    this.detach();
    this.signal = signal;
    if (signal?.aborted) this.stop("client disconnected");
    else signal?.addEventListener("abort", this.abort, { once: true });
  }
  private detach() {
    this.signal?.removeEventListener("abort", this.abort);
    this.signal = undefined;
  }
  private deadline(ms: number) {
    clearTimeout(this.timer);
    this.timer = setTimeout(
      () => this.stop("Cursor tool session deadline exceeded"),
      ms,
    );
    this.timer.unref();
  }
  private send(data: Buffer) {
    if (!this.done) this.req.write(agentFrame(data));
  }
  private sendExec(p: Pending, field: number, result: Buffer) {
    this.send(
      b(
        2,
        join(
          n(1, Number(p.exec.get(1) || 0)),
          b(15, str(p.exec, 15)),
          b(field, result),
        ),
      ),
    );
  }
  private emit(e: Event) {
    this.bytesOut += Buffer.byteLength(JSON.stringify(e));
    if (this.bytesOut > 32 * 1024 * 1024)
      throw new Error("Cursor tool output exceeds limit");
    if (this.paused) {
      this.backlog.push(e);
      if (this.backlog.length > 4096)
        throw new Error("Cursor backlog exceeds limit");
      return;
    }
    if (!("__done" in e) && e.toolCall) {
      const p = this.pending.get(e.toolCall.call_id);
      if (p) p.delivered = true;
    }
    if (!("__done" in e)) {
      this.text += e.textDelta;
      this.reasoning += e.reasoningDelta;
    }
    this.queue.push(e);
  }
  private handoff() {
    if (this.handoffScheduled || this.paused || this.done) return;
    this.handoffScheduled = true;
    queueMicrotask(() => {
      this.handoffScheduled = false;
      if (this.done || this.paused || !this.pending.size) return;
      this.queue.push({
        __done: true,
        aggregatedText: this.text,
        aggregatedReasoning: this.reasoning,
      });
      this.queue.close();
      this.paused = true;
      this.detach();
      this.deadline(IDLE_MS);
    });
  }
  private response(): Response {
    const queue = this.queue,
      session = this;
    const generator = __openaiResponsesGenerator(
      this.apiModel,
      queue.read() as any,
    );
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        try {
          for await (const event of generator)
            controller.enqueue(new TextEncoder().encode(event));
          controller.close();
        } catch (e) {
          session.stop();
          controller.error(e);
        }
      },
      cancel() {
        session.stop("client disconnected");
      },
    });
    return new Response(stream, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" },
    });
  }
  private stop(error?: string) {
    if (this.done) return;
    this.done = true;
    this.detach();
    clearTimeout(this.timer);
    clearInterval(this.heartbeat);
    if (!this.paused) {
      this.queue.push({
        __done: true,
        aggregatedText: this.text,
        aggregatedReasoning: this.reasoning,
        error,
      });
      this.queue.close();
    }
    for (const id of this.pending.keys()) waiting.delete(id);
    this.pending.clear();
    this.blobs.clear();
    this.backlog = [];
    sessions.delete(this);
    this.req?.close();
    this.client?.close();
  }
  private bridgeExec(exec: Buffer) {
    const e = fields(exec);
    if (buf(e, 10)) {
      this.send(
        b(
          2,
          join(
            n(1, Number(e.get(1) || 0)),
            b(15, str(e, 15)),
            b(10, b(1, b(1, this.ctx))),
          ),
        ),
      );
      return;
    }
    if (buf(e, 36)) {
      // This is declaration metadata, not execution or approval of an MCP tool.
      const requested = agentFieldList(buf(e, 36)!)
        .filter(([k]) => k === 1)
        .map(([, v]) => (Buffer.isBuffer(v) ? v.toString("utf8") : ""));
      const result = requested.some((id) => id !== "codex")
        ? b(
            2,
            b(1, "Only the client-declared codex tool provider is available"),
          )
        : b(
            1,
            b(
              1,
              join(
                b(1, "codex"),
                b(2, "codex"),
                ...this.input.tools.map((t) =>
                  b(
                    5,
                    join(
                      b(1, t.name),
                      b(2, t.description),
                      b(4, "codex"),
                      b(5, t.name),
                      b(6, t.schema),
                    ),
                  ),
                ),
              ),
            ),
          );
      this.send(
        b(
          2,
          join(n(1, Number(e.get(1) || 0)), b(15, str(e, 15)), b(36, result)),
        ),
      );
      return;
    }
    // Cursor's own allowlist is NOT used to waive Codex approvals.
    if (buf(e, 42)) {
      this.send(
        b(
          2,
          join(n(1, Number(e.get(1) || 0)), b(15, str(e, 15)), b(42, n(1, 0))),
        ),
      );
      return;
    }
    const field = [11, 14, 2, 7, 3, 5].find((k) => buf(e, k));
    if (!field)
      throw new Error(
        "Unsupported native Cursor exec fields: " + [...e.keys()].join(","),
      );
    const args = fields(buf(e, field)!);
    let name: string, parameters: any;
    if (field === 11) {
      if (args.get(7))
        throw new Error(
          "MCP approval-only request cannot be executed as a tool",
        );
      if (str(args, 4) && str(args, 4) !== "codex")
        throw new Error("Unknown MCP provider");
      name = str(args, 5) || str(args, 1);
      const obj = Object.create(null);
      for (const [k, v] of agentFieldList(buf(e, field)!))
        if (k === 2) {
          if (!Buffer.isBuffer(v)) throw new Error("Invalid MCP argument");
          const a = fields(v);
          const val = buf(a, 2);
          if (!val) throw new Error("Missing MCP argument");
          obj[str(a, 1)] = value(val);
        }
      parameters = obj;
    } else {
      name =
        this.input.tools.find(
          (t) => t.name === "exec_command" || t.name.endsWith(".exec_command"),
        )?.name || "";
      if (field === 14 || field === 2) {
        const cmd = str(args, 1);
        if (!cmd.trim()) throw new Error("Empty native shell command");
        parameters = { cmd, yield_time_ms: 1000, max_output_tokens: 10000 };
        if (str(args, 2)) parameters.workdir = str(args, 2);
      } else if (field === 5) {
        // Native GrepArgs must be executed by Codex's declared, sandboxed tool.
        parameters = {
          cmd: cursorGrepCommand(args),
          yield_time_ms: 1000,
          max_output_tokens: 20000,
        };
      } else {
        const path = str(args, 1);
        if (!path) throw new Error("Missing native file path");
        const readScript =
          "import pathlib,json,sys; p=pathlib.Path(sys.argv[1]); data=p.read_text(); lines=data.splitlines(keepends=True); start=max(0,int(sys.argv[2])-1); end=start+int(sys.argv[3]) if int(sys.argv[3]) else len(lines); print(json.dumps(dict(content=''.join(lines[start:end]),total_lines=len(lines),file_size=p.stat().st_size,truncated=end<len(lines))))";
        const text = str(args, 2);
        if (field === 3 && buf(args, 5)?.length)
          throw new Error("Binary file writes are unsupported");
        const writeScript =
          "import pathlib,json,sys; p=pathlib.Path(sys.argv[1]); data=sys.argv[2]; p.write_text(data); print(json.dumps(dict(lines_created=len(data.splitlines()),file_size=p.stat().st_size)))";
        const operands =
          field === 7
            ? [path, String(args.get(4) || 1), String(args.get(5) || 0)]
            : [path, text];
        parameters = {
          cmd:
            "python3 -c " +
            quote(field === 7 ? readScript : writeScript) +
            " " +
            operands.map(quote).join(" "),
          yield_time_ms: 1000,
          max_output_tokens: 10000,
        };
      }
    }
    if (!name || !this.input.tools.some((t) => t.name === name))
      throw new Error(
        "Native tool has no declared Codex client function: " + name,
      );
    const call: Call = {
      call_id: "call_" + randomUUID().replace(/-/g, ""),
      name,
      arguments: JSON.stringify(parameters),
    };
    const p: Pending = { call, field, exec: e, args, delivered: !this.paused };
    if (this.pending.size >= 32)
      throw new Error("Too many pending Cursor tools");
    this.pending.set(call.call_id, p);
    waiting.set(call.call_id, this);
    this.emit({ textDelta: "", reasoningDelta: "", toolCall: call });
    this.handoff();
  }
  resume(
    results: { id: string; output: string }[],
    signal?: AbortSignal,
  ): Response {
    if (this.done || !this.paused)
      throw new Error("Cursor tool session is not awaiting results");
    if (signal?.aborted) throw new Error("request aborted");
    const supplied = new Set(results.map((x) => x.id));
    if ([...this.pending].some(([id, p]) => p.delivered && !supplied.has(id)))
      throw new Error("All delivered pending tool calls require results");
    this.queue = new Queue();
    this.text = "";
    this.reasoning = "";
    this.paused = false;
    this.attach(signal);
    this.deadline(IDLE_MS);
    const response = this.response();
    const backlog = this.backlog;
    this.backlog = [];
    for (const event of backlog) this.emit(event);
    try {
      for (const { id, output } of results) {
        const p = this.pending.get(id)!;
        this.pending.delete(id);
        waiting.delete(id);
        if (p.field === 11)
          this.sendExec(p, 11, b(1, b(1, b(1, b(1, output)))));
        else {
          const parsed = parseClientExecOutput(output);
          // A running Codex process is not an exit: ask its own client to poll.
          // Never execute/poll a process on the proxy or manufacture success.
          if ([14, 2, 5, 7, 3].includes(p.field)) {
            const marker = /^(?:Final output:|Output:)\s*\r?\n/m.exec(output);
            const running =
              marker &&
              output
                .slice(0, marker.index)
                .match(/^Process running with session ID\s+(\d+)\s*$/m);
            if (!parsed && running) {
              const pollName = this.input.tools.find(
                (t) =>
                  t.name === "write_stdin" || t.name.endsWith(".write_stdin"),
              )?.name;
              if (!pollName)
                throw new Error(
                  "Running client command requires a declared write_stdin function",
                );
              const sessionId = Number(running[1]);
              if (!Number.isSafeInteger(sessionId) || (p.polls || 0) >= 128)
                throw new Error("Client command polling limit exceeded");
              p.stdout =
                (p.stdout || "") +
                output.slice(marker!.index + marker![0].length);
              if (Buffer.byteLength(p.stdout) > 2 * 1024 * 1024)
                throw new Error("Client command output exceeds limit");
              p.polls = (p.polls || 0) + 1;
              p.call = {
                call_id: "call_" + randomUUID().replace(/-/g, ""),
                name: pollName,
                arguments: JSON.stringify({
                  session_id: sessionId,
                  chars: "",
                  yield_time_ms: 1000,
                  max_output_tokens: 10000,
                }),
              };
              p.delivered = true;
              this.pending.set(p.call.call_id, p);
              waiting.set(p.call.call_id, this);
              this.emit({
                textDelta: "",
                reasoningDelta: "",
                toolCall: p.call,
              });
              this.handoff();
              continue;
            }
            if (parsed && p.stdout) parsed.stdout = p.stdout + parsed.stdout;
          }
          if (p.field === 14) {
            if (parsed) {
              this.sendExec(p, 14, b(1, b(1, parsed.stdout)));
              this.sendExec(
                p,
                14,
                b(3, join(n(1, parsed.code), b(2, str(p.args, 2)))),
              );
            } else
              this.sendExec(
                p,
                14,
                b(
                  5,
                  join(
                    b(1, str(p.args, 1)),
                    b(2, str(p.args, 2)),
                    b(
                      3,
                      "Client execution did not report a completed exit status: " +
                        output,
                    ),
                  ),
                ),
              );
            this.send(b(5, b(1, n(1, Number(p.exec.get(1) || 0)))));
          } else if (p.field === 2) {
            if (parsed)
              this.sendExec(
                p,
                2,
                b(
                  parsed.code === 0 ? 1 : 2,
                  join(
                    b(1, str(p.args, 1)),
                    b(2, str(p.args, 2)),
                    n(3, parsed.code),
                    b(5, parsed.stdout),
                  ),
                ),
              );
            else
              this.sendExec(
                p,
                2,
                b(
                  4,
                  join(
                    b(1, str(p.args, 1)),
                    b(2, str(p.args, 2)),
                    b(
                      3,
                      "Client execution did not report a completed exit status: " +
                        output,
                    ),
                  ),
                ),
              );
          } else if (p.field === 5) {
            // Includes observed status and structured match data; failures remain errors.
            this.sendExec(p, 5, cursorGrepResult(p.args, parsed));
          } else {
            let metadata: any;
            try {
              if (parsed?.code === 0) metadata = JSON.parse(parsed.stdout);
            } catch {
              /* Preserve actual failure, don't fabricate file success. */
            }
            if (p.field === 7) {
              if (
                metadata &&
                typeof metadata.content === "string" &&
                Number.isSafeInteger(metadata.total_lines) &&
                Number.isSafeInteger(metadata.file_size)
              )
                this.sendExec(
                  p,
                  7,
                  b(
                    1,
                    join(
                      b(1, str(p.args, 1)),
                      b(2, metadata.content),
                      n(3, metadata.total_lines),
                      n(4, metadata.file_size),
                      n(6, metadata.truncated ? 1 : 0),
                    ),
                  ),
                );
              else
                this.sendExec(
                  p,
                  7,
                  b(2, join(b(1, str(p.args, 1)), b(2, output))),
                );
            } else {
              if (
                metadata &&
                Number.isSafeInteger(metadata.lines_created) &&
                Number.isSafeInteger(metadata.file_size)
              )
                this.sendExec(
                  p,
                  3,
                  b(
                    1,
                    join(
                      b(1, str(p.args, 1)),
                      n(2, metadata.lines_created),
                      n(3, metadata.file_size),
                    ),
                  ),
                );
              else
                this.sendExec(
                  p,
                  3,
                  b(5, join(b(1, str(p.args, 1)), b(2, output))),
                );
            }
          }
        }
      }
      if (this.pending.size) this.handoff();
    } catch (e: any) {
      this.stop(e.message);
    }
    return response;
  }
  private start(
    url: string,
    headers: Record<string, string>,
    model: AgentModel,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<Response> {
    return new Promise((resolve, reject) => {
      const target = new URL(url);
      this.client = http2.connect(target.origin);
      this.req = this.client.request({
        ...Object.fromEntries(
          Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]),
        ),
        ":method": "POST",
        ":path": target.pathname,
      });
      this.attach(signal);
      this.deadline(Math.min(timeoutMs, 600_000));
      this.heartbeat = setInterval(
        () => this.send(b(7, Buffer.alloc(0))),
        5000,
      );
      this.heartbeat.unref();
      let status = 0,
        buffered = Buffer.alloc(0),
        failure = Buffer.alloc(0),
        responded = false;
      const fail = (message: string) => {
        if (!responded) {
          responded = true;
          reject(new Error(message));
        }
        this.stop(message);
      };
      this.req.on("response", (h) => {
        status = Number(h[":status"]);
        if (status === 200) {
          responded = true;
          resolve(this.response());
        }
      });
      this.req.on("data", (chunk: Buffer) => {
        if (this.done) return;
        try {
          if (status !== 200) {
            failure = join(failure, chunk);
            if (failure.length > 65536)
              throw new Error("Upstream error body exceeds limit");
            return;
          }
          buffered = join(buffered, chunk);
          while (buffered.length >= 5 && !this.done) {
            const flags = buffered[0],
              size = buffered.readUInt32BE(1);
            if (size > 8 * 1024 * 1024 || flags > 3)
              throw new Error("Invalid/oversized Connect frame");
            if (buffered.length < size + 5) break;
            let packet = buffered.subarray(5, size + 5);
            buffered = buffered.subarray(size + 5);
            if (flags & 1)
              packet = gunzipSync(packet, { maxOutputLength: 8 * 1024 * 1024 });
            if (flags & 2) {
              const end = JSON.parse(packet.toString());
              throw new Error(
                end.error
                  ? `${end.error.code} — ${end.error.message}`
                  : "Cursor stream ended without turn_ended",
              );
            }
            const m = fields(packet),
              interaction = buf(m, 1),
              kv = buf(m, 4),
              exec = buf(m, 2);
            if (interaction) {
              const u = fields(interaction);
              for (const [field, key] of [
                [1, "textDelta"],
                [4, "reasoningDelta"],
              ] as const) {
                const delta = buf(u, field);
                if (delta) {
                  const text = str(fields(delta), 1);
                  if (text)
                    this.emit({
                      textDelta: key === "textDelta" ? text : "",
                      reasoningDelta: key === "reasoningDelta" ? text : "",
                    });
                }
              }
              if (buf(u, 14)) {
                if (this.pending.size)
                  throw new Error("Cursor ended with unresolved tools");
                this.stop();
                break;
              }
            }
            if (kv) {
              const k = fields(kv),
                id = Number(k.get(1) || 0),
                set = buf(k, 3),
                get = buf(k, 2);
              if (set) {
                const a = fields(set),
                  key = buf(a, 1),
                  data = buf(a, 2) || Buffer.alloc(0);
                if (!key?.length) throw new Error("Missing blob ID");
                const hex = key.toString("hex");
                this.blobBytes +=
                  data.length - (this.blobs.get(hex)?.length || 0);
                if (
                  this.blobBytes > 32 * 1024 * 1024 ||
                  this.blobs.size >= 4096
                )
                  throw new Error("Cursor blob store exceeds limit");
                this.blobs.set(hex, data);
                this.send(b(3, join(n(1, id), b(3, Buffer.alloc(0)))));
              } else if (get) {
                const key = buf(fields(get), 1),
                  data = key && this.blobs.get(key.toString("hex"));
                if (!data)
                  throw new Error("Requested Cursor blob is unavailable");
                this.send(b(3, join(n(1, id), b(2, b(1, data)))));
              }
            }
            if (exec) this.bridgeExec(exec);
            if (buf(m, 7))
              throw new Error(
                "Cursor interaction query is unsupported; no approval was granted",
              );
          }
        } catch (e: any) {
          fail(e.message);
        }
      });
      this.req.on("end", () => {
        if (status !== 200 && !responded) {
          responded = true;
          resolve(
            new Response(failure, {
              status: status || 502,
              headers: { "Content-Type": "application/json" },
            }),
          );
          this.stop();
        } else if (!this.done) fail("Cursor stream ended without turn_ended");
      });
      this.req.on("close", () => {
        if (!responded) fail("Cursor stream closed before headers");
      });
      this.req.on("error", (e) => fail(e.message));
      this.client.on("error", (e) => fail(e.message));
      this.send(encodeAgentRun(this.input, model, this.ctx, 1));
    });
  }
}
export function cursorToolSessionCounts() {
  return { sessions: sessions.size, pending: waiting.size };
}
