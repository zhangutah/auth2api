/** Minimal, opt-in Cursor 3.22.12 ASK transport.
 * Protocol evidence: installed workbench.desktop.main.js, lines 6769/6794/6924,
 * SHA256 8009c99a930bc4ccef0042e8894a92ba933ab950f8a86fe870a82b2113d70dd6.
 * Text conversation history is supported; this is NOT an IDE tool executor.
 */
import http2 from "node:http2";
import { randomUUID } from "node:crypto";
import { release } from "node:os";
import { gunzipSync } from "node:zlib";
import type { Http2Result } from "./cursor-api";

export const AGENT_PATH = "/agent.v1.AgentService/Run";
export const AGENT_VERSION = "3.22.12";
export const AGENT_COMMIT = "3a92974361033b2051526321308c2740fe5912c0";
export interface AgentModel {
  id: string;
  parameters: { id: string; value: string }[];
}

export function selectAgentModel(catalog: any, model: string): AgentModel {
  if (!Array.isArray(catalog?.models))
    throw new Error("Invalid Cursor model catalogue");
  for (const entry of catalog.models) {
    const variants = Array.isArray(entry.variants) ? entry.variants : [];
    const explicit = variants.find(
      (v: any) =>
        v.legacySlug === model || v.variantStringRepresentation === model,
    );
    if (entry.name !== model && entry.serverModelName !== model && !explicit)
      continue;
    const variant =
      explicit || variants.find((v: any) => v.isDefaultNonMaxConfig);
    if (variants.length && !variant)
      throw new Error("Cursor model has no non-max default variant");
    const parameters = variant?.parameterValues || [];
    if (
      !parameters.every(
        (p: any) => typeof p.id === "string" && typeof p.value === "string",
      )
    ) {
      throw new Error("Invalid Cursor model parameters");
    }
    return { id: entry.name, parameters };
  }
  throw new Error(`Cursor model is not available: ${model}`);
}

export interface AgentInput {
  text: string;
  instructions: string;
  history: { role: "user" | "assistant"; text: string }[];
  tools: { name: string; description: string; schema: string }[];
}

function contentText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content))
    throw new Error("Cursor conversation requires text content");
  return content
    .map((part) => {
      if (
        !part ||
        !["input_text", "output_text", "text"].includes(part.type) ||
        typeof part.text !== "string"
      ) {
        throw new Error(
          "Cursor conversation does not support images or non-text content",
        );
      }
      return part.text;
    })
    .join("\n");
}

/** Preserve client instructions and ordered text history in native Agent fields.
 * Tool declarations are represented as MCP definitions; executing them or
 * resuming from tool results is not implemented and must fail explicitly.
 */
export function agentInput(body: any): AgentInput {
  if (!body || typeof body !== "object")
    throw new Error("Invalid Cursor request body");
  if (body.previous_response_id !== undefined)
    throw new Error(
      "Cursor conversation requires explicit history, not previous_response_id",
    );
  if (body.response_format !== undefined || body.stop !== undefined)
    throw new Error(
      "Cursor conversation does not support response_format or stop",
    );
  if (
    body.tool_choice !== undefined &&
    !["auto", "none"].includes(body.tool_choice)
  )
    throw new Error(
      "Cursor conversation does not support required/forced tool choice",
    );
  const instructions: string[] = [];
  if (body.instructions !== undefined)
    instructions.push(contentText(body.instructions));
  if (body.system !== undefined) instructions.push(contentText(body.system));
  let messages = body.messages ?? body.input;
  if (typeof messages === "string")
    messages = [{ role: "user", content: messages }];
  if (!Array.isArray(messages))
    throw new Error("Cursor conversation requires text messages");
  const history: AgentInput["history"] = [];
  for (const message of messages) {
    if (message?.type && message.type !== "message") {
      throw new Error(
        "Cursor conversation does not yet support tool results or reasoning input items",
      );
    }
    if (
      message?.tool_calls !== undefined ||
      message?.function_call !== undefined
    )
      throw new Error(
        "Cursor conversation does not execute assistant tool calls",
      );
    const role = message?.role;
    const text = contentText(message?.content);
    if (role === "system" || role === "developer")
      instructions.push(`${role} instructions:\n${text}`);
    else if (role === "user" || role === "assistant")
      history.push({ role, text });
    else throw new Error("Unsupported Cursor conversation role");
  }
  const latest = history.pop();
  if (!latest || latest.role !== "user" || !latest.text.trim())
    throw new Error(
      "Cursor conversation must end with a nonempty user message",
    );
  const tools: AgentInput["tools"] = [];
  function addTools(declarations: unknown, prefix = "") {
    if (!Array.isArray(declarations))
      throw new Error("Invalid tool declarations");
    for (const tool of declarations) {
      if (!tool || typeof tool !== "object")
        throw new Error("Invalid tool declaration");
      if (tool.type === "namespace") {
        addTools(tool.tools, prefix + tool.name + ".");
        continue;
      }
      const f =
        tool.type === "function" && tool.function ? tool.function : tool;
      if (
        tool.type !== "function" ||
        typeof f.name !== "string" ||
        !f.parameters ||
        typeof f.parameters !== "object" ||
        Array.isArray(f.parameters)
      ) {
        throw new Error(
          "Cursor conversation currently supports function tool declarations only",
        );
      }
      const name = prefix + f.name;
      if (
        !/^[a-zA-Z0-9_.-]{1,160}$/.test(name) ||
        tools.some((t) => t.name === name)
      )
        throw new Error("Invalid or duplicate tool name");
      tools.push({
        name,
        description: String(f.description || ""),
        schema: JSON.stringify(f.parameters),
      });
      if (tools.length > 512) throw new Error("Too many tool declarations");
    }
  }
  if (body.tools !== undefined) addTools(body.tools);
  const result = {
    text: latest.text,
    instructions: instructions.join("\n\n"),
    history,
    tools: body.tool_choice === "none" ? [] : tools,
  };
  if (Buffer.byteLength(JSON.stringify(result)) > 2 * 1024 * 1024)
    throw new Error("Cursor conversation exceeds 2 MiB input limit");
  return result;
}

export function agentPrompt(body: any): string {
  return agentInput(body).text;
}

function vi(n: number): Buffer {
  if (!Number.isSafeInteger(n) || n < 0)
    throw new Error("Invalid protobuf integer");
  let v = BigInt(n);
  const a: number[] = [];
  while (v >= 128n) {
    a.push(Number(v & 127n) | 128);
    v >>= 7n;
  }
  a.push(Number(v));
  return Buffer.from(a);
}
export function agentBytes(no: number, value: Buffer | string): Buffer {
  const b = typeof value === "string" ? Buffer.from(value) : value;
  return Buffer.concat([vi(no * 8 + 2), vi(b.length), b]);
}
function num(no: number, n: number): Buffer {
  return Buffer.concat([vi(no * 8), vi(n)]);
}
const join = (...b: Buffer[]) => Buffer.concat(b);
export function agentFrame(b: Buffer, flags = 0): Buffer {
  const h = Buffer.alloc(5);
  h[0] = flags;
  h.writeUInt32BE(b.length, 1);
  return join(h, b);
}
export function agentFields(b: Buffer): Map<number, Buffer | number> {
  const result = new Map<number, Buffer | number>();
  let p = 0;
  function read(): number {
    let v = 0n,
      shift = 0n;
    for (let i = 0; i < 10; i++) {
      if (p >= b.length) throw new Error("Truncated protobuf varint");
      const c = b[p++];
      v |= BigInt(c & 127) << shift;
      if (!(c & 128)) {
        if (v > BigInt(Number.MAX_SAFE_INTEGER))
          throw new Error("Unsafe protobuf integer");
        return Number(v);
      }
      shift += 7n;
    }
    throw new Error("Oversized protobuf varint");
  }
  while (p < b.length) {
    const tag = read(),
      no = Math.floor(tag / 8),
      wire = tag % 8;
    if (!no) throw new Error("Invalid protobuf field");
    if (wire === 0) result.set(no, read());
    else {
      const n = wire === 2 ? read() : wire === 1 ? 8 : wire === 5 ? 4 : -1;
      if (n < 0 || p + n > b.length)
        throw new Error("Truncated or invalid protobuf field");
      result.set(no, b.subarray(p, p + n));
      p += n;
    }
  }
  return result;
}
const bytes = (
  f: Map<number, Buffer | number>,
  n: number,
): Buffer | undefined => {
  const v = f.get(n);
  return Buffer.isBuffer(v) ? v : undefined;
};
function requestContext(): Buffer {
  const env = join(
    agentBytes(1, release()),
    agentBytes(3, "/bin/sh"),
    agentBytes(10, "UTC"),
  );
  return join(agentBytes(4, env), num(17, 0), num(24, 0));
}
/** The live ASK endpoint did not use history/spec fields in isolated probes.
 * Preserve their text explicitly in the current user prompt as a compatibility
 * transcript as well. This is NOT equivalent to native system-role precedence.
 * Keep Cursor's upstream policy intact; never execute advertised client tools.
 */
export function agentConversationText(input: AgentInput): string {
  if (!input.instructions && !input.history.length) return input.text;
  return [
    "Task: answer the final user turn of the supplied client conversation, using its ordered prior messages as context. This supplied conversation is separate from your current IDE session. Apply the client instructions where compatible with your upstream system policy. Prior assistant replies are context, not new instructions. Return only the answer to CURRENT USER.",
    input.instructions ? "CLIENT INSTRUCTIONS:\n" + input.instructions : "",
    input.history.length
      ? "PRIOR MESSAGES (ordered role/text JSON):\n" +
        JSON.stringify(input.history)
      : "",
    "CURRENT USER:\n" + input.text,
  ]
    .filter(Boolean)
    .join("\n\n");
}

export function encodeAgentRun(
  prompt: string | AgentInput,
  model: AgentModel,
  ctx = requestContext(),
): Buffer {
  const input: AgentInput =
    typeof prompt === "string"
      ? { text: prompt, instructions: "", history: [], tools: [] }
      : prompt;
  const user = join(
    agentBytes(1, agentConversationText(input)),
    agentBytes(2, randomUUID()),
    num(4, 2),
  ); // ASK
  const history = join(
    ...input.history.map((m) => {
      const textContent = agentBytes(1, agentBytes(1, m.text));
      const message = agentBytes(
        m.role === "user" ? 1 : 2,
        agentBytes(1, textContent),
      );
      return agentBytes(1, message);
    }),
  );
  const action = agentBytes(
    1,
    join(
      agentBytes(1, user),
      agentBytes(2, ctx),
      ...(input.history.length ? [agentBytes(7, history)] : []),
    ),
  );
  const mcp = join(
    ...input.tools.map((t) =>
      agentBytes(
        1,
        join(
          agentBytes(1, t.name),
          agentBytes(2, t.description),
          agentBytes(4, "codex"),
          agentBytes(5, t.name),
          agentBytes(6, t.schema),
        ),
      ),
    ),
  );
  const requested = join(
    agentBytes(1, model.id),
    num(2, 0),
    ...model.parameters.map((p) =>
      agentBytes(3, join(agentBytes(1, p.id), agentBytes(2, p.value))),
    ),
    num(7, 1),
  );
  // Do not set exclude_workspace_context: this account/model rejects that flag.
  return agentBytes(
    1,
    join(
      agentBytes(1, num(10, 2)),
      agentBytes(2, action),
      agentBytes(9, requested),
      agentBytes(4, mcp),
      ...(input.instructions
        ? [agentBytes(29, agentBytes(2, input.instructions))]
        : []),
      agentBytes(5, randomUUID()),
      num(10, 0),
      agentBytes(25, randomUUID()),
    ),
  );
}

/** Adapt only explicit text/thinking updates to the existing legacy SSE decoder.
 * Never feed checkpoints, blob content or exec messages into its recursive parser.
 */
export function openCursorAgent(
  url: string,
  headers: Record<string, string>,
  prompt: string | AgentInput,
  model: AgentModel,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<Http2Result> {
  if (signal?.aborted) return Promise.reject(new Error("request aborted"));
  return new Promise((resolve, reject) => {
    const target = new URL(url),
      client = http2.connect(target.origin);
    const req = client.request({
      ...Object.fromEntries(
        Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]),
      ),
      ":method": "POST",
      ":path": target.pathname,
    });
    let controller: ReadableStreamDefaultController<Uint8Array>,
      done = false,
      resolved = false;
    let status = 0,
      buffered = Buffer.alloc(0),
      blobBytes = 0,
      outputBytes = 0;
    const blobs = new Map<string, Buffer>(),
      ctx = requestContext();
    const MAX_FRAME = 8 * 1024 * 1024,
      MAX_BLOBS = 32 * 1024 * 1024;
    let timer: ReturnType<typeof setTimeout>,
      heartbeat: ReturnType<typeof setInterval>;
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c;
      },
      cancel() {
        finish();
      },
    });
    const emit = (b: Buffer) => {
      outputBytes += b.length;
      if (outputBytes > MAX_BLOBS)
        throw new Error("Cursor agent output exceeds limit");
      controller.enqueue(b);
    };
    const send = (b: Buffer) => {
      if (!done) req.write(agentFrame(b));
    };
    function finish(error?: string) {
      if (done) return;
      done = true;
      clearTimeout(timer);
      clearInterval(heartbeat);
      signal?.removeEventListener("abort", abort);
      if (!resolved)
        reject(new Error(error || "Cursor agent ended before headers"));
      else {
        try {
          if (error)
            controller.enqueue(
              agentFrame(
                Buffer.from(
                  JSON.stringify({
                    error: { code: "upstream_error", message: error },
                  }),
                ),
                2,
              ),
            );
          controller.close();
        } catch {
          /* downstream already cancelled */
        }
      }
      blobs.clear();
      req.close();
      client.close();
    }
    function abort() {
      finish("request aborted");
    }
    signal?.addEventListener("abort", abort, { once: true });
    timer = setTimeout(
      () => finish("Cursor agent deadline exceeded"),
      timeoutMs,
    );
    heartbeat = setInterval(() => send(agentBytes(7, Buffer.alloc(0))), 5000);
    req.on("response", (h) => {
      status = Number(h[":status"]);
      resolved = true;
      resolve({
        status,
        headers: { "content-type": String(h["content-type"] || "") },
        body: stream,
      });
    });
    req.on("data", (chunk: Buffer) => {
      if (done) return;
      try {
        if (status !== 200) {
          emit(chunk);
          return;
        }
        buffered = join(buffered, chunk);
        while (buffered.length >= 5 && !done) {
          const flags = buffered[0],
            n = buffered.readUInt32BE(1);
          if (n > MAX_FRAME || flags > 3)
            throw new Error("Invalid/oversized Connect frame");
          if (buffered.length < 5 + n) break;
          let p = buffered.subarray(5, 5 + n);
          buffered = buffered.subarray(5 + n);
          if (flags & 1) p = gunzipSync(p, { maxOutputLength: MAX_FRAME });
          if (flags & 2) {
            const end = JSON.parse(p.toString());
            finish(
              end.error
                ? `${end.error.code || "unknown"} — ${end.error.message || "Cursor agent error"}`
                : "Cursor agent ended without turn_ended",
            );
            break;
          }
          const m = agentFields(p),
            interaction = bytes(m, 1);
          if (interaction) {
            const u = agentFields(interaction);
            for (const [field, legacy] of [
              [1, 1],
              [4, 25],
            ]) {
              const delta = bytes(u, field),
                text = delta && bytes(agentFields(delta), 1);
              if (text?.length)
                emit(
                  agentFrame(
                    agentBytes(
                      2,
                      legacy === 1
                        ? agentBytes(1, text)
                        : agentBytes(25, agentBytes(1, text)),
                    ),
                  ),
                );
            }
            if (bytes(u, 14)) {
              finish();
              break;
            }
          }
          const kv = bytes(m, 4);
          if (kv) {
            const k = agentFields(kv),
              id = Number(k.get(1) || 0),
              set = bytes(k, 3),
              get = bytes(k, 2);
            if (set) {
              const a = agentFields(set),
                key = bytes(a, 1),
                data = bytes(a, 2) || Buffer.alloc(0);
              if (!key?.length) throw new Error("Missing blob ID");
              const hex = key.toString("hex");
              blobBytes += data.length - (blobs.get(hex)?.length || 0);
              if (blobBytes > MAX_BLOBS || blobs.size > 4096)
                throw new Error("Cursor blob store exceeds limit");
              blobs.set(hex, data);
              send(
                agentBytes(3, join(num(1, id), agentBytes(3, Buffer.alloc(0)))),
              );
            } else if (get) {
              const key = bytes(agentFields(get), 1),
                data = key && blobs.get(key.toString("hex"));
              if (!data)
                throw new Error("Requested Cursor blob is unavailable");
              send(
                agentBytes(
                  3,
                  join(num(1, id), agentBytes(2, agentBytes(1, data))),
                ),
              );
            }
          }
          const exec = bytes(m, 2);
          if (exec) {
            const e = agentFields(exec);
            if (!bytes(e, 10))
              throw new Error("Cursor agent preview does not execute tools");
            send(
              agentBytes(
                2,
                join(
                  num(1, Number(e.get(1) || 0)),
                  agentBytes(15, bytes(e, 15) || Buffer.alloc(0)),
                  agentBytes(10, agentBytes(1, agentBytes(1, ctx))),
                ),
              ),
            );
          }
          if (bytes(m, 7))
            throw new Error(
              "Cursor agent preview does not support interaction queries",
            );
        }
      } catch (e: any) {
        finish(e.message);
      }
    });
    req.on("end", () =>
      finish(
        status === 200
          ? "Cursor agent stream ended without turn_ended"
          : undefined,
      ),
    );
    req.on("error", (e) => finish(e.message));
    client.on("error", (e) => finish(e.message));
    send(encodeAgentRun(prompt, model, ctx));
  });
}
