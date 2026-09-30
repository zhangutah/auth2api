import {
  CursorToolSession,
  resumeCursorTools,
  cursorToolScope,
} from "./cursor-tools";
import {
  AGENT_PATH,
  AGENT_VERSION,
  AGENT_COMMIT,
  agentInput,
  type AgentInput,
  selectAgentModel,
  openCursorAgent,
} from "./cursor-agent";
import { release } from "node:os";
import { gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import http2 from "node:http2";
import { Request } from "express";
import { v4 as uuidv4, v5 as uuidv5 } from "uuid";
import { AccountManager, AvailableAccount } from "../accounts/manager";
import { Config } from "../config";
import { withTimeoutSignal } from "../utils/abort";
import { DEFAULT_CURSOR_CLIENT_VERSION } from "../auth/cursor/storage";

const DEFAULT_API_BASE_URL = "https://api2.cursor.sh";
// Chat is reverse-engineered from Cursor desktop: it lives on api2.cursor.sh
// (HTTP/2, application/connect+proto), not on the agent.api5 hosts. See
// eisbaw/cursor_api_demo for the cross-checked schema we follow here.
const CHAT_PATH = "/aiserver.v1.ChatService/StreamUnifiedChatWithTools";
const MODELS_PATH = "/aiserver.v1.AiService/AvailableModels";
const UUID_DNS_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

const FALLBACK_MODELS = ["cursor-premium", "cursor-fast", "cursor-composer"];

interface ProtoField {
  field: number;
  value: string | number | Uint8Array;
}

/**
 * Wire format the caller wants for the streamed Cursor response.
 *  - openai-responses: OpenAI Responses API SSE (`response.output_text.delta`, …)
 *  - anthropic-messages: Anthropic Messages SSE (`content_block_delta`, …)
 *  - openai-chat-completions: OpenAI Chat Completions SSE (`chat.completion.chunk`)
 */
export type CursorSseFormat =
  | "openai-responses"
  | "anthropic-messages"
  | "openai-chat-completions";

export interface CallCursorResponsesOptions {
  body?: any;
  request: Request;
  /** SSE format the caller wants back. Defaults to OpenAI Responses. */
  responseFormat?: CursorSseFormat;
  account: AvailableAccount;
  config: Config;
  signal?: AbortSignal;
}

/**
 * The HTTP/2 transport result. `body` is a stream of raw HTTP/2 data frames
 * — we deliberately do *not* buffer the whole response so the downstream
 * SSE writer can emit deltas as soon as Cursor's first protobuf frame
 * arrives. Tests can hand back a `Buffer`/`Uint8Array` for convenience and
 * the dispatcher will wrap it in a single-chunk `ReadableStream`.
 */
export interface Http2Result {
  status: number;
  headers: Record<string, string>;
  body: ReadableStream<Uint8Array> | Uint8Array | Buffer;
}

export type CursorTransport = (
  url: string,
  headers: Record<string, string>,
  body: Buffer,
  timeoutMs: number,
  signal?: AbortSignal,
) => Promise<Http2Result>;

let cursorTransport: CursorTransport = http2Post;

/** Test-only: replace the HTTP/2 transport. Production code should not call this. */
export function __setCursorTransport(transport: CursorTransport | null): void {
  cursorTransport = transport ?? http2Post;
}

function bodyToReadableStream(
  body: ReadableStream<Uint8Array> | Uint8Array | Buffer,
): ReadableStream<Uint8Array> {
  if (
    body &&
    typeof (body as ReadableStream<Uint8Array>).getReader === "function"
  ) {
    return body as ReadableStream<Uint8Array>;
  }
  const bytes =
    body instanceof Uint8Array
      ? body
      : new Uint8Array(body as unknown as ArrayBuffer);
  return new ReadableStream<Uint8Array>({
    start(controller) {
      if (bytes.length > 0) controller.enqueue(bytes);
      controller.close();
    },
  });
}

/**
 * POST a Connect-RPC payload over HTTP/2 and stream the response back. The
 * promise resolves as soon as the response *headers* arrive — the body is
 * exposed as a `ReadableStream` that pushes each HTTP/2 data frame as it
 * lands. This is what makes `cursorUpstreamToSse` able to forward deltas
 * to the SSE client in real time instead of waiting for the upstream
 * response to terminate.
 */
function http2Post(
  url: string,
  headers: Record<string, string>,
  body: Buffer,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<Http2Result> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const client = http2.connect(parsed.origin);
    let headersResolved = false;
    let status = 0;
    const responseHeaders: Record<string, string> = {};
    let bodyController: ReadableStreamDefaultController<Uint8Array> | null =
      null;
    let bodyClosed = false;

    const closeBody = (err?: Error) => {
      if (bodyClosed) return;
      bodyClosed = true;
      try {
        if (err && bodyController) bodyController.error(err);
        else bodyController?.close();
      } catch {
        /* controller already closed */
      }
      try {
        client.close();
      } catch {
        /* ignore */
      }
    };

    const onAbort = () => {
      try {
        req.close(http2.constants.NGHTTP2_CANCEL);
      } catch {
        /* ignore */
      }
      const err = new Error("request aborted");
      if (!headersResolved) {
        headersResolved = true;
        clearTimeout(headerTimer);
        client.close();
        reject(err);
      } else {
        closeBody(err);
      }
    };

    // Header-arrival deadline only — once headers arrive we clear this timer
    // and rely on the caller-supplied `signal` (which `callCursorResponses`
    // wraps in withTimeoutSignal(stream-messages-ms, ...)) to bound the body
    // phase. Splitting the two lets a stuck TCP connection fail fast at
    // header time without truncating long thinking-model responses that
    // legitimately take many minutes to finish streaming.
    const headerTimer = setTimeout(() => {
      if (headersResolved) return;
      headersResolved = true;
      try {
        req.close(http2.constants.NGHTTP2_CANCEL);
      } catch {
        /* ignore */
      }
      client.close();
      reject(new Error("cursor upstream HTTP/2 request timed out"));
    }, timeoutMs);

    const h2Headers: http2.OutgoingHttpHeaders = {
      ":method": "POST",
      ":path": `${parsed.pathname}${parsed.search}`,
    };
    for (const [key, value] of Object.entries(headers)) {
      h2Headers[key.toLowerCase()] = value;
    }

    const req = client.request(h2Headers);
    signal?.addEventListener("abort", onAbort, { once: true });

    client.on("error", (err) => {
      if (!headersResolved) {
        headersResolved = true;
        clearTimeout(headerTimer);
        reject(err);
      } else {
        closeBody(err);
      }
    });
    req.on("error", (err) => {
      if (!headersResolved) {
        headersResolved = true;
        clearTimeout(headerTimer);
        reject(err);
      } else {
        closeBody(err);
      }
    });
    req.on("response", (rspHeaders) => {
      headersResolved = true;
      clearTimeout(headerTimer);
      status = Number(rspHeaders[":status"] || 0);
      for (const [key, value] of Object.entries(rspHeaders)) {
        if (key.startsWith(":") || value === undefined) continue;
        responseHeaders[key] = Array.isArray(value)
          ? value.join(",")
          : String(value);
      }

      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          bodyController = controller;
          req.on("data", (chunk: Buffer) => {
            if (bodyClosed) return;
            try {
              controller.enqueue(new Uint8Array(chunk));
            } catch {
              /* consumer cancelled */
            }
          });
          req.on("end", () => closeBody());
        },
        cancel() {
          try {
            req.close(http2.constants.NGHTTP2_CANCEL);
          } catch {
            /* ignore */
          }
          closeBody();
        },
      });

      resolve({ status, headers: responseHeaders, body: stream });
    });
    req.end(body);
  });
}

function encodeVarint(value: number): Uint8Array {
  const bytes: number[] = [];
  let next = value >>> 0;
  while (next >= 0x80) {
    bytes.push((next & 0x7f) | 0x80);
    next >>>= 7;
  }
  bytes.push(next);
  return Uint8Array.from(bytes);
}

function concatBytes(parts: ArrayLike<number>[]): Uint8Array {
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function encodeVarintField(field: number, value: number): Uint8Array {
  return concatBytes([encodeVarint((field << 3) | 0), encodeVarint(value)]);
}

function encodeBytesField(
  field: number,
  value: Uint8Array | string,
): Uint8Array {
  const payload =
    typeof value === "string" ? new TextEncoder().encode(value) : value;
  return concatBytes([
    encodeVarint((field << 3) | 2),
    encodeVarint(payload.length),
    payload,
  ]);
}

function encodeField(
  field: number,
  value: string | number | Uint8Array,
): Uint8Array {
  if (typeof value === "number") return encodeVarintField(field, value);
  return encodeBytesField(field, value);
}

function encodeMessage(fields: ProtoField[]): Uint8Array {
  return concatBytes(fields.map((f) => encodeField(f.field, f.value)));
}

function connectFrame(payload: Uint8Array): Uint8Array {
  const frame = new Uint8Array(5 + payload.length);
  frame[0] = 0;
  new DataView(frame.buffer).setUint32(1, payload.length, false);
  frame.set(payload, 5);
  return frame;
}

/**
 * Mapping from "publicly-known" model names (the ones Anthropic / OpenAI
 * SDKs and Claude Code use by default) to the names Cursor expects on its
 * `StreamUnifiedChatWithTools` endpoint.
 *
 * The Cursor backend has its own internal SKU naming — e.g. Anthropic's
 * `claude-sonnet-4-5` is exposed as `claude-4.5-sonnet`. Without this map a
 * naive client request like `{"model":"claude-sonnet-4-5"}` would either be
 * routed to Cursor's `default` Auto router (silent fallback) or rejected.
 */
const PUBLIC_MODEL_TO_CURSOR: Record<string, string> = {
  // Anthropic Claude — public name → Cursor SKU
  "claude-3-5-haiku-20241022": "claude-4.5-haiku",
  "claude-3-5-haiku-latest": "claude-4.5-haiku",
  "claude-3-5-sonnet-20241022": "claude-4.5-sonnet",
  "claude-3-5-sonnet-latest": "claude-4.5-sonnet",
  "claude-3-7-sonnet-20250219": "claude-4-sonnet",
  "claude-3-7-sonnet-latest": "claude-4-sonnet",
  "claude-haiku-4-5": "claude-4.5-haiku",
  "claude-haiku-4-5-20251001": "claude-4.5-haiku",
  "claude-haiku-4-5-latest": "claude-4.5-haiku",
  "claude-haiku-latest": "claude-4.5-haiku",
  "claude-sonnet-4-5": "claude-4.5-sonnet",
  "claude-sonnet-4-5-20250929": "claude-4.5-sonnet",
  "claude-sonnet-4-5-latest": "claude-4.5-sonnet",
  "claude-sonnet-4-6": "claude-4.6-sonnet-medium",
  "claude-sonnet-4-7": "claude-4.6-sonnet-medium",
  "claude-sonnet-latest": "claude-4.6-sonnet-medium",
  "claude-opus-4-1": "claude-4.5-opus-high",
  "claude-opus-4-1-latest": "claude-4.5-opus-high",
  "claude-opus-4-5": "claude-4.5-opus-high",
  "claude-opus-4-6": "claude-4.6-opus-high",
  "claude-opus-4-7": "claude-opus-4-7-medium",
  "claude-opus-latest": "claude-opus-4-7-medium",
  // Anthropic short aliases used by Claude Code
  haiku: "claude-4.5-haiku",
  sonnet: "claude-4.6-sonnet-medium",
  opus: "claude-opus-4-7-medium",
  // OpenAI — common GPT family names
  "gpt-5": "gpt-5.5-medium",
  "gpt-5-mini": "gpt-5-mini",
  "gpt-5.6": "gpt-5.6-sol",
  "gpt-5.6-sol": "gpt-5.6-sol",
  "gpt-5.6-terra": "gpt-5.6-terra",
  "gpt-5.6-luna": "gpt-5.6-luna",
  "gpt-5.5": "gpt-5.5-medium",
  "gpt-6-astra": "gpt-6-astra",
  "gpt-5-codex": "gpt-5.3-codex",
  "gpt-5.3-codex": "gpt-5.3-codex",
  o3: "gpt-5.5-medium",
  "o4-mini": "gpt-5.4-mini-medium",
  "o4-high": "gpt-5.4-high",
};

/**
 * Normalise an inbound model id for the Cursor upstream:
 *   1. strip the auth2api routing prefix (`cursor-`, `cursor:`, `cr/`)
 *   2. translate well-known public names (`claude-sonnet-4-5`, `opus`,
 *      `gpt-5.6`, `gpt-5.5`, `o3`, …) into Cursor's internal SKU
 *   3. otherwise keep the trimmed name verbatim — it's already a Cursor SKU
 */
function normaliseModel(model: string): string {
  const stripped = model.replace(/^(cursor[:/-]|cr\/)/i, "").trim();
  if (!stripped) return "default";
  const lower = stripped.toLowerCase();
  if (PUBLIC_MODEL_TO_CURSOR[lower]) return PUBLIC_MODEL_TO_CURSOR[lower];
  // Allow alias overrides via env for power users wanting custom mapping
  // without forking. Format: `MODEL_FOO=cursor-bar,MODEL_BAZ=cursor-qux`.
  const overrides = parseModelAliases(process.env.CURSOR_MODEL_ALIASES);
  if (overrides[lower]) return overrides[lower];
  return stripped;
}

function parseModelAliases(input: string | undefined): Record<string, string> {
  if (!input) return {};
  const out: Record<string, string> = {};
  for (const pair of input.split(",")) {
    const [k, v] = pair.split("=", 2).map((s) => s.trim());
    if (k && v) out[k.toLowerCase()] = v;
  }
  return out;
}

export function __resolveCursorModel(model: string): string {
  return normaliseModel(model);
}

interface ChatMessageInput {
  role: "user" | "assistant" | "system";
  content: string;
}

function messagesFromBody(body: any): ChatMessageInput[] {
  const collected: ChatMessageInput[] = [];
  const pushText = (role: ChatMessageInput["role"], raw: unknown): void => {
    if (typeof raw === "string") {
      const trimmed = raw.trim();
      if (trimmed) collected.push({ role, content: raw });
      return;
    }
    if (Array.isArray(raw)) {
      const text = raw
        .map((part: any) => part?.text || part?.input_text || "")
        .filter(Boolean)
        .join("\n");
      if (text) collected.push({ role, content: text });
    } else if (raw && typeof raw === "object") {
      // Anthropic system: [{type:"text", text:"…"}] etc.
      const text = (raw as any).text || (raw as any).input_text || "";
      if (text) collected.push({ role, content: text });
    }
  };
  // Anthropic-style top-level system prompt (string or content-block array).
  if (body?.system) pushText("system", body.system);

  if (typeof body?.input === "string") {
    pushText("user", body.input);
  } else if (Array.isArray(body?.input)) {
    for (const item of body.input) {
      if (typeof item === "string") {
        pushText("user", item);
      } else {
        const role = (item?.role || "user") as ChatMessageInput["role"];
        pushText(role, item?.content ?? item);
      }
    }
  }
  if (Array.isArray(body?.messages)) {
    for (const msg of body.messages) {
      const role = (msg?.role || "user") as ChatMessageInput["role"];
      pushText(role, msg?.content);
    }
  }
  if (collected.length === 0) collected.push({ role: "user", content: "" });
  return collected;
}

function textFromResponsesInput(body: any): string {
  return messagesFromBody(body)
    .map((m) => m.content)
    .filter(Boolean)
    .join("\n");
}

function encodeChatMessage(
  content: string,
  role: number,
  messageId: string,
  chatModeEnum: number,
): Uint8Array {
  return concatBytes([
    encodeBytesField(1, content),
    encodeVarintField(2, role),
    encodeBytesField(13, messageId),
    encodeVarintField(47, chatModeEnum),
  ]);
}

function encodeMessageId(messageId: string, role: number): Uint8Array {
  return concatBytes([
    encodeBytesField(1, messageId),
    encodeVarintField(3, role),
  ]);
}

function encodeModelMsg(modelName: string): Uint8Array {
  return concatBytes([
    encodeBytesField(1, modelName),
    encodeBytesField(4, new Uint8Array(0)),
  ]);
}

function encodeCursorSetting(): Uint8Array {
  const unknown6 = concatBytes([
    encodeBytesField(1, new Uint8Array(0)),
    encodeBytesField(2, new Uint8Array(0)),
  ]);
  return concatBytes([
    encodeBytesField(1, "cursor\\aisettings"),
    encodeBytesField(3, new Uint8Array(0)),
    encodeBytesField(6, unknown6),
    encodeVarintField(8, 1),
    encodeVarintField(9, 1),
  ]);
}

function encodeMetadata(): Uint8Array {
  // Reverse-engineered Metadata fields. Keep static so cloaking decisions stay
  // visible at a single configuration point and not rotated per request.
  return concatBytes([
    encodeBytesField(
      1,
      process.platform === "win32" ? "windows" : process.platform,
    ),
    encodeBytesField(2, process.arch),
    encodeBytesField(3, process.version.replace(/^v/, "")),
    encodeBytesField(4, process.execPath),
    encodeBytesField(5, new Date().toISOString()),
  ]);
}

interface CursorRequestEncoding {
  bytes: Uint8Array;
  prompt: string;
  conversationId: string;
}

export function encodeCursorAgentRequest(body: any): Uint8Array {
  return encodeCursorChatRequest(body).bytes;
}

export function encodeCursorChatRequest(body: any): CursorRequestEncoding {
  const model = normaliseModel(String(body?.model || "default"));
  const messages = messagesFromBody(body);
  const conversationId = uuidv4();
  const messageEntries: { id: string; role: number }[] = [];

  let request: Uint8Array = new Uint8Array(0);
  for (const msg of messages) {
    if (msg.role === "system") continue;
    const role = msg.role === "assistant" ? 2 : 1;
    const id = uuidv4();
    messageEntries.push({ id, role });
    const message = encodeChatMessage(msg.content, role, id, 2);
    request = concatBytes([request, encodeBytesField(1, message)]);
  }

  request = concatBytes([
    request,
    encodeVarintField(2, 1),
    encodeBytesField(3, new Uint8Array(0)),
    encodeVarintField(4, 1),
    encodeBytesField(5, encodeModelMsg(model)),
    encodeBytesField(8, ""),
    encodeVarintField(13, 1),
    encodeBytesField(15, encodeCursorSetting()),
    encodeVarintField(19, 1),
    encodeBytesField(23, conversationId),
    encodeBytesField(26, encodeMetadata()),
    encodeVarintField(27, 1),
  ]);
  for (const entry of messageEntries) {
    request = concatBytes([
      request,
      encodeBytesField(30, encodeMessageId(entry.id, entry.role)),
    ]);
  }
  request = concatBytes([
    request,
    encodeVarintField(35, 0),
    encodeVarintField(38, 0),
    encodeVarintField(46, 2),
    encodeBytesField(47, ""),
    encodeVarintField(48, 0),
    encodeVarintField(49, 0),
    encodeVarintField(51, 0),
    encodeVarintField(53, 1),
    encodeBytesField(54, "agent"),
  ]);

  const wrapped = encodeBytesField(1, request);
  return {
    bytes: connectFrame(wrapped),
    prompt: messages
      .map((m) => m.content)
      .join("\n")
      .trim(),
    conversationId,
  };
}

function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

const URL_SAFE_BASE64 =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

function jyhEncode(bytes: Uint8Array): string {
  // Jyh cipher used by Cursor's desktop client; see cursor_api_demo TASK-18.
  // We replicate it byte-for-byte so api2.cursor.sh accepts our checksum.
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const a = bytes[i];
    const b = i + 1 < bytes.length ? bytes[i + 1] : 0;
    const c = i + 2 < bytes.length ? bytes[i + 2] : 0;
    out += URL_SAFE_BASE64[a >> 2];
    out += URL_SAFE_BASE64[((a & 3) << 4) | (b >> 4)];
    if (i + 1 < bytes.length)
      out += URL_SAFE_BASE64[((b & 15) << 2) | (c >> 6)];
    if (i + 2 < bytes.length) out += URL_SAFE_BASE64[c & 63];
  }
  return out;
}

function buildCursorChecksum(token: string, machineId: string): string {
  const stableMachineId = machineId || sha256Hex(`${token}machineId`);
  const timestamp = Math.floor(Date.now() / 1_000_000);
  const buf = Uint8Array.from([
    (timestamp >>> 40) & 0xff,
    (timestamp >>> 32) & 0xff,
    (timestamp >>> 24) & 0xff,
    (timestamp >>> 16) & 0xff,
    (timestamp >>> 8) & 0xff,
    timestamp & 0xff,
  ]);
  let prev = 165;
  for (let i = 0; i < buf.length; i++) {
    buf[i] = ((buf[i] ^ prev) + (i % 256)) & 0xff;
    prev = buf[i];
  }
  return `${jyhEncode(buf)}${stableMachineId}`;
}

export function __buildCursorHeaders(
  account: AvailableAccount,
  config: Config,
): Record<string, string> {
  const cursor = config.cloaking.cursor || {};
  const token = account.token.accessToken;
  const machineId =
    account.token.cursorServiceMachineId ||
    account.accountUuid ||
    account.deviceId;
  const clientVersion =
    cursor["client-version"] ||
    account.token.cursorClientVersion ||
    DEFAULT_CURSOR_CLIENT_VERSION;
  const configVersion =
    cursor["config-version"] || account.token.cursorConfigVersion || uuidv4();
  const sessionId = uuidv5(token, UUID_DNS_NAMESPACE);
  const clientKey = sha256Hex(token);

  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/connect+proto",
    Accept: "application/connect+proto",
    "Accept-Encoding": "gzip",
    "Connect-Protocol-Version": "1",
    "User-Agent": "connect-es/1.6.1",
    "x-amzn-trace-id": uuidv4(),
    "x-client-key": clientKey,
    "x-cursor-checksum": buildCursorChecksum(token, machineId),
    "x-cursor-client-version": clientVersion,
    "x-cursor-client-type": cursor["client-type"] || "ide",
    "x-cursor-client-os":
      process.platform === "darwin"
        ? "macos"
        : process.platform === "win32"
          ? "windows"
          : "linux",
    "x-cursor-client-arch": process.arch,
    "x-cursor-client-device-type": "desktop",
    "x-cursor-config-version": configVersion,
    "x-cursor-timezone":
      cursor.timezone ||
      Intl.DateTimeFormat().resolvedOptions().timeZone ||
      "UTC",
    "x-ghost-mode": cursor["ghost-mode"] || "true",
    "x-session-id": sessionId,
    "x-request-id": uuidv4(),
  };
}

function decodeVarint(data: Uint8Array, pos: number): [number, number] {
  let value = 0;
  let shift = 0;
  while (pos < data.length) {
    const b = data[pos++];
    value |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
  }
  return [value, pos];
}

interface ProtoFieldRaw {
  field: number;
  wireType: number;
  bytes?: Uint8Array;
  varint?: number;
}

function parseFields(data: Uint8Array): ProtoFieldRaw[] {
  const out: ProtoFieldRaw[] = [];
  let pos = 0;
  while (pos < data.length) {
    const [tag, afterTag] = decodeVarint(data, pos);
    if (afterTag <= pos) break;
    pos = afterTag;
    const field = tag >> 3;
    const wireType = tag & 7;
    if (wireType === 0) {
      const [v, p] = decodeVarint(data, pos);
      out.push({ field, wireType, varint: v });
      pos = p;
    } else if (wireType === 2) {
      const [len, afterLen] = decodeVarint(data, pos);
      pos = afterLen;
      if (len < 0 || pos + len > data.length) break;
      out.push({ field, wireType, bytes: data.slice(pos, pos + len) });
      pos += len;
    } else if (wireType === 1) {
      pos += 8;
    } else if (wireType === 5) {
      pos += 4;
    } else {
      break;
    }
  }
  return out;
}

function getFieldBytes(
  fields: ProtoFieldRaw[],
  field: number,
): Uint8Array | undefined {
  return fields.find((f) => f.field === field && f.wireType === 2)?.bytes;
}

interface CursorFrame {
  type: number;
  payload: Uint8Array;
}

export interface CursorDecodeResult {
  text: string;
  reasoning: string;
  error?: string;
}

/** Streaming decoder that walks Connect-RPC frames as they arrive. */
export interface CursorStreamingDecoder {
  feed(chunk: Uint8Array): { textDelta: string; reasoningDelta: string }[];
  finish(): { error?: string };
}

function readConnectFrames(data: Uint8Array): CursorFrame[] {
  const frames: CursorFrame[] = [];
  let pos = 0;
  while (pos + 5 <= data.length) {
    const type = data[pos];
    const length = new DataView(
      data.buffer,
      data.byteOffset + pos + 1,
      4,
    ).getUint32(0, false);
    pos += 5;
    if (pos + length > data.length) break;
    let payload = data.slice(pos, pos + length);
    pos += length;
    try {
      if (type === 1 || type === 3) payload = gunzipSync(payload);
    } catch {
      // leave as-is; caller will treat unreadable frames as opaque
    }
    frames.push({ type, payload });
  }
  return frames;
}

function isUtfPrintable(text: string): boolean {
  return /^[\x09\x0a\x0d\x20-\x7e\u00a0-\uffff]+$/.test(text);
}

function isUuidLike(text: string): boolean {
  return /^[0-9a-f-]{32,}$/i.test(text);
}

interface ExtractParts {
  text: string;
  reasoning: string;
}

function looksLikeProtoStart(byte: number): boolean {
  // Valid proto wire types are 0,1,2,5; field number must be > 0, so first
  // byte cannot be 0. This filters out plain UTF-8 strings that happen to
  // appear inside non-proto bytes fields (e.g. raw text in field=1 leafs).
  const wire = byte & 0x07;
  return byte !== 0 && (wire === 0 || wire === 1 || wire === 2 || wire === 5);
}

function extractInnerText(payload: Uint8Array, depth = 0): string {
  // Walk the inner StreamUnifiedChatResponse-shaped message and pull any
  // field=1 length-delimited string. Used for both text and reasoning.
  if (depth > 4) return "";
  const fields = parseFields(payload);
  const candidate = fields.find(
    (f) => f.field === 1 && f.wireType === 2 && f.bytes !== undefined,
  );
  if (candidate?.bytes) {
    const text = new TextDecoder("utf-8", { fatal: false }).decode(
      candidate.bytes,
    );
    if (isUtfPrintable(text) && !isUuidLike(text.trim())) return text;
  }
  let acc = "";
  for (const f of fields) {
    if (f.wireType === 2 && f.bytes && f.bytes.length > 1) {
      if (!looksLikeProtoStart(f.bytes[0])) continue;
      try {
        acc += extractInnerText(f.bytes, depth + 1);
      } catch {
        /* ignore */
      }
    }
  }
  return acc;
}

/**
 * Pull text + reasoning out of one Cursor protobuf payload.
 *
 * Schema observed against api2.cursor.sh:
 *   StreamUnifiedChatResponseWithTools {
 *     StreamUnifiedChatResponse stream_unified_chat_response = 2 {
 *       string text = 1;                  // assistant final text
 *       Reasoning reasoning = 25 {        // chain-of-thought stream
 *         string text = 1;
 *       }
 *     }
 *   }
 *
 * For "thinking-but-actually-answering" models like composer-2/kimi, the
 * model dumps the full chain-of-thought into field 25 followed by a
 * literal `</think>` separator and then the real answer. We split on that
 * marker and route the tail back to the text channel so SSE clients see a
 * clean answer instead of leaking reasoning into output_text.
 */
function extractFromPayload(payload: Uint8Array): ExtractParts {
  const fields = parseFields(payload);
  let text = "";
  let reasoning = "";
  for (const f of fields) {
    if (f.wireType !== 2 || !f.bytes) continue;
    if (f.field === 25) {
      reasoning += extractInnerText(f.bytes);
    } else if (f.field === 1) {
      const direct = new TextDecoder("utf-8", { fatal: false }).decode(f.bytes);
      if (isUtfPrintable(direct) && !isUuidLike(direct.trim())) text += direct;
    } else if (f.field === 2 || f.bytes.length > 1) {
      // Recurse into the wrapper layer so we hit the real content fields.
      if (looksLikeProtoStart(f.bytes[0])) {
        const sub = extractFromPayload(f.bytes);
        text += sub.text;
        reasoning += sub.reasoning;
      }
    }
  }
  return { text, reasoning };
}

const THINK_CLOSE_RE = /<\/think>\s*/i;

function splitOnThinkClose(reasoning: string, text: string): ExtractParts {
  // Composer/Kimi-style: the entire response (CoT + answer) goes into the
  // reasoning channel; the answer is whatever follows `</think>`.
  if (text || !THINK_CLOSE_RE.test(reasoning)) {
    return { text, reasoning };
  }
  const idx = reasoning.search(THINK_CLOSE_RE);
  const matchLen = reasoning.match(THINK_CLOSE_RE)![0].length;
  return {
    reasoning: reasoning.slice(0, idx),
    text: reasoning.slice(idx + matchLen),
  };
}

function extractJsonError(payload: Uint8Array): string | undefined {
  try {
    const parsed = JSON.parse(new TextDecoder().decode(payload));
    const code = parsed?.error?.code;
    const message = parsed?.error?.message;
    if (code || message) {
      const debug = parsed?.error?.details?.[0]?.debug?.error;
      const detail = parsed?.error?.details?.[0]?.debug?.details?.detail;
      const parts = [code, debug, message, detail].filter(Boolean);
      return parts.join(" — ");
    }
  } catch {
    /* not JSON */
  }
  return undefined;
}

export function decodeCursorResponse(data: Uint8Array): CursorDecodeResult {
  const frames = readConnectFrames(data);
  let text = "";
  let reasoning = "";
  let error: string | undefined;
  for (const frame of frames) {
    if (frame.type === 0 || frame.type === 1) {
      const parts = extractFromPayload(frame.payload);
      text += parts.text;
      reasoning += parts.reasoning;
    } else if (frame.type === 2 || frame.type === 3) {
      const err = extractJsonError(frame.payload);
      if (err) error = err;
    }
  }
  const split = splitOnThinkClose(reasoning, text);
  return {
    text: split.text.replace(/^[ \t\r\n]+/, "").replace(/[ \t]+$/, ""),
    reasoning: split.reasoning.trim(),
    error,
  };
}

export function decodeCursorText(data: Uint8Array, _prompt = ""): string {
  const result = decodeCursorResponse(data);
  if (result.text) return result.text;
  if (result.reasoning) return result.reasoning;
  if (result.error) return `[cursor] ${result.error}`;
  return "";
}

/**
 * Build a stateful decoder for the streaming SSE path. Each `feed()` parses
 * any complete Connect-RPC frames received so far and returns deltas relative
 * to the prior call so the SSE writer can emit `output_text.delta` and
 * `reasoning_summary_text.delta` events in real time.
 */
export function createCursorStreamingDecoder(): CursorStreamingDecoder {
  let buffer = new Uint8Array(0);
  // Reasoning is buffered until we either see `</think>` (split → text), the
  // first non-empty text delta arrives (flush as reasoning), or the stream
  // ends (flush as reasoning). This avoids leaking partial CoT into the
  // reasoning channel for composer/kimi-style models that store the final
  // answer inside the same field as the chain-of-thought.
  let pendingReasoning = "";
  let resolved: "none" | "split" | "reasoning" = "none";
  let error: string | undefined;

  function maybeSplit(): { reasoningDelta: string; textDelta: string } | null {
    if (resolved !== "none" || !pendingReasoning) return null;
    const m = pendingReasoning.match(THINK_CLOSE_RE);
    if (!m || m.index === undefined) return null;
    const reasoningPart = pendingReasoning.slice(0, m.index);
    const textPart = pendingReasoning.slice(m.index + m[0].length);
    pendingReasoning = "";
    resolved = "split";
    return { reasoningDelta: reasoningPart, textDelta: textPart };
  }

  function flushReasoning(): {
    reasoningDelta: string;
    textDelta: string;
  } | null {
    if (!pendingReasoning) return null;
    const out = { reasoningDelta: pendingReasoning, textDelta: "" };
    pendingReasoning = "";
    resolved = "reasoning";
    return out;
  }

  return {
    feed(chunk: Uint8Array) {
      const merged = new Uint8Array(buffer.length + chunk.length);
      merged.set(buffer);
      merged.set(chunk, buffer.length);
      buffer = merged;

      const out: { textDelta: string; reasoningDelta: string }[] = [];
      let pos = 0;
      while (pos + 5 <= buffer.length) {
        const type = buffer[pos];
        const length = new DataView(
          buffer.buffer,
          buffer.byteOffset + pos + 1,
          4,
        ).getUint32(0, false);
        if (pos + 5 + length > buffer.length) break;
        let payload = buffer.slice(pos + 5, pos + 5 + length);
        pos += 5 + length;
        try {
          if (type === 1 || type === 3) payload = gunzipSync(payload);
        } catch {
          /* leave compressed bytes alone */
        }
        if (type === 0 || type === 1) {
          const parts = extractFromPayload(payload);
          let textDelta = parts.text;
          let reasoningDelta = "";
          if (parts.reasoning) {
            if (resolved === "split") {
              // Once we've resolved the split, the model is just streaming
              // the rest of its answer through the reasoning channel.
              textDelta += parts.reasoning;
            } else if (resolved === "reasoning") {
              reasoningDelta += parts.reasoning;
            } else {
              pendingReasoning += parts.reasoning;
              const split = maybeSplit();
              if (split) {
                reasoningDelta += split.reasoningDelta;
                textDelta = split.textDelta + textDelta;
              }
            }
          }
          if (textDelta && resolved === "none") {
            // First real text delta — commit any pending reasoning as-is.
            const flushed = flushReasoning();
            if (flushed)
              reasoningDelta = flushed.reasoningDelta + reasoningDelta;
          }
          if (textDelta || reasoningDelta)
            out.push({ textDelta, reasoningDelta });
        } else if (type === 2 || type === 3) {
          const err = extractJsonError(payload);
          if (err) error = err;
        }
      }
      buffer = buffer.slice(pos);
      return out;
    },
    finish() {
      return { error };
    },
    // Returns any reasoning still buffered when the stream ends without text.
    drain(): { reasoningDelta: string } {
      const flushed = flushReasoning();
      return { reasoningDelta: flushed?.reasoningDelta || "" };
    },
  } as CursorStreamingDecoder & { drain(): { reasoningDelta: string } };
}

interface ResponsesSseOptions {
  text: string;
  reasoning?: string;
}

function buildResponsesCompletedSse(
  model: string,
  opts: ResponsesSseOptions,
): string {
  const id = `resp_${uuidv4().replace(/-/g, "")}`;
  const created = Math.floor(Date.now() / 1000);
  const reasoningContent =
    opts.reasoning && opts.reasoning.trim()
      ? [
          {
            type: "reasoning",
            summary: [{ type: "summary_text", text: opts.reasoning }],
          },
        ]
      : [];
  const completed = {
    id,
    object: "response",
    created_at: created,
    status: "completed",
    model,
    output: [
      ...reasoningContent,
      {
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: opts.text }],
      },
    ],
    usage: {
      input_tokens: 0,
      output_tokens: 0,
      total_tokens: 0,
      input_tokens_details: { cached_tokens: 0 },
      output_tokens_details: { reasoning_tokens: 0 },
    },
  };
  const events: string[] = [
    `event: response.created\ndata: ${JSON.stringify({ type: "response.created", response: { id, created_at: created, model } })}\n\n`,
  ];
  if (opts.reasoning) {
    events.push(
      `event: response.reasoning_summary_text.delta\ndata: ${JSON.stringify({ type: "response.reasoning_summary_text.delta", delta: opts.reasoning })}\n\n`,
    );
  }
  if (opts.text) {
    events.push(
      `event: response.output_text.delta\ndata: ${JSON.stringify({ type: "response.output_text.delta", delta: opts.text })}\n\n`,
    );
  }
  events.push(
    `event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: completed })}\n\n`,
    "data: [DONE]\n\n",
  );
  return events.join("");
}

async function cursorUpstreamToSse(
  upstream: Response,
  model: string,
  format: CursorSseFormat = "openai-responses",
): Promise<Response> {
  const reader = upstream.body?.getReader();
  if (!reader) {
    const bytes = new Uint8Array(await upstream.arrayBuffer());
    const decoded = decodeCursorResponse(bytes);
    if (!decoded.text && !decoded.reasoning && decoded.error) {
      return errorResponse(decoded.error);
    }
    if (format === "anthropic-messages") {
      return new Response(
        buildAnthropicCompletedSse(model, {
          text: decoded.text,
          reasoning: decoded.reasoning,
        }),
        { status: 200, headers: { "Content-Type": "text/event-stream" } },
      );
    }
    if (format === "openai-chat-completions") {
      // For Chat Completions the non-stream path returns plain JSON, not SSE
      // — the caller will set the JSON content-type. We still return a
      // Response so the rest of the proxy plumbing is uniform.
      return new Response(
        JSON.stringify(
          buildOpenaiChatCompletion(model, {
            text: decoded.text,
            reasoning: decoded.reasoning,
          }),
        ),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }
    return new Response(
      buildResponsesCompletedSse(model, {
        text: decoded.text,
        reasoning: decoded.reasoning,
      }),
      { status: 200, headers: { "Content-Type": "text/event-stream" } },
    );
  }

  const decoder = createCursorStreamingDecoder();

  async function* deltaStream() {
    let aggregatedText = "";
    let aggregatedReasoning = "";
    while (true) {
      const { value, done } = await reader!.read();
      if (done) break;
      const deltas = decoder.feed(value);
      for (const d of deltas) {
        if (d.reasoningDelta) aggregatedReasoning += d.reasoningDelta;
        if (d.textDelta) aggregatedText += d.textDelta;
        yield d;
      }
    }
    const drained = (
      decoder as CursorStreamingDecoder & {
        drain(): { reasoningDelta: string };
      }
    ).drain();
    if (drained.reasoningDelta) {
      aggregatedReasoning += drained.reasoningDelta;
      yield { reasoningDelta: drained.reasoningDelta, textDelta: "" };
    }
    const fin = decoder.finish();
    yield {
      __done: true,
      aggregatedText,
      aggregatedReasoning,
      error: fin.error,
    } as DeltaTerminator;
  }

  // Use a Web ReadableStream rather than `Readable.from(generator)` — the
  // latter pulls into a 16 KB high-water-mark buffer (default for
  // non-object-mode Node streams) which silently coalesces small SSE
  // events and defeats the streaming we just plumbed through HTTP/2.
  // Enqueueing each event individually preserves real-time delivery.
  let generator: AsyncGenerator<string>;
  if (format === "anthropic-messages") {
    generator = anthropicMessagesGenerator(model, deltaStream());
  } else if (format === "openai-chat-completions") {
    generator = openaiChatCompletionsGenerator(model, deltaStream());
  } else {
    generator = __openaiResponsesGenerator(model, deltaStream());
  }
  const encoder = new TextEncoder();
  const sseStream = new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        for await (const event of generator) {
          controller.enqueue(encoder.encode(event));
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
    cancel() {
      // Best-effort: tell the generator to stop. The underlying upstream
      // body reader is closed inside cursorUpstreamToSse via deltaStream
      // termination.
      try {
        generator.return?.(undefined);
      } catch {
        /* ignore */
      }
    },
  });

  return new Response(sseStream, {
    status: 200,
    headers: { "Content-Type": "text/event-stream" },
  });
}

interface DeltaTerminator {
  __done: true;
  aggregatedText: string;
  aggregatedReasoning: string;
  error?: string;
}

type DeltaItem =
  | {
      textDelta: string;
      reasoningDelta: string;
      toolCall?: any;
      __done?: undefined;
    }
  | DeltaTerminator;

export async function* __openaiResponsesGenerator(
  model: string,
  src: AsyncGenerator<DeltaItem>,
): AsyncGenerator<string> {
  const id = `resp_${uuidv4().replace(/-/g, "")}`;
  const messageId = `msg_${uuidv4().replace(/-/g, "")}`;
  const reasoningId = `rs_${uuidv4().replace(/-/g, "")}`;
  const created = Math.floor(Date.now() / 1000);
  let seq = 0,
    messageIndex = -1,
    reasoningIndex = -1;
  const output: any[] = [];
  const event = (type: string, data: Record<string, unknown>) =>
    `event: ${type}\ndata: ${JSON.stringify({ type, sequence_number: seq++, ...data })}\n\n`;
  const base = { id, object: "response", created_at: created, model };
  yield event("response.created", {
    response: { ...base, status: "in_progress", output: [] },
  });
  yield event("response.in_progress", {
    response: { ...base, status: "in_progress", output: [] },
  });
  for await (const item of src) {
    if (item.__done) {
      if (item.error) {
        yield event("response.failed", {
          response: {
            ...base,
            status: "failed",
            output,
            error: { code: "upstream_error", message: item.error },
          },
        });
        yield "data: [DONE]\n\n";
        return;
      }
      if (reasoningIndex >= 0) {
        const part = { type: "summary_text", text: item.aggregatedReasoning };
        output[reasoningIndex].summary = [part];
        yield event("response.reasoning_summary_text.done", {
          item_id: reasoningId,
          output_index: reasoningIndex,
          summary_index: 0,
          text: item.aggregatedReasoning,
        });
        yield event("response.reasoning_summary_part.done", {
          item_id: reasoningId,
          output_index: reasoningIndex,
          summary_index: 0,
          part,
        });
        yield event("response.output_item.done", {
          output_index: reasoningIndex,
          item: output[reasoningIndex],
        });
      }
      if (messageIndex >= 0) {
        const part = {
          type: "output_text",
          text: item.aggregatedText,
          annotations: [],
        };
        output[messageIndex].status = "completed";
        output[messageIndex].content = [part];
        yield event("response.output_text.done", {
          item_id: messageId,
          output_index: messageIndex,
          content_index: 0,
          text: item.aggregatedText,
        });
        yield event("response.content_part.done", {
          item_id: messageId,
          output_index: messageIndex,
          content_index: 0,
          part,
        });
        yield event("response.output_item.done", {
          output_index: messageIndex,
          item: output[messageIndex],
        });
      }
      yield event("response.completed", {
        response: {
          ...base,
          status: "completed",
          output,
          usage: {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            input_tokens_details: { cached_tokens: 0 },
            output_tokens_details: { reasoning_tokens: 0 },
          },
        },
      });
      yield "data: [DONE]\n\n";
      return;
    }
    if (item.toolCall) {
      const call = {
        id: `fc_${uuidv4().replace(/-/g, "")}`,
        type: "function_call",
        status: "completed",
        call_id: item.toolCall.call_id,
        name: item.toolCall.name,
        arguments: item.toolCall.arguments,
      };
      const index = output.length;
      output.push(call);
      yield event("response.output_item.added", {
        output_index: index,
        item: { ...call, status: "in_progress", arguments: "" },
      });
      yield event("response.function_call_arguments.delta", {
        output_index: index,
        item_id: call.id,
        delta: call.arguments,
      });
      yield event("response.function_call_arguments.done", {
        output_index: index,
        item_id: call.id,
        arguments: call.arguments,
      });
      yield event("response.output_item.done", {
        output_index: index,
        item: call,
      });
    }
    if (item.reasoningDelta) {
      if (reasoningIndex < 0) {
        reasoningIndex = output.length;
        const reasoning = { id: reasoningId, type: "reasoning", summary: [] };
        output.push(reasoning);
        yield event("response.output_item.added", {
          output_index: reasoningIndex,
          item: reasoning,
        });
        yield event("response.reasoning_summary_part.added", {
          item_id: reasoningId,
          output_index: reasoningIndex,
          summary_index: 0,
          part: { type: "summary_text", text: "" },
        });
      }
      yield event("response.reasoning_summary_text.delta", {
        item_id: reasoningId,
        output_index: reasoningIndex,
        summary_index: 0,
        delta: item.reasoningDelta,
      });
    }
    if (item.textDelta) {
      if (messageIndex < 0) {
        messageIndex = output.length;
        const message = {
          id: messageId,
          type: "message",
          status: "in_progress",
          role: "assistant",
          content: [],
        };
        output.push(message);
        yield event("response.output_item.added", {
          output_index: messageIndex,
          item: message,
        });
        yield event("response.content_part.added", {
          item_id: messageId,
          output_index: messageIndex,
          content_index: 0,
          part: { type: "output_text", text: "", annotations: [] },
        });
      }
      yield event("response.output_text.delta", {
        item_id: messageId,
        output_index: messageIndex,
        content_index: 0,
        delta: item.textDelta,
      });
    }
  }
}

/**
 * Stream Cursor's responses out as Anthropic Messages SSE so Claude Code
 * (and any other Anthropic-native client) can hit `/v1/messages` against
 * auth2api with vanilla model names like `claude-sonnet-4-5` and have it
 * Just Work over a Cursor account.
 *
 * Reasoning bytes are routed to a `thinking` content block; final text goes
 * to a `text` content block. The two blocks are emitted in order and never
 * overlap so clients that don't understand `thinking_delta` (Claude Code
 * pre-thinking versions) can still process the text block independently.
 */
async function* anthropicMessagesGenerator(
  model: string,
  src: AsyncGenerator<DeltaItem>,
): AsyncGenerator<string> {
  const messageId = `msg_${uuidv4().replace(/-/g, "")}`;
  const sseEvent = (event: string, data: unknown) =>
    `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

  yield sseEvent("message_start", {
    type: "message_start",
    message: {
      id: messageId,
      type: "message",
      role: "assistant",
      content: [],
      model,
      stop_reason: null,
      stop_sequence: null,
      usage: { input_tokens: 0, output_tokens: 0 },
    },
  });

  let thinkingOpen = false;
  let textOpen = false;
  let thinkingIndex = -1;
  let textIndex = -1;
  let nextIndex = 0;
  const closeBlock = (idx: number): string =>
    sseEvent("content_block_stop", { type: "content_block_stop", index: idx });

  for await (const item of src) {
    if (item.__done) {
      if (thinkingOpen) {
        yield closeBlock(thinkingIndex);
        thinkingOpen = false;
      }
      if (textOpen) {
        yield closeBlock(textIndex);
        textOpen = false;
      }
      if (item.error) {
        yield sseEvent("error", {
          type: "error",
          error: { type: "upstream_error", message: item.error },
        });
        return;
      }
      yield sseEvent("message_delta", {
        type: "message_delta",
        delta: { stop_reason: "end_turn", stop_sequence: null },
        usage: { output_tokens: 0 },
      });
      yield sseEvent("message_stop", { type: "message_stop" });
      return;
    }

    if (item.reasoningDelta) {
      // Claude Code expects thinking blocks to come before text blocks.
      // Once text starts we stop emitting reasoning even if more arrives.
      if (textOpen) {
        // After text has begun, treat any trailing reasoning as continuation
        // of the text channel — we cannot interleave two open blocks under
        // the Anthropic schema.
      } else {
        if (!thinkingOpen) {
          thinkingIndex = nextIndex++;
          yield sseEvent("content_block_start", {
            type: "content_block_start",
            index: thinkingIndex,
            content_block: { type: "thinking", thinking: "" },
          });
          thinkingOpen = true;
        }
        yield sseEvent("content_block_delta", {
          type: "content_block_delta",
          index: thinkingIndex,
          delta: { type: "thinking_delta", thinking: item.reasoningDelta },
        });
      }
    }
    if (item.textDelta) {
      if (thinkingOpen) {
        yield closeBlock(thinkingIndex);
        thinkingOpen = false;
      }
      if (!textOpen) {
        textIndex = nextIndex++;
        yield sseEvent("content_block_start", {
          type: "content_block_start",
          index: textIndex,
          content_block: { type: "text", text: "" },
        });
        textOpen = true;
      }
      yield sseEvent("content_block_delta", {
        type: "content_block_delta",
        index: textIndex,
        delta: { type: "text_delta", text: item.textDelta },
      });
    }
  }
}

/**
 * Stream Cursor's responses out as OpenAI Chat Completions SSE so the
 * vanilla OpenAI SDK / langchain / litellm / VS Code Continue and any
 * other Chat-Completions-native client can talk to a Cursor account
 * unchanged. Each chunk is a `chat.completion.chunk` JSON object with
 * `choices[0].delta` carrying either `content` (final text) or
 * `reasoning_content` (thinking — non-standard but widely supported by
 * downstream consumers like Cline, Cherry Studio, etc., mirroring how
 * DeepSeek / OpenRouter expose chain-of-thought).
 *
 * The stream ends with the OpenAI-required `data: [DONE]\n\n` sentinel.
 */
async function* openaiChatCompletionsGenerator(
  model: string,
  src: AsyncGenerator<DeltaItem>,
): AsyncGenerator<string> {
  const id = `chatcmpl-${uuidv4().replace(/-/g, "").slice(0, 24)}`;
  const created = Math.floor(Date.now() / 1000);
  const fingerprint = `fp_${uuidv4().replace(/-/g, "").slice(0, 12)}`;

  const chunk = (
    delta: Record<string, unknown>,
    finishReason: string | null = null,
  ) => {
    const payload = {
      id,
      object: "chat.completion.chunk",
      created,
      model,
      system_fingerprint: fingerprint,
      choices: [
        { index: 0, delta, finish_reason: finishReason, logprobs: null },
      ],
    };
    return `data: ${JSON.stringify(payload)}\n\n`;
  };

  let firstChunkSent = false;
  const sendRoleIfFirst = () => {
    if (firstChunkSent) return "";
    firstChunkSent = true;
    return chunk({ role: "assistant", content: "" });
  };

  for await (const item of src) {
    if (item.__done) {
      if (item.error) {
        // Surface upstream errors via the OpenAI streaming-error convention:
        // emit a final chunk with finish_reason="error" so SDKs that don't
        // know about the error sentinel still terminate cleanly, then a
        // separate JSON error frame for clients that do parse it.
        if (!firstChunkSent) yield sendRoleIfFirst();
        yield `data: ${JSON.stringify({
          error: { message: item.error, type: "upstream_error" },
        })}\n\n`;
        yield "data: [DONE]\n\n";
        return;
      }
      if (!firstChunkSent) yield sendRoleIfFirst();
      yield chunk({}, "stop");
      yield "data: [DONE]\n\n";
      return;
    }
    if (item.reasoningDelta) {
      yield sendRoleIfFirst();
      yield chunk({ reasoning_content: item.reasoningDelta });
    }
    if (item.textDelta) {
      yield sendRoleIfFirst();
      yield chunk({ content: item.textDelta });
    }
  }
}

function buildOpenaiChatCompletion(
  model: string,
  opts: ResponsesSseOptions,
): Record<string, unknown> {
  const id = `chatcmpl-${uuidv4().replace(/-/g, "").slice(0, 24)}`;
  const created = Math.floor(Date.now() / 1000);
  const message: Record<string, unknown> = {
    role: "assistant",
    content: opts.text || "",
  };
  if (opts.reasoning) {
    // Same convention as the streaming generator: non-standard but the de
    // facto field name across DeepSeek / OpenRouter / Continue / Cline.
    message.reasoning_content = opts.reasoning;
  }
  return {
    id,
    object: "chat.completion",
    created,
    model,
    system_fingerprint: `fp_${uuidv4().replace(/-/g, "").slice(0, 12)}`,
    choices: [
      {
        index: 0,
        message,
        finish_reason: "stop",
        logprobs: null,
      },
    ],
    usage: {
      prompt_tokens: 0,
      completion_tokens: 0,
      total_tokens: 0,
    },
  };
}

function buildAnthropicCompletedSse(
  model: string,
  opts: ResponsesSseOptions,
): string {
  const messageId = `msg_${uuidv4().replace(/-/g, "")}`;
  const ev = (event: string, data: unknown) =>
    `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  const events: string[] = [
    ev("message_start", {
      type: "message_start",
      message: {
        id: messageId,
        type: "message",
        role: "assistant",
        content: [],
        model,
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 0, output_tokens: 0 },
      },
    }),
  ];
  let idx = 0;
  if (opts.reasoning) {
    events.push(
      ev("content_block_start", {
        type: "content_block_start",
        index: idx,
        content_block: { type: "thinking", thinking: "" },
      }),
      ev("content_block_delta", {
        type: "content_block_delta",
        index: idx,
        delta: { type: "thinking_delta", thinking: opts.reasoning },
      }),
      ev("content_block_stop", { type: "content_block_stop", index: idx }),
    );
    idx += 1;
  }
  if (opts.text) {
    events.push(
      ev("content_block_start", {
        type: "content_block_start",
        index: idx,
        content_block: { type: "text", text: "" },
      }),
      ev("content_block_delta", {
        type: "content_block_delta",
        index: idx,
        delta: { type: "text_delta", text: opts.text },
      }),
      ev("content_block_stop", { type: "content_block_stop", index: idx }),
    );
  }
  events.push(
    ev("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "end_turn", stop_sequence: null },
      usage: { output_tokens: 0 },
    }),
    ev("message_stop", { type: "message_stop" }),
  );
  return events.join("");
}

function errorResponse(message: string): Response {
  const safeError = message.slice(0, 500);
  return new Response(
    JSON.stringify({
      error: {
        message: `cursor upstream rejected the request: ${safeError}`,
        type: "upstream_error",
        provider: "cursor",
      },
    }),
    { status: 502, headers: { "Content-Type": "application/json" } },
  );
}

export function normalizeCursorResponsesBody(body: any): any {
  if (!body || typeof body !== "object") return body;
  return { ...body, stream: true };
}

export async function callCursorResponses(
  options: CallCursorResponsesOptions,
): Promise<Response> {
  const { account, config } = options;
  const body = normalizeCursorResponsesBody(
    options.body ?? options.request.body,
  );
  if (config.cloaking.cursor?.protocol === "agent") {
    const toolFormat =
      (options.responseFormat || "openai-responses") === "openai-responses";
    const scope = cursorToolScope(
      options.request?.headers,
      account,
      String(body.model),
    );
    if (toolFormat) {
      try {
        const resumed = resumeCursorTools(body, scope, options.signal);
        if (resumed) return resumed;
      } catch (e: any) {
        return new Response(
          JSON.stringify({
            error: { message: e.message, type: "invalid_request_error" },
          }),
          { status: 400, headers: { "Content-Type": "application/json" } },
        );
      }
    }
    let prompt: AgentInput;
    try {
      prompt = agentInput(body);
    } catch (e: any) {
      return new Response(
        JSON.stringify({
          error: { message: e.message, type: "invalid_request_error" },
        }),
        { status: 400, headers: { "Content-Type": "application/json" } },
      );
    }
    const cursor = config.cloaking.cursor;
    const agentConfig: Config = {
      ...config,
      cloaking: {
        ...config.cloaking,
        cursor: {
          ...cursor,
          "client-version": cursor["client-version"] || AGENT_VERSION,
          "client-type": cursor["client-type"] || "ide",
        },
      },
    };
    const headers = __buildCursorHeaders(account, agentConfig);
    headers["x-cursor-client-commit"] = cursor["client-commit"] || AGENT_COMMIT;
    headers["x-cursor-client-os-version"] = release();
    headers["x-amzn-trace-id"] = "Root=" + headers["x-request-id"];
    headers["connect-accept-encoding"] = "gzip";
    delete headers["Accept-Encoding"];
    const signal = withTimeoutSignal(
      config.timeouts["stream-messages-ms"],
      options.signal,
    );
    // Use the live per-account catalogue, not a hardcoded newest-model list.
    const catalogue = await fetch(
      `${cursor["api-base-url"] || DEFAULT_API_BASE_URL}${MODELS_PATH}`,
      {
        method: "POST",
        headers: {
          ...headers,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: "{}",
        signal: AbortSignal.any([signal, AbortSignal.timeout(10_000)]),
      },
    );
    if (!catalogue.ok)
      return new Response(await catalogue.text(), {
        status: catalogue.status,
        headers: { "Content-Type": "application/json" },
      });
    let model;
    try {
      model = selectAgentModel(
        await catalogue.json(),
        normaliseModel(String(body.model || "cursor-default")),
      );
    } catch (e: any) {
      return new Response(
        JSON.stringify({
          error: { message: e.message, type: "invalid_request_error" },
        }),
        { status: 400, headers: { "Content-Type": "application/json" } },
      );
    }
    if (toolFormat && prompt.tools.length)
      return CursorToolSession.open(
        `${cursor["agent-base-url"] || "https://agent.api5.cursor.sh"}${AGENT_PATH}`,
        headers,
        prompt,
        model,
        config.timeouts["stream-messages-ms"],
        scope,
        signal,
        String(body.model),
      );
    const h2 = await openCursorAgent(
      `${cursor["agent-base-url"] || "https://agent.api5.cursor.sh"}${AGENT_PATH}`,
      headers,
      prompt,
      model,
      config.timeouts["stream-messages-ms"],
      signal,
    );
    const upstream = new Response(
      bodyToReadableStream(h2.body) as unknown as BodyInit,
      { status: h2.status, headers: h2.headers },
    );
    if (!upstream.ok) return upstream;
    return cursorUpstreamToSse(
      upstream,
      String(body.model),
      options.responseFormat || "openai-responses",
    );
  }

  // Cursor's chat is reverse-engineered to live at api2.cursor.sh. We expose
  // both legacy "agent-base-url" and "api-base-url" config keys so users can
  // override the host without forcing a code change.
  const baseUrl =
    config.cloaking.cursor?.["agent-base-url"] ||
    config.cloaking.cursor?.["api-base-url"] ||
    DEFAULT_API_BASE_URL;
  const url = `${baseUrl}${CHAT_PATH}`;
  const totalTimeoutMs = config.timeouts["stream-messages-ms"];
  // Header-arrival timer inside the transport is a *separate* shorter
  // deadline so an unreachable Cursor edge fails fast instead of dragging
  // out the full stream-messages-ms budget. Cap at 60s (or the configured
  // total, whichever is lower) — Cursor normally returns headers in well
  // under a second.
  const headerTimeoutMs = Math.min(totalTimeoutMs, 60_000);
  // Combine the per-request abort signal with a stream-messages-ms timer so
  // the body phase is also bounded — once headers arrive the transport
  // clears its internal header timer, and without this combined signal a
  // Cursor edge that stalls mid-stream would keep the client hanging
  // forever (the express layer only aborts on client disconnect).
  // Mirrors how `anthropic-api.ts` wraps fetch() with withTimeoutSignal.
  const effectiveSignal = withTimeoutSignal(totalTimeoutMs, options.signal);
  const encoded = encodeCursorChatRequest(body);

  // Cursor's chat endpoint is HTTP/2-only and rejects HTTP/1.1 requests with
  // a custom 464 status. Node's fetch (undici) does not negotiate HTTP/2 by
  // default, so we always use the hand-rolled HTTP/2 client here. Tests can
  // swap the transport through `__setCursorTransport`.
  let h2: Http2Result;
  try {
    h2 = await cursorTransport(
      url,
      __buildCursorHeaders(account, config),
      Buffer.from(encoded.bytes),
      headerTimeoutMs,
      effectiveSignal,
    );
  } catch (err: any) {
    throw new Error(
      `cursor upstream HTTP/2 request failed: ${err?.message || String(err)}`,
    );
  }
  const bodyStream = bodyToReadableStream(h2.body);
  const upstream = new Response(bodyStream as unknown as BodyInit, {
    status: h2.status,
    headers: h2.headers,
  });

  if (!upstream.ok) return upstream;
  return cursorUpstreamToSse(
    upstream,
    String(body.model || "cursor-default"),
    options.responseFormat || "openai-responses",
  );
}

export async function listCursorModels(
  manager: AccountManager,
  config?: Config,
): Promise<Array<{ id: string; owned_by: string }>> {
  const result = manager.getNextAccount();
  if (!result.account) {
    return FALLBACK_MODELS.map((id) => ({ id, owned_by: "cursor" }));
  }
  const cfg =
    config ||
    ({
      cloaking: {},
      timeouts: { "messages-ms": 120000, "stream-messages-ms": 600000 },
    } as Config);
  const account = result.account;
  const url = `${cfg.cloaking.cursor?.["api-base-url"] || DEFAULT_API_BASE_URL}${MODELS_PATH}`;
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        ...__buildCursorHeaders(account, cfg),
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: "{}",
      signal: AbortSignal.timeout(10_000),
    });
    if (!resp.ok) throw new Error(`status ${resp.status}`);
    const parsed = (await resp.json()) as {
      models?: Array<Record<string, unknown>>;
    };
    const ids = extractCursorModelIds(parsed);
    if (ids.length) return ids.map((id) => ({ id, owned_by: "cursor" }));
  } catch (err: any) {
    console.error(
      `[cursor] AvailableModels failed: ${err?.message || String(err)}`,
    );
  }
  return FALLBACK_MODELS.map((id) => ({ id, owned_by: "cursor" }));
}

/**
 * Pull model IDs out of a Cursor `AvailableModels` response.
 *
 * The response is a single object whose top-level `models` array is the
 * source of truth — each entry is `{ name, serverModelName, ... }`. Earlier
 * we ran a regex over the JSON-serialised payload, which incorrectly
 * surfaced field names (e.g. `isRecommendedForBackgroundComposer`,
 * `backgroundComposerModelConfig`) as model IDs. Now we walk the structured
 * payload, pick `serverModelName` (preferring its routing-friendly form)
 * with a `name` fallback, and skip anything that doesn't look like a real
 * model identifier.
 */
export function extractCursorModelIds(parsed: unknown): string[] {
  if (!parsed || typeof parsed !== "object") return [];
  const root = parsed as { models?: unknown };
  if (!Array.isArray(root.models)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const entry of root.models) {
    if (!entry || typeof entry !== "object") continue;
    const e = entry as Record<string, unknown>;
    const candidate =
      typeof e.serverModelName === "string"
        ? e.serverModelName
        : typeof e.name === "string"
          ? e.name
          : "";
    const trimmed = candidate.trim();
    if (!trimmed) continue;
    // Drop placeholder enum values and obviously invalid identifiers.
    if (
      /^DEGRADATION_/i.test(trimmed) ||
      /[A-Z]{3,}_[A-Z]/.test(trimmed) ||
      !/^[a-z0-9._/-]{1,64}$/i.test(trimmed)
    ) {
      continue;
    }
    const id = trimmed.startsWith("cursor-") ? trimmed : `cursor-${trimmed}`;
    if (seen.has(id)) continue;
    seen.add(id);
    out.push(id);
  }
  return out;
}
