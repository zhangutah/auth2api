import test from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";
import { once } from "node:events";
import { gzipSync } from "node:zlib";
import {
  agentBytes as b,
  agentFields,
  agentFrame,
  agentPrompt,
  agentInput,
  agentConversationText,
  encodeAgentRun,
  selectAgentModel,
  openCursorAgent,
} from "../src/upstream/cursor-agent";
import {
  decodeCursorResponse,
  __openaiResponsesGenerator,
} from "../src/upstream/cursor-api";
const cat = {
  models: [
    {
      name: "kimi-k3",
      variants: [
        {
          legacySlug: "kimi-k3-max",
          isDefaultNonMaxConfig: true,
          parameterValues: [{ id: "reasoning", value: "max" }],
        },
      ],
    },
  ],
};
const model = selectAgentModel(cat, "kimi-k3");
test("agent resolves base name and explicit legacy variant to structured parameters", () => {
  assert.deepEqual(model, {
    id: "kimi-k3",
    parameters: [{ id: "reasoning", value: "max" }],
  });
  assert.deepEqual(selectAgentModel(cat, "kimi-k3-max"), model);
  assert.throws(() => selectAgentModel(cat, "missing"), /not available/);
});
test("agent preserves instructions, ordered history and function declarations", () => {
  assert.equal(agentPrompt({ input: "Hello!", tools: [] }), "Hello!");
  const input = agentInput({
    instructions: "top-level",
    input: [
      {
        role: "developer",
        content: [{ type: "input_text", text: "developer" }],
      },
      { role: "user", content: "first" },
      { role: "assistant", content: [{ type: "output_text", text: "answer" }] },
      { role: "user", content: [{ type: "input_text", text: "next" }] },
    ],
    tools: [
      {
        type: "namespace",
        name: "functions",
        tools: [
          {
            type: "function",
            name: "inspect",
            description: "read",
            parameters: { type: "object", properties: {} },
          },
        ],
      },
    ],
  });
  assert.equal(input.text, "next");
  const transcript = agentConversationText(input);
  assert.ok(transcript.includes("first"));
  assert.ok(transcript.includes("answer"));
  assert.ok(transcript.includes("top-level"));
  assert.ok(transcript.endsWith("CURRENT USER:\nnext"));
  assert.equal(
    input.instructions,
    "top-level\n\ndeveloper instructions:\ndeveloper",
  );
  assert.deepEqual(input.history, [
    { role: "user", text: "first" },
    { role: "assistant", text: "answer" },
  ]);
  assert.equal(input.tools[0].name, "functions.inspect");
  const run = agentFields(
    agentFields(encodeAgentRun(input, model)).get(1) as Buffer,
  );
  assert.equal(
    (agentFields(run.get(29) as Buffer).get(2) as Buffer).toString(),
    input.instructions,
  );
  const action = agentFields(
    agentFields(run.get(2) as Buffer).get(1) as Buffer,
  );
  assert.ok(Buffer.isBuffer(action.get(7)));
  const tool = agentFields(agentFields(run.get(4) as Buffer).get(1) as Buffer);
  assert.equal((tool.get(1) as Buffer).toString(), "functions.inspect");
  assert.deepEqual(JSON.parse((tool.get(6) as Buffer).toString()), {
    type: "object",
    properties: {},
  });
});
test("agent fails explicitly for unsupported inputs rather than dropping them", () => {
  for (const body of [
    {
      messages: [
        { role: "user", content: "hi" },
        { role: "assistant", content: "hi" },
      ],
    },
    { messages: [{ role: "system", content: "system" }] },
    { messages: [{ role: "user", content: [{ type: "image_url" }] }] },
    { input: "hi", tools: [{ type: "web_search" }] },
    { input: "hi", tool_choice: "required" },
    { input: [{ type: "function_call_output", call_id: "a", output: "b" }] },
    { input: "hi", previous_response_id: "resp_1" },
    { input: " " },
    { input: "hi", tools: [null] },
    { input: "hi", tools: [{ type: "function", name: "a", parameters: [] }] },
    {
      input: [
        { role: "assistant", content: "hi", tool_calls: [] },
        { role: "user", content: "next" },
      ],
    },
    {
      input: "hi",
      tools: [
        { type: "function", name: "a", parameters: {} },
        { type: "function", name: "a", parameters: {} },
      ],
    },
  ])
    assert.throws(() => agentInput(body));
  assert.equal(
    agentInput({ input: "hi", instructions: "keep me" }).instructions,
    "keep me",
  );
  assert.deepEqual(
    agentInput({
      input: "hi",
      tools: [{ type: "function", name: "a", parameters: {} }],
      tool_choice: "none",
    }).tools,
    [],
  );
});
async function responsesEvents(items: any[]) {
  async function* source(): AsyncGenerator<any> {
    yield* items;
  }
  const events: any[] = [];
  for await (const frame of __openaiResponsesGenerator(
    "cursor-test",
    source(),
  )) {
    const line = frame.split("\n").find((l) => l.startsWith("data: "));
    if (line && line !== "data: [DONE]") events.push(JSON.parse(line.slice(6)));
  }
  return events;
}
test("Responses SSE has item lifecycle, stable IDs, indexes and completed content", async () => {
  const events = await responsesEvents([
    { reasoningDelta: "think", textDelta: "Hello" },
    { __done: true, aggregatedText: "Hello", aggregatedReasoning: "think" },
  ]);
  assert.deepEqual(
    events.map((e) => e.sequence_number),
    events.map((_, i) => i),
  );
  const final = events.at(-1).response;
  assert.equal(events.at(-1).type, "response.completed");
  assert.equal(final.output[1].content[0].text, "Hello");
  assert.equal(final.output[0].summary[0].text, "think");
  for (const e of events.filter(
    (e) => e.type === "response.output_text.delta",
  )) {
    assert.equal(e.item_id, final.output[1].id);
    assert.equal(e.output_index, 1);
    assert.equal(e.content_index, 0);
  }
  assert.ok(events.some((e) => e.type === "response.output_item.added"));
  assert.ok(events.some((e) => e.type === "response.output_item.done"));
});
test("Responses SSE never reports successful completion after a partial upstream error", async () => {
  const events = await responsesEvents([
    { textDelta: "partial" },
    {
      __done: true,
      aggregatedText: "partial",
      aggregatedReasoning: "",
      error: "denied",
    },
  ]);
  assert.equal(events.at(-1).type, "response.failed");
  assert.equal(events.at(-1).response.error.message, "denied");
  assert.equal(
    events.some((e) => e.type === "response.completed"),
    false,
  );
});
test("run request uses field 9 requested_model and ASK, without exclusion flag", () => {
  const run = agentFields(
    agentFields(encodeAgentRun("Hello!", model)).get(1) as Buffer,
  );
  assert.equal(run.has(12), false);
  assert.equal(agentFields(run.get(1) as Buffer).get(10), 2);
  const requested = agentFields(run.get(9) as Buffer);
  assert.equal((requested.get(1) as Buffer).toString(), "kimi-k3");
  assert.equal(requested.get(7), 1);
});
test("protobuf decoder rejects truncated and unsafe integers", () => {
  for (const data of [
    Buffer.from([10, 20]),
    Buffer.from([8, 128]),
    Buffer.from([0]),
    Buffer.from([15]),
    Buffer.from([8, ...Array(10).fill(255)]),
  ])
    assert.throws(() => agentFields(data));
});
async function scenario(
  send: (s: http2.ServerHttp2Stream, packet: Buffer) => void,
  timeout = 1000,
) {
  const server = http2.createServer();
  let session: http2.ServerHttp2Session | undefined;
  server.on("session", (s) => {
    session = s;
  });
  server.on("stream", (stream) => {
    stream.respond({
      ":status": 200,
      "content-type": "application/connect+proto",
    });
    let buffer = Buffer.alloc(0);
    stream.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 5) {
        const n = buffer.readUInt32BE(1);
        if (buffer.length < n + 5) break;
        const packet = buffer.subarray(5, 5 + n);
        buffer = buffer.subarray(5 + n);
        send(stream, packet);
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    const port = (server.address() as any).port;
    const response = await openCursorAgent(
      `http://127.0.0.1:${port}/agent.v1.AgentService/Run`,
      {},
      "Hello!",
      model,
      timeout,
    );
    const raw = Buffer.from(
      await new Response(response.body as any).arrayBuffer(),
    );
    return decodeCursorResponse(raw);
  } finally {
    session?.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}
test("agent handles fragmented gzip updates, KV set/get, then turn_ended", async () => {
  let kvAcks = 0;
  const result = await scenario((s, packet) => {
    const m = agentFields(packet);
    if (m.has(1)) {
      s.write(
        agentFrame(
          b(
            4,
            Buffer.concat([
              Buffer.from([8, 7]),
              b(3, Buffer.concat([b(1, "blob-id"), b(2, "checkpoint-secret")])),
            ]),
          ),
        ),
      );
    } else if (m.has(3)) {
      const kv = agentFields(m.get(3) as Buffer);
      if (kv.has(3))
        s.write(
          agentFrame(
            b(4, Buffer.concat([Buffer.from([8, 8]), b(2, b(1, "blob-id"))])),
          ),
        );
      if (kv.has(2)) {
        assert.equal(
          (agentFields(kv.get(2) as Buffer).get(1) as Buffer).toString(),
          "checkpoint-secret",
        );
        kvAcks++;
        const delta = agentFrame(gzipSync(b(1, b(1, b(1, "Hello!")))), 1);
        s.write(delta.subarray(0, 3));
        s.write(delta.subarray(3));
        s.write(agentFrame(b(1, b(14, Buffer.alloc(0)))));
      }
    }
  });
  assert.equal(kvAcks, 1);
  assert.equal(result.text, "Hello!");
  assert.equal(result.error, undefined);
  assert.equal(result.text.includes("checkpoint-secret"), false);
});
test("agent propagates Connect errors without treating HTTP 200 as successful generation", async () => {
  const result = await scenario((s, packet) => {
    if (agentFields(packet).has(1))
      s.end(
        agentFrame(
          Buffer.from(
            JSON.stringify({
              error: { code: "permission_denied", message: "account denied" },
            }),
          ),
          2,
        ),
      );
  });
  assert.match(result.error!, /permission_denied.*account denied/);
});
test("agent stops on unsupported exec instead of executing remote commands", async () => {
  const result = await scenario((s, packet) => {
    if (agentFields(packet).has(1))
      s.write(agentFrame(b(2, b(2, "shell-command"))));
  });
  assert.match(result.error!, /does not execute tools/);
});
test("agent has bounded deadline and rejects missing blobs", async () => {
  const timeout = await scenario(() => {}, 30);
  assert.match(timeout.error!, /deadline/);
  const missing = await scenario((s, packet) => {
    if (agentFields(packet).has(1))
      s.write(agentFrame(b(4, b(2, b(1, "missing")))));
  });
  assert.match(missing.error!, /blob is unavailable/);
});
