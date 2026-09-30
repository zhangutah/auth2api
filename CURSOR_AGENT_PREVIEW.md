# Cursor agent protocol preview

Opt in with `cloaking.cursor.protocol: agent`. Legacy remains the default.
The reference is the locally installed official Cursor 3.22.12 desktop bundle;
see the transaction's provenance.json and schema.json for hashes/field offsets.

The preview sends the latest nonempty text user message in ASK mode to
`/agent.v1.AgentService/Run` over HTTP/2 Connect. It resolves the model and its
non-max default parameters from the live account-specific AvailableModels
catalogue, services an in-memory bounded KV/blob exchange, and forwards explicit
text/thinking updates through the existing SSE converters. It does not execute
shell, filesystem, MCP, or other tools on the proxy. ASK server tool requests fail explicitly. Responses requests with function declarations opt into the bounded AGENT bridge described below.

Tested live with cursor-grok-4.7 and cursor-kimi-k3, Hello!, using the VM's existing
Cursor account. The Mac credentials and original source/config were unchanged.

Text conversation history is encoded in native conversation_history and top-level
instructions/system/developer messages in system_prompt_spec.append, but live ASK
probes did not use those fields. Therefore their full text is also preserved in a
role-tagged current-user compatibility transcript. This is prompt-level text-chat
compatibility, NOT native system-role precedence or checkpoint continuity.
Function declarations (including Codex namespaces) are mapped to native MCP schemas.
Responses SSE includes item IDs, indexes and lifecycle events required by Codex.

## Codex-owned tool round trips

Responses function declarations select native AGENT mode. Native shell, text read/write,
and declared MCP calls become Responses function-call lifecycle events. Codex executes
these under its own sandbox/approval policy; text results resume the same live Cursor
HTTP/2 turn. Long-running shell commands use the declared client write_stdin function.
Native approval/interaction queries are not auto-approved. Unsupported exec types fail
explicitly rather than pretending they ran.

The bridge is live-tested with Codex CLI 0.158.0 and cursor-claude-opus-5-5.
The tested VM uses one Cursor account; multi-account deployments require account
affinity for a pending turn. Custom/built-in tools outside declared function schemas
and native background/subagent/IDE operations are not supported.

Live state is scoped to API key, account and model; it is bounded to 16 sessions,
32 pending calls, 120-second idle expiry, 2 MiB client results and 32 MiB blob/output
limits. A restart loses pending turns: start a fresh turn instead of replaying an
unknown result. All delivered parallel calls require results. Usage remains unavailable
(zero placeholders), not a measurement of actual token consumption.

On Ubuntu with AppArmor user-namespace restrictions, install the distribution's
bubblewrap package so Codex uses /usr/bin/bwrap with its shipped profile. Do not disable
the system-wide restriction or Codex sandbox to work around a missing dependency.

Limits: no images, encrypted reasoning, structured output, forced tool choice, or
previous_response_id/persistent checkpoint continuation. Text reasoning summaries are
accepted as prior context. Unsupported inputs return 400; unsupported native tool or
interaction requests fail explicitly, including after partial text. This is not full
Cursor IDE feature parity. Generation remains subject
to Cursor's account/model eligibility. max_tokens/temperature/top_p are NOT mapped
to model controls in this preview; do not treat these as enforced sampling limits.
ASK includes Cursor's normal agent prompt, so it is not equivalent to a bare-model
chat API. A full implementation requires native history/checkpoint/blob mapping,
usage propagation, sampling controls where supported, cancellation/backpressure,
and endpoint-specific integration coverage before changing the default.

Header defaults match the observed reference version and commit; explicit
client-version/client-commit configuration must be updated as a matched pair.
Model catalogue is queried per request rather than globally cached across accounts.
No optional workspace-exclusion flag is sent: the upstream rejects it for this
account/model. The proxy never reads workspace files itself. Data explicitly returned by Codex
tools is sent to Cursor; consider this when choosing permitted directories/tools.
