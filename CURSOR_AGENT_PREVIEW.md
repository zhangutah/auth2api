# Cursor agent protocol preview

Opt in with `cloaking.cursor.protocol: agent`. Legacy remains the default.
The reference is the locally installed official Cursor 3.22.12 desktop bundle;
see the transaction's provenance.json and schema.json for hashes/field offsets.

The preview sends the latest nonempty text user message in ASK mode to
`/agent.v1.AgentService/Run` over HTTP/2 Connect. It resolves the model and its
non-max default parameters from the live account-specific AvailableModels
catalogue, services an in-memory bounded KV/blob exchange, and forwards explicit
text/thinking updates through the existing SSE converters. It does not execute
shell, filesystem, MCP, or other tools. Server tool requests fail explicitly.

Tested live with cursor-grok-4.7 and cursor-kimi-k3, Hello!, using the VM's existing
Cursor account. The Mac credentials and original source/config were unchanged.

Text conversation history is encoded in native conversation_history and top-level
instructions/system/developer messages in system_prompt_spec.append, but live ASK
probes did not use those fields. Therefore their full text is also preserved in a
role-tagged current-user compatibility transcript. This is prompt-level text-chat
compatibility, NOT native system-role precedence or checkpoint continuity.
Function declarations (including Codex namespaces) are mapped to native MCP schemas.
Responses SSE includes item IDs, indexes and lifecycle events required by Codex.

Limits: no images, actual tool execution/results, reasoning input items, structured
output, forced tool choice, or continuation IDs. Unsupported inputs return 400;
upstream tool requests fail explicitly, including after partial text. Accepting
tool declarations does NOT make this a full Codex coding-agent backend. Generation remains subject
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
account/model. No workspace contents are supplied or local files exposed.
