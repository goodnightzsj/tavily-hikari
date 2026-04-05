# MCP and Tavily HTTP

## Responsibility
The product exposes two integrator-facing protocol surfaces: an MCP proxy at `/mcp` and a Tavily-compatible HTTP facade under `/api/tavily/*`. Both surfaces authenticate with access tokens, participate in quota enforcement, and forward requests through the shared backend proxy layer. Source: `src/server/serve.rs:98`, `src/server/serve.rs:102`, `src/server/serve.rs:107`, `src/server/serve.rs:227`.

## MCP surface
- `ANY /mcp` is the MCP entrypoint, and subpaths are explicitly rejected through a separate handler. Source: `src/server/serve.rs:226`.
- Requests authenticate from `Authorization: Bearer` or query token inputs, with a special `dev_open_admin` fallback only when that mode is enabled. Source: `src/server/proxy.rs:173`, `src/server/proxy.rs:178`, `src/server/proxy.rs:188`.
- MCP requests may not rely on the `dev_open_admin` fallback; they must provide an explicit token when that mode is active. Source: `src/server/proxy.rs:244`.
- Active MCP sessions are token-bound, and reconnect attempts with a different token are rejected. Source: `src/server/proxy.rs:210`, `src/server/proxy.rs:244`.
- Existing MCP sessions can pin an upstream key, preserving affinity across later calls in the same session. Source: `src/server/proxy.rs:244`.
- The proxy classifies MCP control-plane vs billable tool calls before forwarding, derives reserved credits from `tools/call.params.name`, and records whether a request is safe for transparent retry. Source: `src/server/proxy.rs:244`, `src/server/proxy.rs:541`, `src/server/proxy.rs:856`.
- MCP session creation now stores the original `initialize` payload and tracks whether `notifications/initialized` was seen; that state is reused to transparently recreate the upstream session on a new key when the pinned key budget becomes unavailable. Source: `src/server/proxy.rs:1053`, `src/server/proxy.rs:1073`, `src/server/proxy.rs:1112`, `src/tavily_proxy/mod.rs:4068`, `src/tavily_proxy/mod.rs:4213`.
- Retry-shadow MCP attempts are hidden from normal operator views by downgrading earlier abandoned request logs to `suppressed_retry_shadow` once a later attempt is selected for return. Source: `src/lib.rs:336`, `src/tavily_proxy/mod.rs:4307`, `src/store/mod.rs:12953`.

## Tavily HTTP facade
- The facade exposes `/api/tavily/search`, `/extract`, `/crawl`, `/map`, `/research`, `/research/:request_id`, and `/usage`. Source: `src/server/serve.rs:98`, `src/server/serve.rs:107`.
- Each POST endpoint is configured with endpoint-specific upstream path, response mode, and quota behavior through `TavilyEndpointConfig`. Source: `src/server/handlers/tavily.rs:1`, `src/server/handlers/tavily.rs:15`, `src/server/handlers/tavily.rs:231`.
- Reserved-credit estimation differs by operation: search depends on depth, extract/crawl/map depend on requested page/url volume, and research enforces a minimum based on model tier. Source: `src/server/handlers/tavily.rs:62`, `src/server/handlers/tavily.rs:153`, `src/server/handlers/tavily.rs:161`, `src/server/handlers/tavily.rs:168`, `src/server/handlers/tavily.rs:179`.
- Query-string `tavilyApiKey` is stripped before forwarding and is treated as the auth token input instead. Source: `src/server/proxy.rs:105`.
- `GET /api/tavily/research/:request_id` is a result retrieval surface and must not bill quota a second time. Source: `src/server/handlers/tavily.rs:260`.
- `/api/tavily/search`, `/extract`, `/crawl`, and `/map` now participate in the same budget-aware key scheduler as MCP and may transparently replay on a different key after upstream `429` or quota exhaustion, while `POST /research` still stays single-key because billing and request-id affinity depend on the original key. Source: `src/tavily_proxy/mod.rs:4372`, `src/tavily_proxy/mod.rs:4428`, `src/tavily_proxy/mod.rs:4529`, `src/tavily_proxy/mod.rs:4589`.
- The HTTP layer precomputes reserved credits for both direct Tavily endpoints and MCP tool calls so key selection and token quota prechecks can happen before the upstream request is sent. Source: `src/server/handlers/tavily.rs:194`, `src/server/handlers/tavily.rs:205`, `src/server/handlers/tavily.rs:623`, `src/server/proxy.rs:545`.

## Frontend/operator touchpoints
- The user console includes guided MCP setup snippets and first-party MCP/HTTP probe actions to validate token usability. Source: `web/src/UserConsole.tsx:23`, `web/src/UserConsole.tsx:74`, `web/src/UserConsole.tsx:163`, `web/src/UserConsole.tsx:186`.
- Shared frontend helpers for these probes live in `web/src/api.ts` and `web/src/lib/mcpProbe.ts`. Source: `web/src/api.ts:1`, `web/src/UserConsole.tsx:53`.
