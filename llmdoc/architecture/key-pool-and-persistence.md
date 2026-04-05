# Key Pool and Persistence

## Key pool responsibilities
The backend owns upstream Tavily API key registration, rotation, health state, short-ID indirection, affinity, and maintenance metadata. The public README describes the pool as fairness-oriented, health-aware, and isolated from raw client-facing secrets. Source: `README.md:22`, `README.md:23`, `README.md:24`.

## Request execution model
- Proxied requests are represented as `ProxyRequest` and `ProxyResponse`, which carry method/path/query/body plus optional auth-token and chosen API-key identity. Source: `src/models.rs:29`, `src/models.rs:41`.
- The proxy path authenticates the request token first, then forwards to upstream and records billing/log metadata without converting upstream success into local persistence failures. Source: `src/server/proxy.rs:173`, `src/server/proxy.rs:438`, `src/server/proxy.rs:461`.
- Key scheduling is now budget-aware: each upstream selection checks a per-key 60-second RPM window, cooldown state, and an effective quota view derived from `quota_remaining` minus local billed credits and in-flight reservations. Source: `src/tavily_proxy/mod.rs:7`, `src/tavily_proxy/mod.rs:443`, `src/tavily_proxy/mod.rs:494`, `src/tavily_proxy/mod.rs:534`.
- MCP and idempotent Tavily HTTP requests can transparently migrate to another key when the current key is rate-limited or quota-exhausted, up to three attempts per request. Source: `src/tavily_proxy/mod.rs:8`, `src/tavily_proxy/mod.rs:4213`, `src/tavily_proxy/mod.rs:4372`.
- MCP replay migration now rebuilds the upstream session with protocol-compatible `Accept` headers and immediately keeps using the refreshed in-memory session binding for the current request, so a successful replay does not bounce back as `session_unavailable` before the retried call is sent. Source: `src/tavily_proxy/mod.rs:4111`, `src/tavily_proxy/mod.rs:4304`, `src/tavily_proxy/mod.rs:4435`, `src/server/tests.rs:20075`.
- Rate-limit and quota-exhausted paths now emit `key-budget` container logs that include the triggering event, source/target key IDs, cooldown application, and a full per-key budget inventory with persisted quota snapshot, effective quota, RPM window usage, and current block reason. Source: `src/tavily_proxy/mod.rs:538`, `src/tavily_proxy/mod.rs:606`, `src/tavily_proxy/mod.rs:908`, `src/tavily_proxy/mod.rs:3974`, `src/tavily_proxy/mod.rs:4536`.

## Persistence responsibilities
Stable persistence spans more than the original `api_keys` and `request_logs` tables described in the README. Current durable data families include:
- API keys and their health/usage state. Source: `README.md:34`.
- Access tokens and user-token bindings. Source: `README.md:139`, `src/server/handlers/user.rs:316`.
- Request logs containing method/path/query, status, bodies, request-kind metadata, and forwarded/dropped headers. Source: `README.md:26`, `src/models.rs:419`.
- Scheduled jobs and maintenance audit trails. Source: `docs/plan/0001:request-logs-gc/PLAN.md:64`, `src/server/schedulers.rs:34`.
- Sticky binding and post-launch usage bucket tables for key↔user affinity. Source: `docs/specs/29w25-admin-key-sticky-users-nodes/SPEC.md:21`, `docs/specs/29w25-admin-key-sticky-users-nodes/contracts/db.md:44`.
- MCP session rows now persist the original `initialize` body plus whether `notifications/initialized` has been seen, so the proxy can rebuild upstream sessions on a different key without forcing clients to reconnect. Source: `src/store/mod.rs:1514`, `src/store/mod.rs:1550`, `src/tavily_proxy/mod.rs:4068`, `src/tavily_proxy/mod.rs:7218`.
- `api_key_runtime_state` persists short-lived runtime facts that are not naturally recoverable from quota snapshots alone, specifically cooldown and last migration metadata. Source: `src/store/mod.rs:1571`, `src/tavily_proxy/mod.rs:730`, `src/tavily_proxy/mod.rs:751`.
- `request_logs.visibility` now distinguishes normal visible logs from retry-shadow attempts, allowing retry chains to keep only the final returned attempt in operator-facing views. Source: `src/lib.rs:335`, `src/store/mod.rs:1098`, `src/store/mod.rs:12953`, `src/tavily_proxy/mod.rs:4307`, `src/tavily_proxy/mod.rs:4550`.

## Affinity and sticky behavior
- Baseline behavior keeps a short-lived token-to-key affinity and falls back to global least-recently-used assignment when affinity expires or a key becomes unavailable. Source: `README.md:22`, `README.zh-CN.md:20`.
- Later features add user↔api_key current bindings and prefer recently successful keys for the token owner before falling back to soft affinity / global LRU. Source: `docs/specs/29w25-admin-key-sticky-users-nodes/SPEC.md:21`, `docs/specs/29w25-admin-key-sticky-users-nodes/SPEC.md:22`.
- MCP privacy hardening adds stronger session-aware affinity and opaque session semantics on top of the pool. Source: `docs/specs/34pgu-mcp-session-privacy-affinity-hardening/SPEC.md:18`.
- Research result affinity remains special-cased: `POST /research` establishes the key mapping and later result fetches stay on that key instead of participating in transparent cross-key replay. Source: `src/lib.rs:446`, `src/tavily_proxy/mod.rs:3602`, `src/tavily_proxy/mod.rs:4746`.
