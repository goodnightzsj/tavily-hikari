# Quota and Usage Accounting

## Why this subsystem exists
Quota and billing logic exists to keep Hikari-issued tokens and user accounts aligned with real Tavily consumption while preserving long-term operator visibility even when hot logs are pruned. Source: `docs/quota-design.md:9`, `docs/plan/0001:request-logs-gc/PLAN.md:140`.

## Main accounting surfaces
- Request-kind analysis and canonicalization are exported centrally from `src/lib.rs`, which indicates that billing, request classification, and operator filtering are first-class backend concepts. Source: `src/lib.rs:8`.
- The proxy records token attempts with request-kind, failure-kind, key-effect, and request-log linkage as part of request completion. Source: `src/server/proxy.rs:442`.
- `request_logs` stores rich per-request audit data, including request-kind labels and business credit fields. Source: `src/models.rs:421`, `src/models.rs:431`, `src/models.rs:432`.
- Key budget accounting now has a second layer alongside token quota: each key keeps a runtime RPM window, in-flight credit reservations, and local billed-credit overlay on top of the persisted upstream quota snapshot. Source: `src/tavily_proxy/mod.rs:10`, `src/tavily_proxy/mod.rs:443`, `src/tavily_proxy/mod.rs:704`.
- Reserved credits are computed before forward for both Tavily HTTP endpoints and known Tavily MCP tools, then settled back to actual charged credits after the request completes. Source: `src/server/handlers/tavily.rs:194`, `src/server/handlers/tavily.rs:623`, `src/server/proxy.rs:541`, `src/tavily_proxy/mod.rs:7301`.

## Rollups and historical invariants
- Request log retention does not change the meaning of all-time operator metrics; historical totals are preserved through rollup buckets. Source: `docs/plan/0001:request-logs-gc/PLAN.md:56`, `docs/plan/0001:request-logs-gc/PLAN.md:61`.
- `api_key_usage_buckets` is the durable rollup source for API-key historical metrics and is updated atomically with request log writes. Source: `docs/plan/0001:request-logs-gc/contracts/db.md:16`, `docs/plan/0001:request-logs-gc/PLAN.md:58`.
- Additional token/account quota tables and monthly rebasing flows are documented in internal quota specs rather than public docs. Source: `docs/specs/u4k9m-monthly-quota-rebase-audit/SPEC.md:5`, `docs/specs/45squ-account-quota-user-console/SPEC.md:16`.
- Retry-shadow request logs still land in `request_logs`, but non-visible entries are filtered out of normal recent-log, summary, and body-fetch surfaces so transparent retries do not double-count operator dashboards. Source: `src/lib.rs:335`, `src/store/mod.rs:1144`, `src/store/mod.rs:13592`, `src/tests/mod.rs:7660`.

## Background jobs tied to accounting
- Quota sync scheduler refreshes external usage-related key state. Source: `src/server/schedulers.rs:15`.
- Token usage rollup scheduler periodically consolidates token usage stats. Source: `src/server/schedulers.rs:82`.
- Request log and auth-token log GC jobs keep hot datasets bounded while retaining summarized history. Source: `src/server/schedulers.rs:123`, `docs/plan/0001:request-logs-gc/PLAN.md:49`.
- Quota sync now also resets the in-memory key quota overlay for the synced key, so future scheduling decisions continue from the fresh upstream snapshot instead of accumulated local estimates. Source: `src/tavily_proxy/mod.rs:722`, `src/tavily_proxy/mod.rs:8002`.

## Related stable concepts
- Effective quota can be shaped by user tags and system tags. Source: `docs/specs/2mt2u-admin-user-tags-quota/SPEC.md:14`.
- New user flows may intentionally start with zero base quota until an admin assigns tags or custom quota. Source: `README.md:217`.
