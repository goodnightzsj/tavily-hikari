# Background Jobs

## Boot-time schedulers
Server startup immediately spawns multiple background schedulers after binding the listener. Source: `src/server/serve.rs:274`.

## Current recurring jobs
- `quota_sync` refreshes keys pending quota synchronization. Source: `src/server/schedulers.rs:15`.
- `token_usage_rollup` consolidates token usage summaries. Source: `src/server/schedulers.rs:82`.
- `auth_token_logs_gc` prunes auth-token logs. Source: `src/server/schedulers.rs:123`.
- `request_logs_gc` prunes hot request log storage while preserving historical aggregates through rollups. Source: `docs/plan/0001:request-logs-gc/PLAN.md:49`.
- `forward_proxy_geo_refresh` updates geo metadata for proxy candidates. Source: `src/server/schedulers.rs:240`.
- Forward proxy maintenance is also started during boot for ongoing subsystem upkeep. Source: `src/server/serve.rs:280`.

## Operational pattern
- Jobs leave traces through `scheduled_job_start` and `scheduled_job_finish`, with status and summary messages persisted for operator inspection. Source: `src/server/schedulers.rs:34`, `src/server/schedulers.rs:104`, `src/server/schedulers.rs:142`, `src/server/schedulers.rs:260`.
- Some jobs add randomized delay or periodic backoff to avoid concentrated startup bursts or tight error loops. Source: `src/server/schedulers.rs:1`, `src/server/schedulers.rs:32`, `src/server/schedulers.rs:92`.
