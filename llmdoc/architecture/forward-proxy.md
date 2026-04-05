# Forward Proxy

## What it covers
The forward proxy subsystem is a separate operator-managed capability for configuring and validating outbound proxy nodes, monitoring their health, and refreshing geo metadata. It is exposed both through backend types and dedicated admin routes. Source: `src/lib.rs:21`, `src/server/serve.rs:109`.

## Core backend signals
- `src/lib.rs` exports `ForwardProxySettings`, live stats responses, validation result types, and progress event types, showing that the subsystem is modeled as a first-class domain, not just raw config storage. Source: `src/lib.rs:21`, `src/lib.rs:60`.
- Validation and maintenance operations emit structured phase/node/error progress events. Source: `src/lib.rs:89`, `src/lib.rs:133`, `src/lib.rs:172`.

## HTTP/API surface
- Settings are read from `/api/settings` and updated via `/api/settings/forward-proxy`. Source: `src/server/serve.rs:109`, `src/server/serve.rs:110`.
- Candidate validation and revalidation endpoints exist for operator workflows. Source: `src/server/serve.rs:111`, `src/server/serve.rs:116`.
- Live and summary stats endpoints exist under `/api/stats/forward-proxy*`. Source: `src/server/serve.rs:120`, `src/server/serve.rs:123`.

## Maintenance jobs
- A dedicated geo refresh job persists refreshed candidate metadata and leaves scheduled job traces. Source: `src/server/schedulers.rs:240`.
- A long-running scheduler rechecks whether geo refresh is due and triggers work incrementally instead of using a one-shot timer. Source: `src/server/schedulers.rs:274`.
- Separate forward-proxy maintenance scheduling is also started during server boot. Source: `src/server/serve.rs:279`, `src/server/serve.rs:280`.

## Frontend surface
The admin UI includes a dedicated proxy settings module and story/test coverage under `web/src/admin/ForwardProxySettingsModule.tsx` and related files. Source: `web/src/admin/ForwardProxySettingsModule.tsx:1`, `web/src/admin/ForwardProxySettingsModule.test.ts:1`.
