# Admin API Domains

## Access model
Admin/operator APIs are authenticated through admin request detection and generally return `403 Forbidden` when the request is not authorized. Source: `src/server/handlers/admin_resources.rs:1`, `src/server/handlers/admin_resources.rs:6`.

## Domain groups
- Registration and session: `/api/admin/registration`, `/api/admin/login`, `/api/admin/logout`. Source: `src/server/serve.rs:91`, `src/server/serve.rs:96`, `src/server/serve.rs:97`.
- Dashboard and settings: `/api/summary`, `/api/summary/windows`, `/api/settings`, `/api/settings/forward-proxy*`, `/api/stats/forward-proxy*`. Source: `src/server/serve.rs:108`, `src/server/serve.rs:111`, `src/server/serve.rs:121`, `src/server/serve.rs:124`.
- Keys: `/api/keys`, `/api/keys/validate`, `/api/keys/batch`, `/api/keys/:id`, `/api/keys/:id/status`, `/api/keys/:id/quarantine`, `/api/keys/:id/sync-usage`, `/api/keys/:id/secret`, `/api/keys/:id/logs*`, `/api/keys/:id/sticky-users`, `/api/keys/:id/sticky-nodes`. Source: `src/server/serve.rs:126`, `src/server/serve.rs:153`, `src/server/serve.rs:154`.
- Access tokens: `/api/tokens`, `/api/tokens/groups`, `/api/tokens/batch`, `/api/tokens/:id`, `/api/tokens/:id/status`, `/api/tokens/:id/note`, `/api/tokens/:id/secret`, `/api/tokens/:id/secret/rotate`, `/api/tokens/:id/metrics*`, `/api/tokens/:id/logs*`, `/api/tokens/:id/events`, `/api/tokens/leaderboard`. Source: `src/server/serve.rs:156`, `src/server/serve.rs:170`, `src/server/serve.rs:172`, `src/server/serve.rs:180`.
- Logs, jobs, users, and user tags: `/api/logs*`, `/api/jobs`, `/api/users*`, `/api/user-tags*`. Source: `src/server/serve.rs:136`, `src/server/serve.rs:143`, `src/server/serve.rs:139`.

## Behavioral patterns
- Token detail reads owner metadata in addition to token state so the admin UI can render ownership context without a second route family. Source: `src/server/handlers/admin_resources.rs:1`, `src/server/handlers/admin_resources.rs:16`, `src/server/handlers/admin_resources.rs:22`.
- Token event streaming uses SSE snapshots plus periodic ping frames to refresh detail pages. Source: `src/server/handlers/admin_resources.rs:34`, `src/server/handlers/admin_resources.rs:43`, `src/server/handlers/admin_resources.rs:57`.
- Forward proxy validation and revalidation can stream progress as event streams when the request accepts SSE. Source: `src/server/handlers/admin_resources.rs:1`, `src/server/handlers/admin_resources.rs:1055`.
- Log and error payloads are redacted before snapshot emission where needed. Source: `src/server/handlers/admin_resources.rs:93`, `src/server/handlers/admin_resources.rs:95`.

## Frontend routing relationship
The admin SPA maps these APIs into path-routed modules for dashboard, tokens, keys, requests, jobs, users, alerts, and proxy settings, plus dedicated detail routes for tokens, keys, and users. Source: `web/src/admin/routes.ts:5`, `web/src/admin/routes.ts:44`.
