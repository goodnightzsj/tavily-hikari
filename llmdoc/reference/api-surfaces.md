# API Surfaces

## Public and shared endpoints
- `GET /health` — liveness check. Source: `src/server/serve.rs:70`.
- `GET /api/profile` — profile/auth surface used by web entrypoints. Source: `src/server/serve.rs:80`.
- `GET /api/public/events|logs|metrics` — public status and observability surface. Source: `src/server/serve.rs:75`, `src/server/serve.rs:124`.

## Protocol endpoints
- `ANY /mcp` — MCP proxy entrypoint. Source: `src/server/serve.rs:225`.
- `POST /api/tavily/search|extract|crawl|map|research` and `GET /api/tavily/research/:request_id`, `GET /api/tavily/usage` — Tavily-style HTTP facade. Source: `src/server/serve.rs:97`, `src/server/serve.rs:103`, `src/server/serve.rs:106`.

## User/auth endpoints
- Linux DO OAuth: `GET|POST /auth/linuxdo`, `GET /auth/linuxdo/callback`. Source: `src/server/serve.rs:81`.
- User session/logout: `POST /api/user/logout`. Source: `src/server/serve.rs:83`.
- User dashboard/token endpoints: `/api/user/token`, `/api/user/dashboard`, `/api/user/tokens`, `/api/user/tokens/:id`, `/api/user/tokens/:id/secret`, `/api/user/tokens/:id/logs`. Source: `src/server/serve.rs:84`, `src/server/serve.rs:89`.

## Admin/operator endpoints
- Registration and admin session: `/api/admin/registration`, `/api/admin/login`, `/api/admin/logout`. Source: `src/server/serve.rs:90`, `src/server/serve.rs:95`.
- Dashboard/settings: `/api/summary`, `/api/summary/windows`, `/api/settings`, `/api/settings/forward-proxy*`, `/api/stats/forward-proxy*`. Source: `src/server/serve.rs:107`, `src/server/serve.rs:109`, `src/server/serve.rs:120`.
- Keys: `/api/keys`, `/api/keys/validate`, `/api/keys/batch`, `/api/keys/:id`, `/api/keys/:id/secret`, `/api/keys/:id/status`, `/api/keys/:id/quarantine`, `/api/keys/:id/sync-usage`, `/api/keys/:id/logs*`, `/api/keys/:id/sticky-users`, `/api/keys/:id/sticky-nodes`. Source: `src/server/serve.rs:125`, `src/server/serve.rs:152`.
- Tokens: `/api/tokens`, `/api/tokens/groups`, `/api/tokens/batch`, `/api/tokens/:id`, `/api/tokens/:id/status`, `/api/tokens/:id/note`, `/api/tokens/:id/secret`, `/api/tokens/:id/secret/rotate`, `/api/tokens/:id/metrics*`, `/api/tokens/:id/logs*`, `/api/tokens/:id/events`, `/api/tokens/leaderboard`. Source: `src/server/serve.rs:155`, `src/server/serve.rs:179`.
- Logs/jobs/users/tags: `/api/logs*`, `/api/jobs`, `/api/users*`, `/api/user-tags*`. Source: `src/server/serve.rs:135`, `src/server/serve.rs:138`, `src/server/serve.rs:142`.
