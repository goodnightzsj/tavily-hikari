# Runtime Surfaces

## HTTP serving composition
The Axum server is assembled in `src/server/serve.rs` and combines health/debug endpoints, public endpoints, user endpoints, admin endpoints, Tavily HTTP facade endpoints, static SPA serving, and the raw MCP proxy surface. Source: `src/server/serve.rs:69`, `src/server/serve.rs:225`.

## Public and browser-facing surfaces
- Public metrics and logs: `/api/public/events`, `/api/public/logs`, `/api/public/metrics`. Source: `src/server/serve.rs:75`, `src/server/serve.rs:124`.
- Public home/profile surface: `/`, `/api/profile`, `/login`, `/registration-paused`. Source: `src/server/serve.rs:80`, `src/server/serve.rs:193`, `src/server/serve.rs:196`.
- User console surface: `/console`, `/api/user/token`, `/api/user/dashboard`, `/api/user/tokens/*`. Source: `src/server/serve.rs:83`, `src/server/serve.rs:84`, `src/server/serve.rs:189`.
- Admin SPA surface: `/admin`, `/admin/*path`, backed by `admin.html` when static assets exist. Source: `src/server/serve.rs:186`, `src/server/serve.rs:192`.

## Machine-facing protocol surfaces
- MCP surface is mounted at `/mcp`; subpaths are explicitly rejected through `mcp_subpath_reject_handler`. Source: `src/server/serve.rs:225`.
- Tavily-compatible HTTP façade is mounted under `/api/tavily/*` and supports search, extract, crawl, map, research, and usage endpoints. Source: `src/server/serve.rs:97`, `src/server/serve.rs:106`.

## Route domains
- Admin/operator APIs: summaries, settings, keys, tokens, logs, jobs, users, tags, and forward proxy stats. Source: `src/server/serve.rs:107`, `src/server/serve.rs:123`, `src/server/serve.rs:125`, `src/server/serve.rs:170`.
- Auth routes: admin login/logout and Linux DO OAuth start/callback. Source: `src/server/serve.rs:81`, `src/server/serve.rs:95`.
- The runtime falls back to an HTML-oriented `__404` landing for browsers and plain 404 for non-HTML clients. Source: `src/server/serve.rs:229`, `src/server/serve.rs:250`.
