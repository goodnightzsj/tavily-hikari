# User API Domains

## Access model
User-facing APIs depend on Linux DO OAuth being enabled and configured. When the feature is disabled they return `404 Not Found`; when the browser lacks a valid user session they return `401 Unauthorized`. Source: `src/server/handlers/user.rs:357`, `src/server/handlers/user.rs:371`, `src/server/handlers/user.rs:437`, `src/server/handlers/user.rs:495`.

## Auth and session routes
- `GET|POST /auth/linuxdo` starts the Linux DO auth flow. Source: `src/server/serve.rs:82`.
- `GET /auth/linuxdo/callback` consumes login state, upserts the user, ensures token binding, creates a user session cookie, and redirects to `/console` when available. Source: `src/server/serve.rs:83`, `src/server/handlers/user.rs:303`, `src/server/handlers/user.rs:316`, `src/server/handlers/user.rs:329`, `src/server/handlers/user.rs:341`.
- `POST /api/user/logout` revokes the current user session cookie. Source: `src/server/serve.rs:84`, `src/server/handlers/user.rs:357`.

## Data routes
- `GET /api/user/token` returns the bound token secret for the signed-in user. Source: `src/server/serve.rs:85`, `src/server/handlers/user.rs:371`.
- `GET /api/user/dashboard` returns per-user quota and recent-activity aggregates. Source: `src/server/serve.rs:86`, `src/server/handlers/user.rs:395`, `src/server/handlers/user.rs:437`.
- `GET /api/user/tokens` lists the user’s accessible tokens with quota, request-rate, and success/failure summaries. Source: `src/server/serve.rs:87`, `src/server/handlers/user.rs:412`, `src/server/handlers/user.rs:495`.
- `GET /api/user/tokens/:id` and `GET /api/user/tokens/:id/secret` expose token detail and secret retrieval. Source: `src/server/serve.rs:88`, `src/server/serve.rs:89`.
- `GET /api/user/tokens/:id/logs` exposes recent public token logs for the chosen token. Source: `src/server/serve.rs:90`, `src/server/handlers/user.rs:432`.

## Frontend consumers
- The user console reads profile, dashboard, token list/detail/secret, and logs through the shared API layer in `web/src/api.ts`. Source: `web/src/UserConsole.tsx:15`, `web/src/UserConsole.tsx:28`.
- The console can collapse to a landing-only guide flow when the user has exactly one token, keeping the API shape stable while simplifying the UI branch. Source: `web/src/UserConsole.tsx:241`, `web/src/UserConsole.tsx:245`.
