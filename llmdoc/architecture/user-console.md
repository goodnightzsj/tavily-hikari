# User Console

## Responsibility
`UserConsole` is the signed-in Linux DO user surface. It combines token dashboard data, token detail navigation, token secret reveal/copy UX, recent token logs, guided client setup snippets, MCP probe actions, Tavily HTTP probe actions, and optional admin-entry affordances in one hash-routed SPA shell. Source: `web/src/UserConsole.tsx:14`, `web/src/UserConsole.tsx:68`, `web/src/UserConsole.tsx:241`.

## Key behaviors
- Uses hash routing for landing sections and token detail pages instead of path routing. Source: `web/src/UserConsole.tsx:68`, `web/src/lib/userConsoleRoutes.ts:3`.
- Loads profile, dashboard, token list, token detail, token secret, and token logs through the shared frontend API layer. Source: `web/src/UserConsole.tsx:15`, `web/src/UserConsole.tsx:28`, `web/src/api.ts:1`.
- Presents setup guidance for multiple MCP-capable clients and editors, including Claude Code, Codex, VS Code, Cursor, Windsurf, Cherry Studio, and generic flows. Source: `web/src/UserConsole.tsx:74`, `web/src/UserConsole.tsx:85`, `web/src/UserConsole.tsx:115`.
- Runs step-based MCP connectivity probes and Tavily HTTP probes using the current user token. Source: `web/src/UserConsole.tsx:23`, `web/src/UserConsole.tsx:163`, `web/src/UserConsole.tsx:172`.
- Masks token labels in guide content and keeps secret reveal/copy behavior short-lived with cache and prewarm timing. Source: `web/src/UserConsole.tsx:81`, `web/src/UserConsole.tsx:82`, `web/src/UserConsole.tsx:237`.
- Surfaces admin-entry links only when the resolved profile/admin state allows it. Source: `web/src/UserConsole.tsx:65`, `web/src/lib/userConsoleAdminEntry.ts:1`.

## Data and UX dependencies
- Shared request helpers and response normalization live in `web/src/api.ts`, including bearer-token request helpers and MCP probe helpers. Source: `web/src/api.ts:1`.
- Console responsiveness and availability heuristics are delegated to small helper modules rather than embedded in the route layer. Source: `web/src/UserConsole.tsx:64`, `web/src/UserConsole.tsx:66`.
- The console entrypoint is wrapped in shared language, theme, and tooltip providers, matching the other browser surfaces. Source: `web/src/console-main.tsx:1`.

## Related backend behavior
- Linux DO callback creates/reuses the user, ensures token binding, creates a user session, and prefers redirecting to `/console` when the console bundle exists. Source: `src/server/handlers/user.rs:303`, `src/server/handlers/user.rs:316`, `src/server/handlers/user.rs:329`, `src/server/handlers/user.rs:341`.
- Console data comes from `/api/profile`, `/api/user/dashboard`, `/api/user/tokens`, `/api/user/tokens/:id`, `/api/user/tokens/:id/secret`, and `/api/user/tokens/:id/logs`. Source: `src/server/serve.rs:81`, `src/server/serve.rs:85`, `src/server/serve.rs:87`, `src/server/serve.rs:88`, `src/server/serve.rs:89`, `src/server/serve.rs:90`.
- `/console` and `/console/` are mounted as SPA entrypoints only when static assets are present. Source: `src/server/serve.rs:182`, `src/server/serve.rs:190`.
