# Web Surface Workflows

## Public visitor flow
1. Visitor lands on `/` and sees public metrics, key availability, token-access entry, and optional admin or Linux DO login actions. Source: `web/src/PublicHome.tsx:499`.
2. If Linux DO OAuth is enabled, the page can start the user login flow. Source: `web/src/PublicHome.tsx:517`.
3. If built-in admin auth is enabled but the browser is not signed in, the public page can surface an admin login action to `/login`. Source: `web/src/PublicHome.tsx:509`, `web/src/PublicHome.tsx:519`.

## End-user console flow
1. Successful Linux DO callback creates or reuses the user binding, then redirects to `/console` when the console bundle exists. Source: `src/server/handlers/user.rs:316`, `src/server/handlers/user.rs:341`.
2. The console uses hash routing to switch between landing sections and token detail pages. Source: `web/src/lib/userConsoleRoutes.ts:3`, `web/src/lib/userConsoleRoutes.ts:28`.
3. The console supports an admin-entry affordance when the profile indicates admin access. Source: `web/src/UserConsole.tsx:315`.

## Admin operator flow
1. `/admin` resolves to the modular admin SPA when the request is authorized; otherwise it redirects to `/login` if built-in auth is enabled or returns forbidden. Source: `src/server/spa.rs:50`, `src/server/spa.rs:57`.
2. Admin navigation is path-based and supports dashboard, tokens, keys, requests, jobs, users, alerts, and proxy settings. Source: `web/src/admin/routes.ts:5`, `web/src/admin/routes.ts:44`.
3. Key and token detail routes branch into deeper operator workflows like sticky panels, recent requests, usage leaderboards, and secret/maintenance actions. Source: `src/server/serve.rs:148`, `src/server/serve.rs:155`, `docs/specs/29w25-admin-key-sticky-users-nodes/SPEC.md:24`.

## Docs workflow orientation
- Published docs live in `docs-site/docs/en|zh` and explain usage/deployment/integration. Source: `docs-site/docs/en/index.md:1`.
- Internal feature truth lives mainly in `docs/specs/**/SPEC.md`, with `docs/plan/**` retained for older planning artifacts. Source: `docs/specs/README.md:1`, `docs/plan/README.md:1`.
- `llmdoc/` should capture the durable cross-cutting understanding needed to navigate the codebase without rereading large specs or huge source files. Source: `llmdoc/index.md:3`.
