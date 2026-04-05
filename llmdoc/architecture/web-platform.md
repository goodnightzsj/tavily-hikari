# Web Platform

## Responsibility
The web platform is a split React SPA layer served by the Rust backend. It exposes three browser-facing entrypoints — public home, admin dashboard, and user console — while sharing API access helpers, language state, theme state, tooltip behavior, and reusable UI components. Source: `src/server/serve.rs:182`, `web/src/public-main.tsx:1`, `web/src/admin-main.tsx:1`, `web/src/console-main.tsx:1`.

## Surface composition
- Public home mounts `PublicHome` through shared language, theme, and tooltip providers. Source: `web/src/public-main.tsx:1`.
- Admin mounts `AdminDashboard` through the same shared providers. Source: `web/src/admin-main.tsx:1`.
- User console mounts `UserConsole` through the same shared providers. Source: `web/src/console-main.tsx:1`.
- The backend serves `/`, `/admin*`, `/console*`, `/login*`, and registration-paused pages from the static bundle when present. Source: `src/server/serve.rs:187`, `src/server/serve.rs:188`, `src/server/serve.rs:190`, `src/server/serve.rs:193`, `src/server/serve.rs:194`, `src/server/serve.rs:197`.

## Shared frontend infrastructure
- `web/src/api.ts` is the shared contract layer for JSON fetching, bearer-token requests, normalized server response shapes, and probe helpers used across browser surfaces. Source: `web/src/api.ts:1`.
- `LanguageProvider` owns persisted language state and browser-language fallback. Source: `web/src/i18n.tsx:1`.
- Common UI primitives and interaction helpers are reused across surfaces rather than per-surface copies. Source: `web/src/UserConsole.tsx:39`, `web/src/UserConsole.tsx:44`.

## Routing model
- Admin uses path routing with explicit parsing of dashboard, tokens, keys, requests, jobs, users, alerts, proxy settings, token detail, key detail, and user detail/user-tag routes. Source: `web/src/admin/routes.ts:5`, `web/src/admin/routes.ts:44`.
- User console uses hash routing for landing and token-detail flows so backend path handling stays simple. Source: `web/src/UserConsole.tsx:68`, `web/src/lib/userConsoleRoutes.ts:3`.
- Unknown backend paths fall back to an HTML-aware redirect to `__404` for browser requests, while non-HTML callers receive a plain 404. Source: `src/server/serve.rs:230`, `src/server/serve.rs:251`.

## Operational coupling
- The Docker and CI flows build `web/dist` before producing container images because the backend serves the static output directly. Source: `.github/workflows/ci.yml:123`, `.github/workflows/release.yml:202`, `llmdoc/guides/development-and-operations.md:19`.
- Docs Pages separately builds `docs-site` and Storybook, then assembles a combined Pages artifact rather than publishing the app SPA bundle. Source: `.github/workflows/docs-pages.yml:31`, `.github/workflows/docs-pages.yml:73`, `.github/workflows/docs-pages.yml:102`.
