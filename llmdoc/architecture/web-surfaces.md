# Web Surfaces

## Surface split
The frontend is intentionally split into separate surfaces with different audiences and route models.

### Admin UI
- Admin UI is centered in `web/src/AdminDashboard.tsx` and organized by explicit path parsing helpers in `web/src/admin/routes.ts`. Source: `web/src/AdminDashboard.tsx:1245`, `web/src/admin/routes.ts:15`, `web/src/admin/routes.ts:44`.
- Stable modules include dashboard, tokens, keys, requests, jobs, users, alerts, and proxy settings. Source: `web/src/admin/routes.ts:5`.
- This modular path-router design replaced older hash subroutes and is backed by a dedicated spec. Source: `docs/specs/m4n7x-admin-path-routing-modular-dashboard/SPEC.md:19`, `docs/specs/m4n7x-admin-path-routing-modular-dashboard/SPEC.md:48`.

### User console
- User console remains a hash-routed SPA flow centered on landing vs token detail sections. Source: `web/src/lib/userConsoleRoutes.ts:1`, `web/src/lib/userConsoleRoutes.ts:7`.
- `web/src/UserConsole.tsx` handles route sync, detail focus, admin-entry affordance, and token-specific views. Source: `web/src/UserConsole.tsx:678`, `web/src/UserConsole.tsx:687`, `web/src/UserConsole.tsx:315`.

### Public home
- Public home combines onboarding, Linux DO login entry, admin/login shortcuts, metrics, and token-access interactions. Source: `web/src/PublicHome.tsx:1`, `web/src/PublicHome.tsx:499`, `web/src/PublicHome.tsx:517`.
- The backend can redirect logged-in end users directly from `/` into `/console` when the console bundle exists. Source: `src/server/spa.rs:28`, `src/server/spa.rs:38`.

## Shared frontend infrastructure
- API fetching/types live in `web/src/api.ts`. Source: `web/src/api.ts:1`.
- Internationalization is centralized in `web/src/i18n.tsx`. Source: `web/src/i18n.tsx:1`.
- Shared UI primitives and operator-focused widgets live under `web/src/components/`. Source: `web/src/components/AdminRecentRequestsPanel.tsx:1`, `web/src/components/ApiKeysValidationDialog.tsx:1`.
- Storybook is used as an acceptance and documentation surface for many components and major pages. Source: `web/package.json:9`, `web/src/admin/AdminPages.stories.tsx:1`, `web/src/PublicHome.stories.tsx:63`.
