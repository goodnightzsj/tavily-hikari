# Public Home

## Responsibility
`PublicHome` is the unauthenticated landing surface for the product. It combines product framing, public status signals, token access affordances, Linux DO login entry, and admin/login shortcuts in one page shell. Source: `web/src/PublicHome.tsx:1`, `web/src/PublicHome.tsx:499`.

## Key behaviors
- Shows update availability, public metrics, and key availability. Source: `web/src/PublicHome.tsx:478`, `web/src/PublicHome.tsx:501`, `web/src/PublicHome.tsx:503`.
- Exposes Linux DO login when enabled. Source: `web/src/PublicHome.tsx:506`, `web/src/PublicHome.tsx:517`.
- Exposes token access dialog/entry when token panels are hidden. Source: `web/src/PublicHome.tsx:508`, `web/src/PublicHome.tsx:518`.
- Exposes admin action that points to `/admin` when already admin or `/login` when built-in auth is enabled but unsigned. Source: `web/src/PublicHome.tsx:509`, `web/src/PublicHome.tsx:519`.

## Related backend behavior
- `/` may auto-redirect to `/console` for signed-in Linux DO users when the console bundle exists. Source: `src/server/spa.rs:38`.
- Admin auto-redirect from `/` only happens in explicit dev-open-admin mode. Source: `src/server/spa.rs:32`.
