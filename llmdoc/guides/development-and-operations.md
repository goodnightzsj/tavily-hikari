# Development and Operations

## Local development
- Backend: run `cargo run -- --bind 127.0.0.1 --port 58087` for a local server. Source: `README.md:47`.
- Frontend dev server: run `bun install --frozen-lockfile && bun run dev` in `web/`. Source: `README.md:51`, `web/package.json:7`.
- Public docs site: run `bun install --frozen-lockfile && bun run dev` in `docs-site/`. Source: `README.md:17`, `docs-site/package.json:5`.
- Storybook: run `bun run storybook` in `web/`. Source: `README.md:18`, `web/package.json:9`.
- Key-budget tuning is controlled by `KEY_RPM_LIMIT_PER_MINUTE` and `KEY_RPM_COOLDOWN_SECS`; if unset, the proxy defaults to 100 RPM per key and a 60-second cooldown window after upstream `429`. Source: `src/lib.rs:440`, `src/lib.rs:441`, `src/lib.rs:627`, `src/lib.rs:632`.

## Testing and validation
- Core backend checks: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test --locked --all-features`. Source: `README.md:287`.
- Frontend checks: `bun test`, production build, and Storybook build. Source: `web/package.json:8`, `web/package.json:11`, `web/package.json:13`.
- Bun-only enforcement exists via `bun run validate:no-node-runtime`. Source: `package.json:7`, `README.md:290`.

## CI and release
- CI runs lint, tests, and builds from `.github/workflows/ci.yml`. Source: `README.md:291`.
- Release automation runs from `.github/workflows/release.yml` on pushed `v*` tags or manual dispatch against an existing tag; tag pushes automatically create the GitHub Release and publish GHCR images. Source: `.github/workflows/release.yml:1`, `.github/workflows/release.yml:4`, `.github/workflows/release.yml:59`, `.github/workflows/release.yml:139`, `.github/workflows/release.yml:647`.
- CI still builds a local Docker image for the ForwardAuth compose smoke job, while production GHCR images are rebuilt only during tag-driven or manual-backfill release runs where Docker publication is enabled. Source: `.github/workflows/ci.yml:91`, `.github/workflows/ci.yml:132`, `.github/workflows/release.yml:139`, `.github/workflows/release.yml:546`.

## Container and deployment modes
- Single-container runtime serves backend + static `web/dist` assets and persists SQLite under `/srv/app/data`. Source: `README.md:63`, `README.md:72`.
- `docker-compose.yml` is the stock multi-service local deployment entrypoint. Source: `README.md:74`, `README.md:87`.
- A ForwardAuth gateway example exists under `examples/forwardauth-caddy/`. Source: `README.md:196`.
- Release smoke containers inject `TAVILY_API_KEYS`, `TAVILY_UPSTREAM`, `TAVILY_USAGE_BASE`, `DEV_OPEN_ADMIN`, and `PROXY_DB_PATH`; production deploys need the same runtime env family plus whichever auth/OAuth variables the chosen admin mode requires. Source: `.github/workflows/release.yml:265`, `.github/workflows/release.yml:269`.

## Hooks and commit hygiene
- Install hooks with `lefthook install` to enable local formatting/lint/commit checks. Source: `README.md:285`, `lefthook.yml:1`.
