# Coding Conventions

## Runtime and tooling
- JS/TS tasks use Bun (`bun@1.3.10`) at the repo root and in `web/`. Source: `package.json:4`, `web/package.json:5`, `docs-site/package.json:3`.
- Rust is pinned via `rust-toolchain.toml`, and the repo expects standard `cargo fmt`, `cargo clippy -- -D warnings`, and `cargo test` flows. Source: `README.md:282`, `lefthook.yml:7`.

## Formatting and linting
- Markdown formatting is standardized with dprint, 100-column width, and maintained wrap style. Source: `dprint.json:1`, `dprint.json:11`.
- Pre-commit hooks run markdown formatting, `cargo fmt`, and `cargo clippy -- -D warnings`. Source: `lefthook.yml:1`, `lefthook.yml:3`, `lefthook.yml:7`, `lefthook.yml:11`.

## Source organization patterns
- Large Rust features are often centralized in `src/lib.rs` and split conceptually via exported modules (`analysis`, `forward_proxy`, `models`, `store`, `tavily_proxy`). Source: `src/lib.rs:1`.
- HTTP routes are assembled centrally in `src/server/serve.rs`, while request/response mapping and handlers live under `src/server/`. Source: `src/server/serve.rs:69`, `src/server/serve.rs:125`.
- Frontend entrypoints are separated by surface (`admin-main.tsx`, `console-main.tsx`, `public-main.tsx`, `login-main.tsx`). Source: `web/src/admin-main.tsx:1`, `web/src/console-main.tsx:1`, `web/src/public-main.tsx:1`, `web/src/login-main.tsx:1`.
- Admin navigation uses explicit path parsing helpers rather than framework-owned route trees. Source: `web/src/admin/routes.ts:15`, `web/src/admin/routes.ts:44`.
- User console navigation uses hash-based route helpers isolated in `web/src/lib/userConsoleRoutes.ts`. Source: `web/src/lib/userConsoleRoutes.ts:1`.

## Documentation split
- `docs-site/` is for published operator/integrator docs.
- `docs/specs/` is the main internal feature contract system.
- `docs/plan/` is legacy or planning-oriented and still contains durable details for older topics.
- `llmdoc/` should summarize stable knowledge, not mirror rollout status or one-off review notes. Source: `docs-site/docs/en/development.md:4`, `docs/specs/README.md:1`, `docs/plan/README.md:1`.
