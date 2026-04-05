# CI, Release, and Docs Publishing

## CI pipeline
- `CI Pipeline` runs on pushes to `main`, pull requests against `main`, and manual dispatch. Source: `.github/workflows/ci.yml:1`.
- The pipeline separates lint/checks, backend tests, compose smoke validation, and release-build verification. Source: `.github/workflows/ci.yml:19`, `.github/workflows/ci.yml:60`, `.github/workflows/ci.yml:91`, `.github/workflows/ci.yml:220`.
- Lint/checks enforce `cargo fmt`, `cargo clippy -- -D warnings`, and `cargo check --locked --all-targets --all-features`. Source: `.github/workflows/ci.yml:51`, `.github/workflows/ci.yml:55`, `.github/workflows/ci.yml:58`.
- The compose smoke job builds frontend assets, builds the container image, boots the ForwardAuth+Caddy example, checks `/health`, and verifies both anonymous denial and authenticated admin profile behavior. Source: `.github/workflows/ci.yml:123`, `.github/workflows/ci.yml:141`, `.github/workflows/ci.yml:153`, `.github/workflows/ci.yml:170`, `.github/workflows/ci.yml:182`.

## Release workflow
- `Release` runs after successful `CI Pipeline` pushes on `main` or by manual backfill dispatch. Source: `.github/workflows/release.yml:1`, `.github/workflows/release.yml:4`, `.github/workflows/release.yml:8`.
- The prepare phase no longer queries PR metadata; it always treats each eligible `main` commit as a stable patch release, computes the next tag idempotently, and exposes whether this run should also publish Docker images. Source: `.github/workflows/release.yml:33`, `.github/workflows/release.yml:59`, `.github/workflows/release.yml:64`, `.github/workflows/release.yml:71`.
- Automatic `main` releases create Git tags and GitHub Releases only; release image production is gated behind manual workflow dispatch with `publish_docker=true`. Source: `.github/workflows/release.yml:8`, `.github/workflows/release.yml:64`, `.github/workflows/release.yml:194`, `.github/workflows/release.yml:701`.
- When manual Docker publication is enabled, the workflow rebuilds web assets, builds native per-arch images, runs an MCP billing smoke gate against `mock_tavily`, pushes per-arch image digests, and then assembles a multi-arch GHCR manifest. Source: `.github/workflows/release.yml:189`, `.github/workflows/release.yml:229`, `.github/workflows/release.yml:259`, `.github/workflows/release.yml:271`, `.github/workflows/release.yml:274`, `.github/workflows/release.yml:596`, `.github/workflows/release.yml:624`, `.github/workflows/release.yml:643`.
- The release image name is derived from `${GITHUB_REPOSITORY,,}`, so moving the repository to a new owner automatically changes the GHCR target without editing the workflow. Source: `.github/workflows/release.yml:238`, `.github/workflows/release.yml:608`.
- No custom Docker registry secret is required for GHCR publishing in the current workflow; it logs in with `${{ secrets.GITHUB_TOKEN }}` and relies on workflow `packages: write`. Source: `.github/workflows/release.yml:195`, `.github/workflows/release.yml:247`, `.github/workflows/release.yml:624`.
- Runtime environment variables used by the smoke container are `TAVILY_API_KEYS`, `TAVILY_UPSTREAM`, `TAVILY_USAGE_BASE`, `DEV_OPEN_ADMIN`, and `PROXY_DB_PATH`; `APP_EFFECTIVE_VERSION` and `VITE_APP_VERSION` are build-time values injected by the workflow itself. Source: `.github/workflows/release.yml:209`, `.github/workflows/release.yml:229`, `.github/workflows/release.yml:320`, `.github/workflows/release.yml:321`.
- The PR label gate no longer blocks merges for missing release labels; it now only records that automatic main-branch releases are enabled. Source: `.github/workflows/label-gate.yml:18`, `.github/workflows/label-gate.yml:22`.

## Docs Pages workflow
- `Docs Pages` runs when docs-site, web, the workflow file, the pages assembly script, or the root READMEs change. Source: `.github/workflows/docs-pages.yml:3`, `.github/workflows/docs-pages.yml:6`, `.github/workflows/docs-pages.yml:10`, `.github/workflows/docs-pages.yml:11`.
- It builds the published docs site and Storybook separately, then assembles a combined Pages artifact with a Storybook entrypoint. Source: `.github/workflows/docs-pages.yml:31`, `.github/workflows/docs-pages.yml:73`, `.github/workflows/docs-pages.yml:102`, `.github/workflows/docs-pages.yml:121`.
- Deployment to GitHub Pages only happens for non-PR runs on `main`. Source: `.github/workflows/docs-pages.yml:146`.

## Relationship to local development docs
Local contributor commands live in `README.md` and are summarized for quick retrieval in `llmdoc/guides/development-and-operations.md`; the workflows above are the automation counterpart of those local checks. Source: `llmdoc/guides/development-and-operations.md:3`, `llmdoc/guides/development-and-operations.md:9`.
