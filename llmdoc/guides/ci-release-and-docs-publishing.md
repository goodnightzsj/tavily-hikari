# CI, Release, and Docs Publishing

## CI pipeline
- `CI Pipeline` runs on pushes to `main`, pull requests against `main`, and manual dispatch. Source: `.github/workflows/ci.yml:1`.
- The pipeline separates lint/checks, backend tests, compose smoke validation, and release-build verification. Source: `.github/workflows/ci.yml:19`, `.github/workflows/ci.yml:60`, `.github/workflows/ci.yml:91`, `.github/workflows/ci.yml:220`.
- Lint/checks enforce `cargo fmt`, `cargo clippy -- -D warnings`, and `cargo check --locked --all-targets --all-features`. Source: `.github/workflows/ci.yml:51`, `.github/workflows/ci.yml:55`, `.github/workflows/ci.yml:58`.
- The compose smoke job builds frontend assets, builds the container image, boots the ForwardAuth+Caddy example, checks `/health`, and verifies both anonymous denial and authenticated admin profile behavior. Source: `.github/workflows/ci.yml:123`, `.github/workflows/ci.yml:141`, `.github/workflows/ci.yml:153`, `.github/workflows/ci.yml:170`, `.github/workflows/ci.yml:182`.

## Release workflow
- `Release` runs on pushed `v*` tags or by manual dispatch against an existing tag. Source: `.github/workflows/release.yml:1`, `.github/workflows/release.yml:4`, `.github/workflows/release.yml:7`.
- The prepare phase is tag-driven: it validates the pushed or manually supplied tag, derives `stable` vs `rc` channel from the tag name, records the checked-out commit SHA, and decides whether Docker publication should run. Source: `.github/workflows/release.yml:30`, `.github/workflows/release.yml:49`, `.github/workflows/release.yml:58`, `.github/workflows/release.yml:86`.
- Tag pushes automatically publish both the GitHub Release and GHCR images; manual dispatch can backfill an existing tag and optionally skip Docker with `publish_docker=false`. Source: `.github/workflows/release.yml:13`, `.github/workflows/release.yml:66`, `.github/workflows/release.yml:139`, `.github/workflows/release.yml:546`, `.github/workflows/release.yml:647`.
- When Docker publication is enabled, the workflow checks out the tagged ref, rebuilds web assets, builds native per-arch images, runs an MCP billing smoke gate against `mock_tavily`, pushes per-arch image digests, and then assembles a multi-arch GHCR manifest. Source: `.github/workflows/release.yml:156`, `.github/workflows/release.yml:174`, `.github/workflows/release.yml:204`, `.github/workflows/release.yml:216`, `.github/workflows/release.yml:219`, `.github/workflows/release.yml:494`, `.github/workflows/release.yml:588`, `.github/workflows/release.yml:632`.
- The release image name is derived from `${GITHUB_REPOSITORY,,}`, so moving the repository to a new owner automatically changes the GHCR target without editing the workflow. Source: `.github/workflows/release.yml:188`, `.github/workflows/release.yml:558`.
- No custom Docker registry secret is required for GHCR publishing in the current workflow; it logs in with `${{ secrets.GITHUB_TOKEN }}` and relies on workflow `packages: write`. Source: `.github/workflows/release.yml:142`, `.github/workflows/release.yml:197`, `.github/workflows/release.yml:549`, `.github/workflows/release.yml:569`.
- Runtime environment variables used by the smoke container are `TAVILY_API_KEYS`, `TAVILY_UPSTREAM`, `TAVILY_USAGE_BASE`, `DEV_OPEN_ADMIN`, and `PROXY_DB_PATH`; `APP_EFFECTIVE_VERSION` and `VITE_APP_VERSION` are build-time values injected by the workflow itself. Source: `.github/workflows/release.yml:109`, `.github/workflows/release.yml:177`, `.github/workflows/release.yml:265`, `.github/workflows/release.yml:269`.
- The PR label gate no longer blocks merges for missing release labels; it now only records that release labels are optional and that automated releases are tag-driven. Source: `.github/workflows/label-gate.yml:19`, `.github/workflows/label-gate.yml:22`, `.github/workflows/label-gate.yml:27`.

## Docs Pages workflow
- `Docs Pages` runs when docs-site, web, the workflow file, the pages assembly script, or the root READMEs change. Source: `.github/workflows/docs-pages.yml:3`, `.github/workflows/docs-pages.yml:6`, `.github/workflows/docs-pages.yml:10`, `.github/workflows/docs-pages.yml:11`.
- It builds the published docs site and Storybook separately, then assembles a combined Pages artifact with a Storybook entrypoint. Source: `.github/workflows/docs-pages.yml:31`, `.github/workflows/docs-pages.yml:73`, `.github/workflows/docs-pages.yml:102`, `.github/workflows/docs-pages.yml:121`.
- Deployment to GitHub Pages only happens for non-PR runs on `main`. Source: `.github/workflows/docs-pages.yml:146`.

## Relationship to local development docs
Local contributor commands live in `README.md` and are summarized for quick retrieval in `llmdoc/guides/development-and-operations.md`; the workflows above are the automation counterpart of those local checks. Source: `llmdoc/guides/development-and-operations.md:3`, `llmdoc/guides/development-and-operations.md:9`.
