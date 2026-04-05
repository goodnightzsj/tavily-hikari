# Docs Sources Map

## Purpose
This file explains where to look first for different kinds of truth in this repository.

## Public documentation
- `docs-site/docs/en/**` and `docs-site/docs/zh/**` are the published docs for operators and integrators. Source: `docs-site/rspress.config.ts:37`, `docs-site/docs/en/index.md:1`.
- These pages cover product framing, quick start, configuration/access, HTTP API usage, deployment, FAQ, and development entrypoints. Source: `docs-site/docs/en/index.md:1`, `docs-site/docs/en/quick-start.md:1`, `docs-site/docs/en/configuration-access.md:8`, `docs-site/docs/en/http-api-guide.md:14`, `docs-site/docs/en/development.md:2`.

## Internal execution docs
- `docs/specs/` is the main feature-contract system for new work. Source: `docs/specs/README.md:1`.
- `docs/plan/` is older plan-first documentation; still consult it for legacy features that never migrated into specs. Source: `docs/plan/README.md:1`.
- Root `docs/*.md` files often contain durable deep dives such as quota or anonymity behavior. Source: `docs/quota-design.md:9`, `docs/high-anonymity-proxy.md:5`.

## llmdoc role
- `llmdoc/` should summarize stable architecture, workflows, and reference maps.
- Avoid mirroring rollout status, PR tracking, or one-off acceptance artifacts from `docs/specs/` or `docs/plan/`.
- Prefer linking back to the real source file and line when a detail is contract-sensitive.
- Public docs and Storybook publishing behavior live in `docs-site/**`, `.github/workflows/docs-pages.yml`, and the docs-site Pages assembly spec rather than in runtime source. Source: `docs-site/docs/en/development.md:44`, `.github/workflows/docs-pages.yml:1`, `docs/specs/zpg6j-docs-site-storybook-pages/SPEC.md:19`.
