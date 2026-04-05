# Tavily Hikari llmdoc

## Purpose
- `llmdoc/` is the durable LLM-oriented knowledge base for this repository.
- It summarizes stable architecture, workflows, and reference surfaces that are hard to recover quickly from specs or large source files.
- It is complementary to `docs-site/` and `docs/specs/`, not a replacement.

## Structure
- `overview/` — product shape, major surfaces, and documentation map.
- `architecture/` — durable backend/frontend/runtime concepts and system flows.
- `guides/` — common operator, contributor, and integrator workflows.
- `reference/` — concise conventions and reference maps.

## Primary entry points
- Product/docs map: `llmdoc/overview/project-overview.md`
- Coding conventions: `llmdoc/reference/coding-conventions.md`
- Git conventions: `llmdoc/reference/git-conventions.md`
- Public vs internal docs map: `llmdoc/reference/docs-sources-map.md`
- Runtime surfaces and routing: `llmdoc/architecture/runtime-surfaces.md`
- Identity and access: `llmdoc/architecture/auth-and-identity.md`
- Key pool and persistence: `llmdoc/architecture/key-pool-and-persistence.md`
- Quota and usage accounting: `llmdoc/architecture/quota-and-usage-accounting.md`
- Forward proxy subsystem: `llmdoc/architecture/forward-proxy.md`
- Web surfaces: `llmdoc/architecture/web-surfaces.md`
- Public home: `llmdoc/architecture/public-home.md`
- User console: `llmdoc/architecture/user-console.md`
- Web platform: `llmdoc/architecture/web-platform.md`
- MCP and Tavily HTTP: `llmdoc/architecture/mcp-and-tavily-http.md`
- Background jobs: `llmdoc/architecture/background-jobs.md`
- API map: `llmdoc/reference/api-surfaces.md`
- Admin API domains: `llmdoc/reference/admin-api-domains.md`
- User API domains: `llmdoc/reference/user-api-domains.md`
- Deploy/test workflows: `llmdoc/guides/development-and-operations.md`
- CI/release/docs publishing: `llmdoc/guides/ci-release-and-docs-publishing.md`
- User/admin/web workflows: `llmdoc/guides/web-surface-workflows.md`
