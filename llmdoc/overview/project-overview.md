# Project Overview

## What this project is
Tavily Hikari is a Rust + Axum service that sits in front of Tavily and exposes two client-facing access modes: MCP over `/mcp` and Tavily-style HTTP endpoints under `/api/tavily/*`. It rotates upstream Tavily API keys, isolates raw keys behind short IDs and Hikari-issued access tokens, persists audit and quota state in SQLite, and serves a React + Vite web console for both operators and end users. Source: `README.md:9`, `README.md:32`, `src/server/serve.rs:69`, `src/server/serve.rs:225`.

## Stable product surfaces
- Protocol surface: `/mcp` for MCP clients plus `/api/tavily/search|extract|crawl|map|research|usage` for direct HTTP clients. Source: `src/server/serve.rs:97`, `src/server/serve.rs:225`, `docs-site/docs/en/http-api-guide.md:14`.
- Admin/operator surface: `/admin` SPA plus admin APIs for keys, tokens, logs, jobs, users, tags, settings, and dashboard summaries. Source: `src/server/serve.rs:107`, `src/server/serve.rs:125`, `src/server/serve.rs:170`, `src/server/serve.rs:181`.
- End-user surface: public home, Linux DO OAuth login, `/console`, user token APIs, and self-serve token details/logs. Source: `src/server/serve.rs:80`, `src/server/serve.rs:83`, `src/server/serve.rs:84`, `src/server/serve.rs:186`.
- Documentation surface: public docs in `docs-site/`, internal feature contracts in `docs/specs/`, older planning artifacts in `docs/plan/`, and durable AI-facing summaries in `llmdoc/`. Source: `docs-site/docs/en/development.md:4`, `docs/specs/README.md:1`, `docs/plan/README.md:1`.

## Main runtime pieces
- Backend runtime is organized around `TavilyProxy` and the library exports in `src/lib.rs`, which own persistence, scheduling, request logging, affinity, quota, and forward proxy logic. Source: `src/lib.rs:1`, `src/lib.rs:8`, `src/lib.rs:21`.
- HTTP serving is assembled in `src/server/serve.rs`, which wires routes, auth modes, static asset serving, and background schedulers. Source: `src/server/serve.rs:1`, `src/server/serve.rs:69`, `src/server/serve.rs:274`.
- The frontend ships from `web/` as separate entrypoints for public, admin, login, registration-paused, and console surfaces. Source: `web/package.json:7`, `web/src/admin-main.tsx:1`, `src/server/serve.rs:181`.
- SQLite is the long-lived system of record for keys, tokens, sessions, logs, quota tables, rollup buckets, jobs, and feature-specific bindings. Source: `README.md:26`, `docs/quota-design.md:9`, `docs/plan/0001:request-logs-gc/contracts/db.md:16`.

## Important concept buckets
- Key pool, soft affinity, sticky bindings, and MCP session privacy. Source: `README.md:22`, `docs/specs/29w25-admin-key-sticky-users-nodes/SPEC.md:20`, `docs/specs/34pgu-mcp-session-privacy-affinity-hardening/SPEC.md:18`.
- Persistence, audit logs, and historical rollups. Source: `README.md:26`, `docs/plan/0001:request-logs-gc/PLAN.md:56`.
- Quota, billing, and request-kind accounting. Source: `docs/quota-design.md:9`, `src/lib.rs:8`.
- Forward proxy configuration, validation, and geo maintenance. Source: `src/lib.rs:21`, `src/server/serve.rs:109`, `src/server/schedulers.rs:240`.
- Web surface split between public home, user console, and modular admin dashboard. Source: `web/src/PublicHome.tsx:1`, `web/src/UserConsole.tsx:677`, `web/src/admin/routes.ts:5`.

## Tech stack
- Backend: Rust 2024, Axum, SQLx, Tokio, Reqwest, Clap. Source: `Cargo.toml:3`, `Cargo.toml:6`, `Cargo.toml:14`, `Cargo.toml:16`.
- Frontend: React 18, Vite 5, Tailwind CSS, Radix UI, Iconify, Storybook. Source: `web/package.json:15`, `web/package.json:38`.
- Tooling: Bun runtime for JS tasks, dprint for markdown formatting, lefthook + commitlint for commit gates. Source: `package.json:4`, `dprint.json:1`, `lefthook.yml:1`, `commitlint.config.mjs:2`.
