# Git Conventions

## Branch and release posture
- `main` is the primary integration branch for CI, but release automation is tag-driven rather than merge-driven. Source: `README.md:292`, `.github/workflows/ci.yml:1`, `.github/workflows/release.yml:1`.
- Release versioning is driven by pushed `v*` tags; tag pushes create the GitHub Release and GHCR images automatically, while manual dispatch is only for backfilling an existing tag and can skip Docker. Source: `README.md:296`, `.github/workflows/release.yml:4`, `.github/workflows/release.yml:59`, `.github/workflows/release.yml:139`, `.github/workflows/release.yml:647`.

## Commit message style
- Conventional Commits are enforced via commitlint. Allowed types are `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`. Source: `commitlint.config.mjs:3`, `commitlint.config.mjs:6`.
- Commit type and scope must be lowercase; subject must be non-empty and not start with uppercase. Source: `commitlint.config.mjs:11`, `commitlint.config.mjs:13`, `commitlint.config.mjs:21`, `commitlint.config.mjs:29`.
- Commit subject and body are expected to be English-only. Source: `commitlint.config.mjs:24`, `commitlint.config.mjs:48`.
- Recent history shows frequent patterns like `fix(scope): ...`, `feat(scope): ...`, and `docs(spec): ...`. Source: `git log -12` snapshot from initialization.

## Local commit gates
- `lefthook` runs formatting/lint checks on pre-commit and `commitlint` on commit-msg. Source: `lefthook.yml:1`, `lefthook.yml:15`.
