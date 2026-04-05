# Git Conventions

## Branch and release posture
- `main` is the primary integration branch. Release automation runs after merges to `main` and CI success. Source: `README.md:296`, `README.md:301`.
- Each successful push to `main` now produces an automatic stable patch release; PR labels are no longer part of the release contract. Source: `.github/workflows/release.yml:29`, `.github/workflows/release.yml:56`, `.github/workflows/label-gate.yml:18`.

## Commit message style
- Conventional Commits are enforced via commitlint. Allowed types are `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`. Source: `commitlint.config.mjs:3`, `commitlint.config.mjs:6`.
- Commit type and scope must be lowercase; subject must be non-empty and not start with uppercase. Source: `commitlint.config.mjs:11`, `commitlint.config.mjs:13`, `commitlint.config.mjs:21`, `commitlint.config.mjs:29`.
- Commit subject and body are expected to be English-only. Source: `commitlint.config.mjs:24`, `commitlint.config.mjs:48`.
- Recent history shows frequent patterns like `fix(scope): ...`, `feat(scope): ...`, and `docs(spec): ...`. Source: `git log -12` snapshot from initialization.

## Local commit gates
- `lefthook` runs formatting/lint checks on pre-commit and `commitlint` on commit-msg. Source: `lefthook.yml:1`, `lefthook.yml:15`.
